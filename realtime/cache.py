"""
realtime/cache.py
=================
Thread-safe and multi-worker safe CacheStore for GTFS-Realtime data.
Supports both Redis and In-Memory caching with secondary indexing by:
  - route_id
  - trip_id
  - stop_id
  - vehicle_id
"""

import asyncio
import json
import logging
import os
import time
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

from .models import (
    Alert,
    FeedMetadata,
    HealthState,
    RouteRealtime,
    StopRealtime,
    TripRealtime,
    TripUpdate,
    VehiclePosition,
)

log = logging.getLogger("gtfs_realtime.cache")


class CacheStore(ABC):
    """Abstract interface for storing and querying GTFS-Realtime snapshots."""

    @abstractmethod
    async def update_snapshot(
        self,
        feed_name: str,
        entities: List[Dict[str, Any]],
        metadata: FeedMetadata,
        is_differential: bool = False,
    ) -> None:
        """Atomically update a feed snapshot and its secondary indices."""
        pass

    @abstractmethod
    async def get_metadata(self, feed_name: str) -> FeedMetadata:
        """Retrieve current metadata for a feed."""
        pass

    @abstractmethod
    async def get_all_entities(
        self,
        feed_name: str,
        limit: int = 100,
        offset: int = 0,
    ) -> Tuple[List[Dict[str, Any]], int, FeedMetadata]:
        """Return paginated raw entities for a feed."""
        pass

    @abstractmethod
    async def get_by_route(self, route_id: str) -> RouteRealtime:
        """Fetch all realtime updates, vehicles, and alerts for a specific route."""
        pass

    @abstractmethod
    async def get_by_trip(self, trip_id: str) -> TripRealtime:
        """Fetch trip update and vehicle position for a specific trip."""
        pass

    @abstractmethod
    async def get_by_stop(self, stop_id: str) -> StopRealtime:
        """Fetch upcoming stop updates and alerts for a specific stop."""
        pass

    @abstractmethod
    async def acquire_leader_lock(self, lock_key: str, worker_id: str, ttl: int = 45) -> bool:
        """Attempt to acquire a distributed leader election lock."""
        pass

    @abstractmethod
    async def renew_leader_lock(self, lock_key: str, worker_id: str, ttl: int = 45) -> bool:
        """Renew the distributed leader lock."""
        pass

    @abstractmethod
    async def release_leader_lock(self, lock_key: str, worker_id: str) -> None:
        """Release the distributed leader lock."""
        pass

    @abstractmethod
    async def close(self) -> None:
        """Clean up resources."""
        pass


class MemoryCacheStore(CacheStore):
    """High-performance thread-safe in-memory cache with secondary indexing."""

    def __init__(self):
        self._lock = asyncio.Lock()
        # feed_name -> {entity_id: entity_dict}
        self._feeds: Dict[str, Dict[str, Dict[str, Any]]] = {
            "trip_updates": {},
            "train_updates": {},
            "vehicle_positions": {},
            "alerts": {},
        }
        # feed_name -> FeedMetadata
        self._metadata: Dict[str, FeedMetadata] = {
            k: FeedMetadata(feed_type=k) for k in self._feeds
        }

        # Secondary indices
        # route_id -> set of (feed_name, entity_id)
        self._index_by_route: Dict[str, Set[Tuple[str, str]]] = {}
        # trip_id -> set of (feed_name, entity_id)
        self._index_by_trip: Dict[str, Set[Tuple[str, str]]] = {}
        # stop_id -> set of (feed_name, entity_id)
        self._index_by_stop: Dict[str, Set[Tuple[str, str]]] = {}
        # vehicle_id -> (feed_name, entity_id)
        self._index_by_vehicle: Dict[str, Tuple[str, str]] = {}

        # Local process lock
        self._leader_id: Optional[str] = None
        self._lock_expiry: float = 0.0

    def _rebuild_indices_locked(self) -> None:
        """Rebuild secondary indices across all feeds."""
        by_route: Dict[str, Set[Tuple[str, str]]] = {}
        by_trip: Dict[str, Set[Tuple[str, str]]] = {}
        by_stop: Dict[str, Set[Tuple[str, str]]] = {}
        by_vehicle: Dict[str, Tuple[str, str]] = {}

        for feed_name, entities in self._feeds.items():
            for entity_id, e in entities.items():
                ref = (feed_name, entity_id)

                # Route
                route_id = e.get("route_id")
                if route_id:
                    by_route.setdefault(str(route_id), set()).add(ref)

                # Trip
                trip_id = e.get("trip_id")
                if trip_id:
                    by_trip.setdefault(str(trip_id), set()).add(ref)

                # Vehicle
                vehicle_id = e.get("vehicle_id") or (e.get("vehicle") or {}).get("id")
                if vehicle_id:
                    by_vehicle[str(vehicle_id)] = ref

                # Stops from Trip Updates
                for stu in e.get("stop_time_updates", []):
                    sid = stu.get("stop_id")
                    if sid:
                        by_stop.setdefault(str(sid), set()).add(ref)

                # Stop and Route from Alerts
                for ie in e.get("informed_entities", []):
                    rid = ie.get("route_id")
                    if rid:
                        by_route.setdefault(str(rid), set()).add(ref)
                    tid = ie.get("trip_id")
                    if tid:
                        by_trip.setdefault(str(tid), set()).add(ref)
                    sid = ie.get("stop_id")
                    if sid:
                        by_stop.setdefault(str(sid), set()).add(ref)

        self._index_by_route = by_route
        self._index_by_trip = by_trip
        self._index_by_stop = by_stop
        self._index_by_vehicle = by_vehicle

    def _get_metadata_locked(self, feed_name: str) -> FeedMetadata:
        meta = self._metadata.get(feed_name)
        if not meta:
            return FeedMetadata(feed_type=feed_name, health_state=HealthState.UNAVAILABLE)
        if meta.last_success:
            try:
                last_dt = datetime.fromisoformat(meta.last_success)
                if last_dt.tzinfo is None:
                    last_dt = last_dt.replace(tzinfo=timezone.utc)
                age = max(0.0, (datetime.now(timezone.utc) - last_dt).total_seconds())
                meta.cache_age_seconds = round(age, 1)
            except Exception:
                pass
        if hasattr(meta, "model_copy"):
            return meta.model_copy(deep=True)
        return meta.copy(deep=True)

    async def update_snapshot(
        self,
        feed_name: str,
        entities: List[Dict[str, Any]],
        metadata: FeedMetadata,
        is_differential: bool = False,
    ) -> None:
        async with self._lock:
            if not is_differential:
                new_map = {e["entity_id"]: e for e in entities if "entity_id" in e}
                self._feeds[feed_name] = new_map
            else:
                current_map = self._feeds.setdefault(feed_name, {}).copy()
                for e in entities:
                    eid = e.get("entity_id")
                    if not eid:
                        continue
                    if e.get("is_deleted"):
                        current_map.pop(eid, None)
                    else:
                        current_map[eid] = e
                self._feeds[feed_name] = current_map

            metadata.entity_count = len(self._feeds[feed_name])
            self._metadata[feed_name] = metadata
            self._rebuild_indices_locked()

    async def get_metadata(self, feed_name: str) -> FeedMetadata:
        async with self._lock:
            return self._get_metadata_locked(feed_name)

    async def get_all_entities(
        self,
        feed_name: str,
        limit: int = 100,
        offset: int = 0,
    ) -> Tuple[List[Dict[str, Any]], int, FeedMetadata]:
        async with self._lock:
            meta = self._get_metadata_locked(feed_name)
            entities_dict = self._feeds.get(feed_name, {})
            all_items = list(entities_dict.values())
            total = len(all_items)
            paginated = all_items[offset : offset + limit]
            return paginated, total, meta

    async def get_by_route(self, route_id: str) -> RouteRealtime:
        rid = str(route_id)
        res = RouteRealtime(route_id=rid)
        async with self._lock:
            refs = self._index_by_route.get(rid, set())
            for feed_name, eid in refs:
                item = self._feeds.get(feed_name, {}).get(eid)
                if not item:
                    continue
                if feed_name in ("trip_updates", "train_updates"):
                    res.trip_updates.append(TripUpdate(**item))
                elif feed_name == "vehicle_positions":
                    res.vehicles.append(VehiclePosition(**item))
                elif feed_name == "alerts":
                    res.alerts.append(Alert(**item))
        return res

    async def get_by_trip(self, trip_id: str) -> TripRealtime:
        tid = str(trip_id)
        res = TripRealtime(trip_id=tid)
        async with self._lock:
            refs = self._index_by_trip.get(tid, set())
            for feed_name, eid in refs:
                item = self._feeds.get(feed_name, {}).get(eid)
                if not item:
                    continue
                if feed_name in ("trip_updates", "train_updates") and not res.trip_update:
                    res.trip_update = TripUpdate(**item)
                elif feed_name == "vehicle_positions" and not res.vehicle:
                    res.vehicle = VehiclePosition(**item)
        return res

    async def get_by_stop(self, stop_id: str) -> StopRealtime:
        sid = str(stop_id)
        res = StopRealtime(stop_id=sid)
        async with self._lock:
            refs = self._index_by_stop.get(sid, set())
            for feed_name, eid in refs:
                item = self._feeds.get(feed_name, {}).get(eid)
                if not item:
                    continue
                if feed_name in ("trip_updates", "train_updates"):
                    for stu in item.get("stop_time_updates", []):
                        if str(stu.get("stop_id")) == sid:
                            res.upcoming_updates.append({
                                "trip_id": item.get("trip_id"),
                                "route_id": item.get("route_id"),
                                "feed_source": feed_name,
                                "stop_sequence": stu.get("stop_sequence"),
                                "arrival": stu.get("arrival"),
                                "departure": stu.get("departure"),
                                "schedule_relationship": stu.get("schedule_relationship"),
                            })
                elif feed_name == "alerts":
                    res.alerts.append(Alert(**item))
        return res

    async def acquire_leader_lock(self, lock_key: str, worker_id: str, ttl: int = 45) -> bool:
        now = time.time()
        async with self._lock:
            if self._leader_id is None or self._leader_id == worker_id or now > self._lock_expiry:
                self._leader_id = worker_id
                self._lock_expiry = now + ttl
                return True
            return False

    async def renew_leader_lock(self, lock_key: str, worker_id: str, ttl: int = 45) -> bool:
        now = time.time()
        async with self._lock:
            if self._leader_id == worker_id:
                self._lock_expiry = now + ttl
                return True
            return False

    async def release_leader_lock(self, lock_key: str, worker_id: str) -> None:
        async with self._lock:
            if self._leader_id == worker_id:
                self._leader_id = None
                self._lock_expiry = 0.0

    async def close(self) -> None:
        pass


class RedisCacheStore(CacheStore):
    """Redis-backed distributed cache store with secondary indices and atomic swapping."""

    def __init__(self, redis_client):
        self._r = redis_client
        self._prefix = "gtfs_rt:"

    async def update_snapshot(
        self,
        feed_name: str,
        entities: List[Dict[str, Any]],
        metadata: FeedMetadata,
        is_differential: bool = False,
    ) -> None:
        staging_data_key = f"{self._prefix}{feed_name}:staging"
        active_data_key = f"{self._prefix}{feed_name}:data"
        meta_key = f"{self._prefix}{feed_name}:meta"

        async with self._r.pipeline(transaction=True) as pipe:
            if not is_differential:
                pipe.delete(staging_data_key)
                if entities:
                    mapping = {e["entity_id"]: json.dumps(e) for e in entities if "entity_id" in e}
                    if mapping:
                        pipe.hset(staging_data_key, mapping=mapping)
                pipe.rename(staging_data_key, active_data_key)
            else:
                for e in entities:
                    eid = e.get("entity_id")
                    if not eid:
                        continue
                    if e.get("is_deleted"):
                        pipe.hdel(active_data_key, eid)
                    else:
                        pipe.hset(active_data_key, eid, json.dumps(e))

            metadata.entity_count = len(entities)
            pipe.set(meta_key, metadata.json())

            # Update indices
            for e in entities:
                eid = e.get("entity_id")
                if not eid:
                    continue
                ref = f"{feed_name}:{eid}"
                rid = e.get("route_id")
                if rid:
                    pipe.sadd(f"{self._prefix}idx:route:{rid}", ref)
                tid = e.get("trip_id")
                if tid:
                    pipe.sadd(f"{self._prefix}idx:trip:{tid}", ref)
                vid = e.get("vehicle_id") or (e.get("vehicle") or {}).get("id")
                if vid:
                    pipe.set(f"{self._prefix}idx:vehicle:{vid}", ref)

                for stu in e.get("stop_time_updates", []):
                    sid = stu.get("stop_id")
                    if sid:
                        pipe.sadd(f"{self._prefix}idx:stop:{sid}", ref)

                for ie in e.get("informed_entities", []):
                    if ie.get("route_id"):
                        pipe.sadd(f"{self._prefix}idx:route:{ie['route_id']}", ref)
                    if ie.get("trip_id"):
                        pipe.sadd(f"{self._prefix}idx:trip:{ie['trip_id']}", ref)
                    if ie.get("stop_id"):
                        pipe.sadd(f"{self._prefix}idx:stop:{ie['stop_id']}", ref)

            await pipe.execute()

    async def get_metadata(self, feed_name: str) -> FeedMetadata:
        raw = await self._r.get(f"{self._prefix}{feed_name}:meta")
        if not raw:
            return FeedMetadata(feed_type=feed_name, health_state=HealthState.UNAVAILABLE)
        try:
            meta = FeedMetadata.parse_raw(raw)
            if meta.last_success:
                last_dt = datetime.fromisoformat(meta.last_success)
                if last_dt.tzinfo is None:
                    last_dt = last_dt.replace(tzinfo=timezone.utc)
                meta.cache_age_seconds = round(
                    max(0.0, (datetime.now(timezone.utc) - last_dt).total_seconds()), 1
                )
            return meta
        except Exception:
            return FeedMetadata(feed_type=feed_name, health_state=HealthState.UNAVAILABLE)

    async def get_all_entities(
        self,
        feed_name: str,
        limit: int = 100,
        offset: int = 0,
    ) -> Tuple[List[Dict[str, Any]], int, FeedMetadata]:
        meta = await self.get_metadata(feed_name)
        active_key = f"{self._prefix}{feed_name}:data"
        raw_vals = await self._r.hvals(active_key)
        total = len(raw_vals)
        slice_vals = raw_vals[offset : offset + limit]
        parsed = [json.loads(v) for v in slice_vals]
        return parsed, total, meta

    async def _resolve_refs(self, refs: Set[str]) -> List[Tuple[str, Dict[str, Any]]]:
        res = []
        for ref in refs:
            feed_name, _, eid = ref.partition(":")
            raw = await self._r.hget(f"{self._prefix}{feed_name}:data", eid)
            if raw:
                res.append((feed_name, json.loads(raw)))
        return res

    async def get_by_route(self, route_id: str) -> RouteRealtime:
        rid = str(route_id)
        res = RouteRealtime(route_id=rid)
        refs = await self._r.smembers(f"{self._prefix}idx:route:{rid}")
        for feed_name, item in await self._resolve_refs(refs):
            if feed_name in ("trip_updates", "train_updates"):
                res.trip_updates.append(TripUpdate(**item))
            elif feed_name == "vehicle_positions":
                res.vehicles.append(VehiclePosition(**item))
            elif feed_name == "alerts":
                res.alerts.append(Alert(**item))
        return res

    async def get_by_trip(self, trip_id: str) -> TripRealtime:
        tid = str(trip_id)
        res = TripRealtime(trip_id=tid)
        refs = await self._r.smembers(f"{self._prefix}idx:trip:{tid}")
        for feed_name, item in await self._resolve_refs(refs):
            if feed_name in ("trip_updates", "train_updates") and not res.trip_update:
                res.trip_update = TripUpdate(**item)
            elif feed_name == "vehicle_positions" and not res.vehicle:
                res.vehicle = VehiclePosition(**item)
        return res

    async def get_by_stop(self, stop_id: str) -> StopRealtime:
        sid = str(stop_id)
        res = StopRealtime(stop_id=sid)
        refs = await self._r.smembers(f"{self._prefix}idx:stop:{sid}")
        for feed_name, item in await self._resolve_refs(refs):
            if feed_name in ("trip_updates", "train_updates"):
                for stu in item.get("stop_time_updates", []):
                    if str(stu.get("stop_id")) == sid:
                        res.upcoming_updates.append({
                            "trip_id": item.get("trip_id"),
                            "route_id": item.get("route_id"),
                            "feed_source": feed_name,
                            "stop_sequence": stu.get("stop_sequence"),
                            "arrival": stu.get("arrival"),
                            "departure": stu.get("departure"),
                            "schedule_relationship": stu.get("schedule_relationship"),
                        })
            elif feed_name == "alerts":
                res.alerts.append(Alert(**item))
        return res

    async def acquire_leader_lock(self, lock_key: str, worker_id: str, ttl: int = 45) -> bool:
        res = await self._r.set(f"{self._prefix}lock:{lock_key}", worker_id, nx=True, ex=ttl)
        return bool(res)

    async def renew_leader_lock(self, lock_key: str, worker_id: str, ttl: int = 45) -> bool:
        key = f"{self._prefix}lock:{lock_key}"
        curr = await self._r.get(key)
        if curr and curr == worker_id:
            await self._r.expire(key, ttl)
            return True
        return False

    async def release_leader_lock(self, lock_key: str, worker_id: str) -> None:
        key = f"{self._prefix}lock:{lock_key}"
        curr = await self._r.get(key)
        if curr and curr == worker_id:
            await self._r.delete(key)

    async def close(self) -> None:
        await self._r.aclose()


_global_cache_store: Optional[CacheStore] = None


async def get_cache_store() -> CacheStore:
    global _global_cache_store
    if _global_cache_store is not None:
        return _global_cache_store

    redis_url = os.environ.get("REALTIME_REDIS_URL", "").strip()
    engine = os.environ.get("REALTIME_ENGINE", "auto").strip().lower()

    if (engine in ("redis", "auto")) and redis_url:
        try:
            import redis.asyncio as aioredis
            client = aioredis.from_url(redis_url, decode_responses=True)
            await client.ping()
            _global_cache_store = RedisCacheStore(client)
            log.info("Initialized RedisCacheStore at %s", redis_url)
            return _global_cache_store
        except Exception as exc:
            log.warning("Failed to connect to Redis (%s). Falling back to MemoryCacheStore.", exc)

    _global_cache_store = MemoryCacheStore()
    log.info("Initialized MemoryCacheStore (In-Memory)")
    return _global_cache_store


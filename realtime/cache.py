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
from pathlib import Path
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
    async def get_feed_names(self) -> List[str]:
        """Return all tracked feed names."""
        pass

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
        self._feeds: Dict[str, Dict[str, Dict[str, Any]]] = {}
        # feed_name -> FeedMetadata
        self._metadata: Dict[str, FeedMetadata] = {}

        # Secondary indices
        # route_id -> set of (feed_name, entity_id)
        self._index_by_route: Dict[str, Set[Tuple[str, str]]] = {}
        # trip_id -> set of (feed_name, entity_id)
        self._index_by_trip: Dict[str, Set[Tuple[str, str]]] = {}
        # stop_id -> set of (feed_name, entity_id)
        self._index_by_stop: Dict[str, Set[Tuple[str, str]]] = {}
        # vehicle_id -> (feed_name, entity_id)
        self._index_by_vehicle: Dict[str, Tuple[str, str]] = {}
        # Per-feed secondary index cache: feed_name -> (by_route, by_trip, by_stop, by_vehicle)
        self._feed_indices: Dict[str, Tuple[Dict[str, Set[Tuple[str, str]]], Dict[str, Set[Tuple[str, str]]], Dict[str, Set[Tuple[str, str]]], Dict[str, Tuple[str, str]]]] = {}

        # Local process lock
        self._leader_id: Optional[str] = None
        self._lock_expiry: float = 0.0

    @staticmethod
    def _build_single_feed_indices(
        feed_name: str,
        entities: Dict[str, Dict[str, Any]],
    ) -> Tuple[Dict[str, Set[str]], Dict[str, Set[str]], Dict[str, Set[str]], Dict[str, str]]:
        """Build index only for this feed in worker thread with cooperative GIL yielding."""
        by_route: Dict[str, Set[str]] = {}
        by_trip: Dict[str, Set[str]] = {}
        by_stop: Dict[str, Set[str]] = {}
        by_vehicle: Dict[str, str] = {}

        count = 0
        for entity_id, e in entities.items():
            count += 1
            if count % 150 == 0:
                time.sleep(0.002)

            # Route
            route_id = e.get("route_id")
            if route_id:
                by_route.setdefault(str(route_id), set()).add(entity_id)

            # Trip
            trip_id = e.get("trip_id")
            if trip_id:
                by_trip.setdefault(str(trip_id), set()).add(entity_id)

            # Vehicle
            vehicle_id = e.get("vehicle_id") or (e.get("vehicle") or {}).get("id")
            if vehicle_id:
                by_vehicle[str(vehicle_id)] = entity_id

            # Stops from Trip Updates
            for stu in e.get("stop_time_updates", []):
                sid = stu.get("stop_id")
                if sid:
                    by_stop.setdefault(str(sid), set()).add(entity_id)

            # Stop and Route from Alerts
            for ie in e.get("informed_entities", []):
                rid = ie.get("route_id")
                if rid:
                    by_route.setdefault(str(rid), set()).add(entity_id)
                tid = ie.get("trip_id")
                if tid:
                    by_trip.setdefault(str(tid), set()).add(entity_id)
                sid = ie.get("stop_id")
                if sid:
                    by_stop.setdefault(str(sid), set()).add(entity_id)

        return by_route, by_trip, by_stop, by_vehicle

    def _rebuild_indices_locked(self) -> None:
        """Rebuild secondary indices per feed (synchronous fallback)."""
        feeds_copy = dict(self._feeds)
        all_indices = {}
        for fname, f_entities in feeds_copy.items():
            all_indices[fname] = self._build_single_feed_indices(fname, f_entities)
        self._feed_indices = all_indices

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

    @staticmethod
    def _process_snapshot_worker(
        feed_name: str,
        entities: List[Dict[str, Any]],
        is_differential: bool,
        current_feed: Dict[str, Dict[str, Any]],
    ):
        """Build new feed mapping and updated secondary indices in worker thread without multi-feed merging."""
        if not is_differential:
            new_map = {e["entity_id"]: e for e in entities if "entity_id" in e}
        else:
            new_map = dict(current_feed)
            for e in entities:
                eid = e.get("entity_id")
                if not eid:
                    continue
                if e.get("is_deleted"):
                    new_map.pop(eid, None)
                else:
                    new_map[eid] = e

        feed_idx = MemoryCacheStore._build_single_feed_indices(feed_name, new_map)
        return new_map, feed_idx

    async def update_snapshot(
        self,
        feed_name: str,
        entities: List[Dict[str, Any]],
        metadata: FeedMetadata,
        is_differential: bool = False,
    ) -> None:
        async with self._lock:
            current_feed = self._feeds.get(feed_name, {})

        # Offload dict construction and index building to worker thread
        new_map, feed_idx = await asyncio.to_thread(
            self._process_snapshot_worker,
            feed_name,
            entities,
            is_differential,
            current_feed,
        )

        async with self._lock:
            self._feeds[feed_name] = new_map
            self._feed_indices[feed_name] = feed_idx
            metadata.entity_count = len(new_map)
            self._metadata[feed_name] = metadata

    async def get_feed_names(self) -> List[str]:
        async with self._lock:
            names = set(self._metadata.keys()) | set(self._feeds.keys())
            return sorted(names)

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
            for feed_name, indices in self._feed_indices.items():
                by_r = indices[0] if indices else {}
                eids = by_r.get(rid)
                if not eids:
                    continue
                feed_items = self._feeds.get(feed_name, {})
                for eid in eids:
                    item = feed_items.get(eid)
                    if not item:
                        continue
                    if feed_name in ("trip_updates", "train_updates") or "trip" in feed_name or "train" in feed_name or "update" in feed_name:
                        res.trip_updates.append(TripUpdate(**item))
                    elif feed_name == "vehicle_positions" or "vehicle" in feed_name:
                        res.vehicles.append(VehiclePosition(**item))
                    elif feed_name == "alerts" or "alert" in feed_name:
                        res.alerts.append(Alert(**item))
        return res

    async def get_by_trip(self, trip_id: str) -> TripRealtime:
        tid = str(trip_id)
        res = TripRealtime(trip_id=tid)
        async with self._lock:
            for feed_name, indices in self._feed_indices.items():
                by_t = indices[1] if indices else {}
                eids = by_t.get(tid)
                if not eids:
                    continue
                feed_items = self._feeds.get(feed_name, {})
                for eid in eids:
                    item = feed_items.get(eid)
                    if not item:
                        continue
                    if (feed_name in ("trip_updates", "train_updates") or "trip" in feed_name or "train" in feed_name or "update" in feed_name) and not res.trip_update:
                        res.trip_update = TripUpdate(**item)
                    elif (feed_name == "vehicle_positions" or "vehicle" in feed_name) and not res.vehicle:
                        res.vehicle = VehiclePosition(**item)
        return res

    async def get_by_stop(self, stop_id: str) -> StopRealtime:
        sid = str(stop_id)
        res = StopRealtime(stop_id=sid)
        async with self._lock:
            for feed_name, indices in self._feed_indices.items():
                by_s = indices[2] if indices else {}
                eids = by_s.get(sid)
                if not eids:
                    continue
                feed_items = self._feeds.get(feed_name, {})
                for eid in eids:
                    item = feed_items.get(eid)
                    if not item:
                        continue
                    if feed_name in ("trip_updates", "train_updates") or "trip" in feed_name or "train" in feed_name or "update" in feed_name:
                        for stu in item.get("stop_time_updates", []):
                            if str(stu.get("stop_id")) == sid:
                                res.upcoming_updates.append({
                                    "trip_id": item.get("trip_id"),
                                    "route_id": item.get("route_id"),
                                    "feed_source": item.get("feed_source") or feed_name,
                                    "stop_sequence": stu.get("stop_sequence"),
                                    "arrival": stu.get("arrival"),
                                    "departure": stu.get("departure"),
                                    "schedule_relationship": stu.get("schedule_relationship"),
                                })
                    elif feed_name == "alerts" or "alert" in feed_name:
                        res.alerts.append(Alert(**item))
        return res

    def _get_leader_file_path(self, lock_key: str) -> Path:
        return Path(__file__).resolve().parent.parent / f".{lock_key}.lock"

    def _read_file_leader(self, lock_key: str) -> Tuple[Optional[str], float]:
        fpath = self._get_leader_file_path(lock_key)
        if not fpath.is_file():
            return None, 0.0
        try:
            raw = fpath.read_text(encoding="utf-8").strip()
            parts = raw.split(":")
            if len(parts) >= 2:
                return parts[0], float(parts[1])
        except Exception:
            pass
        return None, 0.0

    def _write_file_leader(self, lock_key: str, worker_id: Optional[str], expiry: float) -> None:
        fpath = self._get_leader_file_path(lock_key)
        try:
            if worker_id is None:
                if fpath.exists():
                    fpath.unlink(missing_ok=True)
            else:
                tmp = fpath.with_suffix(".tmp")
                tmp.write_text(f"{worker_id}:{expiry}", encoding="utf-8")
                tmp.replace(fpath)
        except Exception:
            pass

    async def acquire_leader_lock(self, lock_key: str, worker_id: str, ttl: int = 45) -> bool:
        now = time.time()
        async with self._lock:
            # Check cross-process leader file first
            f_leader, f_expiry = self._read_file_leader(lock_key)
            if f_leader is not None and f_leader != worker_id and now < f_expiry:
                return False

            if self._leader_id is None or self._leader_id == worker_id or now > self._lock_expiry:
                self._leader_id = worker_id
                self._lock_expiry = now + ttl
                self._write_file_leader(lock_key, worker_id, self._lock_expiry)
                return True
            return False

    async def renew_leader_lock(self, lock_key: str, worker_id: str, ttl: int = 45) -> bool:
        now = time.time()
        async with self._lock:
            f_leader, f_expiry = self._read_file_leader(lock_key)
            if f_leader is not None and f_leader != worker_id and now < f_expiry:
                return False

            if self._leader_id == worker_id or (f_leader == worker_id):
                self._leader_id = worker_id
                self._lock_expiry = now + ttl
                self._write_file_leader(lock_key, worker_id, self._lock_expiry)
                return True
            return False

    async def release_leader_lock(self, lock_key: str, worker_id: str) -> None:
        async with self._lock:
            if self._leader_id == worker_id:
                self._leader_id = None
                self._lock_expiry = 0.0
                self._write_file_leader(lock_key, None, 0.0)

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

                pipe.sadd(f"{self._prefix}known_feeds", feed_name)

            await pipe.execute()

    async def get_feed_names(self) -> List[str]:
        raw_names = await self._r.smembers(f"{self._prefix}known_feeds")
        if raw_names:
            return sorted([n.decode() if isinstance(n, bytes) else str(n) for n in raw_names])
        return ["trip_updates", "train_updates", "vehicle_positions", "alerts"]

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
            if feed_name in ("trip_updates", "train_updates") or "trip" in feed_name or "train" in feed_name or "update" in feed_name:
                res.trip_updates.append(TripUpdate(**item))
            elif feed_name == "vehicle_positions" or "vehicle" in feed_name:
                res.vehicles.append(VehiclePosition(**item))
            elif feed_name == "alerts" or "alert" in feed_name:
                res.alerts.append(Alert(**item))
        return res

    async def get_by_trip(self, trip_id: str) -> TripRealtime:
        tid = str(trip_id)
        res = TripRealtime(trip_id=tid)
        refs = await self._r.smembers(f"{self._prefix}idx:trip:{tid}")
        for feed_name, item in await self._resolve_refs(refs):
            if (feed_name in ("trip_updates", "train_updates") or "trip" in feed_name or "train" in feed_name or "update" in feed_name) and not res.trip_update:
                res.trip_update = TripUpdate(**item)
            elif (feed_name == "vehicle_positions" or "vehicle" in feed_name) and not res.vehicle:
                res.vehicle = VehiclePosition(**item)
        return res

    async def get_by_stop(self, stop_id: str) -> StopRealtime:
        sid = str(stop_id)
        res = StopRealtime(stop_id=sid)
        refs = await self._r.smembers(f"{self._prefix}idx:stop:{sid}")
        for feed_name, item in await self._resolve_refs(refs):
            if feed_name in ("trip_updates", "train_updates") or "trip" in feed_name or "train" in feed_name or "update" in feed_name:
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
            elif feed_name == "alerts" or "alert" in feed_name:
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


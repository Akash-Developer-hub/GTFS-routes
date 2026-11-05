"""
realtime/poller.py
==================
Multi-worker safe background poller for Netherlands GTFS-Realtime feeds.
Fetches, validates, parses, normalizes and updates CacheStore with:
  - Incrementality handling (FULL_DATASET vs DIFFERENTIAL)
  - Fault tolerance & stale preservation
  - Feed health state monitoring
  - Redis distributed leader lock
"""

import asyncio
import logging
import os
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import httpx
from google.transit import gtfs_realtime_pb2

from .cache import CacheStore, get_cache_store
from .models import FeedMetadata, HealthState

log = logging.getLogger("gtfs_realtime.poller")


class FeedConfig:
    def __init__(
        self,
        name: str,
        env_url_key: str,
        default_url: str,
        env_interval_key: str,
        default_interval: int,
        env_stale_key: str,
        default_stale: int,
    ):
        self.name = name
        self.url = os.environ.get(env_url_key, default_url).strip() or default_url
        self.interval = int(os.environ.get(env_interval_key, str(default_interval)).strip() or default_interval)
        self.stale_threshold = int(os.environ.get(env_stale_key, str(default_stale)).strip() or default_stale)
        self.etag: Optional[str] = None
        self.last_modified: Optional[str] = None


FEED_CONFIGS = [
    FeedConfig(
        name="trip_updates",
        env_url_key="REALTIME_TRIP_UPDATES_URL",
        default_url="https://gtfs.ovapi.nl/nl/tripUpdates.pb",
        env_interval_key="REALTIME_POLL_INTERVAL_TRIP_UPDATES",
        default_interval=30,
        env_stale_key="REALTIME_STALE_THRESHOLD_UPDATES",
        default_stale=90,
    ),
    FeedConfig(
        name="train_updates",
        env_url_key="REALTIME_TRAIN_UPDATES_URL",
        default_url="https://gtfs.ovapi.nl/nl/trainUpdates.pb",
        env_interval_key="REALTIME_POLL_INTERVAL_TRAIN_UPDATES",
        default_interval=30,
        env_stale_key="REALTIME_STALE_THRESHOLD_UPDATES",
        default_stale=90,
    ),
    FeedConfig(
        name="vehicle_positions",
        env_url_key="REALTIME_VEHICLE_POSITIONS_URL",
        default_url="https://gtfs.ovapi.nl/nl/vehiclePositions.pb",
        env_interval_key="REALTIME_POLL_INTERVAL_VEHICLE_POSITIONS",
        default_interval=30,
        env_stale_key="REALTIME_STALE_THRESHOLD_UPDATES",
        default_stale=90,
    ),
    FeedConfig(
        name="alerts",
        env_url_key="REALTIME_ALERTS_URL",
        default_url="https://gtfs.ovapi.nl/nl/alerts.pb",
        env_interval_key="REALTIME_POLL_INTERVAL_ALERTS",
        default_interval=60,
        env_stale_key="REALTIME_STALE_THRESHOLD_ALERTS",
        default_stale=180,
    ),
]


def _parse_trip_update(entity, feed_source: str) -> Dict[str, Any]:
    tu = entity.trip_update
    trip = tu.trip
    trip_desc = {
        "trip_id": trip.trip_id if trip.HasField("trip_id") else "",
        "route_id": trip.route_id if trip.HasField("route_id") else None,
        "direction_id": trip.direction_id if trip.HasField("direction_id") else None,
        "start_time": trip.start_time if trip.HasField("start_time") else None,
        "start_date": trip.start_date if trip.HasField("start_date") else None,
        "schedule_relationship": (
            gtfs_realtime_pb2.TripDescriptor.ScheduleRelationship.Name(trip.schedule_relationship)
            if trip.HasField("schedule_relationship")
            else "SCHEDULED"
        ),
    }

    vehicle_desc = None
    if tu.HasField("vehicle"):
        v = tu.vehicle
        vehicle_desc = {
            "id": v.id if v.HasField("id") else None,
            "label": v.label if v.HasField("label") else None,
            "license_plate": v.license_plate if v.HasField("license_plate") else None,
        }

    stop_time_updates = []
    for stu in tu.stop_time_update:
        arr = None
        if stu.HasField("arrival"):
            arr = {
                "delay": stu.arrival.delay if stu.arrival.HasField("delay") else None,
                "time": stu.arrival.time if stu.arrival.HasField("time") else None,
                "uncertainty": stu.arrival.uncertainty if stu.arrival.HasField("uncertainty") else None,
            }
        dep = None
        if stu.HasField("departure"):
            dep = {
                "delay": stu.departure.delay if stu.departure.HasField("delay") else None,
                "time": stu.departure.time if stu.departure.HasField("time") else None,
                "uncertainty": stu.departure.uncertainty if stu.departure.HasField("uncertainty") else None,
            }

        stop_time_updates.append({
            "stop_sequence": stu.stop_sequence if stu.HasField("stop_sequence") else None,
            "stop_id": stu.stop_id if stu.HasField("stop_id") else None,
            "arrival": arr,
            "departure": dep,
            "schedule_relationship": (
                gtfs_realtime_pb2.TripUpdate.StopTimeUpdate.ScheduleRelationship.Name(stu.schedule_relationship)
                if stu.HasField("schedule_relationship")
                else "SCHEDULED"
            ),
        })

    return {
        "entity_id": entity.id,
        "is_deleted": getattr(entity, "is_deleted", False),
        "trip_id": trip_desc["trip_id"],
        "route_id": trip_desc["route_id"],
        "direction_id": trip_desc["direction_id"],
        "start_time": trip_desc["start_time"],
        "start_date": trip_desc["start_date"],
        "schedule_relationship": trip_desc["schedule_relationship"],
        "vehicle": vehicle_desc,
        "timestamp": tu.timestamp if tu.HasField("timestamp") else None,
        "delay": tu.delay if tu.HasField("delay") else None,
        "stop_time_updates": stop_time_updates,
        "feed_source": feed_source,
    }


def _parse_vehicle_position(entity, feed_source: str) -> Dict[str, Any]:
    vp = entity.vehicle if entity.HasField("vehicle") else getattr(entity, "vehicle_position", None)
    v_desc = vp.vehicle if (vp and vp.HasField("vehicle")) else None
    t_desc = vp.trip if (vp and vp.HasField("trip")) else None
    pos = vp.position if (vp and vp.HasField("position")) else None

    return {
        "entity_id": entity.id,
        "is_deleted": getattr(entity, "is_deleted", False),
        "vehicle_id": v_desc.id if (v_desc and v_desc.HasField("id")) else entity.id,
        "label": v_desc.label if (v_desc and v_desc.HasField("label")) else None,
        "license_plate": v_desc.license_plate if (v_desc and v_desc.HasField("license_plate")) else None,
        "trip_id": t_desc.trip_id if (t_desc and t_desc.HasField("trip_id")) else None,
        "route_id": t_desc.route_id if (t_desc and t_desc.HasField("route_id")) else None,
        "direction_id": t_desc.direction_id if (t_desc and t_desc.HasField("direction_id")) else None,
        "start_time": t_desc.start_time if (t_desc and t_desc.HasField("start_time")) else None,
        "start_date": t_desc.start_date if (t_desc and t_desc.HasField("start_date")) else None,
        "position": {
            "latitude": pos.latitude if pos else 0.0,
            "longitude": pos.longitude if pos else 0.0,
            "bearing": pos.bearing if (pos and pos.HasField("bearing")) else None,
            "odometer": pos.odometer if (pos and pos.HasField("odometer")) else None,
            "speed": pos.speed if (pos and pos.HasField("speed")) else None,
        },
        "current_stop_sequence": vp.current_stop_sequence if vp.HasField("current_stop_sequence") else None,
        "stop_id": vp.stop_id if vp.HasField("stop_id") else None,
        "current_status": (
            gtfs_realtime_pb2.VehiclePosition.VehicleStopStatus.Name(vp.current_status)
            if vp.HasField("current_status")
            else None
        ),
        "timestamp": vp.timestamp if vp.HasField("timestamp") else None,
        "congestion_level": (
            gtfs_realtime_pb2.VehiclePosition.CongestionLevel.Name(vp.congestion_level)
            if vp.HasField("congestion_level")
            else None
        ),
        "occupancy_status": (
            gtfs_realtime_pb2.VehiclePosition.OccupancyStatus.Name(vp.occupancy_status)
            if vp.HasField("occupancy_status")
            else None
        ),
        "feed_source": feed_source,
    }


def _parse_alert(entity, feed_source: str) -> Dict[str, Any]:
    al = entity.alert
    active_periods = []
    for ap in al.active_period:
        active_periods.append({
            "start": ap.start if ap.HasField("start") else None,
            "end": ap.end if ap.HasField("end") else None,
        })

    informed_entities = []
    for ie in al.informed_entity:
        informed_entities.append({
            "agency_id": ie.agency_id if ie.HasField("agency_id") else None,
            "route_id": ie.route_id if ie.HasField("route_id") else None,
            "route_type": ie.route_type if ie.HasField("route_type") else None,
            "trip_id": ie.trip.trip_id if (ie.HasField("trip") and ie.trip.HasField("trip_id")) else None,
            "stop_id": ie.stop_id if ie.HasField("stop_id") else None,
        })

    header = al.header_text.translation[0].text if al.header_text.translation else None
    desc = al.description_text.translation[0].text if al.description_text.translation else None

    return {
        "entity_id": entity.id,
        "is_deleted": getattr(entity, "is_deleted", False),
        "alert_id": entity.id,
        "active_periods": active_periods,
        "informed_entities": informed_entities,
        "cause": (
            gtfs_realtime_pb2.Alert.Cause.Name(al.cause)
            if al.HasField("cause")
            else "UNKNOWN_CAUSE"
        ),
        "effect": (
            gtfs_realtime_pb2.Alert.Effect.Name(al.effect)
            if al.HasField("effect")
            else "UNKNOWN_EFFECT"
        ),
        "url": al.url.translation[0].text if (al.HasField("url") and al.url.translation) else None,
        "header_text": header,
        "description_text": desc,
        "severity_level": (
            gtfs_realtime_pb2.Alert.SeverityLevel.Name(al.severity_level)
            if al.HasField("severity_level")
            else "UNKNOWN_SEVERITY"
        ),
        "feed_source": feed_source,
    }


def parse_feed_message(content: bytes, feed_name: str) -> Tuple[List[Dict[str, Any]], str]:
    """Parse GTFS-RT FeedMessage protobuf into normalized dictionaries."""
    msg = gtfs_realtime_pb2.FeedMessage()
    msg.ParseFromString(content)

    inc = "FULL_DATASET"
    if msg.header.HasField("incrementality"):
        inc = gtfs_realtime_pb2.FeedHeader.Incrementality.Name(msg.header.incrementality)

    entities = []
    for entity in msg.entity:
        if getattr(entity, "is_deleted", False):
            entities.append({"entity_id": entity.id, "is_deleted": True})
            continue
        if feed_name in ("trip_updates", "train_updates") and entity.HasField("trip_update"):
            entities.append(_parse_trip_update(entity, feed_name))
        elif feed_name == "vehicle_positions" and (entity.HasField("vehicle") or entity.HasField("vehicle_position")):
            entities.append(_parse_vehicle_position(entity, feed_name))
        elif feed_name == "alerts" and entity.HasField("alert"):
            entities.append(_parse_alert(entity, feed_name))

    return entities, inc


class RealtimePoller:
    """Manages background fetching, validation, and multi-worker leader election."""

    def __init__(self, cache_store: CacheStore):
        self.cache = cache_store
        self.worker_id = f"worker_{uuid.uuid4().hex[:8]}"
        self.timeout = float(os.environ.get("REALTIME_HTTP_TIMEOUT_SECONDS", "15"))
        self.max_retries = int(os.environ.get("REALTIME_HTTP_MAX_RETRIES", "2"))
        self.lock_ttl = int(os.environ.get("REALTIME_LOCK_TTL_SECONDS", "45"))
        self._running = False
        self._tasks: List[asyncio.Task] = []
        self._client: Optional[httpx.AsyncClient] = None

    async def start(self) -> None:
        self._running = True
        self._client = httpx.AsyncClient(
            timeout=self.timeout,
            follow_redirects=True,
            headers={"User-Agent": "GTFSRealtimeClient/1.0 (+https://github.com)"},
        )
        log.info("Starting RealtimePoller [%s]...", self.worker_id)
        # Spawn one worker loop per feed plus leader renewal loop
        self._tasks.append(asyncio.create_task(self._leader_supervision_loop()))
        for cfg in FEED_CONFIGS:
            self._tasks.append(asyncio.create_task(self._feed_polling_loop(cfg)))

    async def stop(self) -> None:
        self._running = False
        log.info("Stopping RealtimePoller [%s]...", self.worker_id)
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
        if self._client:
            await self._client.aclose()
            self._client = None
        await self.cache.release_leader_lock("gtfs_rt_leader", self.worker_id)

    async def _leader_supervision_loop(self) -> None:
        """Periodically try to acquire or renew the leader lock."""
        while self._running:
            try:
                acquired = await self.cache.acquire_leader_lock("gtfs_rt_leader", self.worker_id, ttl=self.lock_ttl)
                if not acquired:
                    await self.cache.renew_leader_lock("gtfs_rt_leader", self.worker_id, ttl=self.lock_ttl)
            except Exception as exc:
                log.warning("Leader lock exception: %s", exc)
            await asyncio.sleep(15)

    async def _is_leader(self) -> bool:
        return await self.cache.renew_leader_lock("gtfs_rt_leader", self.worker_id, ttl=self.lock_ttl)

    async def _feed_polling_loop(self, cfg: FeedConfig) -> None:
        # Initial stagger
        await asyncio.sleep(1.0)
        while self._running:
            start_time = time.time()
            if await self._is_leader():
                await self._fetch_feed(cfg)
            else:
                log.debug("Worker %s is standing by (not leader)", self.worker_id)

            elapsed = time.time() - start_time
            sleep_time = max(1.0, cfg.interval - elapsed)
            try:
                await asyncio.sleep(sleep_time)
            except asyncio.CancelledError:
                break

    async def _fetch_feed(self, cfg: FeedConfig) -> None:
        now_iso = datetime.now(timezone.utc).isoformat()
        meta = await self.cache.get_metadata(cfg.name)
        meta.last_attempt = now_iso

        headers = {}
        if cfg.etag:
            headers["If-None-Match"] = cfg.etag
        if cfg.last_modified:
            headers["If-Modified-Since"] = cfg.last_modified

        attempt = 0
        success = False
        last_exc: Optional[Exception] = None
        fetch_start = time.time()

        while attempt <= self.max_retries and not success:
            attempt += 1
            try:
                assert self._client is not None
                resp = await self._client.get(cfg.url, headers=headers)
                meta.http_status = resp.status_code

                if resp.status_code == 304:
                    # Not modified: retain cached snapshot and reset errors
                    log.debug("Feed %s not modified (304)", cfg.name)
                    meta.error_count = 0
                    meta.last_error = None
                    meta.last_success = now_iso
                    meta.health_state = HealthState.HEALTHY
                    meta.cache_age_seconds = 0.0
                    meta.fetch_duration_ms = round((time.time() - fetch_start) * 1000, 1)
                    await self.cache.update_snapshot(cfg.name, [], meta, is_differential=True)
                    return

                resp.raise_for_status()

                # Validate payload
                content = resp.content
                if not content or len(content) < 4:
                    raise ValueError(f"Received empty or truncated payload ({len(content)} bytes)")

                # Parse & Normalize
                entities, inc = parse_feed_message(content, cfg.name)

                # Store headers
                if "ETag" in resp.headers:
                    cfg.etag = resp.headers["ETag"]
                if "Last-Modified" in resp.headers:
                    cfg.last_modified = resp.headers["Last-Modified"]

                # Update metadata
                meta.incrementality = inc
                meta.last_success = now_iso
                meta.error_count = 0
                meta.last_error = None
                meta.health_state = HealthState.HEALTHY
                meta.cache_age_seconds = 0.0
                meta.fetch_duration_ms = round((time.time() - fetch_start) * 1000, 1)

                is_diff = (inc == "DIFFERENTIAL")
                await self.cache.update_snapshot(cfg.name, entities, meta, is_differential=is_diff)
                log.info(
                    "Fetched feed %-17s: %5d entities [%s] in %4.0f ms",
                    cfg.name,
                    len(entities),
                    inc,
                    meta.fetch_duration_ms,
                )
                success = True

            except Exception as exc:
                last_exc = exc
                await asyncio.sleep(0.5 * attempt)

        if not success:
            meta.error_count += 1
            meta.last_error = str(last_exc)
            # Evaluate health state with stale preservation
            if meta.last_success:
                try:
                    last_dt = datetime.fromisoformat(meta.last_success)
                    age = (datetime.now(timezone.utc) - last_dt).total_seconds()
                    meta.cache_age_seconds = round(age, 1)
                    if age > cfg.stale_threshold:
                        meta.health_state = HealthState.STALE
                    else:
                        meta.health_state = HealthState.DEGRADED
                except Exception:
                    meta.health_state = HealthState.DEGRADED
            else:
                meta.health_state = HealthState.UNAVAILABLE

            log.warning(
                "Fetch failed for %s (attempt %d/%d, state=%s): %s",
                cfg.name,
                meta.error_count,
                self.max_retries,
                meta.health_state.value,
                last_exc,
            )
            # Update metadata only, leaving the existing entity snapshot intact!
            await self.cache.update_snapshot(cfg.name, [], meta, is_differential=True)


_global_poller: Optional[RealtimePoller] = None


async def get_realtime_poller() -> RealtimePoller:
    global _global_poller
    if _global_poller is None:
        cache = await get_cache_store()
        _global_poller = RealtimePoller(cache)
    return _global_poller


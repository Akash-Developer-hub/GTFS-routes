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
import json
import logging
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import httpx
from google.transit import gtfs_realtime_pb2

from .cache import CacheStore, get_cache_store
from .models import FeedMetadata, HealthState

log = logging.getLogger("gtfs_realtime.poller")

BASE_DIR = Path(__file__).resolve().parent.parent


class FeedConfig:
    def __init__(
        self,
        name: str,
        env_url_key: Optional[str] = None,
        default_url: str = "",
        env_interval_key: Optional[str] = None,
        default_interval: int = 30,
        env_stale_key: Optional[str] = None,
        default_stale: int = 90,
        headers: Optional[Dict[str, str]] = None,
        feed_type: Optional[str] = None,
    ):
        self.name = name
        self.url = (os.environ.get(env_url_key, default_url).strip() or default_url) if env_url_key else default_url
        self.interval = (int(os.environ.get(env_interval_key, str(default_interval)).strip() or default_interval)) if env_interval_key else default_interval
        self.stale_threshold = (int(os.environ.get(env_stale_key, str(default_stale)).strip() or default_stale)) if env_stale_key else default_stale
        self.headers: Dict[str, str] = headers or {}
        self.feed_type = feed_type or self._infer_feed_type(name, self.url)
        self.etag: Optional[str] = None
        self.last_modified: Optional[str] = None

    @staticmethod
    def _infer_feed_type(name: str, url: str) -> str:
        s = f"{name} {url}".lower()
        if "alert" in s:
            return "alerts"
        if "vehicle" in s or "position" in s:
            return "vehicle_positions"
        return "trip_updates"


ENTUR_SUPPORTED_CODESPACES: Dict[str, Dict[str, bool]] = {
    "AKT": {"trip_updates": True, "vehicle_positions": True, "alerts": True},
    "ATB": {"trip_updates": True, "vehicle_positions": True, "alerts": True},
    "AVI": {"trip_updates": False, "vehicle_positions": False, "alerts": False},
    "BNR": {"trip_updates": False, "vehicle_positions": False, "alerts": False},
    "BRA": {"trip_updates": True, "vehicle_positions": True, "alerts": True},
    "FIN": {"trip_updates": True, "vehicle_positions": True, "alerts": True},
    "FLT": {"trip_updates": True, "vehicle_positions": False, "alerts": False},
    "GJB": {"trip_updates": False, "vehicle_positions": True, "alerts": False},
    "GOA": {"trip_updates": False, "vehicle_positions": True, "alerts": True},
    "INN": {"trip_updates": True, "vehicle_positions": True, "alerts": False},
    "KOL": {"trip_updates": True, "vehicle_positions": True, "alerts": False},
    "MOR": {"trip_updates": True, "vehicle_positions": True, "alerts": True},
    "NBU": {"trip_updates": True, "vehicle_positions": True, "alerts": True},
    "NOR": {"trip_updates": True, "vehicle_positions": False, "alerts": False},
    "NSB": {"trip_updates": False, "vehicle_positions": True, "alerts": False},
    "OST": {"trip_updates": True, "vehicle_positions": True, "alerts": True},
    "RUT": {"trip_updates": True, "vehicle_positions": False, "alerts": True},
    "SJN": {"trip_updates": False, "vehicle_positions": False, "alerts": False},
    "SKY": {"trip_updates": False, "vehicle_positions": False, "alerts": False},
    "SOF": {"trip_updates": False, "vehicle_positions": False, "alerts": False},
    "TEL": {"trip_updates": True, "vehicle_positions": True, "alerts": False},
    "TRO": {"trip_updates": True, "vehicle_positions": True, "alerts": False},
    "VKT": {"trip_updates": False, "vehicle_positions": False, "alerts": False},
    "VOT": {"trip_updates": True, "vehicle_positions": True, "alerts": False},
    "VYB": {"trip_updates": True, "vehicle_positions": True, "alerts": True},
    "VYG": {"trip_updates": True, "vehicle_positions": True, "alerts": False},
    "VYX": {"trip_updates": True, "vehicle_positions": True, "alerts": True},
}


def load_feeds_from_json(file_path: Optional[Path] = None) -> List[FeedConfig]:
    """Load feed configurations from a JSON file (e.g. realtime_feeds.json or feeds.json)."""
    if file_path is None:
        env_path = os.environ.get("REALTIME_FEEDS_FILE", "").strip()
        candidates = [
            Path(env_path) if env_path else None,
            BASE_DIR / "realtime_feeds.json",
            BASE_DIR / "feeds.json",
            BASE_DIR / "realtime" / "feeds.json",
        ]
        file_path = next((p for p in candidates if p and p.is_file()), None)

    if not file_path or not file_path.is_file():
        return []

    try:
        data = json.loads(file_path.read_text(encoding="utf-8"))
        raw_list = data if isinstance(data, list) else data.get("feeds", [])
        configs = []
        for item in raw_list:
            if not isinstance(item, dict):
                continue
            if not item.get("enabled", True):
                continue
            name = str(item.get("name", "")).strip()
            url = str(item.get("url", "")).strip()
            if not name or not url:
                continue
            interval = int(item.get("interval", 30))
            stale_threshold = int(item.get("stale_threshold", max(interval * 3, 90)))
            headers = item.get("headers") or {}
            feed_type = item.get("feed_type")

            configs.append(
                FeedConfig(
                    name=name,
                    default_url=url,
                    default_interval=interval,
                    default_stale=stale_threshold,
                    headers=headers,
                    feed_type=feed_type,
                )
            )
        log.info("Loaded %d feed(s) from %s", len(configs), file_path)
        return configs
    except Exception as exc:
        log.error("Failed to load realtime feeds JSON from %s: %s", file_path, exc)
        return []


def get_feed_configs(feeds_file: Optional[Path] = None) -> List[FeedConfig]:
    """Build list of feed configurations dynamically from JSON file and/or environment variables."""
    # 1. Load from JSON file if present
    json_configs = load_feeds_from_json(feeds_file)

    # 2. Build configs from environment variables
    env_configs: List[FeedConfig] = []

    # Norway / Entur feeds (if ENTUR_DATASOURCES configured)
    entur_raw = os.environ.get("ENTUR_DATASOURCES", "").strip()
    if entur_raw:
        codespaces = [c.strip().upper() for c in entur_raw.split(",") if c.strip()]
        if "ALL" in codespaces:
            codespaces = [k for k, v in ENTUR_SUPPORTED_CODESPACES.items() if any(v.values())]

        poll_interval = int(os.environ.get("REALTIME_POLL_INTERVAL_ENTUR", "30"))
        stale_threshold = int(os.environ.get("REALTIME_STALE_THRESHOLD_ENTUR", "90"))

        # If user specifies 'ALL', 'NATIONAL', or 'NORWAY', poll the unified national endpoints
        if any(c in ("ALL", "NATIONAL", "NORWAY") for c in codespaces):
            env_configs.append(
                FeedConfig(
                    name="entur_national_trip_updates",
                    env_url_key="ENTUR_TRIP_UPDATES_URL",
                    default_url="https://api.entur.io/realtime/v1/gtfs-rt/trip-updates",
                    env_interval_key="REALTIME_POLL_INTERVAL_ENTUR",
                    default_interval=poll_interval,
                    env_stale_key="REALTIME_STALE_THRESHOLD_ENTUR",
                    default_stale=stale_threshold,
                )
            )
            env_configs.append(
                FeedConfig(
                    name="entur_national_vehicle_positions",
                    env_url_key="ENTUR_VEHICLE_POSITIONS_URL",
                    default_url="https://api.entur.io/realtime/v1/gtfs-rt/vehicle-positions",
                    env_interval_key="REALTIME_POLL_INTERVAL_ENTUR",
                    default_interval=poll_interval,
                    env_stale_key="REALTIME_STALE_THRESHOLD_ENTUR",
                    default_stale=stale_threshold,
                )
            )
            env_configs.append(
                FeedConfig(
                    name="entur_national_alerts",
                    env_url_key="ENTUR_ALERTS_URL",
                    default_url="https://api.entur.io/realtime/v1/gtfs-rt/alerts",
                    env_interval_key="REALTIME_POLL_INTERVAL_ENTUR_ALERTS",
                    default_interval=max(poll_interval * 2, 60),
                    env_stale_key="REALTIME_STALE_THRESHOLD_ENTUR_ALERTS",
                    default_stale=stale_threshold * 2,
                )
            )
        else:
            for cs in codespaces:
                caps = ENTUR_SUPPORTED_CODESPACES.get(cs, {"trip_updates": True, "vehicle_positions": True, "alerts": True})
                cs_lower = cs.lower()
                if caps.get("trip_updates"):
                    env_configs.append(
                        FeedConfig(
                            name=f"entur_{cs_lower}_trip_updates",
                            env_url_key=f"ENTUR_{cs}_TRIP_UPDATES_URL",
                            default_url=f"https://api.entur.io/realtime/v1/gtfs-rt/trip-updates?datasource={cs}",
                            env_interval_key="REALTIME_POLL_INTERVAL_ENTUR",
                            default_interval=poll_interval,
                            env_stale_key="REALTIME_STALE_THRESHOLD_ENTUR",
                            default_stale=stale_threshold,
                        )
                    )
                if caps.get("vehicle_positions"):
                    env_configs.append(
                        FeedConfig(
                            name=f"entur_{cs_lower}_vehicle_positions",
                            env_url_key=f"ENTUR_{cs}_VEHICLE_POSITIONS_URL",
                            default_url=f"https://api.entur.io/realtime/v1/gtfs-rt/vehicle-positions?datasource={cs}",
                            env_interval_key="REALTIME_POLL_INTERVAL_ENTUR",
                            default_interval=poll_interval,
                            env_stale_key="REALTIME_STALE_THRESHOLD_ENTUR",
                            default_stale=stale_threshold,
                        )
                    )
                if caps.get("alerts"):
                    env_configs.append(
                        FeedConfig(
                            name=f"entur_{cs_lower}_alerts",
                            env_url_key=f"ENTUR_{cs}_ALERTS_URL",
                            default_url=f"https://api.entur.io/realtime/v1/gtfs-rt/alerts?datasource={cs}",
                            env_interval_key="REALTIME_POLL_INTERVAL_ENTUR_ALERTS",
                            default_interval=max(poll_interval * 2, 60),
                            env_stale_key="REALTIME_STALE_THRESHOLD_ENTUR_ALERTS",
                            default_stale=stale_threshold * 2,
                        )
                    )

    # Base feeds (Netherlands) if neither JSON file nor ENTUR_ONLY is present
    entur_only = os.environ.get("ENTUR_ONLY", "false").strip().lower() in ("1", "true", "yes", "on")
    if not entur_only and not json_configs:
        env_configs.extend([
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
        ])

    # Merge JSON configs with env configs (deduplicating by feed name)
    merged: List[FeedConfig] = []
    seen = set()
    for cfg in (json_configs + env_configs):
        if cfg.name not in seen:
            seen.add(cfg.name)
            merged.append(cfg)

    return merged


FEED_CONFIGS = get_feed_configs()


_SCHEDULE_RELATIONSHIP_MAP = {
    0: "SCHEDULED",
    1: "SKIPPED",
    2: "NO_DATA",
    3: "UNSCHEDULED",
}

_VEHICLE_STOP_STATUS_MAP = {
    0: "INCOMING_AT",
    1: "STOPPED_AT",
    2: "IN_TRANSIT_TO",
}

_CONGESTION_LEVEL_MAP = {
    0: "UNKNOWN_CONGESTION_LEVEL",
    1: "RUNNING_SMOOTHLY",
    2: "STOP_AND_GO",
    3: "CONGESTION",
    4: "SEVERE_CONGESTION",
}

_OCCUPANCY_STATUS_MAP = {
    0: "EMPTY",
    1: "MANY_SEATS_AVAILABLE",
    2: "FEW_SEATS_AVAILABLE",
    3: "STANDING_ROOM_ONLY",
    4: "CRUSHED_STANDING_ROOM_ONLY",
    5: "FULL",
    6: "NOT_ACCEPTING_PASSENGERS",
}

_ALERT_CAUSE_MAP = {
    1: "UNKNOWN_CAUSE",
    2: "OTHER_CAUSE",
    3: "TECHNICAL_PROBLEM",
    4: "STRIKE",
    5: "DEMONSTRATION",
    6: "ACCIDENT",
    7: "HOLIDAY",
    8: "WEATHER",
    9: "MAINTENANCE",
    10: "CONSTRUCTION",
    11: "POLICE_ACTIVITY",
    12: "MEDICAL_EMERGENCY",
}

_ALERT_EFFECT_MAP = {
    1: "NO_SERVICE",
    2: "REDUCED_SERVICE",
    3: "SIGNIFICANT_DELAYS",
    4: "DETOUR",
    5: "ADDITIONAL_SERVICE",
    6: "MODIFIED_SERVICE",
    7: "OTHER_EFFECT",
    8: "UNKNOWN_EFFECT",
    9: "STOP_MOVED",
}

_ALERT_SEVERITY_MAP = {
    1: "UNKNOWN_SEVERITY",
    2: "INFO",
    3: "WARNING",
    4: "SEVERE",
}


def _parse_trip_update(entity, feed_source: str) -> Dict[str, Any]:
    tu = entity.trip_update
    has_trip = tu.HasField("trip")
    trip = tu.trip if has_trip else None

    trip_desc = {
        "trip_id": trip.trip_id if (trip and trip.HasField("trip_id")) else "",
        "route_id": trip.route_id if (trip and trip.HasField("route_id")) else None,
        "direction_id": trip.direction_id if (trip and trip.HasField("direction_id")) else None,
        "start_time": trip.start_time if (trip and trip.HasField("start_time")) else None,
        "start_date": trip.start_date if (trip and trip.HasField("start_date")) else None,
        "schedule_relationship": (
            _SCHEDULE_RELATIONSHIP_MAP.get(trip.schedule_relationship, "SCHEDULED")
            if (trip and trip.HasField("schedule_relationship"))
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
            a = stu.arrival
            arr = {
                "delay": a.delay if a.HasField("delay") else None,
                "time": a.time if a.HasField("time") else None,
                "uncertainty": a.uncertainty if a.HasField("uncertainty") else None,
            }
        dep = None
        if stu.HasField("departure"):
            d = stu.departure
            dep = {
                "delay": d.delay if d.HasField("delay") else None,
                "time": d.time if d.HasField("time") else None,
                "uncertainty": d.uncertainty if d.HasField("uncertainty") else None,
            }

        stop_time_updates.append({
            "stop_sequence": stu.stop_sequence if stu.HasField("stop_sequence") else None,
            "stop_id": stu.stop_id if stu.HasField("stop_id") else None,
            "arrival": arr,
            "departure": dep,
            "schedule_relationship": _SCHEDULE_RELATIONSHIP_MAP.get(stu.schedule_relationship, "SCHEDULED"),
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
        "current_stop_sequence": vp.current_stop_sequence if (vp and vp.HasField("current_stop_sequence")) else None,
        "stop_id": vp.stop_id if (vp and vp.HasField("stop_id")) else None,
        "current_status": (
            _VEHICLE_STOP_STATUS_MAP.get(vp.current_status, "IN_TRANSIT_TO")
            if (vp and vp.HasField("current_status"))
            else None
        ),
        "timestamp": vp.timestamp if (vp and vp.HasField("timestamp")) else None,
        "congestion_level": (
            _CONGESTION_LEVEL_MAP.get(vp.congestion_level, "UNKNOWN_CONGESTION_LEVEL")
            if (vp and vp.HasField("congestion_level"))
            else None
        ),
        "occupancy_status": (
            _OCCUPANCY_STATUS_MAP.get(vp.occupancy_status, "EMPTY")
            if (vp and vp.HasField("occupancy_status"))
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

    header = (
        al.header_text.translation[0].text
        if (al.HasField("header_text") and al.header_text.translation)
        else (al.header_text.translation[0].text if getattr(al, "header_text", None) and getattr(al.header_text, "translation", None) else None)
    )
    desc = (
        al.description_text.translation[0].text
        if (al.HasField("description_text") and al.description_text.translation)
        else (al.description_text.translation[0].text if getattr(al, "description_text", None) and getattr(al.description_text, "translation", None) else None)
    )

    return {
        "entity_id": entity.id,
        "is_deleted": getattr(entity, "is_deleted", False),
        "alert_id": entity.id,
        "active_periods": active_periods,
        "informed_entities": informed_entities,
        "cause": (
            _ALERT_CAUSE_MAP.get(al.cause, "UNKNOWN_CAUSE")
            if al.HasField("cause")
            else "UNKNOWN_CAUSE"
        ),
        "effect": (
            _ALERT_EFFECT_MAP.get(al.effect, "UNKNOWN_EFFECT")
            if al.HasField("effect")
            else "UNKNOWN_EFFECT"
        ),
        "url": al.url.translation[0].text if (al.HasField("url") and al.url.translation) else None,
        "header_text": header,
        "description_text": desc,
        "severity_level": (
            _ALERT_SEVERITY_MAP.get(al.severity_level, "UNKNOWN_SEVERITY")
            if al.HasField("severity_level")
            else "UNKNOWN_SEVERITY"
        ),
        "feed_source": feed_source,
    }


if sys.platform == "win32":
    try:
        import ctypes
        ctypes.windll.winmm.timeBeginPeriod(1)
    except Exception:
        pass


def parse_feed_message(content: bytes, feed_name: str) -> Tuple[List[Dict[str, Any]], str]:
    """Parse GTFS-RT FeedMessage protobuf into normalized dictionaries with cooperative GIL yielding."""
    msg = gtfs_realtime_pb2.FeedMessage()
    msg.ParseFromString(content)

    inc = "FULL_DATASET"
    if msg.header.HasField("incrementality"):
        inc = gtfs_realtime_pb2.FeedHeader.Incrementality.Name(msg.header.incrementality)

    entities = []
    entity_count = 0
    for entity in msg.entity:
        entity_count += 1
        # Frequent cooperative GIL yield (2ms every 100 entities) keeps HTTP response times sub-50ms
        if entity_count % 100 == 0:
            time.sleep(0.002)

        if getattr(entity, "is_deleted", False):
            entities.append({"entity_id": entity.id, "is_deleted": True})
            continue
        if entity.HasField("trip_update"):
            entities.append(_parse_trip_update(entity, feed_name))
        elif entity.HasField("vehicle"):
            entities.append(_parse_vehicle_position(entity, feed_name))
        elif entity.HasField("alert"):
            entities.append(_parse_alert(entity, feed_name))

    return entities, inc


_SYNC_HTTP_CLIENT: Optional[httpx.Client] = None

def _get_sync_http_client(timeout: float = 15.0) -> httpx.Client:
    global _SYNC_HTTP_CLIENT
    if _SYNC_HTTP_CLIENT is None:
        client_name = os.environ.get("ENTUR_CLIENT_NAME", os.environ.get("REALTIME_CLIENT_NAME", "zeus-gtfsroutes")).strip()
        headers = {
            "User-Agent": "GTFSRealtimeClient/1.0 (+https://github.com)",
            "ET-Client-Name": client_name,
        }
        _SYNC_HTTP_CLIENT = httpx.Client(
            timeout=httpx.Timeout(timeout, connect=10.0),
            follow_redirects=True,
            headers=headers,
        )
    return _SYNC_HTTP_CLIENT

def _download_and_parse_worker(url: str, headers: dict, feed_name: str, timeout: float = 15.0):
    """Run full HTTP download and protobuf parsing completely off the event loop in a worker thread."""
    client = _get_sync_http_client(timeout)
    resp = client.get(url, headers=headers)
    status_code = resp.status_code
    resp_headers = dict(resp.headers)
    if status_code == 304:
        return 304, resp_headers, [], "DIFFERENTIAL"
    resp.raise_for_status()
    content = resp.content
    if not content or len(content) < 4:
        raise ValueError(f"Received empty or truncated payload ({len(content)} bytes)")
    entities, inc = parse_feed_message(content, feed_name)
    return status_code, resp_headers, entities, inc


class RealtimePoller:
    """Manages background fetching, validation, and multi-worker leader election."""

    def __init__(self, cache_store: CacheStore):
        self.cache = cache_store
        self.worker_id = f"worker_{uuid.uuid4().hex[:8]}"
        self.timeout = float(os.environ.get("REALTIME_HTTP_TIMEOUT_SECONDS", "15"))
        self.max_retries = int(os.environ.get("REALTIME_HTTP_MAX_RETRIES", "2"))
        self.lock_ttl = int(os.environ.get("REALTIME_LOCK_TTL_SECONDS", "45"))
        max_concurrent = int(os.environ.get("REALTIME_MAX_CONCURRENT_FETCHES", "1"))
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._running = False
        self._tasks: List[asyncio.Task] = []
        self._client: Optional[httpx.AsyncClient] = None

    async def start(self) -> None:
        self._running = True
        client_name = os.environ.get("ENTUR_CLIENT_NAME", os.environ.get("REALTIME_CLIENT_NAME", "zeus-gtfsroutes")).strip()
        headers = {
            "User-Agent": "GTFSRealtimeClient/1.0 (+https://github.com)",
            "ET-Client-Name": client_name,
        }
        self._client = httpx.AsyncClient(
            timeout=self.timeout,
            follow_redirects=True,
            headers=headers,
        )
        feed_configs = get_feed_configs()
        log.info(
            "Starting RealtimePoller [%s] with ET-Client-Name: '%s' (%d feeds configured)...",
            self.worker_id,
            client_name,
            len(feed_configs),
        )
        # Spawn one worker loop per feed plus leader renewal loop
        self._tasks.append(asyncio.create_task(self._leader_supervision_loop()))
        for idx, cfg in enumerate(feed_configs):
            stagger = (idx * 3.5) + 1.0
            self._tasks.append(asyncio.create_task(self._feed_polling_loop(cfg, initial_stagger=stagger)))

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

    async def _feed_polling_loop(self, cfg: FeedConfig, initial_stagger: float = 1.0) -> None:
        # Stagger initial feed start times to avoid simultaneous download/parse CPU spikes
        await asyncio.sleep(initial_stagger)
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
        if cfg.headers:
            headers.update(cfg.headers)
        if cfg.etag:
            headers["If-None-Match"] = cfg.etag
        if cfg.last_modified:
            headers["If-Modified-Since"] = cfg.last_modified

        attempt = 0
        success = False
        last_exc: Optional[Exception] = None
        fetch_start = time.time()

        async with self._semaphore:
            while attempt <= self.max_retries and not success:
                attempt += 1
                try:
                    # Download & parse completely off the event loop in a dedicated worker thread
                    status_code, resp_headers, entities, inc = await asyncio.to_thread(
                        _download_and_parse_worker, cfg.url, headers, cfg.name, self.timeout
                    )
                    meta.http_status = status_code

                    if status_code == 304:
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

                    # Store headers
                    if "ETag" in resp_headers:
                        cfg.etag = resp_headers["ETag"]
                    if "Last-Modified" in resp_headers:
                        cfg.last_modified = resp_headers["Last-Modified"]

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


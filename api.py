#!/usr/bin/env python3
"""
api.py – FastAPI backend serving GTFS + OSM stop data from gtfs.sqlite.

Run with:
    python3 run_api.py --reload        # binds API_PORT from .env
    # or explicitly:
    uvicorn api:app --reload --port 5004

Note: `uvicorn api:app` alone always binds port 8000 — uvicorn's --port flag
decides the bind address, not API_PORT. Use run_api.py so the port and the
CORS allow-list stay in sync.
"""

import html
import httpx
import json
import os
import re
import sqlite3
from contextlib import contextmanager, asynccontextmanager
from datetime import date, datetime, time, timedelta
import math
from pathlib import Path
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

from fastapi import FastAPI, HTTPException, Query, Response
from pydantic import BaseModel, Field
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

DB_PATH = Path(__file__).parent / "gtfs.sqlite"


def _load_dotenv() -> None:
    """Load .env from the script directory (real env vars win)."""
    env_file = Path(__file__).parent / ".env"
    if not env_file.is_file():
        return
    for raw in env_file.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        # Strip inline comments for unquoted values
        if not (value.startswith('"') or value.startswith("'")):
            value = value.split("#")[0]
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


_load_dotenv()


def _resolve_db(name: str) -> Path:
    p = Path(name).expanduser()
    return p if p.is_absolute() else (Path(__file__).parent / p)


# One database per service mode. Both are served by this single app; every
# endpoint keeps its existing path and picks the DB from the `mode` parameter.
#   mode=transit (default) -> transit routes/stops
#   mode=ferry             -> ferry (water) routes/stops
DB_PATHS = {
    "transit": _resolve_db(
        os.environ.get("API_DB") or os.environ.get("GTFS_DB") or "gtfs.sqlite"
    ),
    "ferry": _resolve_db(
        os.environ.get("API_FERRY_DB")
        or os.environ.get("WATER_GTFS_DB")
        or "water-gtfs.sqlite"
    ),
    "rail": _resolve_db(
        os.environ.get("API_RAIL_DB")
        or os.environ.get("RAIL_GTFS_DB")
        or "rail-gtfs.sqlite"
    ),
}

# Backwards-compatible alias: the transit database.
DB_PATH = DB_PATHS["transit"]

DEFAULT_MODE = (os.environ.get("API_DEFAULT_MODE") or "transit").strip().lower()

# Spatial & nearby search radius thresholds from environment
DEFAULT_NEARBY_RADIUS = float(os.environ.get("NEARBY_RADIUS_METERS", "500.0").strip() or 500.0)
DEFAULT_CITY_RADIUS = float(os.environ.get("NEARBY_CITY_RADIUS_METERS", "15000.0").strip() or 15000.0)
DEFAULT_STATE_RADIUS = float(os.environ.get("NEARBY_STATE_RADIUS_METERS", "60000.0").strip() or 60000.0)

# The water feed carries both ferries and water taxis, but GTFS has no field
# that tells them apart (every route is route_type=4 from one agency), so the
# split is configured here. Routes not listed fall back to WATER_DEFAULT_TYPE.
# Override with e.g. API_FERRY_ROUTES=DAL-ALD,SIAI,RED
FERRY_ROUTE_NAMES = {
    n.strip().upper()
    for n in (os.environ.get("API_FERRY_ROUTES") or "DAL-ALD,SIAI").split(",")
    if n.strip()
}
WATER_DEFAULT_TYPE = (os.environ.get("API_WATER_DEFAULT_TYPE") or "water_taxi").strip()


def _service_type(mode: str, route_short_name: Optional[str],
                  route_long_name: Optional[str] = None) -> Optional[str]:
    """
    Classify a route for the ``service_type`` response field.

    - ``transit`` -> ``bus``
    - ``rail``    -> ``rail``
    - ``ferry``   -> ``ferry`` or ``water_taxi`` (see FERRY_ROUTE_NAMES)
    """
    if mode == "transit":
        return "bus"
    if mode == "rail":
        return "rail"
    if mode == "ferry":
        for name in (route_short_name, route_long_name):
            if name and name.strip().upper() in FERRY_ROUTE_NAMES:
                return "ferry"
        return WATER_DEFAULT_TYPE
    return None
MODES = tuple(DB_PATHS)

# Port this API listens on (used to build the self-referencing CORS list).
# The bind port comes from uvicorn's --port flag, NOT from this value — use
# run_api.py so both stay in sync:
#     python3 run_api.py            # binds API_PORT
#     python3 run_api.py --port 9000
API_PORT  = int(os.environ.get("API_PORT",  8000))

# Port the Vite dev-server (or preview) runs on.
# Override with:  VITE_PORT=3000  (must match vite.config.js server.port)
VITE_PORT = int(os.environ.get("VITE_PORT", 5173))

@asynccontextmanager
async def lifespan(app: FastAPI):
    poller = None
    if os.environ.get("REALTIME_ENABLED", "true").strip().lower() in ("1", "true", "yes", "on"):
        try:
            from realtime import get_realtime_poller
            poller = await get_realtime_poller()
            await poller.start()
        except Exception as exc:
            import logging
            logging.getLogger("api").warning("Failed to start realtime poller: %s", exc)
    yield
    if poller:
        try:
            await poller.stop()
        except Exception:
            pass


app = FastAPI(title="GTFS-OSM Stop API", lifespan=lifespan)

from realtime import realtime_router
app.include_router(realtime_router)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        f"http://localhost:{VITE_PORT}",
        f"http://127.0.0.1:{VITE_PORT}",
        # Vite preview default
        "http://localhost:4173",
        "http://127.0.0.1:4173",
        # Self (for the bundled HTML viewer)
        f"http://localhost:{API_PORT}",
        f"http://127.0.0.1:{API_PORT}",
        # file:// origin (null)
        "null",
        # Extra origins (e.g. LAN access): comma-separated in API_CORS_ORIGINS
        *[o.strip() for o in os.environ.get("API_CORS_ORIGINS", "").split(",")
          if o.strip()],
    ],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


STATIC_DIR = Path(__file__).parent / "static"
STATIC_DIR.mkdir(exist_ok=True)
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

# Shared lang Query parameter – must be defined before any endpoint uses it.
_LANG_QUERY = Query(
    default=None,
    description="Language code for translations. Use 'ar' to get Arabic translations. "
                "Omit or pass 'en' for English (default).",
)


def _parse_iso_datetime(value: str) -> datetime:
    """
    Parse an ISO datetime, tolerating an unencoded '+' in the timezone offset.

    In a URL query string '+' decodes to a space, so a client that sends
    `...T17:26:11+04:00` without percent-encoding arrives as
    `...T17:26:11 04:00`. Both forms are accepted here.
    """
    text = value.strip().replace("Z", "+00:00")
    # "2026-10-26T17:26:11 04:00" -> "2026-10-26T17:26:11+04:00"
    text = re.sub(r"[ T](\d{2}):?(\d{2})$", r"+\1:\2", text)
    return datetime.fromisoformat(text)


@app.get("/", include_in_schema=False)
def root():
    from fastapi.responses import RedirectResponse
    return RedirectResponse(url="/static/routes.html")


@contextmanager
def _open_db(db_path: Path):
    """Open an SQLite database connection with optimized WAL pragmas."""
    if not db_path.exists():
        raise HTTPException(
            status_code=503,
            detail=f"Database not found at {db_path}.",
        )
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    # WAL mode: readers don't block writers and vice versa; larger cache speeds
    # up join-heavy queries; memory-mapped I/O avoids syscall overhead.
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA cache_size=-32768")   # 32 MB page cache
    conn.execute("PRAGMA mmap_size=268435456") # 256 MB memory-mapped I/O
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA temp_store=MEMORY")
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def _db(mode: str = DEFAULT_MODE):
    """
    Open the database for *mode* ('transit', 'ferry', or 'rail').

    Every endpoint passes its `mode` query parameter here, so one service
    serves both the transit and the ferry feed.
    """
    if mode not in DB_PATHS:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown mode '{mode}'. Valid values: {', '.join(MODES)}",
        )
    with _open_db(DB_PATHS[mode]) as conn:
        yield conn


@contextmanager
def _db_or_region(region: Optional[str] = None, mode: str = DEFAULT_MODE):
    """
    Open database connection.
    If region / region_id is provided, resolve and open that specific SQLite file.
    Otherwise, fall back to mode-based DB_PATHS[mode].
    """
    if region and str(region).strip():
        db_path, _ = _resolve_region_db(str(region).strip())
        with _open_db(db_path) as conn:
            yield conn
    else:
        with _db(mode) as conn:
            yield conn


# ── Regions & Multi-Database Discovery (Dynamic / No Hardcoding) ──────────────

_REGION_CACHE: dict = {}
_AGENCY_COUNTS_CACHE: dict = {}
_GEOCODE_CACHE: dict = {}


def _list_available_sqlite_paths() -> list[Path]:
    """
    Discover all SQLite databases in the workspace or configured GTFS_DATA_DIR.
    No region or country names are hardcoded.
    """
    found: set[Path] = set()
    root = Path(__file__).parent
    
    # Check configured data directory if provided
    data_dir_env = os.environ.get("GTFS_DATA_DIR")
    if data_dir_env:
        data_dir = Path(data_dir_env).expanduser()
        if data_dir.is_dir():
            for p in data_dir.glob("*.sqlite"):
                if p.is_file():
                    found.add(p.resolve())

    # Check project workspace
    for p in root.glob("*.sqlite"):
        if p.is_file():
            found.add(p.resolve())
    for p in DB_PATHS.values():
        if p.is_file():
            found.add(p.resolve())
    return sorted(found, key=lambda p: p.name)


ROUTE_TYPE_NAMES: Dict[int, str] = {
    0: "tram",
    1: "subway",
    2: "rail",
    3: "bus",
    4: "ferry",
    5: "cable_car",
    6: "gondola",
    7: "funicular",
    11: "trolleybus",
    12: "monorail",
}


def _route_type_to_mode(rtype: int) -> str:
    """Map GTFS and extended NeTEx/HVT route_type integer to human-friendly transit mode name."""
    if rtype in ROUTE_TYPE_NAMES:
        return ROUTE_TYPE_NAMES[rtype]
    if 100 <= rtype <= 117:
        return "rail"
    if 200 <= rtype <= 209:
        return "coach"
    if 400 <= rtype <= 405:
        return "subway"
    if 700 <= rtype <= 717:
        return "bus"
    if 800 <= rtype <= 802:
        return "trolleybus"
    if 900 <= rtype <= 907:
        return "tram"
    if 1000 <= rtype <= 1002:
        return "ferry"
    return "other"


def _inspect_database(db_path: Path, force_refresh: bool = False) -> dict:
    """
    Dynamically inspect an SQLite GTFS database and extract metadata, bounds, and stats.
    All attributes (id, name, region, bounding box) are derived dynamically from
    the database tables without any hardcoded region maps.
    """
    mtime = db_path.stat().st_mtime
    cache_key = f"{db_path.resolve()}:{mtime}"
    if not force_refresh and cache_key in _REGION_CACHE:
        return _REGION_CACHE[cache_key]

    reg_id = db_path.stem.lower()
    name = db_path.stem.replace("-gtfs", "").replace("-", " ").replace("_", " ").title()
    aliases = list({db_path.stem, db_path.name, db_path.stem.lower(), db_path.stem.replace("-gtfs", "")})

    stat = db_path.stat()
    agencies: list[dict] = []
    timezone: Optional[str] = None
    feed_info: Optional[dict] = None
    stats: dict = {}
    bbox: Optional[dict] = None

    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        tables = {r[0] for r in cur.execute("SELECT name FROM sqlite_master WHERE type='table'")}

        if "agency" in tables:
            for r in cur.execute("SELECT agency_id, agency_name, agency_timezone, agency_url FROM agency"):
                agencies.append(dict(r))
                if not timezone and r["agency_timezone"]:
                    timezone = r["agency_timezone"]

        if "feed_info" in tables:
            row = cur.execute("SELECT * FROM feed_info LIMIT 1").fetchone()
            if row:
                feed_info = {k: row[k] for k in row.keys() if row[k]}

        if "stops" in tables:
            stats["stops"] = cur.execute("SELECT COUNT(*) FROM stops").fetchone()[0]
            # Valid global coordinate bounds (-90 to 90 lat, -180 to 180 lon, non-zero)
            row = cur.execute("""
                SELECT MIN(CAST(stop_lat AS FLOAT)), MAX(CAST(stop_lat AS FLOAT)),
                       MIN(CAST(stop_lon AS FLOAT)), MAX(CAST(stop_lon AS FLOAT))
                FROM stops
                WHERE CAST(stop_lat AS FLOAT) != 0.0 AND CAST(stop_lon AS FLOAT) != 0.0
                  AND CAST(stop_lat AS FLOAT) BETWEEN -90.0 AND 90.0
                  AND CAST(stop_lon AS FLOAT) BETWEEN -180.0 AND 180.0
            """).fetchone()
            if row and row[0] is not None:
                bbox = {
                    "min_lat": row[0],
                    "max_lat": row[1],
                    "min_lon": row[2],
                    "max_lon": row[3],
                }

        transit_modes = []
        route_types = []
        if "routes" in tables:
            stats["routes"] = cur.execute("SELECT COUNT(*) FROM routes").fetchone()[0]
            try:
                type_rows = cur.execute(
                    "SELECT DISTINCT route_type FROM routes WHERE route_type IS NOT NULL"
                ).fetchall()
                modes_set = set()
                for tr in type_rows:
                    try:
                        rtype = int(tr[0])
                        route_types.append(rtype)
                        modes_set.add(_route_type_to_mode(rtype))
                    except (ValueError, TypeError):
                        pass
                transit_modes = sorted(list(modes_set))
                route_types.sort()
            except Exception:
                pass
        if "trips" in tables:
            stats["trips"] = cur.execute("SELECT COUNT(*) FROM trips").fetchone()[0]
        stats["agencies"] = len(agencies)
        conn.close()
    except Exception as exc:
        stats["error"] = str(exc)

    # Derive human-friendly region / country name dynamically from metadata
    region_name = name
    if feed_info and feed_info.get("feed_publisher_name"):
        region_name = feed_info["feed_publisher_name"]
    elif agencies and len(agencies) == 1 and agencies[0].get("agency_name"):
        region_name = agencies[0]["agency_name"]
    elif timezone and "/" in timezone:
        region_name = timezone.split("/")[-1].replace("_", " ")

    info = {
        "id": reg_id,
        "name": name,
        "region": region_name,
        "database": db_path.name,
        "transit_modes": transit_modes,
        "route_types": route_types,
        "database_path": str(db_path.resolve()),
        "size_mb": round(stat.st_size / (1024 * 1024), 2),
        "size_bytes": stat.st_size,
        "timezone": timezone,
        "bbox": bbox,
        "stats": stats,
        "feed_info": feed_info,
        "agencies": agencies,
        "aliases": aliases,
    }
    _REGION_CACHE[cache_key] = info
    return info


def _get_all_regions_info(force_refresh: bool = False) -> list[dict]:
    """Return metadata for all dynamically discovered SQLite databases."""
    paths = _list_available_sqlite_paths()
    return [_inspect_database(p, force_refresh=force_refresh) for p in paths]


def _resolve_region_db(region: str) -> tuple[Path, dict]:
    """Resolve a region identifier, slug, alias, or database filename to (Path, info)."""
    target = region.strip().lower()
    clean_target = target.replace("-", " ").replace("_", " ")
    regions = _get_all_regions_info()
    for reg in regions:
        reg_id = reg["id"].lower()
        reg_db = reg["database"].lower()
        reg_name = reg.get("name", "").lower()
        reg_region = reg.get("region", "").lower()
        aliases = [str(alias).lower() for alias in reg.get("aliases", [])]

        if (
            target == reg_id
            or target == reg_db
            or target == reg_name
            or target == reg_region
            or clean_target == reg_name
            or clean_target == reg_region
            or any(target == a or clean_target == a.replace("-", " ").replace("_", " ") for a in aliases)
        ):
            return Path(reg["database_path"]), reg

    # Try direct file match in project directory
    direct_path = Path(__file__).parent / target
    if direct_path.is_file():
        return direct_path, _inspect_database(direct_path)
    direct_sqlite = Path(__file__).parent / f"{target}.sqlite"
    if direct_sqlite.is_file():
        return direct_sqlite, _inspect_database(direct_sqlite)

    available = [f"{r['id']} ({r['database']})" for r in regions]
    raise HTTPException(
        status_code=404,
        detail=f"Unknown region / database '{region}'. Available: {', '.join(available)}",
    )

_ROUTE_DB_CACHE: dict = {}
_STOP_DB_CACHE: dict = {}
_TRIP_DB_CACHE: dict = {}


def _find_db_for_route(
    route_id: str,
    region: Optional[str] = None,
    region_id: Optional[str] = None,
    regionId: Optional[str] = None,
    mode: str = DEFAULT_MODE,
) -> Path:
    """
    Resolve the correct SQLite database file for a route:
    1. If region / region_id / regionId is provided, resolve that specific DB.
    2. Check route-to-database memory cache.
    3. Check default DB (DB_PATHS[mode] or DB_PATH).
    4. If not found in default DB, scan all discovered SQLite databases to find which contains route_id.
    """
    target_region = regionId or region_id or region
    if target_region and str(target_region).strip():
        db_path, _ = _resolve_region_db(str(target_region).strip())
        _ROUTE_DB_CACHE[route_id] = db_path
        return db_path

    if route_id in _ROUTE_DB_CACHE:
        cached_p = _ROUTE_DB_CACHE[route_id]
        if cached_p.is_file():
            return cached_p

    # Check default DB first
    default_db = DB_PATHS.get(mode) or DB_PATH
    if default_db and default_db.is_file():
        try:
            with _open_db(default_db) as conn:
                row = conn.execute("SELECT 1 FROM routes WHERE route_id = ? LIMIT 1", (route_id,)).fetchone()
                if row:
                    _ROUTE_DB_CACHE[route_id] = default_db
                    return default_db
        except Exception:
            pass

    # Scan all discovered SQLite databases dynamically
    for p in _list_available_sqlite_paths():
        if default_db and p.resolve() == default_db.resolve():
            continue
        try:
            with _open_db(p) as conn:
                row = conn.execute("SELECT 1 FROM routes WHERE route_id = ? LIMIT 1", (route_id,)).fetchone()
                if row:
                    _ROUTE_DB_CACHE[route_id] = p
                    return p
        except Exception:
            continue

    return default_db


def _find_db_for_stop(
    stop_id: str,
    region: Optional[str] = None,
    region_id: Optional[str] = None,
    regionId: Optional[str] = None,
    mode: str = DEFAULT_MODE,
) -> Path:
    """Resolve correct SQLite DB for a stop, checking explicit region first, then default DB, then all DBs."""
    target_region = regionId or region_id or region
    if target_region and str(target_region).strip():
        db_path, _ = _resolve_region_db(str(target_region).strip())
        _STOP_DB_CACHE[stop_id] = db_path
        return db_path

    if stop_id in _STOP_DB_CACHE:
        cached_p = _STOP_DB_CACHE[stop_id]
        if cached_p.is_file():
            return cached_p

    default_db = DB_PATHS.get(mode) or DB_PATH
    if default_db and default_db.is_file():
        try:
            with _open_db(default_db) as conn:
                row = conn.execute("SELECT 1 FROM stops WHERE stop_id = ? LIMIT 1", (stop_id,)).fetchone()
                if row:
                    _STOP_DB_CACHE[stop_id] = default_db
                    return default_db
        except Exception:
            pass

    for p in _list_available_sqlite_paths():
        if default_db and p.resolve() == default_db.resolve():
            continue
        try:
            with _open_db(p) as conn:
                row = conn.execute("SELECT 1 FROM stops WHERE stop_id = ? LIMIT 1", (stop_id,)).fetchone()
                if row:
                    _STOP_DB_CACHE[stop_id] = p
                    return p
        except Exception:
            continue

    return default_db



def _get_db_timezone(conn: sqlite3.Connection) -> ZoneInfo:
    """Return the primary ZoneInfo for the database, defaulting to UTC."""
    try:
        row = conn.execute(
            "SELECT agency_timezone FROM agency WHERE agency_timezone IS NOT NULL AND agency_timezone != '' LIMIT 1"
        ).fetchone()
        if row and row[0]:
            return ZoneInfo(row[0].strip())
    except Exception:
        pass
    return ZoneInfo("UTC")


def _find_db_for_coords(lat: float, lon: float, region: Optional[str] = None) -> Optional[tuple[Path, dict]]:
    """
    Find the SQLite database covering (lat, lon) dynamically from geographic bounding boxes.
    No hardcoded regions or country names are used.
    """
    if region:
        try:
            return _resolve_region_db(region)
        except Exception:
            return None

    regions = _get_all_regions_info()
    matched = []
    for reg in regions:
        bbox = reg.get("bbox")
        if bbox:
            min_lat, max_lat = bbox["min_lat"], bbox["max_lat"]
            min_lon, max_lon = bbox["min_lon"], bbox["max_lon"]
            # Allow a buffer (0.15 deg ~ 16 km) around country/region bounds
            if (min_lat - 0.15) <= lat <= (max_lat + 0.15) and (min_lon - 0.15) <= lon <= (max_lon + 0.15):
                matched.append((Path(reg["database_path"]), reg))

    if matched:
        # If multiple databases cover the point, prefer the more comprehensive transit network (more stops),
        # with secondary tie-breaker distance to bounding box center.
        def _rank(item):
            b = item[1].get("bbox")
            c_lat = (b["min_lat"] + b["max_lat"]) / 2.0
            c_lon = (b["min_lon"] + b["max_lon"]) / 2.0
            dist_sq = (lat - c_lat) ** 2 + (lon - c_lon) ** 2
            stop_count = item[1].get("stats", {}).get("stops", 0)
            return (-stop_count, dist_sq)
        return min(matched, key=_rank)

    # If no database strictly contains (lat, lon), return the closest database by center
    valid_with_bbox = [(Path(r["database_path"]), r) for r in regions if r.get("bbox")]
    if valid_with_bbox:
        def _dist_to_center(item):
            b = item[1].get("bbox")
            c_lat = (b["min_lat"] + b["max_lat"]) / 2.0
            c_lon = (b["min_lon"] + b["max_lon"]) / 2.0
            return (lat - c_lat) ** 2 + (lon - c_lon) ** 2
        return min(valid_with_bbox, key=_dist_to_center)

    return None


def _reverse_geocode(lat: float, lon: float) -> dict:
    """
    Reverse geocode (lat, lon) dynamically using OpenStreetMap Nominatim.
    Caches responses to avoid redundant lookups. Gracefully falls back if offline.
    """
    key = f"{round(lat, 3)}:{round(lon, 3)}"
    if key in _GEOCODE_CACHE:
        return _GEOCODE_CACHE[key]

    res = {
        "city": None,
        "state": None,
        "country": None,
        "country_code": None,
        "display_name": None,
    }
    try:
        url = f"https://nominatim.openstreetmap.org/reverse?lat={lat}&lon={lon}&format=jsonv2"
        with httpx.Client(timeout=2.5, headers={"User-Agent": "GTFSHierarchicalLocator/1.0"}) as client:
            resp = client.get(url)
            if resp.status_code == 200:
                data = resp.json()
                addr = data.get("address", {})
                res["city"] = (
                    addr.get("city")
                    or addr.get("town")
                    or addr.get("municipality")
                    or addr.get("village")
                    or addr.get("city_district")
                    or addr.get("county")
                )
                res["state"] = addr.get("state") or addr.get("province") or addr.get("region")
                res["country"] = addr.get("country")
                res["country_code"] = addr.get("country_code")
                res["display_name"] = data.get("display_name")
    except Exception:
        pass

    _GEOCODE_CACHE[key] = res
    return res


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate great-circle distance between two points in km."""
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2
         + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c


class _SpatialGrid:
    """In-memory spatial grid index for ultra-fast stop radius queries."""
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.grid: dict[tuple[int, int], list[dict]] = {}
        self.parent_stations: dict[str, dict] = {}
        self.mtime: Optional[float] = None
        self._build()

    def _build(self):
        self.mtime = self.db_path.stat().st_mtime
        self.grid.clear()
        self.parent_stations.clear()
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cols = {r[1] for r in cur.execute("PRAGMA table_info(stops)").fetchall()}
        has_parent = "parent_station" in cols
        has_loc_type = "location_type" in cols
        has_code = "stop_code" in cols

        query = "SELECT stop_id, stop_name, CAST(stop_lat AS REAL) as lat, CAST(stop_lon AS REAL) as lon"
        if has_code:
            query += ", stop_code"
        if has_parent:
            query += ", parent_station"
        if has_loc_type:
            query += ", location_type"
        query += " FROM stops"

        for row in cur.execute(query).fetchall():
            s = dict(row)
            lat = s["lat"]
            lon = s["lon"]
            if lat == 0.0 and lon == 0.0:
                continue
            if has_loc_type and str(s.get("location_type")) == "1":
                self.parent_stations[s["stop_id"]] = s
            cell = (int(lat / 0.01), int(lon / 0.01))
            self.grid.setdefault(cell, []).append(s)
        conn.close()

    def query(self, lat: float, lon: float, radius_km: float) -> list[dict]:
        if self.db_path.stat().st_mtime != self.mtime:
            self._build()
        delta_lat = radius_km / 111.0
        cos_lat = math.cos(math.radians(lat))
        delta_lon = radius_km / (111.0 * max(0.01, cos_lat))
        cell_delta_lat = int(math.ceil(delta_lat / 0.01))
        cell_delta_lon = int(math.ceil(delta_lon / 0.01))
        c_lat = int(lat / 0.01)
        c_lon = int(lon / 0.01)

        candidates = []
        for d_lat in range(-cell_delta_lat, cell_delta_lat + 1):
            for d_lon in range(-cell_delta_lon, cell_delta_lon + 1):
                cell = (c_lat + d_lat, c_lon + d_lon)
                if cell in self.grid:
                    candidates.extend(self.grid[cell])

        res = []
        for s in candidates:
            d = _haversine_km(lat, lon, s["lat"], s["lon"])
            if d <= radius_km:
                item = dict(s)
                item["distance_m"] = round(d * 1000)
                item["distance_km"] = round(d, 3)
                res.append(item)
        res.sort(key=lambda x: x["distance_m"])
        return res

    def query_closest_nationwide(self, lat: float, lon: float, top_n: int = 100) -> list[dict]:
        if self.db_path.stat().st_mtime != self.mtime:
            self._build()
        all_stops = []
        for cell_stops in self.grid.values():
            all_stops.extend(cell_stops)
        if not all_stops:
            return []
        all_stops.sort(key=lambda s: (lat - s["lat"]) ** 2 + (lon - s["lon"]) ** 2)
        candidates = []
        for s in all_stops[:top_n]:
            d = _haversine_km(lat, lon, s["lat"], s["lon"])
            item = dict(s)
            item["distance_m"] = round(d * 1000)
            item["distance_km"] = round(d, 3)
            candidates.append(item)
        candidates.sort(key=lambda x: x["distance_m"])
        return candidates


_SPATIAL_GRIDS: dict[str, _SpatialGrid] = {}


def _get_spatial_grid(db_path: Path) -> _SpatialGrid:
    key = str(db_path.resolve())
    if key not in _SPATIAL_GRIDS:
        _SPATIAL_GRIDS[key] = _SpatialGrid(db_path)
    return _SPATIAL_GRIDS[key]


# ── Agencies ─────────────────────────────────────────────────────────────────

_AGENCY_SELECT = """
    SELECT a.agency_id, a.agency_name, a.agency_url, a.agency_timezone
"""


def _agency_columns(conn: sqlite3.Connection) -> set:
    """Column names present in the agency table (feeds vary in optional ones)."""
    return {r[1] for r in conn.execute("PRAGMA table_info(agency)")}


def _agency_rows(conn: sqlite3.Connection, agency_id: Optional[str] = None) -> list:
    """Return agency rows, optionally for a single agency_id."""
    cols = _agency_columns(conn)
    optional = [c for c in ("agency_lang", "agency_phone", "agency_email",
                            "agency_fare_url") if c in cols]
    select = ("SELECT agency_id, agency_name, agency_url, agency_timezone"
              + "".join(f", {c}" for c in optional)
              + " FROM agency")
    params: list = []
    if agency_id is not None:
        select += " WHERE agency_id = ?"
        params.append(agency_id)
    select += " ORDER BY agency_name"
    return conn.execute(select, params).fetchall()


def _agency_payload(
    conn: sqlite3.Connection,
    row,
    include_counts: bool = True,
    route_counts: Optional[dict] = None,
    stop_counts: Optional[dict] = None,
) -> dict:
    """Build one agency object, including route/stop counts."""
    d = dict(row)
    for col in ("agency_lang", "agency_phone", "agency_email", "agency_fare_url"):
        d.setdefault(col, None)

    if not include_counts:
        d["route_count"] = None
        d["stop_count"] = None
        return d

    agency_id = d["agency_id"]
    if route_counts is not None and agency_id in route_counts:
        d["route_count"] = route_counts[agency_id]
    else:
        d["route_count"] = conn.execute(
            "SELECT COUNT(*) FROM routes WHERE agency_id = ?", (agency_id,)
        ).fetchone()[0]

    if stop_counts is not None and agency_id in stop_counts:
        d["stop_count"] = stop_counts[agency_id]
    else:
        d["stop_count"] = conn.execute(
            """
            SELECT COUNT(DISTINCT st.stop_id)
            FROM stop_times st
            JOIN trips  t ON st.trip_id  = t.trip_id
            JOIN routes r ON t.route_id  = r.route_id
            WHERE r.agency_id = ?
            """,
            (agency_id,),
        ).fetchone()[0]
    return d


# ── Region Endpoints ─────────────────────────────────────────────────────────

@app.get("/api/regions/list")
def list_regions_simple():
    """
    List only the available regions with their unique identifier, name, and database.
    Use 'id' or 'name' as the 'region' parameter in route API calls to query a specific database.
    """
    regions = _get_all_regions_info()
    items = [
        {
            "id": r["id"],
            "name": r["name"],
            "database": r["database"],
            "transit_modes": r.get("transit_modes", []),
        }
        for r in regions
    ]
    return {
        "total": len(items),
        "regions": items,
    }


@app.get("/api/regions")
@app.get("/api/regoins")
def list_regions(refresh: bool = Query(default=False, description="Force refresh database cache")):
    """List all available GTFS SQLite databases and their covered regions."""
    if hasattr(refresh, "default"):
        refresh = False
    regions = _get_all_regions_info(force_refresh=refresh)
    return {
        "total": len(regions),
        "regions": regions,
    }


@app.get("/api/{region}/agency")
@app.get("/api/{region}/agencies")
def region_agencies(
    region: str,
    agency_id: Optional[str] = Query(default=None, description="Optional agency_id to filter"),
    include_counts: bool = Query(default=True, description="Whether to include route and stop counts per agency"),
    lang: Optional[str] = _LANG_QUERY,
):
    """List agencies for the specified region or SQLite database."""
    if hasattr(agency_id, "default"):
        agency_id = None
    if hasattr(include_counts, "default"):
        include_counts = True
    if hasattr(lang, "default"):
        lang = None

    db_path, reg_info = _resolve_region_db(region)
    with _open_db(db_path) as conn:
        rows = _agency_rows(conn, agency_id=agency_id)
        if agency_id is not None and not rows:
            raise HTTPException(
                status_code=404,
                detail=f"agency_id '{agency_id}' not found in region '{region}'",
            )

        # Cached or batch-computed agency counts
        route_counts = None
        stop_counts = None
        if include_counts:
            mtime = db_path.stat().st_mtime
            cache_key = f"{db_path.resolve()}:{mtime}"
            if cache_key in _AGENCY_COUNTS_CACHE:
                route_counts, stop_counts = _AGENCY_COUNTS_CACHE[cache_key]
            else:
                try:
                    route_counts = {
                        r[0]: r[1]
                        for r in conn.execute("SELECT agency_id, COUNT(*) FROM routes GROUP BY agency_id").fetchall()
                    }
                    if len(rows) == 1:
                        aid = rows[0]["agency_id"]
                        sc = conn.execute(
                            """
                            SELECT COUNT(DISTINCT st.stop_id)
                            FROM stop_times st
                            JOIN trips  t ON st.trip_id  = t.trip_id
                            JOIN routes r ON t.route_id  = r.route_id
                            WHERE r.agency_id = ?
                            """,
                            (aid,),
                        ).fetchone()[0]
                        stop_counts = {aid: sc}
                    else:
                        stop_counts = {
                            r[0]: r[1]
                            for r in conn.execute(
                                """
                                SELECT r.agency_id, COUNT(DISTINCT st.stop_id)
                                FROM stop_times st
                                JOIN trips  t ON st.trip_id  = t.trip_id
                                JOIN routes r ON t.route_id  = r.route_id
                                GROUP BY r.agency_id
                                """
                            ).fetchall()
                        }
                    _AGENCY_COUNTS_CACHE[cache_key] = (route_counts, stop_counts)
                except Exception:
                    pass

        result = [
            _agency_payload(
                conn,
                r,
                include_counts=include_counts,
                route_counts=route_counts,
                stop_counts=stop_counts,
            )
            for r in rows
        ]
        agency_tr = _get_agency_translations(conn, lang) if lang else {}
        if agency_tr:
            for a in result:
                name = agency_tr.get(a["agency_id"])
                if name:
                    a["agency_name"] = name

        if agency_id is not None:
            return result[0]

        return {
            "region": reg_info["id"],
            "region_name": reg_info["name"],
            "database": reg_info["database"],
            "total": len(result),
            "agencies": result,
        }


@app.get("/api/{region}/agency/{agency_id}")
def region_agency_detail(
    region: str,
    agency_id: str,
    include_counts: bool = Query(default=True, description="Whether to include route and stop counts"),
    lang: Optional[str] = _LANG_QUERY,
):
    """Return a single agency by agency_id for the specified region."""
    return region_agencies(
        region=region,
        agency_id=agency_id,
        include_counts=include_counts,
        lang=lang,
    )


# ── Nearby & Spatial Endpoints ──────────────────────────────────────────────

def _query_active_stops_for_candidates(
    db_path: Path,
    candidates: list[dict],
    grid: "_SpatialGrid",
    limit: int = 20,
    route_type: Optional[int] = None,
    group_parent: bool = True,
    require_active: bool = True,
) -> list[dict]:
    """
    Given spatial stop candidates, resolve parent stations, query scheduled routes
    from GTFS, and filter strictly to active stops (having routes).
    """
    if not candidates:
        return []

    groups: dict[str, dict] = {}
    for s in candidates:
        p_id = s.get("parent_station") if group_parent else None
        key = p_id if (p_id and str(p_id).strip()) else s["stop_id"]
        if key not in groups:
            groups[key] = {
                "stop_id": s["stop_id"],
                "stop_name": s["stop_name"],
                "lat": s["lat"],
                "lon": s["lon"],
                "distance_m": s["distance_m"],
                "distance_km": s.get("distance_km", round(s["distance_m"] / 1000.0, 3)),
                "parent_station": p_id if p_id else None,
                "platforms": [s["stop_id"]],
            }
        else:
            groups[key]["platforms"].append(s["stop_id"])
            if s["distance_m"] < groups[key]["distance_m"]:
                groups[key]["distance_m"] = s["distance_m"]
                groups[key]["distance_km"] = s.get("distance_km", round(s["distance_m"] / 1000.0, 3))

    with _open_db(db_path) as conn:
        parent_keys = [k for k in groups.keys() if k.startswith("stoparea:") or k in grid.parent_stations]
        if parent_keys:
            ph = ",".join("?" * len(parent_keys))
            for pr in conn.execute(
                f"SELECT stop_id, stop_name, CAST(stop_lat AS REAL) as lat, CAST(stop_lon AS REAL) as lon FROM stops WHERE stop_id IN ({ph})",
                parent_keys,
            ).fetchall():
                sid = pr["stop_id"]
                if sid in groups:
                    groups[sid]["stop_name"] = pr["stop_name"]
                    if pr["lat"] and pr["lon"]:
                        groups[sid]["lat"] = pr["lat"]
                        groups[sid]["lon"] = pr["lon"]

        sorted_groups = sorted(groups.values(), key=lambda g: g["distance_m"])
        top_groups = sorted_groups[:limit * 4]
        all_platforms = []
        for g in top_groups:
            all_platforms.extend(g["platforms"])

        if not all_platforms:
            return []

        route_filter_sql = ""
        route_params: list = list(all_platforms)
        if route_type is not None:
            route_filter_sql = " AND CAST(r.route_type AS INTEGER) = ?"
            route_params.append(route_type)

        ph_plat = ",".join("?" * len(all_platforms))
        routes_query = f"""
            SELECT DISTINCT st.stop_id, r.route_id, r.route_short_name, r.route_long_name,
                   r.route_type, r.route_color, t.trip_headsign
            FROM stop_times st
            JOIN trips t ON st.trip_id = t.trip_id
            JOIN routes r ON t.route_id = r.route_id
            WHERE st.stop_id IN ({ph_plat}){route_filter_sql}
        """
        stop_routes_map: dict[str, list] = {}
        for r in conn.execute(routes_query, route_params).fetchall():
            sid = r["stop_id"]
            color = r["route_color"]
            if color:
                color = f"#{color}" if not str(color).startswith("#") else color
            else:
                color = None
            stop_routes_map.setdefault(sid, []).append({
                "route_id": r["route_id"],
                "short_name": r["route_short_name"] or r["route_long_name"] or r["route_id"],
                "route_type": int(r["route_type"]),
                "color": color,
                "headsign": r["trip_headsign"] or "",
            })

    results = []
    for g in top_groups:
        # Group and deduplicate by unique route_id (avoiding duplicate lines per headsign)
        routes_map: dict[str, dict] = {}
        for pid in g["platforms"]:
            for r in stop_routes_map.get(pid, []):
                rid = r["route_id"]
                if rid not in routes_map:
                    routes_map[rid] = {
                        "route_id": r["route_id"],
                        "short_name": r["short_name"],
                        "route_type": r["route_type"],
                        "color": r["color"],
                        "headsign": r["headsign"],
                        "headsigns": [r["headsign"]] if r.get("headsign") else [],
                    }
                else:
                    hs = r.get("headsign")
                    if hs and hs not in routes_map[rid]["headsigns"]:
                        routes_map[rid]["headsigns"].append(hs)
        routes = list(routes_map.values())
        if require_active and not routes:
            continue
        results.append({
            "stop_id": g["stop_id"],
            "stop_name": g["stop_name"],
            "lat": g["lat"],
            "lon": g["lon"],
            "distance_m": g["distance_m"],
            "distance_km": g.get("distance_km", round(g["distance_m"] / 1000.0, 3)),
            "parent_station": g["parent_station"],
            "active": len(routes) > 0,
            "route_count": len(routes),
            "routes": routes,
        })
        if len(results) >= limit:
            break

    return results


def _search_active_stops_hierarchy(
    lat: float,
    lon: float,
    radius: float = 200.0,
    limit: int = 20,
    route_type: Optional[int] = None,
    group_parent: bool = True,
    region: Optional[str] = None,
) -> dict:
    """
    Search active stops with automatic hierarchical cascading:
      Level 1: 500m radius
      Level 2: City (15km radius + reverse-geocoded city name)
      Level 3: State (60km radius + reverse-geocoded state name)
      Level 4: Country (Nationwide database closest active stops)
    """
    found = _find_db_for_coords(lat, lon, region=region)
    if not found:
        return {
            "success": False,
            "level": "none",
            "searched_levels": ["radius_500m", "city", "state", "country"],
            "location": {"lat": lat, "lon": lon},
            "error": "No transit database found covering these coordinates.",
            "database": None,
            "geo": _reverse_geocode(lat, lon),
            "count": 0,
            "stops": [],
        }

    db_path, reg_info = found
    grid = _get_spatial_grid(db_path)
    geo = _reverse_geocode(lat, lon)
    searched_levels = []

    # ── Level 1: 500m Radius ──────────────────────────────────────────────────
    searched_levels.append("radius_500m")
    radius_km = radius / 1000.0
    cand_500m = grid.query(lat, lon, radius_km)
    stops_500m = _query_active_stops_for_candidates(
        db_path, cand_500m, grid=grid, limit=limit, route_type=route_type, group_parent=group_parent, require_active=True
    )
    if stops_500m:
        return {
            "success": True,
            "level": "radius_500m",
            "searched_levels": searched_levels,
            "search_radius_meters": radius,
            "location": {"lat": lat, "lon": lon},
            "geo": geo,
            "database": db_path.name,
            "count": len(stops_500m),
            "stops": stops_500m,
        }

    # ── Level 2: City (15 km) ────────────────────────────────────────────────
    searched_levels.append("city")
    cand_city = grid.query(lat, lon, DEFAULT_CITY_RADIUS / 1000.0)
    stops_city = _query_active_stops_for_candidates(
        db_path, cand_city, grid=grid, limit=limit, route_type=route_type, group_parent=group_parent, require_active=True
    )
    if stops_city:
        return {
            "success": True,
            "level": "city",
            "searched_levels": searched_levels,
            "search_radius_meters": DEFAULT_CITY_RADIUS,
            "location": {"lat": lat, "lon": lon},
            "geo": geo,
            "resolved_city": geo.get("city"),
            "database": db_path.name,
            "count": len(stops_city),
            "stops": stops_city,
        }

    # ── Level 3: State (60 km) ───────────────────────────────────────────────
    searched_levels.append("state")
    cand_state = grid.query(lat, lon, DEFAULT_STATE_RADIUS / 1000.0)
    stops_state = _query_active_stops_for_candidates(
        db_path, cand_state, grid=grid, limit=limit, route_type=route_type, group_parent=group_parent, require_active=True
    )
    if stops_state:
        return {
            "success": True,
            "level": "state",
            "searched_levels": searched_levels,
            "search_radius_meters": DEFAULT_STATE_RADIUS,
            "location": {"lat": lat, "lon": lon},
            "geo": geo,
            "resolved_state": geo.get("state"),
            "database": db_path.name,
            "count": len(stops_state),
            "stops": stops_state,
        }

    # ── Level 4: Country (Nationwide) ─────────────────────────────────────────
    searched_levels.append("country")
    cand_country = grid.query_closest_nationwide(lat, lon, top_n=100)
    stops_country = _query_active_stops_for_candidates(
        db_path, cand_country, grid=grid, limit=limit, route_type=route_type, group_parent=group_parent, require_active=True
    )
    if stops_country:
        return {
            "success": True,
            "level": "country",
            "searched_levels": searched_levels,
            "search_radius_meters": None,
            "location": {"lat": lat, "lon": lon},
            "geo": geo,
            "resolved_country": geo.get("country") or reg_info.get("name"),
            "database": db_path.name,
            "count": len(stops_country),
            "stops": stops_country,
        }

    # ── Level 5: None ────────────────────────────────────────────────────────
    return {
        "success": False,
        "level": "none",
        "searched_levels": searched_levels,
        "location": {"lat": lat, "lon": lon},
        "geo": geo,
        "database": db_path.name,
        "count": 0,
        "stops": [],
    }


ROUTE_TYPE_NAMES = {
    0: "tram",
    1: "subway",
    2: "rail",
    3: "bus",
    4: "ferry",
    5: "cable_car",
    6: "gondola",
    7: "funicular",
    11: "trolleybus",
    12: "monorail",
}


def _query_active_routes_for_candidates(
    db_path: Path,
    candidates: list[dict],
    limit: int = 50,
    route_type: Optional[int] = None,
) -> list[dict]:
    """
    Given spatial stop candidates, query distinct transit routes from GTFS stop_times,
    trips, routes, and agency, and attach closest stop proximity information.
    """
    if not candidates:
        return []

    stop_map = {s["stop_id"]: s for s in candidates}
    platform_ids = list(stop_map.keys())

    with _open_db(db_path) as conn:
        route_filter_sql = ""
        params: list = list(platform_ids)
        if route_type is not None:
            route_filter_sql = " AND CAST(r.route_type AS INTEGER) = ?"
            params.append(route_type)

        ph = ",".join("?" * len(platform_ids))
        query = f"""
            SELECT DISTINCT st.stop_id, r.route_id, r.route_short_name, r.route_long_name,
                   r.route_type, r.route_color, r.agency_id,
                   a.agency_name, t.trip_headsign
            FROM stop_times st
            JOIN trips t ON st.trip_id = t.trip_id
            JOIN routes r ON t.route_id = r.route_id
            LEFT JOIN agency a ON r.agency_id = a.agency_id
            WHERE st.stop_id IN ({ph}){route_filter_sql}
        """
        route_dict: dict[str, dict] = {}
        for row in conn.execute(query, params).fetchall():
            rid = row["route_id"]
            sid = row["stop_id"]
            stop_info = stop_map.get(sid)
            if not stop_info:
                continue
            dist_m = stop_info["distance_m"]
            color = row["route_color"]
            if color:
                color = f"#{color}" if not str(color).startswith("#") else color
            else:
                color = None

            headsign = row["trip_headsign"] or ""

            if rid not in route_dict:
                rtype = int(row["route_type"])
                route_dict[rid] = {
                    "route_id": rid,
                    "short_name": row["route_short_name"] or row["route_long_name"] or rid,
                    "long_name": row["route_long_name"] or "",
                    "route_type": rtype,
                    "route_type_name": ROUTE_TYPE_NAMES.get(rtype, "transit"),
                    "color": color,
                    "agency_id": row["agency_id"],
                    "agency_name": row["agency_name"],
                    "headsign": headsign,
                    "headsigns": [headsign] if headsign else [],
                    "closest_stop": {
                        "stop_id": sid,
                        "stop_name": stop_info["stop_name"],
                        "lat": stop_info["lat"],
                        "lon": stop_info["lon"],
                        "distance_m": dist_m,
                        "distance_km": round(dist_m / 1000.0, 3),
                    },
                }
            else:
                if dist_m < route_dict[rid]["closest_stop"]["distance_m"]:
                    route_dict[rid]["closest_stop"] = {
                        "stop_id": sid,
                        "stop_name": stop_info["stop_name"],
                        "lat": stop_info["lat"],
                        "lon": stop_info["lon"],
                        "distance_m": dist_m,
                        "distance_km": round(dist_m / 1000.0, 3),
                    }
                if headsign and headsign not in route_dict[rid]["headsigns"]:
                    route_dict[rid]["headsigns"].append(headsign)

    results = sorted(route_dict.values(), key=lambda r: r["closest_stop"]["distance_m"])[:limit]
    return results


def _search_active_routes_hierarchy(
    lat: float,
    lon: float,
    radius: float = DEFAULT_NEARBY_RADIUS,
    limit: int = 50,
    route_type: Optional[int] = None,
    region: Optional[str] = None,
) -> dict:
    """
    Search active transit routes with automatic hierarchical cascading:
      Level 1: 500m radius
      Level 2: City (15km radius + reverse-geocoded city name)
      Level 3: State (60km radius + reverse-geocoded state name)
      Level 4: Country (Nationwide database closest active routes)
    """
    found = _find_db_for_coords(lat, lon, region=region)
    if not found:
        return {
            "success": False,
            "level": "none",
            "searched_levels": ["radius_500m", "city", "state", "country"],
            "location": {"lat": lat, "lon": lon},
            "error": "No transit database found covering these coordinates.",
            "database": None,
            "geo": _reverse_geocode(lat, lon),
            "count": 0,
            "routes": [],
        }

    db_path, reg_info = found
    grid = _get_spatial_grid(db_path)
    geo = _reverse_geocode(lat, lon)
    searched_levels = []

    # ── Level 1: 500m Radius ──────────────────────────────────────────────────
    searched_levels.append("radius_500m")
    radius_km = radius / 1000.0
    cand_500m = grid.query(lat, lon, radius_km)
    routes_500m = _query_active_routes_for_candidates(
        db_path, cand_500m, limit=limit, route_type=route_type
    )
    if routes_500m:
        return {
            "success": True,
            "level": "radius_500m",
            "searched_levels": searched_levels,
            "search_radius_meters": radius,
            "location": {"lat": lat, "lon": lon},
            "geo": geo,
            "database": db_path.name,
            "count": len(routes_500m),
            "routes": routes_500m,
        }

    # ── Level 2: City (15 km) ────────────────────────────────────────────────
    searched_levels.append("city")
    cand_city = grid.query(lat, lon, DEFAULT_CITY_RADIUS / 1000.0)
    routes_city = _query_active_routes_for_candidates(
        db_path, cand_city, limit=limit, route_type=route_type
    )
    if routes_city:
        return {
            "success": True,
            "level": "city",
            "searched_levels": searched_levels,
            "search_radius_meters": DEFAULT_CITY_RADIUS,
            "location": {"lat": lat, "lon": lon},
            "geo": geo,
            "resolved_city": geo.get("city"),
            "database": db_path.name,
            "count": len(routes_city),
            "routes": routes_city,
        }

    # ── Level 3: State (60 km) ───────────────────────────────────────────────
    searched_levels.append("state")
    cand_state = grid.query(lat, lon, DEFAULT_STATE_RADIUS / 1000.0)
    routes_state = _query_active_routes_for_candidates(
        db_path, cand_state, limit=limit, route_type=route_type
    )
    if routes_state:
        return {
            "success": True,
            "level": "state",
            "searched_levels": searched_levels,
            "search_radius_meters": DEFAULT_STATE_RADIUS,
            "location": {"lat": lat, "lon": lon},
            "geo": geo,
            "resolved_state": geo.get("state"),
            "database": db_path.name,
            "count": len(routes_state),
            "routes": routes_state,
        }

    # ── Level 4: Country (Nationwide) ─────────────────────────────────────────
    searched_levels.append("country")
    cand_country = grid.query_closest_nationwide(lat, lon, top_n=200)
    routes_country = _query_active_routes_for_candidates(
        db_path, cand_country, limit=limit, route_type=route_type
    )
    if routes_country:
        return {
            "success": True,
            "level": "country",
            "searched_levels": searched_levels,
            "search_radius_meters": None,
            "location": {"lat": lat, "lon": lon},
            "geo": geo,
            "resolved_country": geo.get("country") or reg_info.get("name"),
            "database": db_path.name,
            "count": len(routes_country),
            "routes": routes_country,
        }

    # ── Level 5: None ────────────────────────────────────────────────────────
    return {
        "success": False,
        "level": "none",
        "searched_levels": searched_levels,
        "location": {"lat": lat, "lon": lon},
        "geo": geo,
        "database": db_path.name,
        "count": 0,
        "routes": [],
    }


def _safe_header_value(val: Any) -> str:
    """Safely format string for HTTP header (RFC 7230 / latin-1). URL-quotes non-latin-1 characters."""
    if val is None:
        return ""
    s = str(val).strip()
    try:
        s.encode("latin-1")
        return s
    except UnicodeEncodeError:
        import urllib.parse
        return urllib.parse.quote(s)


def _apply_search_headers(response: Optional[Response], res: Dict[str, Any]) -> None:
    if not response or not isinstance(res, dict):
        return
    if "level" in res:
        response.headers["X-Search-Level"] = str(res.get("level", "none"))
    geo = res.get("geo") or {}
    if geo.get("city"):
        response.headers["X-Resolved-City"] = _safe_header_value(geo["city"])
    if geo.get("country"):
        response.headers["X-Resolved-Country"] = _safe_header_value(geo["country"])
    if res.get("database"):
        response.headers["X-Resolved-Database"] = _safe_header_value(res["database"])
    response.headers["Cache-Control"] = "public, max-age=60"


@app.get("/api/routes/search-hierarchy")
@app.get("/api/routes/nearby-hierarchy")
def routes_search_hierarchy(
    lat: float = Query(..., description="Latitude"),
    lon: float = Query(..., description="Longitude"),
    radius: float = Query(default=DEFAULT_NEARBY_RADIUS, description="Initial radius in meters (from env NEARBY_RADIUS_METERS)"),
    limit: int = Query(default=50, ge=1, le=100, description="Max routes to return"),
    route_type: Optional[int] = Query(default=None, description="Optional GTFS route_type filter"),
    region: Optional[str] = Query(default=None, description="Unique identifier (e.g. 'netherlands-gtfs') or name of the region/database"),
    region_id: Optional[str] = Query(default=None, description="Alias for region identifier"),
    response: Response = None,
):
    """
    Search active routes starting with 500m radius.
    If no active routes are found, automatically cascades to:
      1. Radius (500m)
      2. City (15km / reverse-geocoded city boundary)
      3. State (60km / reverse-geocoded state boundary)
      4. Country (Nationwide database closest active routes)
    Returns distinct routes ordered by distance to user location.
    If 'region' or 'region_id' is provided, only that specific database is queried.
    """
    selected_region = region_id or region
    res = _search_active_routes_hierarchy(
        lat=lat,
        lon=lon,
        radius=radius,
        limit=limit,
        route_type=route_type,
        region=selected_region,
    )
    _apply_search_headers(response, res)
    return res


@app.get("/api/stops/search-hierarchy")
@app.get("/api/stops/nearby-hierarchy")
def stops_search_hierarchy(
    lat: float = Query(..., description="Latitude"),
    lon: float = Query(..., description="Longitude"),
    radius: float = Query(default=DEFAULT_NEARBY_RADIUS, description="Initial radius in meters (from env NEARBY_RADIUS_METERS)"),
    limit: int = Query(default=50, ge=1, le=100, description="Max items to return"),
    route_type: Optional[int] = Query(default=None, description="Optional GTFS route_type filter"),
    group_parent: bool = Query(default=True, description="Group platforms under parent_station"),
    region: Optional[str] = Query(default=None, description="Optional region / database override"),
    region_id: Optional[str] = Query(default=None, description="Alias for region identifier"),
    type: str = Query(default="routes", description="What to display in hierarchy: 'routes' (default) or 'stops'"),
    response: Response = None,
):
    """
    Hierarchical active search starting with 500m radius, cascading to city -> state -> country.
    By default displays the routes present in the hierarchy; set type='stops' to view stops.
    """
    if type == "stops":
        res = _search_active_stops_hierarchy(
            lat=lat,
            lon=lon,
            radius=radius,
            limit=limit,
            route_type=route_type,
            group_parent=group_parent,
            region=region_id or region,
        )
    else:
        res = _search_active_routes_hierarchy(
            lat=lat,
            lon=lon,
            radius=radius,
            limit=limit,
            route_type=route_type,
            region=region_id or region,
        )
    _apply_search_headers(response, res)
    return res


@app.get("/api/stops/nearby")
def stops_nearby(
    lat: float = Query(..., description="Latitude"),
    lon: float = Query(..., description="Longitude"),
    radius: float = Query(default=DEFAULT_NEARBY_RADIUS, description="Radius in meters (from env NEARBY_RADIUS_METERS)"),
    limit: int = Query(default=20, ge=1, le=100, description="Max stops to return"),
    route_type: Optional[int] = Query(default=None, description="Optional GTFS route_type filter (0=tram, 1=subway, 2=rail, 3=bus, 4=ferry)"),
    group_parent: bool = Query(default=True, description="Group platforms under parent_station"),
    region: Optional[str] = Query(default=None, description="Optional region / database"),
    hierarchy: bool = Query(default=False, description="Return full hierarchical envelope with geo & level metadata"),
    fallback: bool = Query(default=True, description="If no active stops within 500m, cascade to city, state, country"),
    response: Response = None,
):
    """
    Return active stops near (lat, lon) sorted by distance, with routes serving each stop.
    If fallback=True and no active stops are found within 500m, automatically searches
    city, then state, then country.
    """
    if hasattr(radius, "default"):
        radius = DEFAULT_NEARBY_RADIUS
    if hasattr(limit, "default"):
        limit = 20
    if hasattr(route_type, "default"):
        route_type = None
    if hasattr(group_parent, "default"):
        group_parent = True
    if hasattr(region, "default"):
        region = None
    if hasattr(hierarchy, "default"):
        hierarchy = False
    if hasattr(fallback, "default"):
        fallback = True

    if fallback:
        res = _search_active_stops_hierarchy(
            lat=lat,
            lon=lon,
            radius=radius,
            limit=limit,
            route_type=route_type,
            group_parent=group_parent,
            region=region_id or region,
        )
    else:
        # Non-cascading single radius query
        found = _find_db_for_coords(lat, lon, region=region)
        if not found:
            if response:
                response.headers["X-Coverage"] = "false"
            return {"success": False, "level": "none", "stops": []} if hierarchy else []
        db_path, _ = found
        grid = _get_spatial_grid(db_path)
        cand = grid.query(lat, lon, radius / 1000.0)
        stops = _query_active_stops_for_candidates(
            db_path, cand, grid=grid, limit=limit, route_type=route_type, group_parent=group_parent, require_active=True
        )
        res = {
            "success": len(stops) > 0,
            "level": "radius_500m" if stops else "none",
            "searched_levels": ["radius_500m"],
            "search_radius_meters": radius,
            "location": {"lat": lat, "lon": lon},
            "geo": _reverse_geocode(lat, lon),
            "database": db_path.name,
            "count": len(stops),
            "stops": stops,
        }

    _apply_search_headers(response, res)

    if hierarchy:
        return res
    return res["stops"]


@app.get("/api/routes/nearby")
def routes_nearby(
    lat: float = Query(..., description="Latitude"),
    lon: float = Query(..., description="Longitude"),
    radius: float = Query(default=DEFAULT_NEARBY_RADIUS, description="Radius in meters (from env NEARBY_RADIUS_METERS)"),
    limit: int = Query(default=30, ge=1, le=100, description="Max routes to return"),
    route_type: Optional[int] = Query(default=None, description="Optional GTFS route_type filter"),
    region: Optional[str] = Query(default=None, description="Unique identifier or name of the region/database"),
    region_id: Optional[str] = Query(default=None, description="Alias for region identifier"),
    hierarchy: bool = Query(default=False, description="Return full hierarchical envelope with geo & level metadata"),
    fallback: bool = Query(default=True, description="If no active routes within radius, cascade to city, state, country"),
    response: Response = None,
):
    """
    Return distinct transit routes passing near (lat, lon) sorted by distance.
    If fallback=True (default) and no active routes are found within radius (e.g. 500m),
    it automatically cascades through city -> state -> country level.
    """
    if hasattr(radius, "default"):
        radius = DEFAULT_NEARBY_RADIUS
    if hasattr(limit, "default"):
        limit = 30
    if hasattr(route_type, "default"):
        route_type = None
    if hasattr(region, "default"):
        region = None
    if hasattr(hierarchy, "default"):
        hierarchy = False
    if hasattr(fallback, "default"):
        fallback = True

    if fallback:
        res = _search_active_routes_hierarchy(
            lat=lat,
            lon=lon,
            radius=radius,
            limit=limit,
            route_type=route_type,
            region=region_id or region,
        )
    else:
        found = _find_db_for_coords(lat, lon, region=region)
        if not found:
            if response:
                response.headers["X-Coverage"] = "false"
            return {"success": False, "level": "none", "routes": []} if hierarchy else []
        db_path, _ = found
        grid = _get_spatial_grid(db_path)
        cand = grid.query(lat, lon, radius / 1000.0)
        routes = _query_active_routes_for_candidates(
            db_path, cand, limit=limit, route_type=route_type
        )
        res = {
            "success": len(routes) > 0,
            "level": "radius_500m" if routes else "none",
            "searched_levels": ["radius_500m"],
            "search_radius_meters": radius,
            "location": {"lat": lat, "lon": lon},
            "geo": _reverse_geocode(lat, lon),
            "database": db_path.name,
            "count": len(routes),
            "routes": routes,
        }

    _apply_search_headers(response, res)

    if hierarchy:
        return res
    return res["routes"]


@app.get("/api/departures/nearby")
def departures_nearby(
    lat: float = Query(..., description="Latitude"),
    lon: float = Query(..., description="Longitude"),
    radius: float = Query(default=200.0, description="Radius in meters (default 500)"),
    at: Optional[str] = Query(default=None, description="ISO datetime or time (defaults to local time at the stop)"),
    limit: int = Query(default=3, ge=1, le=20, description="Departures per route and direction"),
    route_type: Optional[int] = Query(default=None, description="Optional GTFS route_type filter"),
    region: Optional[str] = Query(default=None, description="Optional region / database"),
    response: Response = None,
):
    """
    Return the next N live departures per route and direction from nearby stops.
    Uses the stop's local timezone.
    """
    if hasattr(radius, "default"):
        radius = 200.0
    if hasattr(at, "default"):
        at = None
    if hasattr(limit, "default"):
        limit = 3
    if hasattr(route_type, "default"):
        route_type = None
    if hasattr(region, "default"):
        region = None

    found = _find_db_for_coords(lat, lon, region=region)
    if not found:
        if response:
            response.headers["X-Coverage"] = "false"
            response.headers["Cache-Control"] = "no-cache"
        return {"coverage": False, "departures": []}

    db_path, reg_info = found
    grid = _get_spatial_grid(db_path)
    candidates = grid.query(lat, lon, radius / 1000.0)
    if not candidates:
        if response:
            response.headers["Cache-Control"] = "public, max-age=30"
        return {"coverage": True, "departures": []}

    stop_map = {s["stop_id"]: s for s in candidates}
    platform_ids = list(stop_map.keys())

    with _open_db(db_path) as conn:
        tz = _get_db_timezone(conn)

        # Parse `at` in the local timezone
        if at is None or not str(at).strip() or str(at).strip().lower() == "now":
            local_now = datetime.now(tz)
            on_date = local_now.date()
            from_secs = local_now.hour * 3600 + local_now.minute * 60 + local_now.second
        else:
            at_clean = str(at).strip()
            parsed = None
            date_only = False
            for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M", "%Y-%m-%d", "%H:%M:%S", "%H:%M"):
                try:
                    parsed = datetime.strptime(at_clean, fmt)
                    date_only = (fmt == "%Y-%m-%d")
                    break
                except ValueError:
                    pass
            if parsed is None:
                raise HTTPException(
                    status_code=422,
                    detail="'at' must be a date (YYYY-MM-DD), ISO datetime (YYYY-MM-DDTHH:MM), or time (HH:MM)",
                )
            local_now = datetime.now(tz)
            on_date = parsed.date() if parsed.year != 1900 else local_now.date()
            from_secs = 0 if date_only else (parsed.hour * 3600 + parsed.minute * 60 + parsed.second)

        from_hhmmss = _secs_to_hhmm(from_secs) + ":00"

        active = _active_services(conn, on_date)
        prev_date = on_date - timedelta(days=1)
        active_prev = _active_services(conn, prev_date)
        all_active = active | active_prev
        if not all_active:
            if response:
                response.headers["Cache-Control"] = "public, max-age=30"
            return {
                "coverage": True,
                "timezone": str(tz),
                "date": on_date.isoformat(),
                "from_time": _secs_to_hhmm(from_secs),
                "departures": [],
            }

        route_filter_sql = ""
        params: list = []
        if route_type is not None:
            route_filter_sql = " AND CAST(r.route_type AS INTEGER) = ?"
            params.append(route_type)

        ph_stops = ",".join("?" * len(platform_ids))
        ph_services = ",".join("?" * len(all_active))

        query = f"""
            SELECT st.stop_id, s.stop_name, st.departure_time,
                   r.route_id, r.route_short_name, r.route_long_name, r.route_type, r.route_color,
                   t.trip_id, t.trip_headsign, t.direction_id
            FROM stop_times st
            JOIN stops s ON st.stop_id = s.stop_id
            JOIN trips t ON st.trip_id = t.trip_id
            JOIN routes r ON t.route_id = r.route_id
            WHERE st.stop_id IN ({ph_stops})
              AND t.service_id IN ({ph_services})
              AND st.departure_time >= ?
              {route_filter_sql}
            ORDER BY st.departure_time ASC
            LIMIT 200
        """
        full_params = [*platform_ids, *all_active, from_hhmmss, *params]
        rows = conn.execute(query, full_params).fetchall()

        grouped_deps: dict[tuple, dict] = {}
        for r in rows:
            rid = r["route_id"]
            dir_id = str(r["direction_id"]) if r["direction_id"] is not None else "0"
            headsign = r["trip_headsign"] or ""
            group_key = (rid, dir_id, headsign)

            sid = r["stop_id"]
            stop_info = stop_map.get(sid, {})
            dist_m = stop_info.get("distance_m", 0)

            dep_time_str = r["departure_time"]
            dep_parts = dep_time_str.split(":")
            dep_secs = int(dep_parts[0]) * 3600 + int(dep_parts[1]) * 60 + int(dep_parts[2])
            mins_until = max(0, (dep_secs - from_secs) // 60)

            color = r["route_color"]
            if color:
                color = f"#{color}" if not str(color).startswith("#") else color
            else:
                color = None

            if group_key not in grouped_deps:
                grouped_deps[group_key] = {
                    "route_id": rid,
                    "short_name": r["route_short_name"] or r["route_long_name"] or rid,
                    "long_name": r["route_long_name"] or "",
                    "route_type": int(r["route_type"]),
                    "color": color,
                    "headsign": headsign,
                    "direction_id": dir_id,
                    "stop_id": sid,
                    "stop_name": r["stop_name"],
                    "distance_m": dist_m,
                    "next_departures": [],
                }

            if len(grouped_deps[group_key]["next_departures"]) < limit:
                dep_hour = int(dep_parts[0])
                dep_day_offset = dep_hour // 24
                dep_actual_hour = dep_hour % 24
                dep_dt = datetime(
                    on_date.year, on_date.month, on_date.day,
                    dep_actual_hour, int(dep_parts[1]), int(dep_parts[2]),
                    tzinfo=tz
                ) + timedelta(days=dep_day_offset)

                grouped_deps[group_key]["next_departures"].append({
                    "trip_id": r["trip_id"],
                    "departure_time": dep_time_str,
                    "departure_datetime": dep_dt.isoformat(),
                    "minutes_until": mins_until,
                })

    if response:
        response.headers["Cache-Control"] = "public, max-age=30"

    return {
        "coverage": True,
        "timezone": str(tz),
        "at": on_date.isoformat() + "T" + _secs_to_hhmm(from_secs),
        "departures": list(grouped_deps.values()),
    }


@app.get("/api/stops/bbox")
def stops_bbox(
    min_lat: float = Query(..., description="South bounding latitude"),
    min_lon: float = Query(..., description="West bounding longitude"),
    max_lat: float = Query(..., description="North bounding latitude"),
    max_lon: float = Query(..., description="East bounding longitude"),
    zoom: Optional[float] = Query(default=None, description="Current map zoom level"),
    limit: int = Query(default=200, ge=1, le=2000, description="Max stops to return"),
    region: Optional[str] = Query(default=None, description="Optional region / database"),
    response: Response = None,
):
    """
    Return stops within the visible map bounding box.
    Returns nothing at zoom < 11, only stations at 11 <= zoom < 14, and all stops at zoom >= 14.
    """
    if hasattr(zoom, "default"):
        zoom = None
    if hasattr(limit, "default"):
        limit = 200
    if hasattr(region, "default"):
        region = None

    if zoom is not None and zoom < 11:
        if response:
            response.headers["Cache-Control"] = "public, max-age=3600"
        return []

    center_lat = (min_lat + max_lat) / 2.0
    center_lon = (min_lon + max_lon) / 2.0
    found = _find_db_for_coords(center_lat, center_lon, region=region)
    if not found:
        if response:
            response.headers["X-Coverage"] = "false"
            response.headers["Cache-Control"] = "public, max-age=3600"
        return []

    db_path, reg_info = found
    with _open_db(db_path) as conn:
        cols = {r[1] for r in conn.execute("PRAGMA table_info(stops)").fetchall()}
        has_parent = "parent_station" in cols
        has_loc_type = "location_type" in cols
        has_platform = "platform_code" in cols

        select_cols = [
            "stop_id", "stop_name", "CAST(stop_lat AS REAL) as lat", "CAST(stop_lon AS REAL) as lon"
        ]
        if has_parent:
            select_cols.append("parent_station")
        if has_loc_type:
            select_cols.append("location_type")
        if has_platform:
            select_cols.append("platform_code")

        where_clauses = [
            "CAST(stop_lat AS REAL) BETWEEN ? AND ?",
            "CAST(stop_lon AS REAL) BETWEEN ? AND ?",
        ]

        if zoom is not None and zoom < 14:
            if has_loc_type:
                where_clauses.append("location_type = '1'")
            elif has_parent:
                where_clauses.append("(parent_station IS NULL OR parent_station = '')")

        query = f"SELECT {', '.join(select_cols)} FROM stops WHERE {' AND '.join(where_clauses)} LIMIT ?"
        rows = conn.execute(query, (min_lat, max_lat, min_lon, max_lon, limit)).fetchall()
        result = [dict(r) for r in rows]

    if response:
        response.headers["Cache-Control"] = "public, max-age=3600"
    return result


@app.get("/api/agencies")
def agencies(
    lang: Optional[str] = _LANG_QUERY,
    mode: str = Query(
        default=DEFAULT_MODE,
        description="Which service to query: 'transit' (bus), 'ferry' "
                    "(water) or 'rail'. All are served by this one API; the "
                    "endpoint paths are identical.",
    ),
):
    """List every agency in the database for *mode*, with route/stop counts."""
    with _db(mode) as conn:
        rows = _agency_rows(conn)
        result = [_agency_payload(conn, r) for r in rows]
        agency_tr = _get_agency_translations(conn, lang) if lang else {}
    if agency_tr:
        for a in result:
            name = agency_tr.get(a["agency_id"])
            if name:
                a["agency_name"] = name
    return {"total": len(result), "mode": mode, "agencies": result}


@app.get("/api/agencies/{agency_id}")
def agency_detail(
    agency_id: str,
    lang: Optional[str] = _LANG_QUERY,
    mode: str = Query(
        default=DEFAULT_MODE,
        description="Which service to query: 'transit' (bus), 'ferry' "
                    "(water) or 'rail'. All are served by this one API; the "
                    "endpoint paths are identical.",
    ),
):
    """Return a single agency by agency_id."""
    with _db(mode) as conn:
        rows = _agency_rows(conn, agency_id)
        if not rows:
            raise HTTPException(
                status_code=404,
                detail=f"agency_id '{agency_id}' not found in mode '{mode}'",
            )
        detail = _agency_payload(conn, rows[0])
        agency_tr = _get_agency_translations(conn, lang) if lang else {}
    if agency_tr.get(agency_id):
        detail["agency_name"] = agency_tr[agency_id]
    return detail


@app.get("/api/agencies/{agency_id}/routes")
def agency_routes(
    agency_id: str,
    offset: int = Query(default=0, ge=0, description="Number of routes to skip"),
    limit: int = Query(default=50, ge=1, le=500, description="Max routes to return"),
    lang: Optional[str] = _LANG_QUERY,
    mode: str = Query(
        default=DEFAULT_MODE,
        description="Which service to query: 'transit' (bus), 'ferry' "
                    "(water) or 'rail'. All are served by this one API; the "
                    "endpoint paths are identical.",
    ),
):
    """Paginated routes belonging to *agency_id*."""
    with _db(mode) as conn:
        if not _agency_rows(conn, agency_id):
            raise HTTPException(
                status_code=404,
                detail=f"agency_id '{agency_id}' not found in mode '{mode}'",
            )
        total = conn.execute(
            "SELECT COUNT(*) FROM routes WHERE agency_id = ?", (agency_id,)
        ).fetchone()[0]
        rows = conn.execute(
            """
            SELECT route_id, route_short_name, route_long_name, route_color,
                   route_text_color, route_type, route_url, agency_id
            FROM routes
            WHERE agency_id = ?
            ORDER BY route_short_name
            LIMIT ? OFFSET ?
            """,
            (agency_id, limit, offset),
        ).fetchall()
        route_ids = [r["route_id"] for r in rows]
        headsigns_map: dict = {}
        if route_ids:
            placeholders = ",".join("?" for _ in route_ids)
            for tr in conn.execute(
                f"""
                SELECT route_id, trip_headsign
                FROM trips
                WHERE route_id IN ({placeholders})
                GROUP BY route_id, trip_headsign
                """,
                route_ids,
            ).fetchall():
                headsigns_map.setdefault(tr["route_id"], []).append(tr["trip_headsign"])

    result = []
    for r in rows:
        d = dict(r)
        d["headsigns"] = headsigns_map.get(r["route_id"], [])
        result.append(d)
    return {
        "agency_id": agency_id,
        "total": total,
        "offset": offset,
        "limit": limit,
        "routes": result,
    }


@app.get("/api/agencies/{agency_id}/stops")
def agency_stops(
    agency_id: str,
    lang: Optional[str] = _LANG_QUERY,
    mode: str = Query(
        default=DEFAULT_MODE,
        description="Which service to query: 'transit' (bus), 'ferry' "
                    "(water) or 'rail'. All are served by this one API; the "
                    "endpoint paths are identical.",
    ),
):
    """Every stop served by at least one route of *agency_id*."""
    with _db(mode) as conn:
        if not _agency_rows(conn, agency_id):
            raise HTTPException(
                status_code=404,
                detail=f"agency_id '{agency_id}' not found in mode '{mode}'",
            )
        rows = conn.execute(
            """
            SELECT DISTINCT s.stop_id, s.stop_code, s.stop_name,
                   CAST(s.stop_lat AS REAL) AS lat,
                   CAST(s.stop_lon AS REAL) AS lon
            FROM stops s
            JOIN stop_times st ON st.stop_id  = s.stop_id
            JOIN trips      t  ON st.trip_id  = t.trip_id
            JOIN routes     r  ON t.route_id  = r.route_id
            WHERE r.agency_id = ?
            ORDER BY s.stop_name
            """,
            (agency_id,),
        ).fetchall()
        stop_tr = _get_stop_translations(conn, lang or "en")
    result = [dict(r) for r in rows]
    if stop_tr:
        result = [_translated_stop(r, stop_tr, lang) for r in result]
    return {"agency_id": agency_id, "total": len(result), "stops": result}


@app.get("/api/gtfs-stops")
def gtfs_stops(
    agency_id: Optional[str] = Query(
        default=None,
        description="Only stops served by this agency_id (omit for all stops)",
    ),
    lang: Optional[str] = _LANG_QUERY,
    mode: str = Query(
        default=DEFAULT_MODE,
        description="Which service to query: 'transit' (bus), 'ferry' "
                    "(water) or 'rail'. All are served by this one API; the "
                    "endpoint paths are identical.",
    ),
):
    with _db(mode) as conn:
        if agency_id and agency_id.strip():
            rows = conn.execute(
                """
                SELECT DISTINCT s.stop_id, s.stop_code, s.stop_name,
                       CAST(s.stop_lat AS REAL) AS lat,
                       CAST(s.stop_lon AS REAL) AS lon,
                       s.zone_id, s.platform_code
                FROM stops s
                JOIN stop_times st ON st.stop_id = s.stop_id
                JOIN trips      t  ON st.trip_id = t.trip_id
                JOIN routes     r  ON t.route_id = r.route_id
                WHERE r.agency_id = ?
                ORDER BY s.stop_name
                """,
                (agency_id.strip(),),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT stop_id, stop_code, stop_name,"
                " CAST(stop_lat AS REAL) AS lat,"
                " CAST(stop_lon AS REAL) AS lon,"
                " zone_id, platform_code FROM stops"
            ).fetchall()
        stop_tr = _get_stop_translations(conn, lang or "en")
    result = [dict(r) for r in rows]
    if stop_tr:
        result = [_translated_stop(r, stop_tr, lang) for r in result]
    return result


@app.get("/api/osm-stops")
def osm_stops(
    mode: str = Query(
        default=DEFAULT_MODE,
        description="Which service to query: 'transit' (bus), 'ferry' "
                    "(water) or 'rail'. All are served by this one API; the "
                    "endpoint paths are identical.",
    ),
):
    with _db(mode) as conn:
        rows = conn.execute(
            "SELECT osm_node_id, name, name_en, lat, lon, tags FROM osm_stops"
        ).fetchall()
    result = []
    for r in rows:
        d = dict(r)
        if d.get("tags"):
            try:
                d["tags"] = json.loads(d["tags"])
            except (json.JSONDecodeError, TypeError):
                d["tags"] = {}
        result.append(d)
    return result


@app.get("/api/mapping")
def mapping(
    mode: str = Query(
        default=DEFAULT_MODE,
        description="Which service to query: 'transit' (bus), 'ferry' "
                    "(water) or 'rail'. All are served by this one API; the "
                    "endpoint paths are identical.",
    ),
):
    with _db(mode) as conn:
        rows = conn.execute(
            """
            SELECT m.stop_id, m.osm_node_id, m.distance_meters, m.match_status,
                   s.stop_name,
                   CAST(s.stop_lat AS REAL) AS gtfs_lat,
                   CAST(s.stop_lon AS REAL) AS gtfs_lon,
                   o.name AS osm_name,
                   o.lat  AS osm_lat,
                   o.lon  AS osm_lon
            FROM stop_osm_mapping m
            JOIN stops s ON m.stop_id = s.stop_id
            LEFT JOIN osm_stops o ON m.osm_node_id = o.osm_node_id
            """
        ).fetchall()
    return [dict(r) for r in rows]


@app.get("/api/stats")
def stats(
    mode: str = Query(
        default=DEFAULT_MODE,
        description="Which service to query: 'transit' (bus), 'ferry' "
                    "(water) or 'rail'. All are served by this one API; the "
                    "endpoint paths are identical.",
    ),
):
    with _db(mode) as conn:
        gtfs_count = conn.execute("SELECT COUNT(*) FROM stops").fetchone()[0]
        osm_count  = conn.execute("SELECT COUNT(*) FROM osm_stops").fetchone()[0]
        matched    = conn.execute(
            "SELECT COUNT(*) FROM stop_osm_mapping WHERE match_status='matched'"
        ).fetchone()[0]
        no_match   = conn.execute(
            "SELECT COUNT(*) FROM stop_osm_mapping WHERE match_status='no_match'"
        ).fetchone()[0]
    return {
        "gtfs_stops": gtfs_count,
        "osm_stops":  osm_count,
        "matched":    matched,
        "no_match":   no_match,
    }


# ── Helpers ───────────────────────────────────────────────────────────────────

_DAY_COLS = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]


def _gtfs_time_seconds(t: str) -> int:
    """Convert a GTFS time string (HH:MM:SS, may exceed 24h) to seconds since midnight."""
    parts = t.strip().split(":")
    return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])


def _parse_at(at: Optional[str]) -> tuple[date, int]:
    """
    Parse an ``at`` query value into (service_date, seconds_since_midnight).

    Accepts 'now'/empty (current moment), 'HH:MM', 'HH:MM:SS',
    'YYYY-MM-DD' (full day, from 00:00), 'YYYY-MM-DDTHH:MM' and
    'YYYY-MM-DDTHH:MM:SS'.
    """
    at_clean = (at or "now").strip()
    if not at_clean or at_clean.lower() == "now":
        now = datetime.now()
        return now.date(), now.hour * 3600 + now.minute * 60 + now.second

    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M", "%Y-%m-%d", "%H:%M:%S", "%H:%M"):
        try:
            parsed = datetime.strptime(at_clean, fmt)
        except ValueError:
            continue
        on_date = parsed.date() if parsed.year != 1900 else date.today()
        if fmt == "%Y-%m-%d":
            return on_date, 0
        return on_date, parsed.hour * 3600 + parsed.minute * 60 + parsed.second

    raise HTTPException(
        status_code=422,
        detail=(
            "'at' must be 'now', a date (YYYY-MM-DD), an ISO datetime (YYYY-MM-DDTHH:MM), "
            "or a time (HH:MM)"
        ),
    )


def _active_services(conn: sqlite3.Connection, on_date: date) -> set[str]:
    """
    Return the set of service_ids active on *on_date*, applying calendar_dates
    exceptions (type 1 = added, type 2 = removed) on top of the base calendar.
    """
    day_col    = _DAY_COLS[on_date.weekday()]
    date_str   = on_date.strftime("%Y%m%d")

    # Base services active on this weekday and within their date range
    base_rows = conn.execute(
        f"""
        SELECT service_id FROM calendar
        WHERE  {day_col} = '1'
          AND  start_date <= ?
          AND  end_date   >= ?
        """,
        (date_str, date_str),
    ).fetchall()
    active = {r[0] for r in base_rows}

    # Apply exceptions
    exc_rows = conn.execute(
        "SELECT service_id, exception_type FROM calendar_dates WHERE date = ?",
        (date_str,),
    ).fetchall()
    for service_id, exc_type in exc_rows:
        if exc_type == "1":
            active.add(service_id)
        elif exc_type == "2":
            active.discard(service_id)

    return active


# ── Translation helpers ────────────────────────────────────────────────────────

def _get_stop_translations(conn: sqlite3.Connection, lang: str) -> dict[str, str]:
    """
    Return a mapping of {stop_id: translated_stop_name} for the requested language.
    Only populated when lang != 'en'; returns empty dict for English (use default stop_name).
    """
    if not lang or lang.lower() == "en":
        return {}
    rows = conn.execute(
        """
        SELECT record_id, translation FROM translations
        WHERE table_name = 'stops' AND field_name = 'stop_name' AND language = ?
        """,
        (lang.lower(),),
    ).fetchall()
    return {r["record_id"]: r["translation"] for r in rows}


def _get_agency_translations(conn: sqlite3.Connection, lang: str) -> dict[str, str]:
    """
    Return a mapping of {agency_id: translated_agency_name} for the requested
    language. Only populated when lang != 'en'.
    """
    if not lang or lang.lower() == "en":
        return {}
    try:
        rows = conn.execute(
            """
            SELECT record_id, translation FROM translations
            WHERE table_name = 'agency' AND field_name = 'agency_name'
              AND language = ?
            """,
            (lang.lower(),),
        ).fetchall()
    except sqlite3.OperationalError:
        return {}
    return {r["record_id"]: r["translation"] for r in rows}


def _get_trip_translations(conn: sqlite3.Connection, lang: str) -> dict[str, str]:
    """
    Return a mapping of {trip_id: translated_trip_headsign} for the requested language.
    Only populated when lang != 'en'; returns empty dict for English.
    """
    if not lang or lang.lower() == "en":
        return {}
    rows = conn.execute(
        """
        SELECT record_id, translation FROM translations
        WHERE table_name = 'trips' AND field_name = 'trip_headsign' AND language = ?
        """,
        (lang.lower(),),
    ).fetchall()
    return {r["record_id"]: r["translation"] for r in rows}


def _translated_stop(stop_dict: dict, stop_tr: dict[str, str], lang: str) -> dict:
    """
    Overwrite the stop_name in-place with translation if available.
    """
    if not stop_tr:
        return stop_dict
    sid = stop_dict.get("stop_id")
    translated = stop_tr.get(sid)
    if translated:
        stop_dict["stop_name"] = translated
    return stop_dict


def _translated_trip_fields(trip_dict: dict, trip_tr: dict[str, str], lang: str) -> dict:
    """
    Overwrite the trip_headsign and stop_headsign in-place with translation if available.
    """
    if not trip_tr:
        return trip_dict
    tid = trip_dict.get("trip_id")
    translated = trip_tr.get(tid)
    if translated:
        trip_dict["trip_headsign"] = translated
        if "stop_headsign" in trip_dict:
            trip_dict["stop_headsign"] = translated
    return trip_dict


# ── Stop routes ───────────────────────────────────────────────────────────────

@app.get("/api/routes")
def all_routes(
    q:      Optional[str] = Query(default=None, description="Search by route name or headsign (case-insensitive substring)"),
    agency_id: Optional[str] = Query(default=None, description="Only routes belonging to this agency_id"),
    offset: int           = Query(default=0,    ge=0,         description="Number of routes to skip"),
    limit:  int           = Query(default=50,   ge=1, le=500, description="Max routes to return"),
    lang:   Optional[str] = _LANG_QUERY,
    include_between_stops: bool = Query(
        default=True,
        description="Include ``in_between_stops`` (the stops between the first "
                    "and last stop of each trip) for every returned route.",
    ),
    mode: str = Query(
        default=DEFAULT_MODE,
        description="Which service to query: 'transit' (bus), 'ferry' "
                    "(water) or 'rail'. All are served by this one API; the "
                    "endpoint paths are identical.",
    ),
    region: Optional[str] = Query(
        default=None,
        description="Unique identifier (e.g. 'netherlands-gtfs') or name of the region/database. "
                    "If passed, only this particular SQLite file will be used.",
    ),
    region_id: Optional[str] = Query(
        default=None,
        description="Alias for region identifier.",
    ),
):
    """
    Return paginated routes, optionally filtered by a search term.

    - ``q``      – substring match against route_short_name, route_long_name, or any headsign
    - ``offset`` – how many records to skip (for pagination)
    - ``limit``  – page size (1–500, default 50)
    - ``lang``   – language code for translations (e.g. 'ar')
    - ``include_between_stops`` – set ``false`` to skip ``in_between_stops``

    Each route carries ``start_stop_name`` / ``end_stop_name`` (the terminals)
    and ``in_between_stops`` – every other stop the route passes through, in
    travel order, derived from the trips active today.

    ``round_trip`` is ``true`` when every trip of the route returns to its
    first stop (e.g. the ferry PURPLE line: Yas Bay → … → Yas Bay); such a
    route lists all of its stops in ``in_between_stops``.

    Response includes ``total`` (matching count) for the client to render pages.
    """
    params: list = []
    where_clauses: list[str] = []

    if agency_id and agency_id.strip():
        where_clauses.append("r.agency_id = ?")
        params.append(agency_id.strip())

    if q and q.strip():
        term = f"%{q.strip()}%"
        # Match route name OR any headsign belonging to this route
        where_clauses.append(
            """(
                 r.route_short_name LIKE ? COLLATE NOCASE
              OR r.route_long_name  LIKE ? COLLATE NOCASE
              OR EXISTS (
                     SELECT 1 FROM trips t2
                     WHERE t2.route_id = r.route_id
                       AND t2.trip_headsign LIKE ? COLLATE NOCASE
                 )
            )"""
        )
        params.extend([term, term, term])

    where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

    selected_region = region_id or region
    with _db_or_region(region=selected_region, mode=mode) as conn:
        total = conn.execute(
            f"SELECT COUNT(*) FROM routes r {where_sql}", params
        ).fetchone()[0]

        routes_rows = conn.execute(
            f"""
            SELECT r.route_id, r.route_short_name, r.route_long_name,
                   r.route_color, r.route_text_color, r.route_type, r.route_url,
                   r.agency_id
            FROM routes r
            {where_sql}
            ORDER BY r.route_short_name
            LIMIT ? OFFSET ?
            """,
            params + [limit, offset],
        ).fetchall()

        route_ids = [r["route_id"] for r in routes_rows]
        headsigns_map = {}
        stats_map: dict = {}
        if route_ids:
            placeholders = ",".join("?" for _ in route_ids)
            trip_rows = conn.execute(
                f"""
                SELECT route_id, trip_id, trip_headsign
                FROM trips
                WHERE route_id IN ({placeholders})
                GROUP BY route_id, trip_headsign
                """,
                route_ids
            ).fetchall()

            trip_tr = _get_trip_translations(conn, lang or "en")

            for tr in trip_rows:
                rid = tr["route_id"]
                tid = tr["trip_id"]
                hs = tr["trip_headsign"]
                if trip_tr:
                    hs = trip_tr.get(tid, hs)
                if hs:
                    headsigns_map.setdefault(rid, []).append(hs)

            stop_tr = _get_stop_translations(conn, lang or "en")

            # Per-route counts in one batched query:
            #   stop_count  – distinct stops served by the route
            #   trip_count  – trips in the feed
            #   trips_today – trips active today (calendar + calendar_dates)
            today = date.today()
            active = sorted(_active_services(conn, today))
            stats_rows = conn.execute(
                f"""
                SELECT r.route_id,
                       (SELECT COUNT(DISTINCT st.stop_id)
                          FROM stop_times st
                          JOIN trips t3 ON t3.trip_id = st.trip_id
                         WHERE t3.route_id = r.route_id) AS stop_count,
                       (SELECT COUNT(*)
                          FROM trips t2
                         WHERE t2.route_id = r.route_id) AS trip_count,
                       -- first stop of the route's first trip (by sequence)
                       (SELECT s.stop_name
                          FROM stop_times st
                          JOIN stops s ON s.stop_id = st.stop_id
                         WHERE st.trip_id = (
                               SELECT t4.trip_id FROM trips t4
                                WHERE t4.route_id = r.route_id
                                ORDER BY t4.trip_id LIMIT 1)
                         ORDER BY CAST(st.stop_sequence AS INTEGER) LIMIT 1)
                       AS start_stop_name,
                       (SELECT s.stop_name
                          FROM stop_times st
                          JOIN stops s ON s.stop_id = st.stop_id
                         WHERE st.trip_id = (
                               SELECT t5.trip_id FROM trips t5
                                WHERE t5.route_id = r.route_id
                                ORDER BY t5.trip_id LIMIT 1)
                         ORDER BY CAST(st.stop_sequence AS INTEGER) DESC LIMIT 1)
                       AS end_stop_name
                FROM routes r
                WHERE r.route_id IN ({placeholders})
                """,
                route_ids,
            ).fetchall()

            today_counts: dict = {}
            if active:
                ph_active = ",".join("?" * len(active))
                for row in conn.execute(
                    f"""
                    SELECT route_id, COUNT(*) AS n
                    FROM trips
                    WHERE route_id IN ({placeholders})
                      AND service_id IN ({ph_active})
                    GROUP BY route_id
                    """,
                    [*route_ids, *active],
                ).fetchall():
                    today_counts[row["route_id"]] = row["n"]

            for row in stats_rows:
                stats_map[row["route_id"]] = {
                    "stop_count":      row["stop_count"],
                    "trip_count":      row["trip_count"],
                    "trips_today":     today_counts.get(row["route_id"], 0),
                    "start_stop_name": row["start_stop_name"],
                    "end_stop_name":   row["end_stop_name"],
                }

        between_map: dict = {}
        # A round trip is a route whose trips return to their first stop, so
        # its first and last stop are the same (e.g. the ferry PURPLE line).
        # Computed from the timetable, not from the headsign text.
        round_trip_map: dict = {}
        if route_ids:
            # Prefer trips running today; a route with no service today (e.g.
            # a weekend-only line) falls back to all of its trips.
            between_map = _route_stops_between(conn, route_ids, active)
            missing = [rid for rid in route_ids if rid not in between_map]
            if missing:
                between_map.update(_route_stops_between(conn, missing))
            for rid, info in between_map.items():
                round_trip_map[rid] = info["round_trip"]
                for s in info["stops"]:
                    s["stop_name"] = stop_tr.get(s["stop_id"], s["stop_name"]) \
                        if stop_tr else s["stop_name"]

    routes_list = []
    for r in routes_rows:
        rid = r["route_id"]
        hs_list = []
        for hs in headsigns_map.get(rid, []):
            if hs not in hs_list:
                hs_list.append(hs)
        stats = stats_map.get(rid, {})
        start_name = stats.get("start_stop_name")
        end_name = stats.get("end_stop_name")
        long_name = r["route_long_name"]
        # Feeds such as rail leave route_long_name empty; fall back to
        # "<first stop> → <last stop>" so the route is readable.
        if not (long_name or "").strip() and start_name and end_name:
            long_name = f"{start_name} → {end_name}"
        routes_list.append({
            "route_id":         r["route_id"],
            "route_short_name": r["route_short_name"],
            "route_long_name":  long_name,
            "start_stop_name":  start_name,
            "end_stop_name":    end_name,
            "round_trip":       round_trip_map.get(rid, False),
            "in_between_stops": between_map[rid]["stops"] if rid in between_map else [],
            "service_type":     _service_type(mode, r["route_short_name"],
                                              r["route_long_name"]),
            "route_color":      r["route_color"],
            "route_text_color": r["route_text_color"],
            "route_type":       r["route_type"],
            "route_url":        r["route_url"],
            "headsigns":        hs_list,
            "stop_count":       stats.get("stop_count", 0),
            "trip_count":       stats.get("trip_count", 0),
            "trips_today":      stats.get("trips_today", 0),
        })

    return {
        "total":  total,
        "offset": offset,
        "limit":  limit,
        "routes": routes_list,
    }


@app.get("/api/stops/{stop_id}/routes")
def stop_routes(
    stop_id: str,
    lang: Optional[str] = _LANG_QUERY,
    region: Optional[str] = Query(default=None, description="Optional region / database override"),
    region_id: Optional[str] = Query(default=None, description="Alias for region identifier"),
    regionId: Optional[str] = Query(default=None, description="Alias for region identifier (camelCase)"),
    mode: str = Query(
        default=DEFAULT_MODE,
        description="Which service to query: 'transit' (bus), 'ferry' "
                    "(water) or 'rail'. All are served by this one API; the "
                    "endpoint paths are identical.",
    ),
):
    """All distinct routes that serve *stop_id*."""
    db_path = _find_db_for_stop(stop_id, region=region, region_id=region_id, regionId=regionId, mode=mode)
    with _open_db(db_path) as conn:
        # Verify stop exists
        row = conn.execute(
            "SELECT stop_id, stop_name FROM stops WHERE stop_id = ?", (stop_id,)
        ).fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail=f"stop_id '{stop_id}' not found")

        # stop_times reference platforms, not the parent station (e.g. rail
        # "PMB" has no stop_times; "PMB1"/"PMB2" do). Expand to the platforms.
        stop_ids = _station_stop_ids(conn, stop_id)
        stop_ph = ",".join("?" * len(stop_ids))

        rows = conn.execute(
            f"""
            SELECT
                r.route_id,
                r.route_short_name,
                r.route_long_name,
                r.route_color,
                r.route_text_color,
                r.route_type,
                r.route_url,
                t.trip_id,
                t.trip_headsign
            FROM  stop_times st
            JOIN  trips      t  ON st.trip_id  = t.trip_id
            JOIN  routes     r  ON t.route_id  = r.route_id
            WHERE st.stop_id IN ({stop_ph})
            GROUP BY r.route_id, t.trip_headsign
            ORDER BY r.route_short_name
            """,
            stop_ids,
        ).fetchall()

        stop_tr = _get_stop_translations(conn, lang or "en")
        trip_tr = _get_trip_translations(conn, lang or "en")

    lang_key = lang.lower() if lang else "en"

    # Group headsigns per route
    routes_map: dict = {}
    for r in rows:
        rid = r["route_id"]
        if rid not in routes_map:
            routes_map[rid] = {
                "route_id":         r["route_id"],
                "route_short_name": r["route_short_name"],
                "route_long_name":  r["route_long_name"],
                "route_color":      r["route_color"],
                "route_text_color": r["route_text_color"],
                "route_type":       r["route_type"],
                "route_url":        r["route_url"],
                "headsigns":        [],
            }
        hs = r["trip_headsign"]
        if trip_tr:
            tid = r["trip_id"]
            hs = trip_tr.get(tid, hs)
        if hs and hs not in routes_map[rid]["headsigns"]:
            routes_map[rid]["headsigns"].append(hs)

    stop_name = row["stop_name"]
    if stop_tr:
        stop_name = stop_tr.get(stop_id, stop_name)

    result = {
        "stop_id":   row["stop_id"],
        "stop_name": stop_name,
        "routes":    list(routes_map.values()),
    }
    return result


@app.get("/api/osm-stops/{osm_node_id}/routes")
def osm_stop_routes(
    osm_node_id: int,
    lang: Optional[str] = _LANG_QUERY,
    mode: str = Query(
        default=DEFAULT_MODE,
        description="Which service to query: 'transit' (bus), 'ferry' "
                    "(water) or 'rail'. All are served by this one API; the "
                    "endpoint paths are identical.",
    ),
):
    """All distinct routes that serve the GTFS stop mapped to *osm_node_id*."""
    with _db(mode) as conn:
        # Look up the GTFS stop mapped to this OSM node
        mapping_row = conn.execute(
            """
            SELECT m.stop_id, m.osm_node_id, m.distance_meters, m.match_status,
                   s.stop_name,
                   o.name AS osm_name
            FROM stop_osm_mapping m
            JOIN stops s ON m.stop_id = s.stop_id
            LEFT JOIN osm_stops o ON m.osm_node_id = o.osm_node_id
            WHERE m.osm_node_id = ?
            """,
            (osm_node_id,),
        ).fetchone()

        if mapping_row is None:
            raise HTTPException(
                status_code=404,
                detail=f"osm_node_id '{osm_node_id}' not found in stop_osm_mapping",
            )

        if mapping_row["match_status"] != "matched":
            raise HTTPException(
                status_code=404,
                detail=f"osm_node_id '{osm_node_id}' has no matched GTFS stop (status: {mapping_row['match_status']})",
            )

        stop_id = mapping_row["stop_id"]

        rows = conn.execute(
            """
            SELECT
                r.route_id,
                r.route_short_name,
                r.route_long_name,
                r.route_color,
                r.route_text_color,
                r.route_type,
                r.route_url,
                t.trip_id,
                t.trip_headsign
            FROM  stop_times st
            JOIN  trips      t  ON st.trip_id  = t.trip_id
            JOIN  routes     r  ON t.route_id  = r.route_id
            WHERE st.stop_id = ?
            GROUP BY r.route_id, t.trip_headsign
            ORDER BY r.route_short_name
            """,
            (stop_id,),
        ).fetchall()

        stop_tr = _get_stop_translations(conn, lang or "en")
        trip_tr = _get_trip_translations(conn, lang or "en")

    # Group headsigns per route
    routes_map: dict = {}
    for r in rows:
        rid = r["route_id"]
        if rid not in routes_map:
            routes_map[rid] = {
                "route_id":         r["route_id"],
                "route_short_name": r["route_short_name"],
                "route_long_name":  r["route_long_name"],
                "route_color":      r["route_color"],
                "route_text_color": r["route_text_color"],
                "route_type":       r["route_type"],
                "route_url":        r["route_url"],
                "headsigns":        [],
            }
        hs = r["trip_headsign"]
        if trip_tr:
            tid = r["trip_id"]
            hs = trip_tr.get(tid, hs)
        if hs and hs not in routes_map[rid]["headsigns"]:
            routes_map[rid]["headsigns"].append(hs)

    stop_name = mapping_row["stop_name"]
    if stop_tr:
        stop_name = stop_tr.get(stop_id, stop_name)

    result = {
        "osm_node_id":     osm_node_id,
        "osm_name":        mapping_row["osm_name"],
        "stop_id":         mapping_row["stop_id"],
        "stop_name":       stop_name,
        "distance_meters": mapping_row["distance_meters"],
        "routes":          list(routes_map.values()),
    }
    return result


# ── Next departures ───────────────────────────────────────────────────────────

@app.get("/api/stops/{stop_id}/next-departures")
def next_departures(
    stop_id: str,
    at: Optional[str] = Query(
        default=None,
        description="ISO datetime or time, e.g. '2026-04-12T14:30' or '14:30'. "
                    "Defaults to now.",
    ),
    route: Optional[str] = Query(
        default=None,
        description="Optional route_short_name filter (exact match, case-insensitive).",
    ),
    limit: int = Query(default=10, ge=1, le=100, description="Max results to return"),
    lang: Optional[str] = _LANG_QUERY,
    mode: str = Query(
        default=DEFAULT_MODE,
        description="Which service to query: 'transit' (bus), 'ferry' "
                    "(water) or 'rail'. All are served by this one API; the "
                    "endpoint paths are identical.",
    ),
):
    """
    Return the next *limit* departures from *stop_id* on the date/time given by
    *at*.  Active services are resolved from the calendar for that date.

    GTFS departure_time may exceed 24:00:00 (next-day trips); those are
    included when the requested wall-clock time is close to midnight.

    Optional query param:
    - ``route`` – route_short_name exact match (case-insensitive)
    """
    with _db(mode) as conn:
        tz = _get_db_timezone(conn)
        # Parse `at` → (on_date, from_seconds). An empty value means "now".
        if at is None or not str(at).strip() or str(at).strip().lower() == "now":
            now         = datetime.now(tz)
            on_date     = now.date()
            from_secs   = now.hour * 3600 + now.minute * 60 + now.second
        else:
            at = str(at).strip()
            # Accept "YYYY-MM-DD" (full day), "HH:MM", "HH:MM:SS",
            #        "YYYY-MM-DDTHH:MM", "YYYY-MM-DDTHH:MM:SS"
            parsed      = None
            date_only   = False
            for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M", "%Y-%m-%d", "%H:%M:%S", "%H:%M"):
                try:
                    parsed = datetime.strptime(at, fmt)
                    date_only = (fmt == "%Y-%m-%d")
                    break
                except ValueError:
                    pass
            if parsed is None:
                raise HTTPException(
                    status_code=422,
                    detail="'at' must be a date (YYYY-MM-DD), ISO datetime (YYYY-MM-DDTHH:MM), or time (HH:MM)",
                )
            on_date   = parsed.date() if parsed.year != 1900 else datetime.now(tz).date()
            # Date-only → return the full day starting from midnight
            from_secs = 0 if date_only else (parsed.hour * 3600 + parsed.minute * 60 + parsed.second)

        # Verify stop exists
        stop_row = conn.execute(
            "SELECT stop_id, stop_name FROM stops WHERE stop_id = ?", (stop_id,)
        ).fetchone()
        if stop_row is None:
            raise HTTPException(status_code=404, detail=f"stop_id '{stop_id}' not found")

        active = _active_services(conn, on_date)

        # Also check previous day's services for >24h trips (past-midnight)
        prev_date = on_date - timedelta(days=1)
        active_prev = _active_services(conn, prev_date)

        # Include next day's regular services
        next_date = on_date + timedelta(days=1)
        active_next = _active_services(conn, next_date)
        next_day_offset_secs = (next_date - on_date).days * 86400

        if not active and not active_prev and not active_next:
            stop_name = stop_row["stop_name"]
            result = {
                "stop_id":    stop_id,
                "stop_name":  stop_name,
                "date":       on_date.isoformat(),
                "from_time":  _secs_to_hhmm(from_secs),
                "departures": [],
                "note":       "No active services on this date",
            }
            if lang and lang.lower() != "en":
                stop_tr = _get_stop_translations(conn, lang)
                lang_key = lang.lower()
                result[f"stop_name_{lang_key}"] = stop_tr.get(stop_id, stop_name)
            return result

        all_rows = []

        route_filter_sql = ""
        route_filter_params: list = []
        if route and route.strip():
            route_filter_sql = "AND r.route_short_name = ? COLLATE NOCASE"
            route_filter_params = [route.strip()]

        if active:
            placeholders = ",".join("?" * len(active))
            rows_today = conn.execute(
                f"""
                SELECT
                    st.departure_time,
                    st.arrival_time,
                    st.stop_sequence,
                    st.stop_headsign,
                    t.trip_id,
                    t.trip_headsign,
                    t.direction_id,
                    r.route_id,
                    r.route_short_name,
                    r.route_long_name,
                    r.route_color,
                    r.route_text_color
                FROM  stop_times st
                JOIN  trips  t ON st.trip_id = t.trip_id
                JOIN  routes r ON t.route_id = r.route_id
                WHERE st.stop_id   = ?
                  AND t.service_id IN ({placeholders})
                                    {route_filter_sql}
                ORDER BY st.departure_time
                """,
                                [stop_id, *active, *route_filter_params],
            ).fetchall()
            for r in rows_today:
                d = dict(r)
                d["_day_offset"] = 0
                d["_service_date"] = on_date
                all_rows.append(d)

        # Previous day's services: only >24h trips (past-midnight spillover)
        if active_prev:
            placeholders_prev = ",".join("?" * len(active_prev))
            rows_prev = conn.execute(
                f"""
                SELECT
                    st.departure_time,
                    st.arrival_time,
                    st.stop_sequence,
                    st.stop_headsign,
                    t.trip_id,
                    t.trip_headsign,
                    t.direction_id,
                    r.route_id,
                    r.route_short_name,
                    r.route_long_name,
                    r.route_color,
                    r.route_text_color
                FROM  stop_times st
                JOIN  trips  t ON st.trip_id = t.trip_id
                JOIN  routes r ON t.route_id = r.route_id
                WHERE st.stop_id   = ?
                  AND t.service_id IN ({placeholders_prev})
                                    {route_filter_sql}
                ORDER BY st.departure_time
                """,
                                [stop_id, *active_prev, *route_filter_params],
            ).fetchall()
            for r in rows_prev:
                try:
                    dep_secs = _gtfs_time_seconds(r["departure_time"])
                except (ValueError, IndexError):
                    continue
                if dep_secs >= 86400:
                    d = dict(r)
                    d["_day_offset"] = -86400
                    d["_service_date"] = prev_date
                    all_rows.append(d)

        # Next day's services: include all departures shifted by the day delta
        if active_next:
            placeholders_next = ",".join("?" * len(active_next))
            rows_next = conn.execute(
                f"""
                SELECT
                    st.departure_time,
                    st.arrival_time,
                    st.stop_sequence,
                    st.stop_headsign,
                    t.trip_id,
                    t.trip_headsign,
                    t.direction_id,
                    r.route_id,
                    r.route_short_name,
                    r.route_long_name,
                    r.route_color,
                    r.route_text_color
                FROM  stop_times st
                JOIN  trips  t ON st.trip_id = t.trip_id
                JOIN  routes r ON t.route_id = r.route_id
                WHERE st.stop_id   = ?
                  AND t.service_id IN ({placeholders_next})
                                    {route_filter_sql}
                ORDER BY st.departure_time
                """,
                                [stop_id, *active_next, *route_filter_params],
            ).fetchall()
            for r in rows_next:
                d = dict(r)
                d["_day_offset"] = next_day_offset_secs
                d["_service_date"] = next_date
                all_rows.append(d)

        # Load translations inside the same connection
        stop_tr = _get_stop_translations(conn, lang or "en")
        trip_tr = _get_trip_translations(conn, lang or "en")

    # Filter and sort by wall-clock time
    candidates = []
    for d in all_rows:
        try:
            dep_secs = _gtfs_time_seconds(d["departure_time"])
        except (ValueError, IndexError):
            continue
        wall_secs = dep_secs + d["_day_offset"]
        if wall_secs >= from_secs:
            d["departure_seconds"] = wall_secs
            candidates.append(d)

    candidates.sort(key=lambda x: x["departure_seconds"])

    departures = []
    for d in candidates:
        departure_date = _gtfs_service_date(d["_service_date"], d["departure_time"])
        arrival_date = _gtfs_service_date(d["_service_date"], d["arrival_time"])
        d.pop("_day_offset", None)
        d.pop("_service_date", None)
        d["departure_date"] = departure_date.isoformat()
        d["arrival_date"] = arrival_date.isoformat()
        d["departure_time"] = _normalize_gtfs_time(d["departure_time"])
        d["arrival_time"]   = _normalize_gtfs_time(d["arrival_time"])
        # Apply trip headsign translation
        if trip_tr:
            _translated_trip_fields(d, trip_tr, lang)
        departures.append(d)
        if len(departures) >= limit:
            break

    # Fetch start/end stop names and end time for each trip in one batch query
    if departures:
        trip_ids = list({d["trip_id"] for d in departures})
        placeholders2 = ",".join("?" * len(trip_ids))
        with _db(mode) as conn2:
            terminal_rows = conn2.execute(
                f"""
                SELECT
                    st.trip_id,
                    s_first.stop_id    AS start_stop_id,
                    s_first.stop_name  AS start_stop_name,
                    s_last.stop_id     AS end_stop_id,
                    s_last.stop_name   AS end_stop_name,
                    st_last.arrival_time AS end_time
                FROM (
                    SELECT trip_id,
                           MIN(CAST(stop_sequence AS INTEGER)) AS first_seq,
                           MAX(CAST(stop_sequence AS INTEGER)) AS last_seq
                    FROM stop_times
                    WHERE trip_id IN ({placeholders2})
                    GROUP BY trip_id
                ) seqs
                JOIN stop_times st      ON st.trip_id = seqs.trip_id
                                       AND CAST(st.stop_sequence AS INTEGER) = seqs.first_seq
                JOIN stops s_first      ON s_first.stop_id = st.stop_id
                JOIN stop_times st_last ON st_last.trip_id = seqs.trip_id
                                       AND CAST(st_last.stop_sequence AS INTEGER) = seqs.last_seq
                JOIN stops s_last       ON s_last.stop_id = st_last.stop_id
                """,
                trip_ids,
            ).fetchall()

        trip_terminals: dict = {
            tr["trip_id"]: {
                "start_stop_id":   tr["start_stop_id"],
                "start_stop_name": tr["start_stop_name"],
                "end_stop_id":     tr["end_stop_id"],
                "end_stop_name":   tr["end_stop_name"],
                "end_time":        tr["end_time"],
            }
            for tr in terminal_rows
        }

        for d in departures:
            terminals = trip_terminals.get(d["trip_id"], {})
            start_stop_name = terminals.get("start_stop_name")
            end_stop_name = terminals.get("end_stop_name")
            
            # Translate terminal stop names directly in-place if requested
            if stop_tr:
                start_sid = terminals.get("start_stop_id")
                end_sid   = terminals.get("end_stop_id")
                if start_sid:
                    start_stop_name = stop_tr.get(start_sid, start_stop_name)
                if end_sid:
                    end_stop_name = stop_tr.get(end_sid, end_stop_name)

            d["start_stop_name"] = start_stop_name
            d["end_stop_name"]   = end_stop_name
            end_t = terminals.get("end_time")
            d["end_time"]        = _normalize_gtfs_time(end_t) if end_t else None
            d["end_date"]        = _gtfs_service_date(
                date.fromisoformat(d["departure_date"]), end_t
            ).isoformat() if end_t else None

    stop_name = stop_row["stop_name"]
    if stop_tr:
        stop_name = stop_tr.get(stop_id, stop_name)

    result = {
        "stop_id":    stop_id,
        "stop_name":  stop_name,
        "date":       on_date.isoformat(),
        "from_time":  _secs_to_hhmm(from_secs),
        "departures": departures,
    }
    return result


def _secs_to_hhmm(secs: int) -> str:
    h, rem = divmod(secs, 3600)
    m      = rem // 60
    return f"{h:02d}:{m:02d}"


def _secs_to_hhmmss(secs: int) -> str:
    """Seconds since midnight -> HH:MM:SS (hours may exceed 24)."""
    h, rem = divmod(int(secs), 3600)
    m, s   = divmod(rem, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"


def _route_stops_between(
    conn: sqlite3.Connection,
    route_ids: list,
    services: Optional[list] = None,
) -> dict:
    """
    The stops a route passes *through* — everything except the first and last
    stop of each trip — keyed by route_id, in travel order.

    Rail/ferry feeds often leave route_long_name empty and the terminals are
    already exposed as start_stop_name/end_stop_name, so this is what is left.

    Loop trips (first stop == last stop, e.g. the ferry PURPLE line) have no
    distinct terminals, so every stop they visit is returned instead.

    *services* (optional) restricts the scan to trips active on a date, which
    keeps the query cheap on large bus feeds.

    Trips are merged in trip_id order and rows are streamed one trip at a time,
    so the whole stop_times slice is never held in memory.

    Returns ``{route_id: {"round_trip": bool, "stops": [...]}}``.
    """
    if not route_ids:
        return {}
    ph = ",".join("?" * len(route_ids))
    sql = f"""
        SELECT t.route_id, st.trip_id, s.stop_id, s.stop_name
        FROM trips t
        JOIN stop_times st ON st.trip_id = t.trip_id
        JOIN stops      s  ON s.stop_id  = st.stop_id
        WHERE t.route_id IN ({ph})
    """
    params: list = list(route_ids)
    if services is not None:
        if not services:
            return {}
        sql += f" AND t.service_id IN ({','.join('?' * len(services))})"
        params.extend(services)
    sql += " ORDER BY t.route_id, st.trip_id, CAST(st.stop_sequence AS INTEGER)"

    between: dict = {}
    cur_route = cur_trip = None
    trip_stops: list = []
    seen: set = set()

    def flush_trip() -> None:
        """Fold the finished trip into the current route's entry."""
        if not trip_stops:
            return
        is_loop = len(trip_stops) > 1 and trip_stops[0][0] == trip_stops[-1][0]
        between[cur_route]["round_trip"] &= is_loop
        for stop_id, stop_name in (trip_stops if is_loop else trip_stops[1:-1]):
            if stop_id not in seen:
                seen.add(stop_id)
                between[cur_route]["stops"].append(
                    {"stop_id": stop_id, "stop_name": stop_name}
                )

    for r in conn.execute(sql, params):
        if r["route_id"] != cur_route:
            flush_trip()
            cur_route = r["route_id"]
            cur_trip = None
            trip_stops = []
            seen = set()
            between[cur_route] = {"round_trip": True, "stops": []}
        elif r["trip_id"] != cur_trip:
            flush_trip()
            trip_stops = []
        cur_trip = r["trip_id"]
        trip_stops.append((r["stop_id"], r["stop_name"]))
    flush_trip()

    return between


def _normalize_gtfs_time(t: str) -> str:
    """Convert a GTFS time (may exceed 24h) to wall-clock HH:MM:SS."""
    parts = t.strip().split(":")
    h, m, s = int(parts[0]), int(parts[1]), int(parts[2])
    h = h % 24
    return f"{h:02d}:{m:02d}:{s:02d}"


def _gtfs_service_date(service_date: date, gtfs_time_str: str) -> date:
    """Resolve the calendar date for a GTFS event relative to its service date."""
    if _gtfs_time_seconds(gtfs_time_str) >= 86400:
        return service_date + timedelta(days=1)
    return service_date


# ── Trips lookup ──────────────────────────────────────────────────────────────

def _normalize_headsign(text: str) -> str:
    """Normalize a headsign for matching (HTML-unescape, casefold, strip)."""
    if not text:
        return ""
    return html.unescape(text).strip().casefold()


def _match_stop_sequence(
    stops: list[dict],
    stop_ids: list[str],
    trip: dict,
    trip_tr: dict[str, str],
) -> Optional[dict]:
    """
    Check that *stop_ids* appear in order within the trip's ordered *stops*
    (stop membership/order match — the first requested stop does NOT need to
    be the trip's origin).  Returns a trip dict for the response, or None.
    """
    if not stop_ids:
        return None

    ptr = 0
    matched: list[dict] = []
    for st in stops:
        if st["stop_id"] == stop_ids[ptr]:
            matched.append(st)
            ptr += 1
            if ptr == len(stop_ids):
                break
    if ptr != len(stop_ids):
        return None

    board = matched[0]
    try:
        dep_secs = _gtfs_time_seconds(board["departure_time"])
    except (ValueError, IndexError):
        return None

    trip_headsign = trip.get("trip_headsign")
    if trip_tr:
        translated = trip_tr.get(trip["trip_id"])
        if translated:
            trip_headsign = translated

    return {
        "trip_id":          trip["trip_id"],
        "service_id":       trip["service_id"],
        "direction_id":     trip["direction_id"],
        "trip_headsign":    trip_headsign,
        "block_id":         trip.get("block_id"),
        "route_id":         trip["route_id"],
        "route_short_name": trip["route_short_name"],
        "route_long_name":  trip["route_long_name"],
        "route_color":      trip["route_color"],
        "route_text_color": trip["route_text_color"],
        "boarding_stop": {
            "stop_id":        board["stop_id"],
            "stop_name":      board["stop_name"],
            "stop_sequence":  board["seq"],
            "departure_time": _normalize_gtfs_time(board["departure_time"]),
        },
        "matched_stops": [
            {
                "stop_id":        s["stop_id"],
                "stop_name":      s["stop_name"],
                "stop_sequence":  s["seq"],
                "arrival_time":   _normalize_gtfs_time(s["arrival_time"]),
                "departure_time": _normalize_gtfs_time(s["departure_time"]),
            }
            for s in matched
        ],
        "departure_seconds": dep_secs % 86400,
    }


@app.get("/api/trips/lookup")
def trips_lookup(
    route_id: str = Query(..., description="GTFS route_id"),
    headsign: Optional[str] = Query(
        default=None,
        description="Trip headsign to match (case-insensitive, HTML entities tolerated).",
    ),
    stop_ids: Optional[List[str]] = Query(
        default=None,
        description="Ordered GTFS stop_ids (repeat the param or pass comma-separated values).",
    ),
    departure_date_time: Optional[str] = Query(
        default=None,
        description="ISO datetime, e.g. '2026-09-15T17:26:11+04:00'. Defaults to now.",
    ),
    max_results: int = Query(default=3, ge=1, le=50, description="Max trips to return"),
    lang: Optional[str] = _LANG_QUERY,
    mode: str = Query(
        default=DEFAULT_MODE,
        description="Which service to query: 'transit' (bus), 'ferry' "
                    "(water) or 'rail'. All are served by this one API; the "
                    "endpoint paths are identical.",
    ),
):
    """
    Find trips for *route_id* whose headsign matches and that serve the given
    *stop_ids* in order on the requested date.

    The first requested stop is the boarding stop; it does NOT have to be the
    trip's origin.  Trips are matched by stop membership/order, so mid-route
    boarding works.

    Returns:
    - ``first_possible_trip``  – earliest trip departing at/after the requested time
    - ``latest_missed_trip``   – latest trip that already departed before the requested time
    - ``next_available_trips`` – up to ``max_results`` trips departing at/after the requested time
    - ``matching_trip_count``  – all trips matching route/headsign/stops on this date
    """
    # Parse departure datetime (wall-clock local, matching the GTFS timebase)
    if departure_date_time is None:
        now       = datetime.now()
        on_date   = now.date()
        from_secs = now.hour * 3600 + now.minute * 60 + now.second
    else:
        try:
            parsed = _parse_iso_datetime(departure_date_time)
        except ValueError:
            raise HTTPException(
                status_code=422,
                detail="'departure_date_time' must be an ISO datetime, e.g. '2026-09-15T17:26:11+04:00'",
            )
        on_date   = parsed.date()
        from_secs = parsed.hour * 3600 + parsed.minute * 60 + parsed.second

    # Normalize requested stop_ids (flatten repeated / comma-separated params)
    ids: list[str] = []
    for chunk in stop_ids or []:
        ids.extend(s.strip() for s in chunk.split(",") if s.strip())

    hs_norm = _normalize_headsign(headsign) if headsign else ""

    result = {
        "route_id":              route_id,
        "departure_date":        on_date.isoformat(),
        "departure_date_time":   departure_date_time,
        "stop_ids":              ids,
        "headsign":              headsign,
        "first_possible_trip":   None,
        "latest_missed_trip":    None,
        "next_available_trips":  [],
        "matching_trip_count":   0,
        "note":                  None,
    }

    with _db(mode) as conn:
        route_row = conn.execute(
            "SELECT route_id FROM routes WHERE route_id = ?", (route_id,)
        ).fetchone()
        if route_row is None:
            raise HTTPException(status_code=404, detail=f"route_id '{route_id}' not found")

        active = _active_services(conn, on_date)
        # Previous day's services for >24h trips that spill past midnight
        active_prev = _active_services(conn, on_date - timedelta(days=1))

        if not active and not active_prev:
            result["note"] = "No active services on this date"
            return result

        # Candidate trips: route + headsign + active services
        trip_rows: list[dict] = []
        seen_trips: set[str] = set()
        for services in (active, active_prev):
            if not services:
                continue
            placeholders = ",".join("?" * len(services))
            rows = conn.execute(
                f"""
                SELECT t.trip_id, t.service_id, t.direction_id, t.trip_headsign,
                       t.block_id,
                       r.route_id, r.route_short_name, r.route_long_name,
                       r.route_color, r.route_text_color
                FROM  trips  t
                JOIN  routes r ON t.route_id = r.route_id
                WHERE t.route_id    = ?
                  AND t.service_id IN ({placeholders})
                """,
                (route_id, *services),
            ).fetchall()
            for r in rows:
                if r["trip_id"] in seen_trips:
                    continue
                if hs_norm and _normalize_headsign(r["trip_headsign"]) != hs_norm:
                    continue
                seen_trips.add(r["trip_id"])
                trip_rows.append(dict(r))

        if not trip_rows:
            result["note"] = "No trips found matching route and headsign on this date"
            return result

        # All stop_times for the candidate trips in one pass
        trip_ids     = [d["trip_id"] for d in trip_rows]
        placeholders = ",".join("?" * len(trip_ids))
        st_rows = conn.execute(
            f"""
            SELECT st.trip_id, st.stop_id, CAST(st.stop_sequence AS INTEGER) AS seq,
                   st.arrival_time, st.departure_time, st.stop_headsign,
                   s.stop_name
            FROM  stop_times st
            JOIN  stops      s  ON s.stop_id = st.stop_id
            WHERE st.trip_id IN ({placeholders})
            ORDER BY st.trip_id, CAST(st.stop_sequence AS INTEGER)
            """,
            trip_ids,
        ).fetchall()

        stop_tr = _get_stop_translations(conn, lang or "en")
        trip_tr = _get_trip_translations(conn, lang or "en")

    # Group stop_times per trip (sequence order preserved by the query)
    trip_stops: dict[str, list[dict]] = {}
    for r in st_rows:
        d = dict(r)
        if stop_tr and d["stop_id"] in stop_tr:
            d["stop_name"] = stop_tr[d["stop_id"]]
        trip_stops.setdefault(d["trip_id"], []).append(d)

    matches = [
        m for d in trip_rows
        if (m := _match_stop_sequence(trip_stops.get(d["trip_id"], []), ids, d, trip_tr))
    ]

    if not matches:
        result["note"] = "No trips found that serve the requested stop_ids in order on this date"
        return result

    result["matching_trip_count"] = len(matches)
    matches.sort(key=lambda m: m["departure_seconds"])

    missed   = [m for m in matches if m["departure_seconds"] < from_secs]
    upcoming = [m for m in matches if m["departure_seconds"] >= from_secs]

    if missed:
        result["latest_missed_trip"] = missed[-1]
    if upcoming:
        result["first_possible_trip"]  = upcoming[0]
        result["next_available_trips"] = upcoming[:max_results]
    elif missed:
        result["note"] = "All matching trips have already departed before the requested time"
    return result


# ── Departures between two stops ──────────────────────────────────────────────

@app.get("/api/departures")
def departures_between(
    start_stop_id: str = Query(..., description="GTFS stop_id of the boarding stop"),
    end_stop_id: str = Query(..., description="GTFS stop_id of the alighting stop"),
    at: Optional[str] = Query(
        default=None,
        description="ISO datetime or time, e.g. '2026-04-12T14:30' or '14:30'. "
                    "Defaults to now.",
    ),
    limit: int = Query(default=10, ge=1, le=100, description="Max results to return"),
    mode: str = Query(
        default=DEFAULT_MODE,
        description="Which service to query: 'transit' (bus), 'ferry' "
                    "(water) or 'rail'. All are served by this one API; the "
                    "endpoint paths are identical.",
    ),
):
    """
    Return the next *limit* departures from *start_stop_id* that also call at
    *end_stop_id* later in the same trip.

    If *start_stop_id* and *end_stop_id* are the same, returns all next
    departures from that stop (same as ``/api/stops/{stop_id}/next-departures``).

    Active services are resolved from the calendar for the date in *at*.
    """
    # When start and end stop are the same, delegate to next_departures
    if start_stop_id == end_stop_id:
        return next_departures(stop_id=start_stop_id, at=at, limit=limit, mode=mode)

    with _db(mode) as conn:
        tz = _get_db_timezone(conn)
        # Parse `at` → (on_date, from_seconds). An empty value means "now".
        if at is None or not str(at).strip() or str(at).strip().lower() == "now":
            now       = datetime.now(tz)
            on_date   = now.date()
            from_secs = now.hour * 3600 + now.minute * 60 + now.second
        else:
            at = str(at).strip()
            parsed    = None
            date_only = False
            for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M", "%Y-%m-%d", "%H:%M:%S", "%H:%M"):
                try:
                    parsed = datetime.strptime(at, fmt)
                    date_only = (fmt == "%Y-%m-%d")
                    break
                except ValueError:
                    pass
            if parsed is None:
                raise HTTPException(
                    status_code=422,
                    detail="'at' must be a date (YYYY-MM-DD), ISO datetime (YYYY-MM-DDTHH:MM), or time (HH:MM)",
                )
            on_date   = parsed.date() if parsed.year != 1900 else datetime.now(tz).date()
            from_secs = 0 if date_only else (parsed.hour * 3600 + parsed.minute * 60 + parsed.second)

        # Verify both stops exist
        start_row = conn.execute(
            "SELECT stop_id, stop_name FROM stops WHERE stop_id = ?", (start_stop_id,)
        ).fetchone()
        if start_row is None:
            raise HTTPException(status_code=404, detail=f"start_stop_id '{start_stop_id}' not found")

        end_row = conn.execute(
            "SELECT stop_id, stop_name FROM stops WHERE stop_id = ?", (end_stop_id,)
        ).fetchone()
        if end_row is None:
            raise HTTPException(status_code=404, detail=f"end_stop_id '{end_stop_id}' not found")

        active = _active_services(conn, on_date)

        # Also check previous day's services for >24h trips (past-midnight)
        prev_date = on_date - timedelta(days=1)
        active_prev = _active_services(conn, prev_date)

        if not active and not active_prev:
            return {
                "start_stop_id":   start_stop_id,
                "start_stop_name": start_row["stop_name"],
                "end_stop_id":     end_stop_id,
                "end_stop_name":   end_row["stop_name"],
                "date":            on_date.isoformat(),
                "from_time":       _secs_to_hhmm(from_secs),
                "departures":      [],
                "note":            "No active services on this date",
            }

        all_rows = []

        # Today's services: departure_time >= from_secs
        if active:
            placeholders = ",".join("?" * len(active))
            rows_today = conn.execute(
                f"""
                SELECT
                    st_start.departure_time   AS departure_time,
                    st_end.arrival_time       AS arrival_time,
                    st_start.stop_sequence    AS start_sequence,
                    st_end.stop_sequence      AS end_sequence,
                    t.trip_id,
                    t.trip_headsign,
                    t.direction_id,
                    r.route_id,
                    r.route_short_name,
                    r.route_long_name,
                    r.route_color,
                    r.route_text_color
                FROM  stop_times st_start
                JOIN  stop_times st_end   ON  st_end.trip_id   = st_start.trip_id
                                          AND st_end.stop_id   = ?
                                          AND CAST(st_end.stop_sequence   AS INTEGER)
                                            > CAST(st_start.stop_sequence AS INTEGER)
                JOIN  trips  t ON t.trip_id  = st_start.trip_id
                JOIN  routes r ON r.route_id = t.route_id
                WHERE st_start.stop_id   = ?
                  AND t.service_id IN ({placeholders})
                ORDER BY st_start.departure_time
                """,
                (end_stop_id, start_stop_id, *active),
            ).fetchall()
            for r in rows_today:
                d = dict(r)
                d["_day_offset"] = 0
                all_rows.append(d)

        # Previous day's services: only >24h trips that spill into today
        if active_prev:
            placeholders_prev = ",".join("?" * len(active_prev))
            rows_prev = conn.execute(
                f"""
                SELECT
                    st_start.departure_time   AS departure_time,
                    st_end.arrival_time       AS arrival_time,
                    st_start.stop_sequence    AS start_sequence,
                    st_end.stop_sequence      AS end_sequence,
                    t.trip_id,
                    t.trip_headsign,
                    t.direction_id,
                    r.route_id,
                    r.route_short_name,
                    r.route_long_name,
                    r.route_color,
                    r.route_text_color
                FROM  stop_times st_start
                JOIN  stop_times st_end   ON  st_end.trip_id   = st_start.trip_id
                                          AND st_end.stop_id   = ?
                                          AND CAST(st_end.stop_sequence   AS INTEGER)
                                            > CAST(st_start.stop_sequence AS INTEGER)
                JOIN  trips  t ON t.trip_id  = st_start.trip_id
                JOIN  routes r ON r.route_id = t.route_id
                WHERE st_start.stop_id   = ?
                  AND t.service_id IN ({placeholders_prev})
                ORDER BY st_start.departure_time
                """,
                (end_stop_id, start_stop_id, *active_prev),
            ).fetchall()
            for r in rows_prev:
                try:
                    dep_secs = _gtfs_time_seconds(r["departure_time"])
                except (ValueError, IndexError):
                    continue
                # Only include >24h trips (past-midnight spillover)
                if dep_secs >= 86400:
                    d = dict(r)
                    d["_day_offset"] = -86400  # subtract 24h for wall-clock comparison
                    all_rows.append(d)

    # Filter and sort by wall-clock time
    candidates = []
    for d in all_rows:
        try:
            dep_secs = _gtfs_time_seconds(d["departure_time"])
        except (ValueError, IndexError):
            continue
        wall_secs = dep_secs + d["_day_offset"]
        if wall_secs >= from_secs:
            d["departure_seconds"] = wall_secs
            candidates.append(d)

    candidates.sort(key=lambda x: x["departure_seconds"])

    departures = []
    for d in candidates:
        service_date = on_date + timedelta(days=d["_day_offset"] // 86400)
        departure_date = _gtfs_service_date(service_date, d["departure_time"])
        arrival_date = _gtfs_service_date(service_date, d["arrival_time"])
        d.pop("_day_offset", None)
        d["departure_date"] = departure_date.isoformat()
        d["arrival_date"] = arrival_date.isoformat()
        d["departure_time"] = _normalize_gtfs_time(d["departure_time"])
        d["arrival_time"]   = _normalize_gtfs_time(d["arrival_time"])
        departures.append(d)
        if len(departures) >= limit:
            break

    return {
        "start_stop_id":   start_stop_id,
        "start_stop_name": start_row["stop_name"],
        "end_stop_id":     end_stop_id,
        "end_stop_name":   end_row["stop_name"],
        "date":            on_date.isoformat(),
        "from_time":       _secs_to_hhmm(from_secs),
        "departures":      departures,
    }


# ── Journey planning (direct + optional transfer) ────────────────────────────

def _secs_to_gtfs_time(secs: int) -> str:
    """Seconds since midnight -> zero-padded HH:MM:SS (hours may exceed 24)."""
    h, rem = divmod(int(secs), 3600)
    m, s = divmod(rem, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"


def _station_stop_ids(conn: sqlite3.Connection, stop_id: str) -> list:
    """
    The stop itself plus, for a parent station, every platform beneath it.
    Feeds such as the rail feed record stop_times only against platforms, so a
    journey planned between station ids must expand to those platforms.
    """
    cols = {r[1] for r in conn.execute("PRAGMA table_info(stops)").fetchall()}
    if "parent_station" not in cols:
        return [stop_id]
    rows = conn.execute(
        "SELECT stop_id FROM stops WHERE stop_id = ? OR parent_station = ?",
        (stop_id, stop_id),
    ).fetchall()
    return [r["stop_id"] for r in rows] or [stop_id]


def _journey_legs_from_stop(
    conn: sqlite3.Connection,
    stop_ids: list,
    services: list,
    after_secs: int,
    limit: int,
    exclude_trip_ids: set,
) -> list:
    """
    Every onward leg departing any of *stop_ids* at or after *after_secs* on the
    active services, with the stop reached next. One row per (trip, next stop).

    Bounded by limit, so it stays cheap even on a large feed.
    """
    if not services or not stop_ids:
        return []
    ph = ",".join("?" * len(services))
    stop_ph = ",".join("?" * len(stop_ids))
    rows = conn.execute(
        f"""
        SELECT st.trip_id,
               st.stop_id        AS from_stop_id,
               st.stop_sequence  AS from_seq,
               st.departure_time AS departure_time,
               nx.stop_id        AS to_stop_id,
               nx.stop_sequence  AS to_seq,
               nx.arrival_time   AS arrival_time,
               t.route_id, t.trip_headsign, t.direction_id, t.service_id,
               r.route_short_name, r.route_long_name,
               r.route_color, r.route_text_color
        FROM stop_times st
        JOIN stop_times nx ON nx.trip_id = st.trip_id
                          AND CAST(nx.stop_sequence AS INTEGER)
                            = CAST(st.stop_sequence AS INTEGER) + 1
        JOIN trips  t ON t.trip_id  = st.trip_id
        JOIN routes r ON r.route_id = t.route_id
        WHERE st.stop_id IN ({stop_ph})
          AND t.service_id IN ({ph})
        ORDER BY st.departure_time
        LIMIT ?
        """,
        [*stop_ids, *services, limit],
    ).fetchall()

    legs = []
    for r in rows:
        try:
            dep = _gtfs_time_seconds(r["departure_time"])
            arr = _gtfs_time_seconds(r["arrival_time"])
        except (ValueError, IndexError):
            continue
        if dep < after_secs or r["trip_id"] in exclude_trip_ids:
            continue
        legs.append({
            "trip_id":          r["trip_id"],
            "route_id":         r["route_id"],
            "route_short_name": r["route_short_name"],
            "route_long_name":  r["route_long_name"],
            "route_color":      r["route_color"],
            "route_text_color": r["route_text_color"],
            "trip_headsign":    r["trip_headsign"],
            "direction_id":     r["direction_id"],
            "service_id":       r["service_id"],
            "from_stop_id":     r["from_stop_id"],
            "from_stop_name":   None,
            "from_sequence":    r["from_seq"],
            "departure_time":   _normalize_gtfs_time(r["departure_time"]),
            "to_stop_id":       r["to_stop_id"],
            "to_stop_name":     None,
            "to_sequence":      r["to_seq"],
            "arrival_time":     _normalize_gtfs_time(r["arrival_time"]),
            "_dep_secs":        dep,
            "_arr_secs":        arr,
        })
    return legs


def _direct_journeys(
    conn: sqlite3.Connection,
    start_stop_ids: list,
    end_stop_ids: list,
    services: list,
    after_secs: int,
    limit: int,
) -> list:
    """
    Trips that serve one of *start_stop_ids* then one of *end_stop_ids* later in
    the same trip. Passing a station's platforms as the stop lists lets a
    parent-station query match any of its platforms.
    """
    if not services or not start_stop_ids or not end_stop_ids:
        return []
    ph = ",".join("?" * len(services))
    start_ph = ",".join("?" * len(start_stop_ids))
    end_ph = ",".join("?" * len(end_stop_ids))
    rows = conn.execute(
        f"""
        SELECT st_start.trip_id,
               st_start.stop_id        AS from_stop_id,
               st_start.departure_time AS departure_time,
               st_end.stop_id          AS to_stop_id,
               st_end.arrival_time     AS arrival_time,
               st_start.stop_sequence  AS from_seq,
               st_end.stop_sequence    AS to_seq,
               t.route_id, t.trip_headsign, t.direction_id, t.service_id,
               r.route_short_name, r.route_long_name,
               r.route_color, r.route_text_color
        FROM stop_times st_start
        JOIN stop_times st_end ON st_end.trip_id = st_start.trip_id
                              AND st_end.stop_id IN ({end_ph})
                              AND CAST(st_end.stop_sequence AS INTEGER)
                                > CAST(st_start.stop_sequence AS INTEGER)
        JOIN trips  t ON t.trip_id  = st_start.trip_id
        JOIN routes r ON r.route_id = t.route_id
        WHERE st_start.stop_id IN ({start_ph})
          AND t.service_id IN ({ph})
        ORDER BY st_start.departure_time
        """,
        [*end_stop_ids, *start_stop_ids, *services],
    ).fetchall()

    out = []
    for r in rows:
        try:
            dep = _gtfs_time_seconds(r["departure_time"])
            arr = _gtfs_time_seconds(r["arrival_time"])
        except (ValueError, IndexError):
            continue
        if dep < after_secs or arr < dep:
            continue
        out.append({
            "trip_id":          r["trip_id"],
            "route_id":         r["route_id"],
            "route_short_name": r["route_short_name"],
            "route_long_name":  r["route_long_name"],
            "route_color":      r["route_color"],
            "route_text_color": r["route_text_color"],
            "trip_headsign":    r["trip_headsign"],
            "direction_id":     r["direction_id"],
            "service_id":       r["service_id"],
            "from_stop_id":     r["from_stop_id"],
            "from_sequence":    r["from_seq"],
            "departure_time":   _normalize_gtfs_time(r["departure_time"]),
            "to_stop_id":       r["to_stop_id"],
            "to_sequence":      r["to_seq"],
            "arrival_time":     _normalize_gtfs_time(r["arrival_time"]),
            "_dep_secs":        dep,
            "_arr_secs":        arr,
        })
    return out


@app.get("/api/journeys")
def journeys(
    start_stop_id: str = Query(..., description="GTFS stop_id of the boarding stop"),
    end_stop_id: str = Query(..., description="GTFS stop_id of the alighting stop"),
    at: Optional[str] = Query(
        default=None,
        description="Departure time: ISO datetime or HH:MM[:SS], e.g. "
                    "'2026-10-26T08:30' or '08:30'. Defaults to now.",
    ),
    max_transfers: int = Query(
        default=1, ge=0, le=2,
        description="0 = direct only, 1 = allow one transfer (default), 2 = up to two.",
    ),
    min_transfer_seconds: int = Query(
        default=120, ge=0, le=3600,
        description="Minimum time needed to change vehicles at a transfer stop.",
    ),
    max_transfer_wait_seconds: int = Query(
        default=3600, ge=60, le=21600,
        description="Longest acceptable wait at a transfer stop.",
    ),
    limit: int = Query(default=5, ge=1, le=50,
                       description="Max journeys to return (fastest first)."),
    mode: str = Query(
        default=DEFAULT_MODE,
        description="Which service to query: 'transit' (bus), 'ferry' "
                    "(water) or 'rail'. All are served by this one API; the "
                    "endpoint paths are identical.",
    ),
):
    """
    Plan journeys from *start_stop_id* to *end_stop_id*.

    Returns the fastest options first, each as a list of legs:

    - ``max_transfers=0`` → direct trips only
    - ``max_transfers=1`` → direct + one-transfer journeys (default)
    - ``max_transfers=2`` → also two-transfer journeys

    Each leg carries the route, the boarding/alighting stops, and the
    departure/arrival times. Transfer legs include the wait time and the
    transfer stop, so the client can render "change at X, wait N min".
    """
    # ── Resolve the requested time ───────────────────────────────────────────
    now = datetime.now()
    if at is None or not at.strip():
        on_date = now.date()
        from_secs = now.hour * 3600 + now.minute * 60 + now.second
    else:
        raw = at.strip()
        parsed_dt = None
        try:
            parsed_dt = _parse_iso_datetime(raw)
        except ValueError:
            parsed_dt = None
        if parsed_dt is not None:
            on_date = parsed_dt.date()
            from_secs = parsed_dt.hour * 3600 + parsed_dt.minute * 60 + parsed_dt.second
        else:
            parts = raw.split(":")
            try:
                if len(parts) == 2:
                    from_secs = int(parts[0]) * 3600 + int(parts[1]) * 60
                elif len(parts) == 3:
                    from_secs = (int(parts[0]) * 3600 + int(parts[1]) * 60
                                 + int(parts[2]))
                else:
                    raise ValueError
            except (ValueError, IndexError):
                raise HTTPException(
                    status_code=422,
                    detail="'at' must be an ISO datetime or HH:MM[:SS]",
                )
            on_date = now.date()

    with _db(mode) as conn:
        start_row = conn.execute(
            "SELECT stop_id, stop_name FROM stops WHERE stop_id = ?", (start_stop_id,)
        ).fetchone()
        if start_row is None:
            raise HTTPException(status_code=404,
                                detail=f"start_stop_id '{start_stop_id}' not found")
        end_row = conn.execute(
            "SELECT stop_id, stop_name FROM stops WHERE stop_id = ?", (end_stop_id,)
        ).fetchone()
        if end_row is None:
            raise HTTPException(status_code=404,
                                detail=f"end_stop_id '{end_stop_id}' not found")

        if start_stop_id == end_stop_id:
            raise HTTPException(status_code=422,
                                detail="start_stop_id and end_stop_id must differ")

        active = sorted(_active_services(conn, on_date))
        if not active:
            return {
                "start_stop_id": start_stop_id,
                "start_stop_name": start_row["stop_name"],
                "end_stop_id": end_stop_id,
                "end_stop_name": end_row["stop_name"],
                "date": on_date.isoformat(),
                "depart_after": _secs_to_hhmm(from_secs),
                "max_transfers": max_transfers,
                "journeys": [],
                "note": "No active services on this date",
            }

        stop_names: dict = {start_stop_id: start_row["stop_name"],
                            end_stop_id: end_row["stop_name"]}

        # A parent station has no stop_times of its own in some feeds (e.g. the
        # rail feed records them on platforms), so plan against its platforms.
        start_ids = _station_stop_ids(conn, start_stop_id)
        end_ids   = _station_stop_ids(conn, end_stop_id)

        def name_of(stop_id: str) -> str:
            if stop_id not in stop_names:
                r = conn.execute(
                    "SELECT stop_name FROM stops WHERE stop_id = ?", (stop_id,)
                ).fetchone()
                stop_names[stop_id] = r["stop_name"] if r else stop_id
            return stop_names[stop_id]

        candidates: list = []

        # ── Direct journeys ──────────────────────────────────────────────────
        for d in _direct_journeys(conn, start_ids, end_ids,
                                  active, from_secs, limit * 20):
            candidates.append({
                "transfers":    0,
                "departure":    d["_dep_secs"],
                "arrival":      d["_arr_secs"],
                "duration":     d["_arr_secs"] - d["_dep_secs"],
                "legs": [{
                    "type":             "ride",
                    "route_id":         d["route_id"],
                    "route_short_name": d["route_short_name"],
                    "route_long_name":  d["route_long_name"],
                    "route_color":      d["route_color"],
                    "route_text_color": d["route_text_color"],
                    "trip_id":          d["trip_id"],
                    "trip_headsign":    d["trip_headsign"],
                    "direction_id":     d["direction_id"],
                    "from_stop_id":     d["from_stop_id"],
                    "from_stop_name":   name_of(d["from_stop_id"]),
                    "departure_time":   d["departure_time"],
                    "to_stop_id":       d["to_stop_id"],
                    "to_stop_name":     name_of(d["to_stop_id"]),
                    "arrival_time":     d["arrival_time"],
                }],
            })

        # ── One-transfer journeys ────────────────────────────────────────────
        if max_transfers >= 1 and len(candidates) < limit * 4:
            # Legs leaving the start stop, then legs arriving at the end stop.
            first_legs = _journey_legs_from_stop(
                conn, start_ids, active, from_secs, 400, set()
            )
            # Onward legs from each reachable transfer stop are fetched lazily
            # and cached, so a busy feed does not explode the query count.
            onward_cache: dict = {}

            def onward(stop_id: str) -> list:
                if stop_id not in onward_cache:
                    onward_cache[stop_id] = _journey_legs_from_stop(
                        conn, _station_stop_ids(conn, stop_id), active, 0, 400, set()
                    )
                return onward_cache[stop_id]

            # One batched query: every trip that calls at the end stop, with
            # the boarding sequence and arrival time. This replaces a per-leg
            # query and keeps the transfer search to a single round-trip.
            end_ph = ",".join("?" * len(end_ids))
            reach_trips = conn.execute(
                f"""
                SELECT t.trip_id, st.stop_sequence AS end_seq,
                       st.arrival_time AS end_arrival
                FROM stop_times st
                JOIN trips t ON t.trip_id = st.trip_id
                WHERE st.stop_id IN ({end_ph})
                  AND t.service_id IN ({",".join("?" * len(active))})
                """,
                [*end_ids, *active],
            ).fetchall()

            # trip_id -> [(end_seq, end_arrival_secs)]
            end_hits: dict = {}
            for r in reach_trips:
                try:
                    arr_secs = _gtfs_time_seconds(r["end_arrival"])
                    end_seq = int(r["end_seq"])
                except (ValueError, TypeError):
                    continue
                end_hits.setdefault(r["trip_id"], []).append((end_seq, arr_secs))

            seen_pairs: set = set()
            for leg1 in first_legs:
                if leg1["to_stop_id"] in end_ids:
                    continue  # direct, already handled
                if leg1["to_stop_id"] in start_ids:
                    continue
                earliest = leg1["_arr_secs"] + min_transfer_seconds
                # Only trips that actually reach the end stop are candidates.
                for leg2 in onward(leg1["to_stop_id"]):
                    if leg2["trip_id"] == leg1["trip_id"]:
                        continue
                    if leg2["_dep_secs"] < earliest:
                        continue
                    if leg2["_dep_secs"] - leg1["_arr_secs"] > max_transfer_wait_seconds:
                        continue
                    hits = end_hits.get(leg2["trip_id"])
                    if not hits:
                        continue
                    # End stop must come after the boarding sequence.
                    arr = None
                    for end_seq, end_secs in hits:
                        if end_seq > int(leg2["from_sequence"]) and end_secs >= leg2["_dep_secs"]:
                            if arr is None or end_secs < arr:
                                arr = end_secs
                    if arr is None:
                        continue
                    key = (leg1["trip_id"], leg2["trip_id"])
                    if key in seen_pairs:
                        continue
                    seen_pairs.add(key)

                    wait = leg2["_dep_secs"] - leg1["_arr_secs"]
                    transfer_stop = leg1["to_stop_id"]
                    candidates.append({
                        "transfers": 1,
                        "departure": leg1["_dep_secs"],
                        "arrival":   arr,
                        "duration":  arr - leg1["_dep_secs"],
                        "legs": [
                            {
                                "type":             "ride",
                                "route_id":         leg1["route_id"],
                                "route_short_name": leg1["route_short_name"],
                                "route_long_name":  leg1["route_long_name"],
                                "route_color":      leg1["route_color"],
                                "route_text_color": leg1["route_text_color"],
                                "trip_id":          leg1["trip_id"],
                                "trip_headsign":    leg1["trip_headsign"],
                                "direction_id":     leg1["direction_id"],
                                "from_stop_id":     leg1["from_stop_id"],
                                "from_stop_name":   name_of(leg1["from_stop_id"]),
                                "departure_time":   leg1["departure_time"],
                                "to_stop_id":       transfer_stop,
                                "to_stop_name":     name_of(transfer_stop),
                                "arrival_time":     leg1["arrival_time"],
                            },
                            {
                                "type":           "transfer",
                                "stop_id":        transfer_stop,
                                "stop_name":      name_of(transfer_stop),
                                "wait_seconds":   wait,
                                "wait_minutes":   round(wait / 60.0, 1),
                                "arrive_time":    leg1["arrival_time"],
                                "depart_time":    leg2["departure_time"],
                                "min_transfer_seconds": min_transfer_seconds,
                            },
                            {
                                "type":             "ride",
                                "route_id":         leg2["route_id"],
                                "route_short_name": leg2["route_short_name"],
                                "route_long_name":  leg2["route_long_name"],
                                "route_color":      leg2["route_color"],
                                "route_text_color": leg2["route_text_color"],
                                "trip_id":          leg2["trip_id"],
                                "trip_headsign":    leg2["trip_headsign"],
                                "direction_id":     leg2["direction_id"],
                                "from_stop_id":     leg2["from_stop_id"],
                                "from_stop_name":   name_of(leg2["from_stop_id"]),
                                "departure_time":   leg2["departure_time"],
                                "to_stop_id":       leg2["to_stop_id"],
                                "to_stop_name":     name_of(leg2["to_stop_id"]),
                                "arrival_time":     _secs_to_hhmmss(arr),
                            },
                        ],
                    })

        # ── Rank: fewest transfers, then earliest arrival ────────────────────
        candidates.sort(key=lambda j: (j["transfers"], j["arrival"]))
        seen = set()
        result = []
        for j in candidates:
            sig = tuple((l.get("trip_id"), l.get("type")) for l in j["legs"])
            if sig in seen:
                continue
            seen.add(sig)
            j["departure_time"] = _secs_to_hhmmss(j["departure"])
            j["arrival_time"] = _secs_to_hhmmss(j["arrival"])
            j["duration_minutes"] = round(j["duration"] / 60.0, 1)
            leg_dates = sorted({_gtfs_service_date(on_date, l["departure_time"])
                                for l in j["legs"] if l.get("departure_time")})
            if leg_dates:
                j["departure_date"] = leg_dates[0].isoformat()
                j["arrival_date"] = leg_dates[-1].isoformat()
            for l in j["legs"]:
                l.pop("_dep_secs", None)
                l.pop("_arr_secs", None)
            result.append(j)
            if len(result) >= limit:
                break

    return {
        "start_stop_id":   start_stop_id,
        "start_stop_name": start_row["stop_name"],
        "end_stop_id":     end_stop_id,
        "end_stop_name":   end_row["stop_name"],
        "date":            on_date.isoformat(),
        "depart_after":    _secs_to_hhmm(from_secs),
        "max_transfers":   max_transfers,
        "min_transfer_seconds": min_transfer_seconds,
        "max_transfer_wait_seconds": max_transfer_wait_seconds,
        "journeys":        result,
    }


def _gtfs_stop_id_from_osm(conn: sqlite3.Connection, osm_node_id: int) -> str:
    """
    Resolve an OSM node id to its matched GTFS stop_id via stop_osm_mapping.
    Raises HTTP 404 if the node is unknown or has no matched GTFS stop.
    """
    row = conn.execute(
        "SELECT stop_id FROM stop_osm_mapping "
        "WHERE osm_node_id = ? AND match_status = 'matched'",
        (osm_node_id,),
    ).fetchone()
    if row is None:
        # Also check whether the node exists at all in osm_stops
        exists = conn.execute(
            "SELECT 1 FROM osm_stops WHERE osm_node_id = ?", (osm_node_id,)
        ).fetchone()
        if exists is None:
            raise HTTPException(
                status_code=404,
                detail=f"OSM node {osm_node_id} not found in the database.",
            )
        raise HTTPException(
            status_code=404,
            detail=(
                f"OSM node {osm_node_id} exists but has no matched GTFS stop "
                "(it was outside the max-distance threshold when the DB was built)."
            ),
        )
    return row["stop_id"]


# ── Stop-ID direct lookup ─────────────────────────────────────────────────────

@app.get("/api/osm-stops/{stop_id}/routes")
def osm_stop_routes_direct(
    stop_id: str,
    mode: str = Query(
        default=DEFAULT_MODE,
        description="Which service to query: 'transit' (bus), 'ferry' "
                    "(water) or 'rail'. All are served by this one API; the "
                    "endpoint paths are identical.",
    ),
):
    """
    All distinct routes that serve *stop_id* (GTFS stop_id passed directly).
    """
    with _db(mode) as conn:
        stop_row = conn.execute(
            "SELECT stop_id, stop_name FROM stops WHERE stop_id = ?", (stop_id,)
        ).fetchone()
        if stop_row is None:
            raise HTTPException(status_code=404, detail=f"stop_id '{stop_id}' not found")

        routes = conn.execute(
            """
            SELECT
                r.route_id,
                r.route_short_name,
                r.route_long_name,
                r.route_color,
                r.route_text_color,
                r.route_type,
                r.route_url,
                t.trip_headsign
            FROM  stop_times st
            JOIN  trips      t  ON st.trip_id = t.trip_id
            JOIN  routes     r  ON t.route_id = r.route_id
            WHERE st.stop_id = ?
            GROUP BY r.route_id, t.trip_headsign
            ORDER BY r.route_short_name
            """,
            (stop_id,),
        ).fetchall()

    # Group headsigns per route
    routes_map: dict = {}
    for r in routes:
        rid = r["route_id"]
        if rid not in routes_map:
            routes_map[rid] = {
                "route_id":         r["route_id"],
                "route_short_name": r["route_short_name"],
                "route_long_name":  r["route_long_name"],
                "route_color":      r["route_color"],
                "route_text_color": r["route_text_color"],
                "route_type":       r["route_type"],
                "route_url":        r["route_url"],
                "headsigns":        [],
            }
        hs = r["trip_headsign"]
        if hs and hs not in routes_map[rid]["headsigns"]:
            routes_map[rid]["headsigns"].append(hs)

    return {
        "stop_id":   stop_row["stop_id"],
        "stop_name": stop_row["stop_name"],
        "routes":    list(routes_map.values()),
    }


@app.get("/api/osm-stops/{osm_node_id}/next-departures")
def osm_stop_next_departures(
    osm_node_id: int,
    at: Optional[str] = Query(
        default=None,
        description="ISO datetime or time, e.g. '2026-04-12T14:30' or '14:30'. Defaults to now.",
    ),
    limit: int = Query(default=10, ge=1, le=100),
    mode: str = Query(
        default=DEFAULT_MODE,
        description="Which service to query: 'transit' (bus), 'ferry' "
                    "(water) or 'rail'. All are served by this one API; the "
                    "endpoint paths are identical.",
    ),
):
    """
    Next departures for the GTFS stop matched to *osm_node_id*.
    Accepts the same *at* and *limit* parameters as /api/stops/{stop_id}/next-departures.
    """
    with _db(mode) as conn:
        stop_id = _gtfs_stop_id_from_osm(conn, osm_node_id)

    # Reuse the existing endpoint logic by calling it directly
    return next_departures(stop_id=stop_id, at=at, limit=limit)



# ── Route Details ─────────────────────────────────────────────────────────────

@app.get("/api/routes/{route_id}")
def route_detail(
    route_id: str,
    region: Optional[str] = Query(default=None, description="Optional region / database override"),
    region_id: Optional[str] = Query(default=None, description="Alias for region identifier"),
    regionId: Optional[str] = Query(default=None, description="Alias for region identifier (camelCase)"),
    mode: str = Query(default=DEFAULT_MODE),
):
    """
    Return comprehensive route metadata and details for *route_id*.
    Accepts 'region', 'region_id', or 'regionId' to query a specific database;
    if omitted, automatically locates the database containing the route across all available datasets.
    """
    db_path = _find_db_for_route(route_id, region=region, region_id=region_id, regionId=regionId, mode=mode)
    with _open_db(db_path) as conn:
        r = conn.execute(
            """
            SELECT route_id, agency_id, route_short_name, route_long_name,
                   route_type, route_color, route_text_color, route_url
            FROM routes
            WHERE route_id = ?
            """,
            (route_id,),
        ).fetchone()
        if r is None:
            raise HTTPException(status_code=404, detail=f"route_id '{route_id}' not found in database '{db_path.name}'")

        rtype = int(r["route_type"]) if r["route_type"] is not None else 3

        trip_stats = conn.execute(
            """
            SELECT COUNT(trip_id) AS trip_count,
                   COUNT(DISTINCT shape_id) AS shape_count
            FROM trips
            WHERE route_id = ?
            """,
            (route_id,),
        ).fetchone()

        headsign_rows = conn.execute(
            """
            SELECT DISTINCT trip_headsign
            FROM trips
            WHERE route_id = ? AND trip_headsign IS NOT NULL AND TRIM(trip_headsign) != ''
            ORDER BY trip_headsign
            """,
            (route_id,),
        ).fetchall()
        headsigns = [h["trip_headsign"].strip() for h in headsign_rows]

        rep_trip = conn.execute(
            "SELECT trip_id FROM trips WHERE route_id = ? ORDER BY trip_id LIMIT 1",
            (route_id,),
        ).fetchone()

        start_stop = None
        end_stop = None
        stop_count = 0
        if rep_trip:
            stop_rows = conn.execute(
                """
                SELECT s.stop_name
                FROM stop_times st
                JOIN stops s ON s.stop_id = st.stop_id
                WHERE st.trip_id = ?
                ORDER BY CAST(st.stop_sequence AS INTEGER)
                """,
                (rep_trip["trip_id"],),
            ).fetchall()
            stop_count = len(stop_rows)
            if stop_rows:
                start_stop = stop_rows[0]["stop_name"]
                end_stop = stop_rows[-1]["stop_name"]

        return {
            "route_id": r["route_id"],
            "agency_id": r["agency_id"],
            "route_short_name": r["route_short_name"],
            "route_long_name": r["route_long_name"],
            "route_type": rtype,
            "route_type_name": _route_type_to_mode(rtype),
            "route_color": r["route_color"],
            "route_text_color": r["route_text_color"],
            "route_url": r["route_url"],
            "trip_count": trip_stats["trip_count"] if trip_stats else 0,
            "shape_count": trip_stats["shape_count"] if trip_stats else 0,
            "start_stop_name": start_stop,
            "end_stop_name": end_stop,
            "stops_count": stop_count,
            "in_between_stops": max(0, stop_count - 2),
            "headsigns": headsigns,
            "database": db_path.name,
        }


# ── Stops for a route ────────────────────────────────────────────────────────

@app.get("/api/routes/{route_id}/stops")
def route_stops(
    route_id: str,
    direction_id: Optional[int] = Query(
        default=None,
        description="Filter by direction: 0 or 1. Omit to return all directions.",
    ),
    lang: Optional[str] = _LANG_QUERY,
    region: Optional[str] = Query(default=None, description="Optional region / database override"),
    region_id: Optional[str] = Query(default=None, description="Alias for region identifier"),
    regionId: Optional[str] = Query(default=None, description="Alias for region identifier (camelCase)"),
    mode: str = Query(
        default=DEFAULT_MODE,
        description="Which service to query: 'transit' (bus), 'ferry' "
                    "(water) or 'rail'. All are served by this one API; the "
                    "endpoint paths are identical.",
    ),
):
    """
    Return the ordered stops for *route_id*, derived from a representative trip.
    Supports 'region', 'region_id', or 'regionId' parameter; automatically
    locates the correct database across all available datasets if omitted.
    """
    db_path = _find_db_for_route(route_id, region=region, region_id=region_id, regionId=regionId, mode=mode)
    with _open_db(db_path) as conn:
        # Verify route exists
        route_row = conn.execute(
            "SELECT route_id, route_short_name, route_long_name, "
            "route_color, route_text_color FROM routes WHERE route_id = ?",
            (route_id,),
        ).fetchone()
        if route_row is None:
            raise HTTPException(status_code=404, detail=f"route_id '{route_id}' not found")

        stop_tr = _get_stop_translations(conn, lang or "en")
        trip_tr = _get_trip_translations(conn, lang or "en")

        # Find one representative trip per direction (smallest trip_id lexicographically)
        dir_filter = ""
        params: list = [route_id]
        if direction_id is not None:
            dir_filter = "AND t.direction_id = ?"
            params.append(direction_id)

        trip_rows = conn.execute(
            f"""
            SELECT MIN(t.trip_id) AS trip_id, t.direction_id, t.trip_headsign, t.shape_id
            FROM trips t
            WHERE t.route_id = ? {dir_filter}
            GROUP BY t.direction_id, t.trip_headsign, t.shape_id
            ORDER BY t.direction_id, t.shape_id
            """,
            params,
        ).fetchall()

        if not trip_rows:
            raise HTTPException(
                status_code=404,
                detail=f"No trips found for route_id '{route_id}'"
                + (f" with direction_id={direction_id}" if direction_id is not None else ""),
            )

        directions = []
        for trip_row in trip_rows:
            stop_rows = conn.execute(
                """
                SELECT
                    CAST(st.stop_sequence AS INTEGER) AS stop_sequence,
                    st.stop_id,
                    s.stop_name,
                    s.stop_code,
                    CAST(s.stop_lat AS REAL) AS lat,
                    CAST(s.stop_lon AS REAL) AS lon
                FROM stop_times st
                JOIN stops s ON st.stop_id = s.stop_id
                WHERE st.trip_id = ?
                ORDER BY CAST(st.stop_sequence AS INTEGER)
                """,
                (trip_row["trip_id"],),
            ).fetchall()
            dir_id = trip_row["direction_id"]
            segment = "outbound" if str(dir_id) == "0" else "inbound"

            # Translate trip headsign
            trip_headsign = trip_row["trip_headsign"]
            if trip_tr:
                trip_headsign = trip_tr.get(trip_row["trip_id"], trip_headsign)

            # Translate stops
            stops_list = []
            for r in stop_rows:
                sd = dict(r)
                if stop_tr:
                    sd["stop_name"] = stop_tr.get(r["stop_id"], r["stop_name"])
                stops_list.append(sd)

            directions.append({
                "direction_id":  dir_id,
                "segment":       segment,
                "trip_headsign": trip_headsign,
                "shape_id":      trip_row["shape_id"],
                "stops":         stops_list,
            })

        # Sort: outbound first, then inbound; within each segment longest variant first
        directions.sort(key=lambda d: (0 if d["segment"] == "outbound" else 1, -len(d["stops"])))

    return {
        "route_id":         route_row["route_id"],
        "route_short_name": route_row["route_short_name"],
        "route_long_name":  route_row["route_long_name"],
        "route_color":      route_row["route_color"],
        "route_text_color": route_row["route_text_color"],
        "directions":       directions,
    }


# ── Next departure by route + stop ────────────────────────────────────────────

@app.get("/api/routes/{route_id}/stops/{stop_id}/next-departures")
def route_stop_next_departures(
    route_id: str,
    stop_id: str,
    at: Optional[str] = Query(
        default=None,
        description=(
            "Filter time. Use 'now' or omit for the current moment, "
            "or pass an ISO datetime/time, e.g. '2026-04-12T14:30' or '14:30'."
        ),
    ),
    limit: int = Query(default=10, ge=1, le=100, description="Max results to return"),
    region: Optional[str] = Query(default=None, description="Optional region / database override"),
    region_id: Optional[str] = Query(default=None, description="Alias for region identifier"),
    regionId: Optional[str] = Query(default=None, description="Alias for region identifier (camelCase)"),
    mode: str = Query(
        default=DEFAULT_MODE,
        description="Which service to query: 'transit' (bus), 'ferry' "
                    "(water) or 'rail'. All are served by this one API; the "
                    "endpoint paths are identical.",
    ),
):
    """
    Return the next *limit* departures for a specific *route_id* at *stop_id*.

    - ``at=now`` (or omit) → departures from right now.
    - ``at=HH:MM`` → departures from that wall-clock time today.
    - ``at=YYYY-MM-DDTHH:MM`` → departures from that exact date/time.
    """
    # ── Parse `at` ──────────────────────────────────────────────────────────
    at_clean = (at or "now").strip().lower()
    if at_clean == "now" or at_clean == "":
        now        = datetime.now()
        on_date    = now.date()
        from_secs  = now.hour * 3600 + now.minute * 60 + now.second
    else:
        parsed    = None
        date_only = False
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M", "%Y-%m-%d", "%H:%M:%S", "%H:%M"):
            try:
                parsed = datetime.strptime(at_clean, fmt)
                date_only = (fmt == "%Y-%m-%d")
                break
            except ValueError:
                pass
        if parsed is None:
            raise HTTPException(
                status_code=422,
                detail=(
                    "'at' must be 'now', a date (YYYY-MM-DD), an ISO datetime (YYYY-MM-DDTHH:MM), "
                    "or a time (HH:MM)"
                ),
            )
        on_date   = parsed.date() if parsed.year != 1900 else date.today()
        # Date-only → return the full day starting from midnight
        from_secs = 0 if date_only else (parsed.hour * 3600 + parsed.minute * 60 + parsed.second)

    db_path = _find_db_for_route(route_id, region=region, region_id=region_id, regionId=regionId, mode=mode)
    with _open_db(db_path) as conn:
        # Verify route exists
        route_row = conn.execute(
            "SELECT route_id, route_short_name, route_long_name, "
            "route_color, route_text_color FROM routes WHERE route_id = ?",
            (route_id,),
        ).fetchone()
        if route_row is None:
            raise HTTPException(status_code=404, detail=f"route_id '{route_id}' not found")

        # Verify stop exists
        stop_row = conn.execute(
            "SELECT stop_id, stop_name FROM stops WHERE stop_id = ?", (stop_id,)
        ).fetchone()
        if stop_row is None:
            raise HTTPException(status_code=404, detail=f"stop_id '{stop_id}' not found")

        active = _active_services(conn, on_date)
        if not active:
            return {
                "route_id":   route_id,
                "stop_id":    stop_id,
                "stop_name":  stop_row["stop_name"],
                "date":       on_date.isoformat(),
                "from_time":  _secs_to_hhmm(from_secs),
                "departures": [],
                "note":       "No active services on this date",
            }

        placeholders = ",".join("?" * len(active))
        rows = conn.execute(
            f"""
            SELECT
                st.departure_time,
                st.arrival_time,
                st.stop_sequence,
                st.stop_headsign,
                t.trip_id,
                t.trip_headsign,
                t.direction_id,
                r.route_id,
                r.route_short_name,
                r.route_long_name,
                r.route_color,
                r.route_text_color
            FROM  stop_times st
            JOIN  trips  t ON st.trip_id  = t.trip_id
            JOIN  routes r ON t.route_id  = r.route_id
            WHERE st.stop_id   = ?
              AND r.route_id   = ?
              AND t.service_id IN ({placeholders})
            ORDER BY st.departure_time
            """,
            (stop_id, route_id, *active),
        ).fetchall()

    # Verify the route actually serves this stop
    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"route_id '{route_id}' does not serve stop_id '{stop_id}' on {on_date.isoformat()}",
        )

    departures = []
    for r in rows:
        try:
            dep_secs = _gtfs_time_seconds(r["departure_time"])
        except (ValueError, IndexError):
            continue
        if dep_secs >= from_secs:
            d = dict(r)
            d["departure_seconds"] = dep_secs
            departures.append(d)
        if len(departures) >= limit:
            break

    # Fetch start/end stop names for each trip in one batch query
    if departures:
        trip_ids = list({d["trip_id"] for d in departures})
        placeholders2 = ",".join("?" * len(trip_ids))
        with _db(mode) as conn2:
            terminal_rows = conn2.execute(
                f"""
                SELECT
                    st.trip_id,
                    s_first.stop_name    AS start_stop_name,
                    s_last.stop_name     AS end_stop_name,
                    st_last.arrival_time AS end_time
                FROM (
                    SELECT trip_id,
                           MIN(CAST(stop_sequence AS INTEGER)) AS first_seq,
                           MAX(CAST(stop_sequence AS INTEGER)) AS last_seq
                    FROM stop_times
                    WHERE trip_id IN ({placeholders2})
                    GROUP BY trip_id
                ) seqs
                JOIN stop_times st      ON st.trip_id = seqs.trip_id
                                       AND CAST(st.stop_sequence AS INTEGER) = seqs.first_seq
                JOIN stops s_first      ON s_first.stop_id = st.stop_id
                JOIN stop_times st_last ON st_last.trip_id = seqs.trip_id
                                       AND CAST(st_last.stop_sequence AS INTEGER) = seqs.last_seq
                JOIN stops s_last       ON s_last.stop_id = st_last.stop_id
                """,
                trip_ids,
            ).fetchall()

        trip_terminals: dict = {
            tr["trip_id"]: {
                "start_stop_name": tr["start_stop_name"],
                "end_stop_name":   tr["end_stop_name"],
                "end_time":        tr["end_time"],
            }
            for tr in terminal_rows
        }

        for d in departures:
            terminals = trip_terminals.get(d["trip_id"], {})
            d["start_stop_name"] = terminals.get("start_stop_name")
            d["end_stop_name"]   = terminals.get("end_stop_name")
            end_t = terminals.get("end_time")
            d["end_time"]        = _normalize_gtfs_time(end_t) if end_t else None
            # This query does not select departure_date; the service date is
            # the requested date, so normalise it onto the row for the API shape.
            d["departure_date"] = on_date.isoformat()
            if end_t:
                end_date = _gtfs_service_date(
                    on_date,
                    end_t,
                )
                d["end_date"] = end_date.isoformat()
            else:
                d["end_date"] = None

    return {
        "route_id":         route_id,
        "route_short_name": route_row["route_short_name"],
        "route_long_name":  route_row["route_long_name"],
        "stop_id":          stop_id,
        "stop_name":        stop_row["stop_name"],
        "date":             on_date.isoformat(),
        "from_time":        _secs_to_hhmm(from_secs),
        "departures":       departures,
    }


# ── Next departures for an entire route (all stops) ───────────────────────────

@app.get("/api/routes/{route_id}/next-departures")
def route_next_departures(
    route_id: str,
    at: Optional[str] = Query(
        default=None,
        description=(
            "Filter time. Use 'now' or omit for the current moment, "
            "or pass an ISO datetime/time, e.g. '2026-04-13T11:16' or '11:16'."
        ),
    ),
    limit: int = Query(default=10, ge=1, le=100, description="Max results to return"),
    region: Optional[str] = Query(default=None, description="Optional region / database override"),
    region_id: Optional[str] = Query(default=None, description="Alias for region identifier"),
    regionId: Optional[str] = Query(default=None, description="Alias for region identifier (camelCase)"),
    mode: str = Query(
        default=DEFAULT_MODE,
        description="Which service to query: 'transit' (bus), 'ferry' "
                    "(water) or 'rail'. All are served by this one API; the "
                    "endpoint paths are identical.",
    ),
):
    """
    Return the next *limit* departures across all stops for *route_id*.

    - ``at=now`` (or omit) → departures from right now.
    - ``at=HH:MM`` → departures from that wall-clock time today.
    - ``at=YYYY-MM-DDTHH:MM`` → departures from that exact date/time.
    """
    # ── Parse `at` ──────────────────────────────────────────────────────────
    at_clean = (at or "now").strip().lower()
    if at_clean == "now" or at_clean == "":
        now       = datetime.now()
        on_date   = now.date()
        from_secs = now.hour * 3600 + now.minute * 60 + now.second
    else:
        parsed    = None
        date_only = False
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M", "%Y-%m-%d", "%H:%M:%S", "%H:%M"):
            try:
                parsed = datetime.strptime(at_clean, fmt)
                date_only = (fmt == "%Y-%m-%d")
                break
            except ValueError:
                pass
        if parsed is None:
            raise HTTPException(
                status_code=422,
                detail=(
                    "'at' must be 'now', a date (YYYY-MM-DD), an ISO datetime (YYYY-MM-DDTHH:MM), "
                    "or a time (HH:MM)"
                ),
            )
        on_date   = parsed.date() if parsed.year != 1900 else date.today()
        from_secs = 0 if date_only else (parsed.hour * 3600 + parsed.minute * 60 + parsed.second)

    db_path = _find_db_for_route(route_id, region=region, region_id=region_id, regionId=regionId, mode=mode)
    with _open_db(db_path) as conn:
        # Verify route exists
        route_row = conn.execute(
            "SELECT route_id, route_short_name, route_long_name, "
            "route_color, route_text_color FROM routes WHERE route_id = ?",
            (route_id,),
        ).fetchone()
        if route_row is None:
            raise HTTPException(status_code=404, detail=f"route_id '{route_id}' not found")

        active = _active_services(conn, on_date)
        if not active:
            return {
                "route_id":         route_id,
                "route_short_name": route_row["route_short_name"],
                "route_long_name":  route_row["route_long_name"],
                "date":             on_date.isoformat(),
                "from_time":        _secs_to_hhmm(from_secs),
                "departures":       [],
                "note":             "No active services on this date",
            }

        placeholders = ",".join("?" * len(active))
        rows = conn.execute(
            f"""
            SELECT
                st.stop_id,
                s.stop_name,
                CAST(s.stop_lat AS REAL) AS stop_lat,
                CAST(s.stop_lon AS REAL) AS stop_lon,
                st.departure_time,
                st.arrival_time,
                st.stop_sequence,
                st.stop_headsign,
                t.trip_id,
                t.trip_headsign,
                t.direction_id,
                r.route_id,
                r.route_short_name,
                r.route_long_name,
                r.route_color,
                r.route_text_color
            FROM  stop_times st
            JOIN  trips  t ON st.trip_id = t.trip_id
            JOIN  routes r ON t.route_id = r.route_id
            JOIN  stops  s ON st.stop_id = s.stop_id
            WHERE r.route_id   = ?
              AND t.service_id IN ({placeholders})
            ORDER BY st.departure_time
            """,
            (route_id, *active),
        ).fetchall()

    departures = []
    for r in rows:
        try:
            dep_secs = _gtfs_time_seconds(r["departure_time"])
        except (ValueError, IndexError):
            continue
        if dep_secs >= from_secs:
            d = dict(r)
            d["departure_seconds"] = dep_secs
            departures.append(d)
        if len(departures) >= limit:
            break

    return {
        "route_id":         route_id,
        "route_short_name": route_row["route_short_name"],
        "route_long_name":  route_row["route_long_name"],
        "date":             on_date.isoformat(),
        "from_time":        _secs_to_hhmm(from_secs),
        "departures":       departures,
    }


# ── Shapes for all routes through a stop ─────────────────────────────────────

# ── Stop events: departures (upcoming + past) and arrivals ───────────────────

def _stop_events_rows(
    conn: sqlite3.Connection,
    stop_ids: list,
    services: list,
    service_date: date,
    day_offset: int,
    time_col: str,
    route_filter: Optional[str],
) -> list:
    """
    One row per stop_time at any of *stop_ids* for the given services.

    *stop_ids* normally holds one id, but a parent station (location_type=1)
    is expanded to its child platforms by the caller.

    *time_col* is 'departure_time' (departures) or 'arrival_time' (arrivals).
    *day_offset* shifts the service date (e.g. -1 for yesterday's >24h trips).

    Terminal stops are excluded per platform: a departure must not be the
    trip's last stop (the vehicle continues, so it can be boarded) and an
    arrival must not be the trip's first stop (the vehicle came from
    somewhere). This is evaluated on the event's own stop_id, not the parent
    station, so a trip ending at platform PFJ2 is still a valid arrival at
    platform PFJ1.
    """
    if not services or not stop_ids:
        return []
    ph = ",".join("?" * len(services))
    ph_stops = ",".join("?" * len(stop_ids))
    route_sql = ""
    params: list = [*stop_ids, *services]
    if route_filter:
        route_sql = "AND r.route_short_name = ? COLLATE NOCASE"
        params.append(route_filter)

    terminal_sql = (
        "AND st.stop_sequence > trip_bounds.first_seq"
        if time_col == "arrival_time"
        else "AND st.stop_sequence < trip_bounds.last_seq"
    )

    rows = conn.execute(
        f"""
        SELECT st.stop_id          AS event_stop_id,
               st.{time_col}        AS event_time,
               st.arrival_time      AS arrival_time,
               st.departure_time    AS departure_time,
               st.stop_sequence,
               st.stop_headsign,
               t.trip_id,
               t.trip_headsign,
               t.direction_id,
               t.service_id,
               r.route_id,
               r.route_short_name,
               r.route_long_name,
               r.route_color,
               r.route_text_color
        FROM stop_times st
        JOIN trips  t ON t.trip_id  = st.trip_id
        JOIN routes r ON r.route_id = t.route_id
        JOIN (
            SELECT trip_id,
                   MIN(CAST(stop_sequence AS INTEGER)) AS first_seq,
                   MAX(CAST(stop_sequence AS INTEGER)) AS last_seq
            FROM stop_times
            GROUP BY trip_id
        ) trip_bounds ON trip_bounds.trip_id = st.trip_id
        WHERE st.stop_id IN ({ph_stops})
          AND t.service_id IN ({ph})
          {terminal_sql}
          {route_sql}
        ORDER BY st.{time_col}
        """,
        params,
    ).fetchall()

    out = []
    for r in rows:
        try:
            secs = _gtfs_time_seconds(r["event_time"])
        except (ValueError, IndexError):
            continue
        d = dict(r)
        d["_secs"] = secs
        d["_day_offset"] = day_offset
        d["_service_date"] = service_date
        out.append(d)
    return out


@app.get("/api/stops/{stop_id}/events")
def stop_events(
    stop_id: str,
    at: Optional[str] = Query(
        default=None,
        description="Reference time: ISO datetime or HH:MM[:SS]. Defaults to now.",
    ),
    kind: str = Query(
        default="departures",
        description="'departures' (trips you can board here — excludes trips "
                    "that terminate at this stop) or 'arrivals' (trips coming "
                    "into this stop — excludes trips that start here).",
    ),
    when: str = Query(
        default="upcoming",
        description="'upcoming' (at/after *at*), 'past' (before *at*), or "
                    "'all' (both, nearest to *at* first).",
    ),
    route: Optional[str] = Query(
        default=None,
        description="Optional route_short_name filter (exact, case-insensitive).",
    ),
    limit: int = Query(default=20, ge=1, le=200,
                       description="Max events to return."),
    lang: Optional[str] = _LANG_QUERY,
    mode: str = Query(
        default=DEFAULT_MODE,
        description="Which service to query: 'transit' (bus), 'ferry' "
                    "(water) or 'rail'. All are served by this one API; the "
                    "endpoint paths are identical.",
    ),
):
    """
    Departures or arrivals at a single stop, for ferry, rail or transit.

    - ``kind=departures`` + ``when=upcoming`` → next boardable departures
    - ``kind=departures`` + ``when=past``     → departures that already left
    - ``kind=arrivals``   + ``when=upcoming`` → vehicles arriving soon
    - ``kind=arrivals``   + ``when=past``     → arrivals that already happened
    - ``when=all``                            → both, closest to *at* first

    Terminal stops are excluded (per platform stop_id):

    - ``departures`` skips trips whose last stop is this stop — the vehicle
      terminates here, so there is nothing to ride onward.
    - ``arrivals`` skips trips whose first stop is this stop — the vehicle
      originates here, so it is not arriving from anywhere.

    Previous-day services are included for trips that spill past midnight
    (GTFS times >= 24:00:00).

    Every event also carries the trip's origin and destination
    (``start_stop_id``/``start_stop_name``, ``end_stop_id``/``end_stop_name``),
    so clients can show the full ride without a second request.
    """
    kind = (kind or "departures").strip().lower()
    when = (when or "upcoming").strip().lower()
    if kind not in ("departures", "arrivals"):
        raise HTTPException(status_code=422,
                            detail="'kind' must be 'departures' or 'arrivals'")
    if when not in ("upcoming", "past", "all"):
        raise HTTPException(status_code=422,
                            detail="'when' must be 'upcoming', 'past' or 'all'")

    # ── Resolve reference time ───────────────────────────────────────────────
    now = datetime.now()
    if at is None or not at.strip():
        on_date = now.date()
        ref_secs = now.hour * 3600 + now.minute * 60 + now.second
    else:
        raw = at.strip()
        parsed = None
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M", "%Y-%m-%d",
                    "%H:%M:%S", "%H:%M"):
            try:
                parsed = datetime.strptime(raw, fmt)
                break
            except ValueError:
                pass
        if parsed is None:
            raise HTTPException(
                status_code=422,
                detail="'at' must be a date (YYYY-MM-DD), ISO datetime, or time (HH:MM[:SS])",
            )
        on_date = parsed.date() if parsed.year != 1900 else now.date()
        ref_secs = parsed.hour * 3600 + parsed.minute * 60 + parsed.second

    time_col = "departure_time" if kind == "departures" else "arrival_time"

    with _db(mode) as conn:
        stop_row = conn.execute(
            "SELECT stop_id, stop_name FROM stops WHERE stop_id = ?", (stop_id,)
        ).fetchone()
        if stop_row is None:
            raise HTTPException(status_code=404,
                                detail=f"stop_id '{stop_id}' not found")

        # A parent station (location_type=1, e.g. rail "PMB") has no stop_times
        # of its own — its platforms (PMB1, PMB2, ...) do. Expand to children so
        # asking for the station returns every platform's departures. Feeds that
        # omit the optional parent_station/platform_code columns simply have no
        # children.
        stop_cols = {r[1] for r in conn.execute("PRAGMA table_info(stops)")}
        child_rows = []
        if "parent_station" in stop_cols:
            platform_col = ("platform_code" if "platform_code" in stop_cols
                            else "'' AS platform_code")
            child_rows = conn.execute(
                f"SELECT stop_id, stop_name, {platform_col} FROM stops "
                "WHERE parent_station = ? ORDER BY stop_id",
                (stop_id,),
            ).fetchall()
        stop_ids = [stop_id] + [r["stop_id"] for r in child_rows]
        platforms = [
            {"stop_id": r["stop_id"], "stop_name": r["stop_name"],
             "platform_code": r["platform_code"]}
            for r in child_rows
        ]

        prev_date = on_date - timedelta(days=1)
        next_date = on_date + timedelta(days=1)
        active      = sorted(_active_services(conn, on_date))
        active_prev = sorted(_active_services(conn, prev_date))
        active_next = sorted(_active_services(conn, next_date))

        rows: list = []
        rows += _stop_events_rows(conn, stop_ids, active, on_date, 0,
                                  time_col, route)
        # Yesterday's trips that run past midnight (>= 24:00:00)
        for r in _stop_events_rows(conn, stop_ids, active_prev, prev_date, -1,
                                   time_col, route):
            if r["_secs"] >= 86400:
                rows.append(r)
        # Tomorrow's trips (for look-ahead past midnight)
        for r in _stop_events_rows(conn, stop_ids, active_next, next_date, 1,
                                   time_col, route):
            rows.append(r)

        stop_tr = _get_stop_translations(conn, lang or "en")
        trip_tr = _get_trip_translations(conn, lang or "en")

        # Origin/destination of every trip in the payload, resolved in one
        # batched query so each event can report where the ride starts and ends.
        trip_terminals: dict[str, dict] = {}
        trip_ids = sorted({r["trip_id"] for r in rows})
        for i in range(0, len(trip_ids), 500):
            chunk = trip_ids[i:i + 500]
            ph = ",".join("?" * len(chunk))
            for tr in conn.execute(
                f"""
                SELECT st.trip_id,
                       CAST(st.stop_sequence AS INTEGER) AS seq,
                       st.stop_id,
                       s.stop_name,
                       st.departure_time,
                       st.arrival_time
                FROM stop_times st
                JOIN stops s ON s.stop_id = st.stop_id
                JOIN (
                    SELECT trip_id,
                           MIN(CAST(stop_sequence AS INTEGER)) AS first_seq,
                           MAX(CAST(stop_sequence AS INTEGER)) AS last_seq
                    FROM stop_times
                    WHERE trip_id IN ({ph})
                    GROUP BY trip_id
                ) b ON b.trip_id = st.trip_id
                   AND CAST(st.stop_sequence AS INTEGER) IN (b.first_seq, b.last_seq)
                WHERE st.trip_id IN ({ph})
                """,
                [*chunk, *chunk],
            ).fetchall():
                entry = trip_terminals.setdefault(tr["trip_id"], {})
                if "first" not in entry or tr["seq"] < entry["first"]["seq"]:
                    entry["first"] = tr
                if "last" not in entry or tr["seq"] > entry["last"]["seq"]:
                    entry["last"] = tr

        events = []
        for r in rows:
            # Wall-clock seconds relative to the reference date.
            wall = r["_secs"] + r["_day_offset"] * 86400
            if when == "upcoming" and wall < ref_secs:
                continue
            if when == "past" and wall >= ref_secs:
                continue
            service_date = r["_service_date"]
            event_date = _gtfs_service_date(service_date, r["event_time"])
            hs = r["trip_headsign"]
            if trip_tr:
                hs = trip_tr.get(r["trip_id"], hs)
            termin = trip_terminals.get(r["trip_id"], {})
            first = termin.get("first")
            last = termin.get("last")
            event = {
                "route_id":         r["route_id"],
                "route_short_name": r["route_short_name"],
                "route_long_name":  r["route_long_name"],
                "route_color":      r["route_color"],
                "route_text_color": r["route_text_color"],
                "trip_id":          r["trip_id"],
                "service_id":       r["service_id"],
                "trip_headsign":    hs,
                "direction_id":     r["direction_id"],
                "stop_id":          r["event_stop_id"],
                "stop_sequence":    r["stop_sequence"],
                "stop_headsign":    r["stop_headsign"],
                "start_stop_id":    first["stop_id"] if first else None,
                "start_stop_name":  first["stop_name"] if first else None,
                "end_stop_id":      last["stop_id"] if last else None,
                "end_stop_name":    last["stop_name"] if last else None,
                "date":             event_date.isoformat(),
                "time":             _normalize_gtfs_time(r["event_time"]),
                "arrival_time":     _normalize_gtfs_time(r["arrival_time"]),
                "departure_time":   _normalize_gtfs_time(r["departure_time"]),
                "_wall":            wall,
            }
            if stop_tr:
                if first:
                    event["start_stop_name_translated"] = stop_tr.get(
                        first["stop_id"], first["stop_name"])
                if last:
                    event["end_stop_name_translated"] = stop_tr.get(
                        last["stop_id"], last["stop_name"])
            events.append(event)

        if when == "past":
            events.sort(key=lambda e: e["_wall"], reverse=True)
        elif when == "all":
            events.sort(key=lambda e: abs(e["_wall"] - ref_secs))
        else:
            events.sort(key=lambda e: e["_wall"])

        events = events[:limit]
        for e in events:
            e.pop("_wall", None)

    stop_name = stop_row["stop_name"]
    if stop_tr:
        stop_name = stop_tr.get(stop_id, stop_name)

    return {
        "stop_id":    stop_id,
        "stop_name":  stop_name,
        "mode":       mode,
        "platforms":  platforms,
        "kind":       kind,
        "when":       when,
        "excludes_terminus": True,
        "reference":  {
            "date": on_date.isoformat(),
            "time": _secs_to_hhmm(ref_secs),
        },
        "count":      len(events),
        "events":     events,
    }


@app.get("/api/stops/{stop_id}/shapes")
def stop_shapes(
    stop_id: str,
    mode: str = Query(
        default=DEFAULT_MODE,
        description="Which service to query: 'transit' (bus), 'ferry' "
                    "(water) or 'rail'. All are served by this one API; the "
                    "endpoint paths are identical.",
    ),
):
    """
    Return a GeoJSON FeatureCollection of all route shapes that pass through
    *stop_id*.

    - Each route **LineString** feature has route/direction metadata in
      ``properties``.
    - The stop itself is included as a **Point** feature.

    One representative trip per (route, direction) is used to pick the shape.
    Routes without a shape_id have an empty coordinates array.
    """
    with _db(mode) as conn:
        # Verify stop exists
        stop_row = conn.execute(
            "SELECT stop_id, stop_name,"
            " CAST(stop_lat AS REAL) AS lat,"
            " CAST(stop_lon AS REAL) AS lon"
            " FROM stops WHERE stop_id = ?",
            (stop_id,),
        ).fetchone()
        if stop_row is None:
            raise HTTPException(status_code=404, detail=f"stop_id '{stop_id}' not found")

        # stop_times reference platforms, not the parent station (e.g. rail
        # "PMB" has no stop_times; "PMB1"/"PMB2" do). Expand to the platforms.
        stop_ids = _station_stop_ids(conn, stop_id)
        stop_ph = ",".join("?" * len(stop_ids))

        # Fetch one representative trip per (route_id, direction_id) that serves this stop
        trip_rows = conn.execute(
            f"""
            SELECT
                r.route_id,
                r.route_short_name,
                r.route_long_name,
                r.route_color,
                r.route_text_color,
                t.direction_id,
                t.trip_headsign,
                t.shape_id,
                t.trip_id
            FROM  stop_times st
            JOIN  trips  t ON st.trip_id = t.trip_id
            JOIN  routes r ON t.route_id = r.route_id
            WHERE st.stop_id IN ({stop_ph})
            GROUP BY r.route_id, t.direction_id
            ORDER BY r.route_short_name, t.direction_id
            """,
            stop_ids,
        ).fetchall()

        # Collect all unique shape_ids to fetch in one query
        shape_ids = list({r["shape_id"] for r in trip_rows if r["shape_id"]})

        shapes_map: dict[str, list] = {}
        if shape_ids:
            placeholders = ",".join("?" * len(shape_ids))
            shape_rows = conn.execute(
                f"""
                SELECT shape_id,
                       CAST(shape_pt_lat AS REAL) AS lat,
                       CAST(shape_pt_lon AS REAL) AS lon,
                       CAST(shape_pt_sequence AS INTEGER) AS sequence
                FROM shapes
                WHERE shape_id IN ({placeholders})
                ORDER BY shape_id, sequence
                """,
                shape_ids,
            ).fetchall()
            for sr in shape_rows:
                sid = sr["shape_id"]
                if sid not in shapes_map:
                    shapes_map[sid] = []
                # GeoJSON coordinates are [lon, lat]
                shapes_map[sid].append([sr["lon"], sr["lat"]])

    features = []

    # Stop Point feature
    features.append({
        "type": "Feature",
        "geometry": {
            "type": "Point",
            "coordinates": [stop_row["lon"], stop_row["lat"]],
        },
        "properties": {
            "feature_type": "stop",
            "stop_id":      stop_row["stop_id"],
            "stop_name":    stop_row["stop_name"],
        },
    })

    # One LineString feature per (route, direction)
    for r in trip_rows:
        coords = shapes_map.get(r["shape_id"], []) if r["shape_id"] else []
        features.append({
            "type": "Feature",
            "geometry": {
                "type": "LineString",
                "coordinates": coords,
            },
            "properties": {
                "feature_type":     "shape",
                "route_id":         r["route_id"],
                "route_short_name": r["route_short_name"],
                "route_long_name":  r["route_long_name"],
                "route_color":      r["route_color"],
                "route_text_color": r["route_text_color"],
                "direction_id":     r["direction_id"],
                "trip_headsign":    r["trip_headsign"],
                "shape_id":         r["shape_id"],
            },
        })

    return {
        "type":     "FeatureCollection",
        "features": features,
    }


# ── All routes shapes as GeoJSON ──────────────────────────────────────────────

@app.get("/api/shapes")
def all_shapes(
    mode: str = Query(
        default=DEFAULT_MODE,
        description="Which service to query: 'transit' (bus), 'ferry' "
                    "(water) or 'rail'. All are served by this one API; the "
                    "endpoint paths are identical.",
    ),
):
    """
    Return a GeoJSON FeatureCollection containing a LineString for every
    distinct (route, direction) in the feed.

    Each feature's ``properties`` include:
    - ``route_id``, ``route_short_name``, ``route_long_name``
    - ``route_color``, ``route_text_color``
    - ``direction_id``, ``trip_headsign``, ``shape_id``

    One representative trip per (route_id, direction_id) is used to select
    the shape.  Routes with no shape_id are omitted.
    """
    with _db(mode) as conn:
        # One representative trip per (route, direction)
        trip_rows = conn.execute(
            """
            SELECT
                r.route_id,
                r.route_short_name,
                r.route_long_name,
                r.route_color,
                r.route_text_color,
                t.direction_id,
                t.trip_headsign,
                t.shape_id
            FROM trips t
            JOIN routes r ON t.route_id = r.route_id
            WHERE t.shape_id IS NOT NULL AND t.shape_id != ''
            GROUP BY r.route_id, t.direction_id
            ORDER BY r.route_short_name, t.direction_id
            """
        ).fetchall()

        shape_ids = list({r["shape_id"] for r in trip_rows})

        shapes_map: dict[str, list] = {}
        if shape_ids:
            placeholders = ",".join("?" * len(shape_ids))
            shape_rows = conn.execute(
                f"""
                SELECT shape_id,
                       CAST(shape_pt_lon AS REAL) AS lon,
                       CAST(shape_pt_lat AS REAL) AS lat,
                       CAST(shape_pt_sequence AS INTEGER) AS seq
                FROM shapes
                WHERE shape_id IN ({placeholders})
                ORDER BY shape_id, seq
                """,
                shape_ids,
            ).fetchall()
            for sr in shape_rows:
                sid = sr["shape_id"]
                if sid not in shapes_map:
                    shapes_map[sid] = []
                shapes_map[sid].append([sr["lon"], sr["lat"]])

    features = []
    for r in trip_rows:
        coords = shapes_map.get(r["shape_id"], [])
        if not coords:
            continue
        features.append({
            "type": "Feature",
            "geometry": {
                "type": "LineString",
                "coordinates": coords,
            },
            "properties": {
                "route_id":         r["route_id"],
                "route_short_name": r["route_short_name"],
                "route_long_name":  r["route_long_name"],
                "route_color":      r["route_color"],
                "route_text_color": r["route_text_color"],
                "direction_id":     r["direction_id"],
                "trip_headsign":    r["trip_headsign"],
                "shape_id":         r["shape_id"],
            },
        })

    return {
        "type":     "FeatureCollection",
        "features": features,
    }


# ── Shape layer for a single route ───────────────────────────────────────────

@app.get("/api/routes/{route_id}/shapes")
def route_shapes(
    route_id: str,
    region: Optional[str] = Query(default=None, description="Optional region / database override"),
    region_id: Optional[str] = Query(default=None, description="Alias for region identifier"),
    regionId: Optional[str] = Query(default=None, description="Alias for region identifier (camelCase)"),
    mode: str = Query(
        default=DEFAULT_MODE,
        description="Which service to query: 'transit' (bus), 'ferry' "
                    "(water) or 'rail'. All are served by this one API; the "
                    "endpoint paths are identical.",
    ),
):
    """
    Return a GeoJSON FeatureCollection for *route_id*.
    """
    db_path = _find_db_for_route(route_id, region=region, region_id=region_id, regionId=regionId, mode=mode)
    with _open_db(db_path) as conn:
        route_row = conn.execute(
            "SELECT route_id, route_short_name, route_long_name,"
            " route_color, route_text_color FROM routes WHERE route_id = ?",
            (route_id,),
        ).fetchone()
        if route_row is None:
            raise HTTPException(status_code=404, detail=f"route_id '{route_id}' not found")

        # One representative trip per direction+shape variant (same logic as /stops)
        trip_rows = conn.execute(
            """
            SELECT MIN(t.trip_id) AS trip_id, t.direction_id, t.trip_headsign, t.shape_id
            FROM trips t
            WHERE t.route_id = ?
            GROUP BY t.direction_id, t.trip_headsign, t.shape_id
            ORDER BY t.direction_id, t.shape_id
            """,
            (route_id,),
        ).fetchall()

        if not trip_rows:
            raise HTTPException(status_code=404, detail=f"No trips found for route_id '{route_id}'")

        shape_ids = list({r["shape_id"] for r in trip_rows if r["shape_id"]})
        shapes_map: dict[str, list] = {}
        if shape_ids:
            placeholders = ",".join("?" * len(shape_ids))
            shape_rows = conn.execute(
                f"""
                SELECT shape_id,
                       CAST(shape_pt_lon AS REAL) AS lon,
                       CAST(shape_pt_lat AS REAL) AS lat,
                       CAST(shape_pt_sequence AS INTEGER) AS seq
                FROM shapes
                WHERE shape_id IN ({placeholders})
                ORDER BY shape_id, seq
                """,
                shape_ids,
            ).fetchall()
            for sr in shape_rows:
                sid = sr["shape_id"]
                if sid not in shapes_map:
                    shapes_map[sid] = []
                shapes_map[sid].append([sr["lon"], sr["lat"]])

        # Ordered stops per trip (for start/end markers)
        trip_stops: dict[str, list] = {}
        for tr in trip_rows:
            stop_rows = conn.execute(
                """
                SELECT st.stop_id, s.stop_name, s.stop_code,
                       CAST(s.stop_lat AS REAL) AS lat,
                       CAST(s.stop_lon AS REAL) AS lon,
                       CAST(st.stop_sequence AS INTEGER) AS seq
                FROM stop_times st
                JOIN stops s ON st.stop_id = s.stop_id
                WHERE st.trip_id = ?
                ORDER BY seq
                """,
                (tr["trip_id"],),
            ).fetchall()
            trip_stops[tr["trip_id"]] = [dict(s) for s in stop_rows]

    # Sort: outbound first, then inbound; within each segment longest shape first
    sorted_trips = sorted(
        trip_rows,
        key=lambda r: (0 if str(r["direction_id"]) == "0" else 1, r["shape_id"] or ""),
    )

    features = []
    for tr in sorted_trips:
        coords   = shapes_map.get(tr["shape_id"], []) if tr["shape_id"] else []
        segment  = "outbound" if str(tr["direction_id"]) == "0" else "inbound"
        stops    = trip_stops.get(tr["trip_id"], [])

        # Feeds without shapes.txt (e.g. rail) have no geometry. Fall back to a
        # straight line through the trip's stops so the route is still mappable.
        if len(coords) < 2 and len(stops) >= 2:
            coords = [[st["lon"], st["lat"]] for st in stops
                      if st.get("lon") is not None and st.get("lat") is not None]

        # LineString feature
        features.append({
            "type": "Feature",
            "geometry": {"type": "LineString", "coordinates": coords},
            "properties": {
                "feature_type":     "shape",
                "route_id":         route_row["route_id"],
                "route_short_name": route_row["route_short_name"],
                "route_long_name":  route_row["route_long_name"],
                "route_color":      route_row["route_color"],
                "route_text_color": route_row["route_text_color"],
                "direction_id":     tr["direction_id"],
                "segment":          segment,
                "trip_headsign":    tr["trip_headsign"],
                "shape_id":         tr["shape_id"],
            },
        })

        # Start / end stop Point features
        for marker_type, stop in [("start", stops[0] if stops else None),
                                   ("end",   stops[-1] if stops else None)]:
            if stop is None:
                continue
            features.append({
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [stop["lon"], stop["lat"]],
                },
                "properties": {
                    "feature_type":     marker_type,
                    "stop_id":          stop["stop_id"],
                    "stop_name":        stop["stop_name"],
                    "stop_code":        stop["stop_code"],
                    "direction_id":     tr["direction_id"],
                    "segment":          segment,
                    "trip_headsign":    tr["trip_headsign"],
                    "shape_id":         tr["shape_id"],
                    "route_short_name": route_row["route_short_name"],
                },
            })

    return {
        "type":     "FeatureCollection",
        "features": features,
    }


# ── Trips + timings for a route + shape ──────────────────────────────────────

@app.get("/api/routes/{route_id}/shapes/{shape_id}/trips")
def route_shape_trips(
    route_id: str,
    shape_id: str,
    region: Optional[str] = Query(default=None, description="Optional region / database override"),
    region_id: Optional[str] = Query(default=None, description="Alias for region identifier"),
    regionId: Optional[str] = Query(default=None, description="Alias for region identifier (camelCase)"),
    mode: str = Query(
        default=DEFAULT_MODE,
        description="Which service to query: 'transit' (bus), 'ferry' "
                    "(water) or 'rail'. All are served by this one API; the "
                    "endpoint paths are identical.",
    ),
):
    """
    Return all trip_ids for the given *route_id* + *shape_id*.
    """
    db_path = _find_db_for_route(route_id, region=region, region_id=region_id, regionId=regionId, mode=mode)
    with _open_db(db_path) as conn:
        route_row = conn.execute(
            "SELECT route_id, route_short_name, route_long_name, "
            "route_color, route_text_color FROM routes WHERE route_id = ?",
            (route_id,),
        ).fetchone()
        if route_row is None:
            raise HTTPException(status_code=404, detail=f"route_id '{route_id}' not found")

        trip_rows = conn.execute(
            """
            SELECT trip_id, direction_id, trip_headsign, service_id, shape_id
            FROM trips
            WHERE route_id = ? AND shape_id = ?
            ORDER BY trip_id
            """,
            (route_id, shape_id),
        ).fetchall()

        if not trip_rows:
            raise HTTPException(
                status_code=404,
                detail=f"No trips found for route_id '{route_id}' with shape_id '{shape_id}'",
            )

        trips = []
        for tr in trip_rows:
            stop_times = conn.execute(
                """
                SELECT
                    CAST(st.stop_sequence AS INTEGER) AS stop_sequence,
                    st.stop_id,
                    s.stop_name,
                    s.stop_code,
                    CAST(s.stop_lat AS REAL) AS lat,
                    CAST(s.stop_lon AS REAL) AS lon,
                    st.arrival_time,
                    st.departure_time
                FROM stop_times st
                JOIN stops s ON st.stop_id = s.stop_id
                WHERE st.trip_id = ?
                ORDER BY CAST(st.stop_sequence AS INTEGER)
                """,
                (tr["trip_id"],),
            ).fetchall()

            trips.append({
                "trip_id":       tr["trip_id"],
                "service_id":    tr["service_id"],
                "direction_id":  tr["direction_id"],
                "trip_headsign": tr["trip_headsign"],
                "departure":     stop_times[0]["departure_time"] if stop_times else None,
                "arrival":       stop_times[-1]["arrival_time"]  if stop_times else None,
                "stop_times":    [dict(r) for r in stop_times],
            })

    return {
        "route_id":         route_row["route_id"],
        "route_short_name": route_row["route_short_name"],
        "route_long_name":  route_row["route_long_name"],
        "shape_id":         shape_id,
        "trip_count":       len(trips),
        "trips":            trips,
    }


# ── Trip schedule for a route by date filter ─────────────────────────────────

_DAY_NAMES = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]


@app.get("/api/routes/{route_id}/schedule")
def route_schedule(
    route_id: str,
    filter: str = Query(
        default="today",
        description="Date filter: 'today', 'tomorrow', 'this_week' (Mon–Sun of current week), or a specific date in MM/DD/YYYY format.",
    ),
    departure_time: Optional[str] = Query(
        default=None,
        description="Only return trips departing at or after this time. "
                    "Format: HH:MM or HH:MM:SS. Defaults to current time if omitted.",
    ),
    time_param: Optional[str] = Query(
        default=None,
        alias="time",
        description="Filter trips by this time (HH:MM or HH:MM:SS). "
                    "Used with 'upcoming' param to get upcoming or past trips.",
    ),
    upcoming: bool = Query(
        default=True,
        description="If true, return trips starting at or after 'time'. "
                    "If false, return trips that ended before 'time'.",
    ),
    limit: int = Query(default=50, ge=1, le=500, description="Max trips to return per day"),
    offset: int = Query(default=0, ge=0, description="Number of trips to skip per day (for pagination)"),
    lang: Optional[str] = _LANG_QUERY,
    region: Optional[str] = Query(default=None, description="Optional region / database override"),
    region_id: Optional[str] = Query(default=None, description="Alias for region identifier"),
    regionId: Optional[str] = Query(default=None, description="Alias for region identifier (camelCase)"),
    mode: str = Query(
        default=DEFAULT_MODE,
        description="Which service to query: 'transit' (bus), 'ferry' "
                    "(water) or 'rail'. All are served by this one API; the "
                    "endpoint paths are identical.",
    ),
):
    """
    Return trips for *route_id* grouped by day and ordered by first
    departure time.

    - ``filter=today``          → trips for today only
    - ``filter=tomorrow``       → trips for tomorrow only
    - ``filter=this_week``      → trips for every day Mon–Sun of the current week
    - ``filter=MM/DD/YYYY``     → trips for that specific date
    - ``departure_time``        → only trips departing at or after this time (HH:MM or HH:MM:SS); defaults to now
    - ``time`` + ``upcoming=true``  → only trips starting at or after this time
    - ``time`` + ``upcoming=false`` → only trips that ended (last_arrival) before this time
    - ``limit``                 → max trips per day (default 50)
    - ``offset``                → skip N trips per day for pagination
    - ``lang``                  → language code for translations (e.g. 'ar')

    Each trip entry contains:
    - ``trip_id``, ``service_id``, ``direction_id``, ``trip_headsign``
    - ``first_departure`` – departure time at the first stop (HH:MM:SS)
    - ``last_arrival``    – arrival time at the last stop (HH:MM:SS)
    - ``stop_count``      – number of stops on this trip
    """
    today = date.today()
    date_filter = filter.strip()

    def _parse_time_str(t_str: str, param_name: str) -> int:
        """Parse HH:MM or HH:MM:SS into seconds since midnight."""
        parts = t_str.strip().split(":")
        try:
            if len(parts) == 2:
                return int(parts[0]) * 3600 + int(parts[1]) * 60
            elif len(parts) == 3:
                return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
            else:
                raise ValueError
        except (ValueError, IndexError):
            raise HTTPException(
                status_code=422,
                detail=f"'{param_name}' must be in HH:MM or HH:MM:SS format",
            )

    # Determine time filter: 'time' param takes precedence over 'departure_time'
    time_filter_secs = None
    use_upcoming = upcoming  # True = after time, False = before time

    # An empty value (?time= or ?departure_time=) means "not provided" — this
    # is what UI clients send for untouched optional fields.
    time_param = (time_param or "").strip() or None
    departure_time = (departure_time or "").strip() or None

    if time_param is not None:
        time_filter_secs = _parse_time_str(time_param, "time")
    elif departure_time is not None:
        time_filter_secs = _parse_time_str(departure_time, "departure_time")
        use_upcoming = True  # departure_time always means "at or after"

    if date_filter.lower() == "today":
        dates = [today]
    elif date_filter.lower() == "tomorrow":
        dates = [today + timedelta(days=1)]
    elif date_filter.lower() == "this_week":
        monday = today - timedelta(days=today.weekday())
        dates = [monday + timedelta(days=i) for i in range(7)]
    else:
        try:
            specific_date = datetime.strptime(date_filter, "%m/%d/%Y").date()
            dates = [specific_date]
        except ValueError:
            raise HTTPException(
                status_code=422,
                detail="'filter' must be 'today', 'tomorrow', 'this_week', or a date in MM/DD/YYYY format",
            )

    db_path = _find_db_for_route(route_id, region=region, region_id=region_id, regionId=regionId, mode=mode)
    with _open_db(db_path) as conn:
        route_row = conn.execute(
            "SELECT route_id, route_short_name, route_long_name, "
            "route_color, route_text_color FROM routes WHERE route_id = ?",
            (route_id,),
        ).fetchone()
        if route_row is None:
            raise HTTPException(status_code=404, detail=f"route_id '{route_id}' not found")

        stop_tr = _get_stop_translations(conn, lang or "en")
        trip_tr = _get_trip_translations(conn, lang or "en")

        days = []
        for on_date in dates:
            active = _active_services(conn, on_date)

            if not active:
                days.append({
                    "date":     on_date.isoformat(),
                    "day_name": _DAY_NAMES[on_date.weekday()],
                    "total":    0,
                    "offset":   offset,
                    "limit":    limit,
                    "trips":    [],
                })
                continue

            placeholders = ",".join("?" * len(active))

            # Build optional time filter
            if time_filter_secs is not None:
                t_h, t_rem = divmod(time_filter_secs, 3600)
                t_m, t_s   = divmod(t_rem, 60)
                time_threshold = f"{t_h:02d}:{t_m:02d}:{t_s:02d}"

                if use_upcoming:
                    # Upcoming + active: trips not yet ended (last_arrival >= time)
                    # This includes trips already started but still in progress
                    having_clause     = "HAVING MAX(st.arrival_time) >= ?"
                    where_clause_full = "WHERE b.last_arrival >= ?"
                else:
                    # Past: trips that ended (last_arrival) before the given time
                    having_clause     = "HAVING MAX(st.arrival_time) <= ?"
                    where_clause_full = "WHERE b.last_arrival <= ?"

                extra_params = [time_threshold]
            else:
                having_clause     = ""
                where_clause_full = ""
                extra_params      = []

            # Count total matching trips (before limit/offset).
            # Only this route's trips are aggregated — never the whole
            # stop_times table (that was ~6 s per day of schedule).
            total_count = conn.execute(
                f"""
                SELECT COUNT(*) FROM (
                    SELECT t.trip_id,
                           MIN(st.departure_time) AS first_departure,
                           MAX(st.arrival_time)   AS last_arrival
                    FROM trips t
                    JOIN stop_times st ON st.trip_id = t.trip_id
                    WHERE t.route_id   = ?
                      AND t.service_id IN ({placeholders})
                    GROUP BY t.trip_id
                    {having_clause}
                )
                """,
                [route_id, *active] + extra_params,
            ).fetchone()[0]

            # First/last stop of each trip is resolved with correlated
            # subqueries that use idx_stop_times_trip_id, so only the ~100
            # stop_times rows of this route's trips are touched.
            trip_rows = conn.execute(
                f"""
                WITH route_trips AS (
                    SELECT trip_id, service_id, direction_id, trip_headsign
                    FROM trips
                    WHERE route_id   = ?
                      AND service_id IN ({placeholders})
                ),
                bounds AS (
                    SELECT rt.trip_id,
                           (SELECT st.stop_id FROM stop_times st
                             WHERE st.trip_id = rt.trip_id
                             ORDER BY CAST(st.stop_sequence AS INTEGER) LIMIT 1)  AS start_stop_id,
                           (SELECT st.stop_id FROM stop_times st
                             WHERE st.trip_id = rt.trip_id
                             ORDER BY CAST(st.stop_sequence AS INTEGER) DESC LIMIT 1) AS end_stop_id,
                           (SELECT st.departure_time FROM stop_times st
                             WHERE st.trip_id = rt.trip_id
                             ORDER BY CAST(st.stop_sequence AS INTEGER) LIMIT 1)  AS first_departure,
                           (SELECT st.arrival_time FROM stop_times st
                             WHERE st.trip_id = rt.trip_id
                             ORDER BY CAST(st.stop_sequence AS INTEGER) DESC LIMIT 1) AS last_arrival,
                           (SELECT COUNT(DISTINCT st.stop_id) FROM stop_times st
                             WHERE st.trip_id = rt.trip_id) AS stop_count
                    FROM route_trips rt
                )
                SELECT
                    rt.trip_id,
                    rt.service_id,
                    rt.direction_id,
                    rt.trip_headsign,
                    b.first_departure,
                    b.last_arrival,
                    b.stop_count,
                    b.start_stop_id,
                    fs.stop_name  AS start_stop_name,
                    fs.stop_code  AS start_stop_code,
                    CAST(fs.stop_lat AS REAL) AS start_lat,
                    CAST(fs.stop_lon AS REAL) AS start_lon,
                    b.end_stop_id,
                    ls.stop_name  AS end_stop_name,
                    ls.stop_code  AS end_stop_code,
                    CAST(ls.stop_lat AS REAL) AS end_lat,
                    CAST(ls.stop_lon AS REAL) AS end_lon
                FROM route_trips rt
                JOIN bounds b ON b.trip_id = rt.trip_id
                JOIN stops fs ON fs.stop_id = b.start_stop_id
                JOIN stops ls ON ls.stop_id = b.end_stop_id
                {where_clause_full}
                ORDER BY b.first_departure{"" if use_upcoming else " DESC"}
                LIMIT ? OFFSET ?
                """,
                [route_id, *active] + extra_params + [limit, offset],
            ).fetchall()

            trips_list = []
            for r in trip_rows:
                td = dict(r)
                if trip_tr:
                    td["trip_headsign"] = trip_tr.get(r["trip_id"], r["trip_headsign"])
                if stop_tr:
                    td["start_stop_name"] = stop_tr.get(r["start_stop_id"], r["start_stop_name"])
                    td["end_stop_name"] = stop_tr.get(r["end_stop_id"], r["end_stop_name"])
                trips_list.append(td)

            days.append({
                "date":     on_date.isoformat(),
                "day_name": _DAY_NAMES[on_date.weekday()],
                "total":    total_count,
                "offset":   offset,
                "limit":    limit,
                "trips":    trips_list,
            })

    return {
        "route_id":         route_row["route_id"],
        "route_short_name": route_row["route_short_name"],
        "route_long_name":  route_row["route_long_name"],
        "route_color":      route_row["route_color"],
        "route_text_color": route_row["route_text_color"],
        "filter":           date_filter,
        "days":             days,
    }


# ── Stop-sequence timings for a trip ─────────────────────────────────────────

@app.get("/api/trips/{trip_id}/stop-times")
def trip_stop_times(
    trip_id: str,
    lang: Optional[str] = _LANG_QUERY,
    mode: str = Query(
        default=DEFAULT_MODE,
        description="Which service to query: 'transit' (bus), 'ferry' "
                    "(water) or 'rail'. All are served by this one API; the "
                    "endpoint paths are identical.",
    ),
):
    """
    Return the ordered stop-sequence with arrival/departure timings for
    *trip_id*.

    Each stop entry includes:
    - ``stop_sequence``
    - ``stop_id``, ``stop_name``, ``stop_code``
    - ``lat``, ``lon``
    - ``arrival_time``, ``departure_time``  (HH:MM:SS, may exceed 24h)
    - ``stop_headsign``
    - ``pickup_type``, ``drop_off_type``
    - ``shape_dist_traveled``

    When ``lang=ar`` is passed, each stop also includes ``stop_name_ar``
    and the response includes ``trip_headsign_ar``.
    """
    with _db(mode) as conn:
        # Verify trip exists and fetch route/service info
        trip_row = conn.execute(
            """
            SELECT t.trip_id, t.route_id, t.service_id, t.direction_id,
                   t.trip_headsign, t.shape_id,
                   r.route_short_name, r.route_long_name,
                   r.route_color, r.route_text_color
            FROM trips t
            JOIN routes r ON t.route_id = r.route_id
            WHERE t.trip_id = ?
            """,
            (trip_id,),
        ).fetchone()
        if trip_row is None:
            raise HTTPException(status_code=404, detail=f"trip_id '{trip_id}' not found")

        stop_rows = conn.execute(
            """
            SELECT
                CAST(st.stop_sequence AS INTEGER) AS stop_sequence,
                st.stop_id,
                s.stop_name,
                s.stop_code,
                CAST(s.stop_lat AS REAL) AS lat,
                CAST(s.stop_lon AS REAL) AS lon,
                st.arrival_time,
                st.departure_time,
                st.stop_headsign,
                st.pickup_type,
                st.drop_off_type,
                st.shape_dist_traveled
            FROM stop_times st
            JOIN stops s ON st.stop_id = s.stop_id
            WHERE st.trip_id = ?
            ORDER BY CAST(st.stop_sequence AS INTEGER)
            """,
            (trip_id,),
        ).fetchall()

        stop_tr = _get_stop_translations(conn, lang or "en")
        trip_tr = _get_trip_translations(conn, lang or "en")

    stops = [dict(r) for r in stop_rows]
    if stop_tr:
        stops = [_translated_stop(s, stop_tr, lang) for s in stops]

    trip_headsign = trip_row["trip_headsign"]
    if trip_tr:
        trip_headsign = trip_tr.get(trip_id, trip_headsign)
        for s in stops:
            if s.get("stop_headsign"):
                # Overwrite stop_headsign with the translated trip_headsign
                s["stop_headsign"] = trip_headsign

    result = {
        "trip_id":          trip_row["trip_id"],
        "route_id":         trip_row["route_id"],
        "route_short_name": trip_row["route_short_name"],
        "route_long_name":  trip_row["route_long_name"],
        "route_color":      trip_row["route_color"],
        "route_text_color": trip_row["route_text_color"],
        "service_id":       trip_row["service_id"],
        "direction_id":     trip_row["direction_id"],
        "trip_headsign":    trip_headsign,
        "shape_id":         trip_row["shape_id"],
        "stop_count":       len(stops),
        "stops":            stops,
    }
    return result


# ── Shapes ────────────────────────────────────────────────────────────────────

@app.get("/api/trips/{trip_id}/shapes")
def trip_shapes(
    trip_id: str,
    mode: str = Query(
        default=DEFAULT_MODE,
        description="Which service to query: 'transit' (bus), 'ferry' "
                    "(water) or 'rail'. All are served by this one API; the "
                    "endpoint paths are identical.",
    ),
):
    """Return the ordered shape points for the given trip_id."""
    with _db(mode) as conn:
        trip_row = conn.execute(
            "SELECT shape_id FROM trips WHERE trip_id = ?", (trip_id,)
        ).fetchone()
        if trip_row is None:
            raise HTTPException(status_code=404, detail=f"trip_id '{trip_id}' not found")

        shape_id = trip_row["shape_id"]
        if not shape_id:
            raise HTTPException(status_code=404, detail=f"No shape_id associated with trip '{trip_id}'")

        rows = conn.execute(
            """
            SELECT shape_pt_sequence,
                   CAST(shape_pt_lat AS REAL) AS lat,
                   CAST(shape_pt_lon AS REAL) AS lon,
                   shape_dist_traveled
            FROM shapes
            WHERE shape_id = ?
            ORDER BY CAST(shape_pt_sequence AS INTEGER)
            """,
            (shape_id,),
        ).fetchall()

    coordinates = [[r["lon"], r["lat"]] for r in rows]

    return {
        "type": "Feature",
        "properties": {
            "trip_id":  trip_id,
            "shape_id": shape_id,
        },
        "geometry": {
            "type":        "LineString",
            "coordinates": coordinates,
        },
    }


# ── Shape between two stops ───────────────────────────────────────────────────

@app.get("/api/shapes/between")
def shapes_between(
    start_stop_id: str = Query(..., description="GTFS stop_id of the boarding stop"),
    end_stop_id: str = Query(..., description="GTFS stop_id of the alighting stop"),
    mode: str = Query(
        default=DEFAULT_MODE,
        description="Which service to query: 'transit' (bus), 'ferry' "
                    "(water) or 'rail'. All are served by this one API; the "
                    "endpoint paths are identical.",
    ),
):
    """
    Return a GeoJSON FeatureCollection containing:
    - A **LineString** of the route shape clipped between *start_stop_id* and
      *end_stop_id* (uses ``shape_dist_traveled`` when available, otherwise the
      full shape is returned).
    - A **Point** for the start stop.
    - A **Point** for the end stop.

    A representative trip that serves both stops in order is selected
    automatically.
    """
    with _db(mode) as conn:
        # Verify both stops exist
        start_row = conn.execute(
            "SELECT stop_id, stop_name,"
            " CAST(stop_lat AS REAL) AS lat, CAST(stop_lon AS REAL) AS lon"
            " FROM stops WHERE stop_id = ?",
            (start_stop_id,),
        ).fetchone()
        if start_row is None:
            raise HTTPException(status_code=404, detail=f"start_stop_id '{start_stop_id}' not found")

        end_row = conn.execute(
            "SELECT stop_id, stop_name,"
            " CAST(stop_lat AS REAL) AS lat, CAST(stop_lon AS REAL) AS lon"
            " FROM stops WHERE stop_id = ?",
            (end_stop_id,),
        ).fetchone()
        if end_row is None:
            raise HTTPException(status_code=404, detail=f"end_stop_id '{end_stop_id}' not found")

        # Find a representative trip serving start_stop then end_stop in order
        trip_row = conn.execute(
            """
            SELECT
                t.trip_id,
                t.shape_id,
                t.direction_id,
                t.trip_headsign,
                r.route_id,
                r.route_short_name,
                r.route_long_name,
                r.route_color,
                r.route_text_color,
                st_start.stop_sequence    AS start_seq,
                st_end.stop_sequence      AS end_seq,
                st_start.shape_dist_traveled AS start_dist,
                st_end.shape_dist_traveled   AS end_dist
            FROM stop_times st_start
            JOIN stop_times st_end ON st_end.trip_id = st_start.trip_id
                                   AND st_end.stop_id = ?
                                   AND CAST(st_end.stop_sequence AS INTEGER)
                                     > CAST(st_start.stop_sequence AS INTEGER)
            JOIN trips  t ON t.trip_id  = st_start.trip_id
            JOIN routes r ON r.route_id = t.route_id
            WHERE st_start.stop_id = ?
              AND t.shape_id IS NOT NULL AND t.shape_id != ''
            ORDER BY t.trip_id
            LIMIT 1
            """,
            (end_stop_id, start_stop_id),
        ).fetchone()

        if trip_row is None:
            # Try without shape requirement (may have no shape_id)
            trip_row_no_shape = conn.execute(
                """
                SELECT
                    t.trip_id,
                    t.shape_id,
                    t.direction_id,
                    t.trip_headsign,
                    r.route_id,
                    r.route_short_name,
                    r.route_long_name,
                    r.route_color,
                    r.route_text_color
                FROM stop_times st_start
                JOIN stop_times st_end ON st_end.trip_id = st_start.trip_id
                                       AND st_end.stop_id = ?
                                       AND CAST(st_end.stop_sequence AS INTEGER)
                                         > CAST(st_start.stop_sequence AS INTEGER)
                JOIN trips  t ON t.trip_id  = st_start.trip_id
                JOIN routes r ON r.route_id = t.route_id
                WHERE st_start.stop_id = ?
                ORDER BY t.trip_id
                LIMIT 1
                """,
                (end_stop_id, start_stop_id),
            ).fetchone()
            if trip_row_no_shape is None:
                raise HTTPException(
                    status_code=404,
                    detail=(
                        f"No trip found that serves stop '{start_stop_id}' "
                        f"before stop '{end_stop_id}'"
                    ),
                )
            return {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "geometry": {"type": "Point", "coordinates": [start_row["lon"], start_row["lat"]]},
                        "properties": {"feature_type": "start", "stop_id": start_row["stop_id"], "stop_name": start_row["stop_name"]},
                    },
                    {
                        "type": "Feature",
                        "geometry": {"type": "Point", "coordinates": [end_row["lon"], end_row["lat"]]},
                        "properties": {"feature_type": "end", "stop_id": end_row["stop_id"], "stop_name": end_row["stop_name"]},
                    },
                ],
                "note": "No shape geometry available for this trip",
                "trip_id":  trip_row_no_shape["trip_id"],
                "route_id": trip_row_no_shape["route_id"],
            }

        shape_id   = trip_row["shape_id"]
        start_dist = trip_row["start_dist"]
        end_dist   = trip_row["end_dist"]

        # Fetch shape points – clip by shape_dist_traveled when available
        if start_dist is not None and end_dist is not None:
            shape_rows = conn.execute(
                """
                SELECT CAST(shape_pt_lon AS REAL) AS lon,
                       CAST(shape_pt_lat AS REAL) AS lat,
                       CAST(shape_pt_sequence AS INTEGER) AS seq,
                       CAST(shape_dist_traveled AS REAL) AS dist
                FROM shapes
                WHERE shape_id = ?
                  AND CAST(shape_dist_traveled AS REAL) >= ?
                  AND CAST(shape_dist_traveled AS REAL) <= ?
                ORDER BY seq
                """,
                (shape_id, float(start_dist), float(end_dist)),
            ).fetchall()
        else:
            # Fallback: no dist info – return full shape and note it
            shape_rows = conn.execute(
                """
                SELECT CAST(shape_pt_lon AS REAL) AS lon,
                       CAST(shape_pt_lat AS REAL) AS lat,
                       CAST(shape_pt_sequence AS INTEGER) AS seq
                FROM shapes
                WHERE shape_id = ?
                ORDER BY seq
                """,
                (shape_id,),
            ).fetchall()

    coordinates = [[r["lon"], r["lat"]] for r in shape_rows]

    features = [
        {
            "type": "Feature",
            "geometry": {
                "type": "LineString",
                "coordinates": coordinates,
            },
            "properties": {
                "feature_type":     "shape",
                "trip_id":          trip_row["trip_id"],
                "shape_id":         shape_id,
                "route_id":         trip_row["route_id"],
                "route_short_name": trip_row["route_short_name"],
                "route_long_name":  trip_row["route_long_name"],
                "route_color":      trip_row["route_color"],
                "route_text_color": trip_row["route_text_color"],
                "direction_id":     trip_row["direction_id"],
                "trip_headsign":    trip_row["trip_headsign"],
                "clipped":          start_dist is not None and end_dist is not None,
            },
        },
        {
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [start_row["lon"], start_row["lat"]]},
            "properties": {
                "feature_type": "start",
                "stop_id":      start_row["stop_id"],
                "stop_name":    start_row["stop_name"],
            },
        },
        {
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [end_row["lon"], end_row["lat"]]},
            "properties": {
                "feature_type": "end",
                "stop_id":      end_row["stop_id"],
                "stop_name":    end_row["stop_name"],
            },
        },
    ]

    return {
        "type":     "FeatureCollection",
        "features": features,
    }


# ── Serving routes between two stops ─────────────────────────────────────────

@app.get("/api/serving-routes")
def serving_routes(
    start_stop_id: str = Query(..., description="GTFS stop_id of the boarding stop"),
    end_stop_id: str = Query(..., description="GTFS stop_id of the alighting stop"),
    mode: str = Query(
        default=DEFAULT_MODE,
        description="Which service to query: 'transit' (bus), 'ferry' "
                    "(water) or 'rail'. All are served by this one API; the "
                    "endpoint paths are identical.",
    ),
):
    """
    Return a list of route_short_name values for routes that have at least one
    trip serving *start_stop_id* before *end_stop_id* (in stop_sequence order).
    """
    with _db(mode) as conn:
        # Verify both stops exist
        start_row = conn.execute(
            "SELECT stop_id, stop_name FROM stops WHERE stop_id = ?", (start_stop_id,)
        ).fetchone()
        if start_row is None:
            raise HTTPException(status_code=404, detail=f"start_stop_id '{start_stop_id}' not found")

        end_row = conn.execute(
            "SELECT stop_id, stop_name FROM stops WHERE stop_id = ?", (end_stop_id,)
        ).fetchone()
        if end_row is None:
            raise HTTPException(status_code=404, detail=f"end_stop_id '{end_stop_id}' not found")

        rows = conn.execute(
            """
            SELECT DISTINCT r.route_short_name, r.route_color, r.route_text_color
            FROM stop_times st_start
            JOIN stop_times st_end ON st_end.trip_id = st_start.trip_id
                                   AND st_end.stop_id = ?
                                   AND CAST(st_end.stop_sequence AS INTEGER)
                                     > CAST(st_start.stop_sequence AS INTEGER)
            JOIN trips  t ON t.trip_id  = st_start.trip_id
            JOIN routes r ON r.route_id = t.route_id
            WHERE st_start.stop_id = ?
            ORDER BY r.route_short_name
            """,
            (end_stop_id, start_stop_id),
        ).fetchall()

    return {
        "start_stop_id":   start_stop_id,
        "start_stop_name": start_row["stop_name"],
        "end_stop_id":     end_stop_id,
        "end_stop_name":   end_row["stop_name"],
        "routes": [
            {
                "route_short_name": r["route_short_name"],
                "route_color":      r["route_color"],
                "route_text_color": r["route_text_color"],
            }
            for r in rows
        ],
    }


# ── Find latest trip by route, headsign, departure time & stops ───────────────

class TripLookupRequest(BaseModel):
    route_id: str = Field(..., description="GTFS route_id")
    departure_date_time: str = Field(
        ...,
        description="ISO datetime with timezone offset, e.g. '2026-04-28T12:21+04:00'",
    )
    headsign: str = Field(..., description="Trip headsign to match")
    stop_ids: list[str] = Field(
        ..., description="Ordered list of stop_ids the trip must pass through"
    )
    max_results: int = Field(default=3, ge=1, le=50, description="Max next trips to return")
    departure_offset_minutes: int = Field(
        default=5, ge=0, le=60,
        description="Offset window in minutes (±) around departure_date_time. "
                    "Trips departing within [time - offset, time + offset] are "
                    "considered the first possible trip. Default 5.",
    )


def _make_datetime_str(date_str: str, time_str: str, tz_suffix: str) -> str:
    """Combine a date string, HH:MM:SS time, and timezone suffix into ISO datetime."""
    return f"{date_str}T{time_str}{tz_suffix}"


def _build_trip_detail(
    conn: sqlite3.Connection,
    trip_row: dict,
    stop_ids: list[str],
    date_str: str,
    tz_suffix: str,
) -> Optional[dict]:
    """
    For a given trip (with departure_time at the first stop already fetched),
    verify it visits all *stop_ids* in order and return the formatted trip dict.
    Returns None if the trip doesn't qualify.
    """
    trip_id = trip_row["trip_id"]
    stop_ph = ",".join("?" * len(stop_ids))

    stop_rows = conn.execute(
        f"""
        SELECT
            st.stop_id,
            st.arrival_time,
            st.departure_time,
            CAST(st.stop_sequence AS INTEGER) AS stop_sequence
        FROM stop_times st
        WHERE st.trip_id = ?
          AND st.stop_id IN ({stop_ph})
        ORDER BY CAST(st.stop_sequence AS INTEGER)
        """,
        (trip_id, *stop_ids),
    ).fetchall()

    # Build a map of stop_id → stop detail
    visited = {r["stop_id"]: dict(r) for r in stop_rows}

    # All requested stops must be present
    if not all(sid in visited for sid in stop_ids):
        return None

    # Requested stops must appear in increasing sequence order
    sequences = [visited[sid]["stop_sequence"] for sid in stop_ids]
    if sequences != sorted(sequences):
        return None

    # Build matched_stop_times
    matched_stop_times = []
    for sid in stop_ids:
        sd = visited[sid]
        arr = _normalize_gtfs_time(sd["arrival_time"])
        dep = _normalize_gtfs_time(sd["departure_time"])
        matched_stop_times.append({
            "stop_id":            sid,
            "stop_sequence":      sd["stop_sequence"],
            "arrival_time":       arr,
            "departure_time":     dep,
            "arrival_date_time":  _make_datetime_str(date_str, arr, tz_suffix),
            "departure_date_time": _make_datetime_str(date_str, dep, tz_suffix),
        })

    board_stop_id = stop_ids[0]
    alight_stop_id = stop_ids[-1]
    board_dep = _normalize_gtfs_time(trip_row["departure_time"])
    alight_arr = matched_stop_times[-1]["arrival_time"]

    return {
        "trip_id":                   trip_id,
        "service_id":                trip_row["service_id"],
        "direction_id":              trip_row["direction_id"],
        "trip_headsign":             trip_row["trip_headsign"],
        "board_stop_id":             board_stop_id,
        "board_departure_time":      board_dep,
        "board_departure_date_time": _make_datetime_str(date_str, board_dep, tz_suffix),
        "alight_stop_id":            alight_stop_id,
        "alight_arrival_time":       alight_arr,
        "alight_arrival_date_time":  _make_datetime_str(date_str, alight_arr, tz_suffix),
        "matched_stop_times":        matched_stop_times,
    }


@app.post("/api/trips/lookup")
def lookup_trips(
    req: TripLookupRequest,
    mode: str = Query(
        default=DEFAULT_MODE,
        description="Which service to query: 'transit' (bus), 'ferry' "
                    "(water) or 'rail'. All are served by this one API; the "
                    "endpoint paths are identical.",
    ),
):
    """
    Find trips matching a route, headsign, and ordered stop list.

    Returns:
    - **first_possible_trip** – the earliest trip departing at or after
      *departure_date_time* from the first stop.
    - **latest_missed_trip** – the last trip that departed *before*
      *departure_date_time*.
    - **next_available_trips** – up to *max_results* upcoming trips.
    - **matching_trip_count** – total qualifying trips for the day.
    """
    # ── Parse departure_date_time ───────────────────────────────────────────
    try:
        dt = _parse_iso_datetime(req.departure_date_time)
    except ValueError:
        raise HTTPException(
            status_code=422,
            detail="Invalid departure_date_time. Use ISO format, e.g. '2026-04-28T12:21+04:00'.",
        )

    on_date   = dt.date()
    from_secs = dt.hour * 3600 + dt.minute * 60 + dt.second
    date_str  = on_date.isoformat()

    # Extract timezone suffix from the input for output datetimes
    raw = req.departure_date_time.strip()
    tz_suffix = ""
    if "+" in raw[10:]:
        tz_suffix = raw[raw.index("+", 10):]
    elif raw.endswith("Z"):
        tz_suffix = "Z"
    else:
        # Look for negative offset after the date portion
        idx = raw.rfind("-")
        if idx > 10:
            tz_suffix = raw[idx:]

    if not req.stop_ids:
        raise HTTPException(status_code=422, detail="stop_ids must not be empty")

    first_stop_id = req.stop_ids[0]

    with _db(mode) as conn:
        # Verify route exists
        route_row = conn.execute(
            "SELECT route_id FROM routes WHERE route_id = ?",
            (req.route_id,),
        ).fetchone()
        if route_row is None:
            raise HTTPException(status_code=404, detail=f"route_id '{req.route_id}' not found")

        # Active services for the date
        active = _active_services(conn, on_date)

        empty_response = {
            "route_id":             req.route_id,
            "departure_date":       date_str,
            "departure_date_time":  req.departure_date_time,
            "stop_ids":             req.stop_ids,
            "headsign":             req.headsign,
            "first_possible_trip":  None,
            "latest_missed_trip":   None,
            "next_available_trips": [],
            "matching_trip_count":  0,
        }

        if not active:
            empty_response["note"] = "No active services on this date"
            return empty_response

        placeholders = ",".join("?" * len(active))

        # All trips on this route that serve the first stop, ordered by
        # departure_time.  Headsign is matched in Python so that HTML-escaped
        # variants (e.g. '&lt;' from web clients) and case differences are
        # tolerated – an exact SQL comparison was the reason lookups returned
        # "No trips found matching route, headsign, and first stop".
        rows = conn.execute(
            f"""
            SELECT
                t.trip_id,
                t.trip_headsign,
                t.direction_id,
                t.service_id,
                st.departure_time,
                st.arrival_time,
                CAST(st.stop_sequence AS INTEGER) AS stop_sequence
            FROM stop_times st
            JOIN trips t ON st.trip_id = t.trip_id
            WHERE st.stop_id       = ?
              AND t.route_id       = ?
              AND t.service_id IN ({placeholders})
            ORDER BY st.departure_time
            """,
            (first_stop_id, req.route_id, *active),
        ).fetchall()

        # Headsign filter – tolerant of HTML entities and case differences
        hs_norm = _normalize_headsign(req.headsign)
        rows = [r for r in rows if _normalize_headsign(r["trip_headsign"]) == hs_norm]

        if not rows:
            empty_response["note"] = "No trips found matching route, headsign, and first stop on this date"
            return empty_response

        # Apply ± offset window around the requested departure time
        offset_secs = req.departure_offset_minutes * 60
        window_start = from_secs - offset_secs   # earliest "catchable" departure
        window_end   = from_secs + offset_secs   # latest  "catchable" departure

        # Split into missed (before window) and upcoming (>= window_start)
        missed_rows = []
        upcoming_rows = []
        for r in rows:
            try:
                dep_secs = _gtfs_time_seconds(r["departure_time"])
            except (ValueError, IndexError):
                continue
            if dep_secs < window_start:
                missed_rows.append(dict(r))
            else:
                upcoming_rows.append(dict(r))

        # ── Build qualifying upcoming trips ─────────────────────────────────
        next_trips = []
        for row in upcoming_rows:
            if len(next_trips) >= req.max_results:
                break
            detail = _build_trip_detail(conn, row, req.stop_ids, date_str, tz_suffix)
            if detail is not None:
                next_trips.append(detail)

        first_possible = next_trips[0] if next_trips else None

        # ── Build latest missed trip ────────────────────────────────────────
        latest_missed = None
        for row in reversed(missed_rows):
            detail = _build_trip_detail(conn, row, req.stop_ids, date_str, tz_suffix)
            if detail is not None:
                latest_missed = detail
                break

        # ── Count all qualifying trips for the day ──────────────────────────
        all_qualifying = 0
        for r in rows:
            d = dict(r)
            if _build_trip_detail(conn, d, req.stop_ids, date_str, tz_suffix) is not None:
                all_qualifying += 1

    return {
        "route_id":             req.route_id,
        "departure_date":       date_str,
        "departure_date_time":  req.departure_date_time,
        "stop_ids":             req.stop_ids,
        "headsign":             req.headsign,
        "first_possible_trip":  first_possible,
        "latest_missed_trip":   latest_missed,
        "next_available_trips": next_trips,
        "matching_trip_count":  all_qualifying,
    }


# ── Dual-Language Next departures ───────────────────────────────────────────

@app.get("/api/stops/{stop_id}/next-departures-both")
def next_departures_both(
    stop_id: str,
    at: Optional[str] = Query(
        default=None,
        description="ISO datetime or time, e.g. '2026-04-12T14:30' or '14:30'. Defaults to now.",
    ),
    route: Optional[str] = Query(
        default=None,
        description="Optional route_short_name filter (exact match, case-insensitive).",
    ),
    limit: int = Query(default=10, ge=1, le=100, description="Max results to return"),
    mode: str = Query(
        default=DEFAULT_MODE,
        description="Which service to query: 'transit' (bus), 'ferry' "
                    "(water) or 'rail'. All are served by this one API; the "
                    "endpoint paths are identical.",
    ),
):
    """
    Return the next *limit* departures from *stop_id* with BOTH English and Arabic translations in the payload.
    """
    en_result = next_departures(stop_id=stop_id, at=at, route=route, limit=limit,
                                lang="en", mode=mode)

    departures = en_result.get("departures", [])
    trip_ids = [d["trip_id"] for d in departures if d.get("trip_id")]

    with _db(mode) as conn:
        stop_tr_ar = _get_stop_translations(conn, "ar")
        trip_tr_ar = _get_trip_translations(conn, "ar")

        # Single batch query for terminal stop IDs for all trips at once
        terminals_map: dict[str, dict] = {}
        if trip_ids:
            placeholders = ",".join("?" * len(trip_ids))
            terminal_rows = conn.execute(
                f"""
                SELECT
                    seqs.trip_id,
                    st_first.stop_id  AS start_stop_id,
                    st_last.stop_id   AS end_stop_id
                FROM (
                    SELECT trip_id,
                           MIN(CAST(stop_sequence AS INTEGER)) AS first_seq,
                           MAX(CAST(stop_sequence AS INTEGER)) AS last_seq
                    FROM stop_times
                    WHERE trip_id IN ({placeholders})
                    GROUP BY trip_id
                ) seqs
                JOIN stop_times st_first ON st_first.trip_id = seqs.trip_id
                                        AND CAST(st_first.stop_sequence AS INTEGER) = seqs.first_seq
                JOIN stop_times st_last  ON st_last.trip_id  = seqs.trip_id
                                        AND CAST(st_last.stop_sequence  AS INTEGER) = seqs.last_seq
                """,
                trip_ids,
            ).fetchall()
            for row in terminal_rows:
                terminals_map[row["trip_id"]] = {
                    "start_stop_id": row["start_stop_id"],
                    "end_stop_id":   row["end_stop_id"],
                }

    for d in departures:
        trip_id = d.get("trip_id")
        sid     = d.get("stop_id")

        d["stop_name_en"] = d.get("stop_name")
        d["stop_name_ar"] = stop_tr_ar.get(sid, d["stop_name_en"])

        d["trip_headsign_en"] = d.get("trip_headsign")
        d["trip_headsign_ar"] = trip_tr_ar.get(trip_id, d["trip_headsign_en"])

        d["start_stop_name_en"] = d.get("start_stop_name")
        d["end_stop_name_en"]   = d.get("end_stop_name")

        terminal = terminals_map.get(trip_id, {})
        start_sid = terminal.get("start_stop_id")
        end_sid   = terminal.get("end_stop_id")

        d["start_stop_name_ar"] = stop_tr_ar.get(start_sid, d["start_stop_name_en"]) if start_sid else d["start_stop_name_en"]
        d["end_stop_name_ar"]   = stop_tr_ar.get(end_sid,   d["end_stop_name_en"])   if end_sid   else d["end_stop_name_en"]

    en_result["stop_name_en"] = en_result.get("stop_name")
    en_result["stop_name_ar"] = stop_tr_ar.get(stop_id, en_result["stop_name_en"])

    return en_result


@app.get("/api/routes/{route_id}/next-departures-both")
def route_next_departures_both(
    route_id: str,
    at: Optional[str] = Query(default=None),
    limit: int = Query(default=10, ge=1, le=100),
    region: Optional[str] = Query(default=None, description="Optional region / database override"),
    region_id: Optional[str] = Query(default=None, description="Alias for region identifier"),
    regionId: Optional[str] = Query(default=None, description="Alias for region identifier (camelCase)"),
    mode: str = Query(
        default=DEFAULT_MODE,
        description="Which service to query: 'transit' (bus), 'ferry' "
                    "(water) or 'rail'. All are served by this one API; the "
                    "endpoint paths are identical.",
    ),
):
    """
    Return the next *limit* departures across all stops for *route_id* with BOTH English and Arabic translations in the payload.
    """
    en_result = route_next_departures(route_id=route_id, at=at, limit=limit, region=region, region_id=region_id, regionId=regionId, mode=mode)
    
    db_path = _find_db_for_route(route_id, region=region, region_id=region_id, regionId=regionId, mode=mode)
    with _open_db(db_path) as conn:
        stop_tr_ar = _get_stop_translations(conn, "ar")
        trip_tr_ar = _get_trip_translations(conn, "ar")
        
        for d in en_result.get("departures", []):
            trip_id = d.get("trip_id")
            sid = d.get("stop_id")
            
            d["stop_name_en"] = d.get("stop_name")
            d["stop_name_ar"] = stop_tr_ar.get(sid, d["stop_name_en"])
            
            d["trip_headsign_en"] = d.get("trip_headsign")
            d["trip_headsign_ar"] = trip_tr_ar.get(trip_id, d["trip_headsign_en"])
            
    return en_result


@app.get("/api/trips/{trip_id}/stop-times-both")
def trip_stop_times_both(
    trip_id: str,
    mode: str = Query(
        default=DEFAULT_MODE,
        description="Which service to query: 'transit' (bus), 'ferry' "
                    "(water) or 'rail'. All are served by this one API; the "
                    "endpoint paths are identical.",
    ),
):
    """
    Return the ordered stop-sequence with arrival/departure timings for
    *trip_id*, containing BOTH English and Arabic translations in the payload.
    """
    with _db(mode) as conn:
        trip_row = conn.execute(
            """
            SELECT t.trip_id, t.route_id, t.service_id, t.direction_id,
                   t.trip_headsign, t.shape_id,
                   r.route_short_name, r.route_long_name,
                   r.route_color, r.route_text_color
            FROM trips t
            JOIN routes r ON t.route_id = r.route_id
            WHERE t.trip_id = ?
            """,
            (trip_id,),
        ).fetchone()
        if trip_row is None:
            raise HTTPException(status_code=404, detail=f"trip_id '{trip_id}' not found")

        stop_rows = conn.execute(
            """
            SELECT
                CAST(st.stop_sequence AS INTEGER) AS stop_sequence,
                st.stop_id,
                s.stop_name,
                s.stop_code,
                CAST(s.stop_lat AS REAL) AS lat,
                CAST(s.stop_lon AS REAL) AS lon,
                st.arrival_time,
                st.departure_time,
                st.stop_headsign,
                st.pickup_type,
                st.drop_off_type,
                st.shape_dist_traveled
            FROM stop_times st
            JOIN stops s ON st.stop_id = s.stop_id
            WHERE st.trip_id = ?
            ORDER BY CAST(st.stop_sequence AS INTEGER)
            """,
            (trip_id,),
        ).fetchall()

        stop_tr_ar = _get_stop_translations(conn, "ar")
        trip_tr_ar = _get_trip_translations(conn, "ar")

    stops = []
    for r in stop_rows:
        d = dict(r)
        d["stop_name_en"] = d.get("stop_name")
        d["stop_name_ar"] = stop_tr_ar.get(d["stop_id"], d["stop_name_en"])
        d["stop_name"] = d["stop_name_en"]
        
        sh_en = d.get("stop_headsign") or trip_row["trip_headsign"]
        d["stop_headsign_en"] = sh_en
        d["stop_headsign_ar"] = trip_tr_ar.get(trip_id, sh_en)
        d["stop_headsign"] = sh_en
        
        stops.append(d)

    trip_headsign_en = trip_row["trip_headsign"]
    trip_headsign_ar = trip_tr_ar.get(trip_id, trip_headsign_en)

    return {
        "trip_id":          trip_row["trip_id"],
        "route_id":         trip_row["route_id"],
        "route_short_name": trip_row["route_short_name"],
        "route_long_name":  trip_row["route_long_name"],
        "route_color":      trip_row["route_color"],
        "route_text_color": trip_row["route_text_color"],
        "service_id":       trip_row["service_id"],
        "direction_id":     trip_row["direction_id"],
        "trip_headsign_en": trip_headsign_en,
        "trip_headsign_ar": trip_headsign_ar,
        "trip_headsign":    trip_headsign_en,
        "shape_id":         trip_row["shape_id"],
        "stop_count":       len(stops),
        "stops":            stops,
    }


@app.get("/api/gtfs-count")
def routes_stops_count(
    mode: str = Query(
        default=DEFAULT_MODE,
        description="Which service to query: 'transit' (bus), 'ferry' "
                    "(water) or 'rail'. All are served by this one API; the "
                    "endpoint paths are identical.",
    ),
):
    """
    Return the total counts of routes and stops in the database.
    """
    with _db(mode) as conn:
        routes_count = conn.execute("SELECT COUNT(*) FROM routes").fetchone()[0]
        stops_count = conn.execute("SELECT COUNT(*) FROM stops").fetchone()[0]
    return {
        "routes": routes_count,
        "stops": stops_count,
    }


# ── Routes running at a given time ───────────────────────────────────────────

# GTFS route_type values for the modes this endpoint is meant for.
#   0 tram, 1 subway, 2 rail, 4 ferry, 5 cable tram, 6 aerial lift,
#   7 funicular, 11 trolleybus, 12 monorail
RAIL_FERRY_ROUTE_TYPES = ("0", "1", "2", "4", "5", "6", "7", "11", "12")


@app.get("/api/routes/running")
def routes_running(
    at: Optional[str] = Query(
        default=None,
        description=(
            "Time to check. Use 'now' or omit for the current moment, or pass "
            "'HH:MM', 'HH:MM:SS', 'YYYY-MM-DD', 'YYYY-MM-DDTHH:MM' or "
            "'YYYY-MM-DDTHH:MM:SS'."
        ),
    ),
    mode: str = Query(
        default=DEFAULT_MODE,
        description="Which service to query: 'transit' (bus), 'ferry' "
                    "(water) or 'rail'. All are served by this one API; the "
                    "endpoint paths are identical.",
    ),
    route_type: Optional[List[int]] = Query(
        default=None,
        description="Optional GTFS route_type filter (repeatable), e.g. "
                    "route_type=2&route_type=4. Defaults to rail/ferry-like "
                    "modes: " + ", ".join(RAIL_FERRY_ROUTE_TYPES) + ".",
    ),
    q: Optional[str] = Query(
        default=None,
        description="Optional search term matched against route_short_name / route_long_name (case-insensitive substring).",
    ),
    limit: int = Query(default=100, ge=1, le=500, description="Max routes to return"),
    lang: Optional[str] = _LANG_QUERY,
):
    """
    Return the rail/ferry routes that have at least one trip running at *at*.

    A trip is considered running when its first departure is at or before *at*
    and its last arrival is at or after *at* on the requested service date.
    Trips whose GTFS times exceed 24:00:00 (past-midnight) are matched against
    the previous service date as well.

    Response includes, per route, the trip that is running and the stop it is
    currently between/at.
    """
    on_date, from_secs = _parse_at(at)

    types = [str(int(t)) for t in route_type] if route_type else list(RAIL_FERRY_ROUTE_TYPES)
    type_placeholders = ",".join("?" * len(types))

    with _db(mode) as conn:
        # Services active on the requested date and on the previous date
        # (the latter for trips whose times spill past 24:00:00).
        active_today = _active_services(conn, on_date)
        active_prev = _active_services(conn, on_date - timedelta(days=1))

        service_offsets: dict[str, int] = {}
        for sid in active_today:
            service_offsets[sid] = 0
        for sid in active_prev:
            service_offsets.setdefault(sid, -86400)

        if not service_offsets:
            return {
                "date":        on_date.isoformat(),
                "from_time":   _secs_to_hhmm(from_secs),
                "route_types": [int(t) for t in types],
                "total":       0,
                "routes":      [],
                "note":        "No active services on this date",
            }

        service_ids = list(service_offsets)
        svc_placeholders = ",".join("?" * len(service_ids))

        where_clauses = [
            f"t.service_id IN ({svc_placeholders})",
            f"r.route_type IN ({type_placeholders})",
        ]
        params: list = [*service_ids, *types]

        if q and q.strip():
            term = f"%{q.strip()}%"
            where_clauses.append(
                "(r.route_short_name LIKE ? COLLATE NOCASE "
                " OR r.route_long_name LIKE ? COLLATE NOCASE)"
            )
            params.extend([term, term])

        rows = conn.execute(
            f"""
            SELECT
                r.route_id,
                r.route_short_name,
                r.route_long_name,
                r.route_color,
                r.route_text_color,
                r.route_type,
                r.route_url,
                r.agency_id,
                t.trip_id,
                t.trip_headsign,
                t.direction_id,
                t.service_id,
                MIN(CAST(st.stop_sequence AS INTEGER)) AS first_sequence,
                MAX(CAST(st.stop_sequence AS INTEGER)) AS last_sequence
            FROM  trips t
            JOIN  routes r ON r.route_id = t.route_id
            JOIN  stop_times st ON st.trip_id = t.trip_id
            WHERE {" AND ".join(where_clauses)}
            GROUP BY t.trip_id
            """,
            params,
        ).fetchall()

        # Keep trips whose [first departure, last arrival] window contains *at*.
        running_trips: list[dict] = []
        trip_ids: list[str] = []
        for row in rows:
            trip_ids.append(row["trip_id"])
        first_last = {}
        if trip_ids:
            ph = ",".join("?" * len(trip_ids))
            for st_row in conn.execute(
                f"""
                SELECT st.trip_id,
                       CAST(st.stop_sequence AS INTEGER) AS stop_sequence,
                       st.stop_id,
                       st.departure_time,
                       st.arrival_time
                FROM stop_times st
                WHERE st.trip_id IN ({ph})
                """,
                trip_ids,
            ).fetchall():
                first_last.setdefault(st_row["trip_id"], {})[
                    st_row["stop_sequence"]
                ] = st_row

        for row in rows:
            seqs = first_last.get(row["trip_id"], {})
            first = seqs.get(row["first_sequence"])
            last = seqs.get(row["last_sequence"])
            if first is None or last is None:
                continue
            try:
                start_secs = _gtfs_time_seconds(first["departure_time"] or first["arrival_time"])
                end_secs = _gtfs_time_seconds(last["arrival_time"] or last["departure_time"])
            except (ValueError, IndexError, TypeError):
                continue
            offset = service_offsets.get(row["service_id"], 0)
            if start_secs + offset <= from_secs <= end_secs + offset:
                running_trips.append({
                    "trip": row,
                    "first_stop_id": first["stop_id"],
                    "last_stop_id": last["stop_id"],
                    "start_secs": start_secs,
                    "end_secs": end_secs,
                    "offset": offset,
                })

        if not running_trips:
            return {
                "date":        on_date.isoformat(),
                "from_time":   _secs_to_hhmm(from_secs),
                "route_types": [int(t) for t in types],
                "total":       0,
                "routes":      [],
            }

        # Resolve stop names and translations in one pass.
        stop_ids = {rt["first_stop_id"] for rt in running_trips} | {
            rt["last_stop_id"] for rt in running_trips
        }
        stop_names: dict[str, str] = {}
        if stop_ids:
            ph = ",".join("?" * len(stop_ids))
            for s in conn.execute(
                f"SELECT stop_id, stop_name FROM stops WHERE stop_id IN ({ph})",
                list(stop_ids),
            ).fetchall():
                stop_names[s["stop_id"]] = s["stop_name"]

        lang_key = (lang or "en").lower()
        stop_tr = _get_stop_translations(conn, lang_key) if lang_key != "en" else {}
        trip_tr = _get_trip_translations(conn, lang_key) if lang_key != "en" else {}

    # Group running trips by route; keep the earliest-ending trip per route
    # (the one most likely to be "the" train/ferry currently in service).
    routes_map: dict[str, dict] = {}
    for rt in running_trips:
        trip = rt["trip"]
        rid = trip["route_id"]
        entry = routes_map.get(rid)
        if entry is None:
            start_name = stop_names.get(rt["first_stop_id"])
            end_name = stop_names.get(rt["last_stop_id"])
            long_name = trip["route_long_name"]
            # Rail/ferry feeds often leave route_long_name empty.
            if not (long_name or "").strip() and start_name and end_name:
                long_name = f"{start_name} → {end_name}"
            entry = {
                "route_id":         rid,
                "route_short_name": trip["route_short_name"],
                "route_long_name":  long_name,
                "route_type":       trip["route_type"],
                "route_color":      trip["route_color"],
                "route_text_color": trip["route_text_color"],
                "route_url":        trip["route_url"],
                "agency_id":        trip["agency_id"],
                "start_stop_name":  start_name,
                "end_stop_name":    end_name,
                "running_trip_count": 0,
                "trips":            [],
            }
            if lang_key != "en":
                entry[f"start_stop_name_{lang_key}"] = stop_tr.get(rt["first_stop_id"], start_name)
                entry[f"end_stop_name_{lang_key}"] = stop_tr.get(rt["last_stop_id"], end_name)
            routes_map[rid] = entry

        entry["running_trip_count"] += 1
        headsign = trip["trip_headsign"]
        trip_entry = {
            "trip_id":        trip["trip_id"],
            "direction_id":   trip["direction_id"],
            "trip_headsign":  headsign,
            "departure_time": _secs_to_hhmmss(rt["start_secs"]),
            "arrival_time":   _secs_to_hhmmss(rt["end_secs"]),
        }
        if lang_key != "en":
            trip_entry[f"trip_headsign_{lang_key}"] = trip_tr.get(trip["trip_id"], headsign)
        entry["trips"].append(trip_entry)

    for entry in routes_map.values():
        entry["trips"].sort(key=lambda x: x["departure_time"])

    routes_list = sorted(
        routes_map.values(),
        key=lambda r: (r["route_short_name"] or "", r["route_id"]),
    )[:limit]

    return {
        "date":        on_date.isoformat(),
        "from_time":   _secs_to_hhmm(from_secs),
        "route_types": [int(t) for t in types],
        "total":       len(routes_map),
        "routes":      routes_list,
    }



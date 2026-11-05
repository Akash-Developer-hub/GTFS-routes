"""
realtime/router.py
==================
FastAPI router providing route, trip, and stop-based lookups from cached GTFS-Realtime snapshots.
No direct upstream requests are ever made from these endpoints.
"""

import time
from typing import Any, Dict, List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query

from .cache import CacheStore, get_cache_store
from .models import (
    Alert,
    FeedMetadata,
    HealthState,
    ResponseEnvelope,
    RouteRealtime,
    StopRealtime,
    TripRealtime,
    TripUpdate,
    VehiclePosition,
)

realtime_router = APIRouter(prefix="/api/realtime", tags=["GTFS-Realtime"])


# ── Health & Diagnostics ──────────────────────────────────────────────────────

@realtime_router.get("/status")
async def get_realtime_status(cache: CacheStore = Depends(get_cache_store)):
    """Return health, freshness, and entity statistics for all GTFS-Realtime feeds."""
    feeds = ["trip_updates", "train_updates", "vehicle_positions", "alerts"]
    status_map = {}
    overall_health = HealthState.HEALTHY

    for f in feeds:
        meta = await cache.get_metadata(f)
        status_map[f] = meta
        if meta.health_state in (HealthState.STALE, HealthState.UNAVAILABLE):
            overall_health = meta.health_state
        elif meta.health_state == HealthState.DEGRADED and overall_health == HealthState.HEALTHY:
            overall_health = HealthState.DEGRADED

    return {
        "success": True,
        "overall_health": overall_health,
        "engine": "redis" if "Redis" in type(cache).__name__ else "in-memory",
        "feeds": status_map,
    }


# ── Route, Trip, and Stop Indexed Lookups ─────────────────────────────────────

@realtime_router.get("/routes/{route_id}", response_model=ResponseEnvelope[RouteRealtime])
async def get_realtime_for_route(
    route_id: str,
    cache: CacheStore = Depends(get_cache_store),
):
    """
    Fetch all active realtime information for a route:
    - Trip delays & stop-time predictions
    - Live vehicle GPS positions
    - Active service alerts affecting this route
    """
    data = await cache.get_by_route(route_id)
    meta = await cache.get_metadata("trip_updates")
    return ResponseEnvelope[RouteRealtime](
        success=True,
        feed="route_realtime",
        health=meta.health_state,
        metadata=meta,
        data=data,
    )


@realtime_router.get("/trips/{trip_id}", response_model=ResponseEnvelope[TripRealtime])
async def get_realtime_for_trip(
    trip_id: str,
    cache: CacheStore = Depends(get_cache_store),
):
    """
    Fetch realtime updates for a single trip:
    - Stop sequence arrival/departure delays
    - Current vehicle position and status
    """
    data = await cache.get_by_trip(trip_id)
    meta = await cache.get_metadata("trip_updates")
    return ResponseEnvelope[TripRealtime](
        success=True,
        feed="trip_realtime",
        health=meta.health_state,
        metadata=meta,
        data=data,
    )


@realtime_router.get("/stops/{stop_id}", response_model=ResponseEnvelope[StopRealtime])
async def get_realtime_for_stop(
    stop_id: str,
    cache: CacheStore = Depends(get_cache_store),
):
    """
    Fetch upcoming realtime arrivals, departures, and alerts calling at a specific stop.
    """
    data = await cache.get_by_stop(stop_id)
    meta = await cache.get_metadata("trip_updates")
    return ResponseEnvelope[StopRealtime](
        success=True,
        feed="stop_realtime",
        health=meta.health_state,
        metadata=meta,
        data=data,
    )


# ── Filtered Feed Endpoints ───────────────────────────────────────────────────

@realtime_router.get("/trip-updates", response_model=ResponseEnvelope[List[TripUpdate]])
async def get_trip_updates(
    feed: str = Query(default="all", pattern="^(all|bus|train)$", description="Feed: 'all', 'bus', or 'train'"),
    route_id: Optional[str] = Query(default=None, description="Filter by route_id"),
    trip_id: Optional[str] = Query(default=None, description="Filter by trip_id"),
    stop_id: Optional[str] = Query(default=None, description="Filter updates touching this stop_id"),
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
    cache: CacheStore = Depends(get_cache_store),
):
    """
    Query cached trip and train updates with filtering.
    """
    # Fast path if single route requested
    if route_id and not stop_id and not trip_id:
        rt_route = await cache.get_by_route(route_id)
        updates = rt_route.trip_updates
        if feed == "bus":
            updates = [u for u in updates if u.feed_source == "trip_updates"]
        elif feed == "train":
            updates = [u for u in updates if u.feed_source == "train_updates"]
        meta = await cache.get_metadata("trip_updates")
        return ResponseEnvelope(
            success=True,
            feed=feed,
            health=meta.health_state,
            metadata=meta,
            data=updates[offset : offset + limit],
        )

    # Collect feeds
    feed_names = []
    if feed in ("all", "bus"):
        feed_names.append("trip_updates")
    if feed in ("all", "train"):
        feed_names.append("train_updates")

    all_entities = []
    meta = await cache.get_metadata(feed_names[0])
    for fn in feed_names:
        entities, _, _ = await cache.get_all_entities(fn, limit=5000, offset=0)
        all_entities.extend(entities)

    # Filter
    filtered = []
    for item in all_entities:
        if route_id and str(item.get("route_id")) != str(route_id):
            continue
        if trip_id and str(item.get("trip_id")) != str(trip_id):
            continue
        if stop_id:
            stus = item.get("stop_time_updates", [])
            if not any(str(stu.get("stop_id")) == str(stop_id) for stu in stus):
                continue
        filtered.append(TripUpdate(**item))

    paginated = filtered[offset : offset + limit]
    return ResponseEnvelope(
        success=True,
        feed=feed,
        health=meta.health_state,
        metadata=meta,
        data=paginated,
    )


@realtime_router.get("/vehicle-positions", response_model=ResponseEnvelope[List[VehiclePosition]])
async def get_vehicle_positions(
    route_id: Optional[str] = Query(default=None, description="Filter by route_id"),
    vehicle_id: Optional[str] = Query(default=None, description="Filter by vehicle_id"),
    bbox: Optional[str] = Query(default=None, description="Bounding box min_lon,min_lat,max_lon,max_lat"),
    limit: int = Query(default=200, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
    cache: CacheStore = Depends(get_cache_store),
):
    """
    Query cached vehicle GPS positions with optional route, vehicle, and bounding box filters.
    """
    if route_id and not bbox and not vehicle_id:
        rt_route = await cache.get_by_route(route_id)
        meta = await cache.get_metadata("vehicle_positions")
        return ResponseEnvelope(
            success=True,
            feed="vehicle_positions",
            health=meta.health_state,
            metadata=meta,
            data=rt_route.vehicles[offset : offset + limit],
        )

    parsed_bbox = None
    if bbox:
        try:
            parts = [float(x.strip()) for x in bbox.split(",")]
            if len(parts) == 4:
                parsed_bbox = parts  # min_lon, min_lat, max_lon, max_lat
        except Exception:
            pass

    entities, _, meta = await cache.get_all_entities("vehicle_positions", limit=10000, offset=0)
    filtered = []
    for item in entities:
        if route_id and str(item.get("route_id")) != str(route_id):
            continue
        if vehicle_id and str(item.get("vehicle_id")) != str(vehicle_id):
            continue
        if parsed_bbox:
            pos = item.get("position") or {}
            lon = pos.get("longitude", 0.0)
            lat = pos.get("latitude", 0.0)
            if not (parsed_bbox[0] <= lon <= parsed_bbox[2] and parsed_bbox[1] <= lat <= parsed_bbox[3]):
                continue
        filtered.append(VehiclePosition(**item))

    return ResponseEnvelope(
        success=True,
        feed="vehicle_positions",
        health=meta.health_state,
        metadata=meta,
        data=filtered[offset : offset + limit],
    )


@realtime_router.get("/alerts", response_model=ResponseEnvelope[List[Alert]])
async def get_alerts(
    route_id: Optional[str] = Query(default=None, description="Filter alerts by route_id"),
    stop_id: Optional[str] = Query(default=None, description="Filter alerts by stop_id"),
    active_only: bool = Query(default=True, description="Only return currently active disruptions"),
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    cache: CacheStore = Depends(get_cache_store),
):
    """
    Query cached service alerts with route and stop filtering.
    """
    if route_id and not stop_id:
        rt_route = await cache.get_by_route(route_id)
        meta = await cache.get_metadata("alerts")
        return ResponseEnvelope(
            success=True,
            feed="alerts",
            health=meta.health_state,
            metadata=meta,
            data=rt_route.alerts[offset : offset + limit],
        )

    entities, _, meta = await cache.get_all_entities("alerts", limit=2000, offset=0)
    now_epoch = int(time.time())
    filtered = []

    for item in entities:
        if active_only:
            periods = item.get("active_periods") or []
            if periods:
                in_active = False
                for p in periods:
                    start = p.get("start") or 0
                    end = p.get("end") or 9999999999
                    if start <= now_epoch <= end:
                        in_active = True
                        break
                if not in_active:
                    continue

        if route_id or stop_id:
            ies = item.get("informed_entities") or []
            match = False
            for ie in ies:
                if route_id and str(ie.get("route_id")) == str(route_id):
                    match = True
                    break
                if stop_id and str(ie.get("stop_id")) == str(stop_id):
                    match = True
                    break
            if not match:
                continue

        filtered.append(Alert(**item))

    return ResponseEnvelope(
        success=True,
        feed="alerts",
        health=meta.health_state,
        metadata=meta,
        data=filtered[offset : offset + limit],
    )


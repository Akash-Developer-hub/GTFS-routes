"""
realtime
========
Netherlands GTFS-Realtime Integration Package.
Provides cached polling, normalization, and API routes for:
  - Trip Updates (bus & train)
  - Vehicle Positions
  - Service Alerts
"""

from .models import (
    Alert,
    FeedMetadata,
    HealthState,
    ResponseEnvelope,
    RouteRealtime,
    StopRealtime,
    StopTimeUpdate,
    TripRealtime,
    TripUpdate,
    VehiclePosition,
)
from .cache import CacheStore, MemoryCacheStore, RedisCacheStore, get_cache_store
from .poller import RealtimePoller, get_realtime_poller
from .router import realtime_router

__all__ = [
    "Alert",
    "FeedMetadata",
    "HealthState",
    "ResponseEnvelope",
    "RouteRealtime",
    "StopRealtime",
    "StopTimeUpdate",
    "TripRealtime",
    "TripUpdate",
    "VehiclePosition",
    "CacheStore",
    "MemoryCacheStore",
    "RedisCacheStore",
    "get_cache_store",
    "RealtimePoller",
    "get_realtime_poller",
    "realtime_router",
]


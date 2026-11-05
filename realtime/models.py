"""
realtime/models.py
==================
Pydantic data models, enums, and response envelopes for GTFS-Realtime data.
"""

from datetime import datetime
from enum import Enum
from typing import Any, Dict, Generic, List, Optional, TypeVar
from pydantic import BaseModel, Field

T = TypeVar("T")


class HealthState(str, Enum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    STALE = "stale"
    UNAVAILABLE = "unavailable"


class ScheduleRelationship(str, Enum):
    SCHEDULED = "SCHEDULED"
    ADDED = "ADDED"
    UNSCHEDULED = "UNSCHEDULED"
    CANCELED = "CANCELED"
    DUPLICATED = "DUPLICATED"
    DELETED = "DELETED"
    SKIPPED = "SKIPPED"
    NO_DATA = "NO_DATA"


class VehicleStopStatus(str, Enum):
    INCOMING_AT = "INCOMING_AT"
    STOPPED_AT = "STOPPED_AT"
    IN_TRANSIT_TO = "IN_TRANSIT_TO"


class OccupancyStatus(str, Enum):
    EMPTY = "EMPTY"
    MANY_SEATS_AVAILABLE = "MANY_SEATS_AVAILABLE"
    FEW_SEATS_AVAILABLE = "FEW_SEATS_AVAILABLE"
    STANDING_ROOM_ONLY = "STANDING_ROOM_ONLY"
    CRUSHED_STANDING_ROOM_ONLY = "CRUSHED_STANDING_ROOM_ONLY"
    FULL = "FULL"
    NOT_ACCEPTING_PASSENGERS = "NOT_ACCEPTING_PASSENGERS"
    NO_DATA_AVAILABLE = "NO_DATA_AVAILABLE"
    NOT_BOARDABLE = "NOT_BOARDABLE"


class FeedMetadata(BaseModel):
    feed_id: str = "netherland"
    feed_type: str
    fetched_at: Optional[str] = None
    cache_age_seconds: float = 0.0
    fetch_duration_ms: float = 0.0
    entity_count: int = 0
    health_state: HealthState = HealthState.UNAVAILABLE
    error_count: int = 0
    last_error: Optional[str] = None
    last_attempt: Optional[str] = None
    last_success: Optional[str] = None
    http_status: Optional[int] = None
    incrementality: str = "FULL_DATASET"


class ResponseEnvelope(BaseModel, Generic[T]):
    success: bool = True
    feed: str
    health: HealthState
    metadata: FeedMetadata
    data: T


# ── Sub-components ────────────────────────────────────────────────────────────

class StopTimeEvent(BaseModel):
    delay: Optional[int] = None
    time: Optional[int] = None
    uncertainty: Optional[int] = None


class StopTimeUpdate(BaseModel):
    stop_sequence: Optional[int] = None
    stop_id: Optional[str] = None
    arrival: Optional[StopTimeEvent] = None
    departure: Optional[StopTimeEvent] = None
    schedule_relationship: Optional[str] = "SCHEDULED"


class VehicleDescriptor(BaseModel):
    id: Optional[str] = None
    label: Optional[str] = None
    license_plate: Optional[str] = None


class Position(BaseModel):
    latitude: float
    longitude: float
    bearing: Optional[float] = None
    odometer: Optional[float] = None
    speed: Optional[float] = None


class InformedEntity(BaseModel):
    agency_id: Optional[str] = None
    route_id: Optional[str] = None
    route_type: Optional[int] = None
    trip_id: Optional[str] = None
    stop_id: Optional[str] = None


class TimeRange(BaseModel):
    start: Optional[int] = None
    end: Optional[int] = None


# ── Main Realtime Entities ───────────────────────────────────────────────────

class TripUpdate(BaseModel):
    entity_id: str
    trip_id: str
    route_id: Optional[str] = None
    direction_id: Optional[int] = None
    start_time: Optional[str] = None
    start_date: Optional[str] = None
    schedule_relationship: Optional[str] = "SCHEDULED"
    vehicle: Optional[VehicleDescriptor] = None
    timestamp: Optional[int] = None
    delay: Optional[int] = None
    stop_time_updates: List[StopTimeUpdate] = Field(default_factory=list)
    feed_source: Optional[str] = None


class VehiclePosition(BaseModel):
    entity_id: str
    vehicle_id: str
    label: Optional[str] = None
    license_plate: Optional[str] = None
    trip_id: Optional[str] = None
    route_id: Optional[str] = None
    direction_id: Optional[int] = None
    start_time: Optional[str] = None
    start_date: Optional[str] = None
    position: Position
    current_stop_sequence: Optional[int] = None
    stop_id: Optional[str] = None
    current_status: Optional[str] = None
    timestamp: Optional[int] = None
    congestion_level: Optional[str] = None
    occupancy_status: Optional[str] = None
    feed_source: Optional[str] = None


class Alert(BaseModel):
    entity_id: str
    alert_id: str
    active_periods: List[TimeRange] = Field(default_factory=list)
    informed_entities: List[InformedEntity] = Field(default_factory=list)
    cause: Optional[str] = "UNKNOWN_CAUSE"
    effect: Optional[str] = "UNKNOWN_EFFECT"
    url: Optional[str] = None
    header_text: Optional[str] = None
    description_text: Optional[str] = None
    severity_level: Optional[str] = "UNKNOWN_SEVERITY"
    feed_source: Optional[str] = None


# ── Filtered Entity Containers ───────────────────────────────────────────────

class RouteRealtime(BaseModel):
    route_id: str
    trip_updates: List[TripUpdate] = Field(default_factory=list)
    vehicles: List[VehiclePosition] = Field(default_factory=list)
    alerts: List[Alert] = Field(default_factory=list)


class TripRealtime(BaseModel):
    trip_id: str
    trip_update: Optional[TripUpdate] = None
    vehicle: Optional[VehiclePosition] = None


class StopRealtime(BaseModel):
    stop_id: str
    upcoming_updates: List[Dict[str, Any]] = Field(default_factory=list)
    alerts: List[Alert] = Field(default_factory=list)


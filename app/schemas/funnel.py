from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from datetime import datetime
from uuid import UUID


class FunnelStepBase(BaseModel):
    step_order: int
    event_name: str
    label: Optional[str] = None


class FunnelStepCreate(FunnelStepBase):
    pass


class FunnelStepUpdate(BaseModel):
    step_order: Optional[int] = None
    event_name: Optional[str] = None
    label: Optional[str] = None


class FunnelStep(FunnelStepBase):
    id: UUID
    org_id: UUID
    funnel_id: UUID
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class FunnelBase(BaseModel):
    name: str
    client_id: Optional[UUID] = None
    slug: Optional[str] = None
    domain: Optional[str] = None
    env: Optional[str] = None


class FunnelCreate(FunnelBase):
    pass


class FunnelUpdate(BaseModel):
    name: Optional[str] = None
    client_id: Optional[UUID] = None
    slug: Optional[str] = None
    domain: Optional[str] = None
    env: Optional[str] = None


class Funnel(FunnelBase):
    id: UUID
    org_id: UUID
    created_at: datetime
    updated_at: datetime
    steps: List[FunnelStep] = []

    class Config:
        from_attributes = True


class FunnelWithSteps(Funnel):
    steps: List[FunnelStep] = []


# Event ingestion schemas
class EventIn(BaseModel):
    funnel_id: Optional[UUID] = None
    client_id: Optional[UUID] = None
    event_name: str = Field(..., min_length=1, max_length=100)
    visitor_id: Optional[str] = None
    session_id: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = Field(default_factory=dict)
    event_timestamp: Optional[datetime] = None
    idempotency_key: Optional[str] = None


class EventResponse(BaseModel):
    event_id: UUID
    status: str = "accepted"


# Analytics schemas
class StepCount(BaseModel):
    step_order: int
    label: Optional[str]
    event_name: str
    count: int
    conversion_rate: Optional[float] = None  # Percentage from previous step


class FunnelHealth(BaseModel):
    funnel_id: UUID
    last_event_at: Optional[datetime]
    events_per_minute: float
    error_count_last_24h: int
    total_events: int
    
    class Config:
        from_attributes = True
        json_encoders = {
            datetime: lambda v: v.isoformat() if v else None
        }


class UTMSourceStats(BaseModel):
    source: str
    count: int
    conversions: int
    revenue_cents: int = 0

class ReferrerStats(BaseModel):
    referrer: str
    count: int
    conversions: int
    revenue_cents: int = 0

class FunnelAnalytics(BaseModel):
    funnel_id: UUID
    range_days: int
    step_counts: List[StepCount]
    total_visitors: int
    total_conversions: int
    overall_conversion_rate: float
    bookings: int = 0
    revenue_cents: int = 0
    top_utm_sources: List[UTMSourceStats] = []
    top_referrers: List[ReferrerStats] = []


class EventExplorerFilter(BaseModel):
    funnel_id: Optional[UUID] = None
    event_name: Optional[str] = None
    visitor_id: Optional[str] = None
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    limit: int = 50
    offset: int = 0


import json

from pydantic import BaseModel, Field, model_validator
from typing import List, Optional, Dict, Any
from datetime import date, datetime
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
    source: Optional[str] = None
    ghl_config: Optional[Dict[str, Any]] = None
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


# Lead capture: create/update client from funnel (lead forms, quiz funnels)
MAX_PROSPECT_BYTES = 50000


class FunnelLeadIn(BaseModel):
    """Payload for creating or updating a client from a funnel lead capture form or quiz."""
    funnel_id: UUID
    email: Optional[str] = None
    name: Optional[str] = Field(None, description="Full name; used if first_name/last_name not provided")
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    phone: Optional[str] = None
    instagram: Optional[str] = Field(None, description="Instagram handle (e.g. @username)")
    notes: Optional[str] = None
    source: Optional[str] = Field(None, description='e.g. "quiz", "opt_in", "form"')
    quiz_answers: Optional[Dict[str, Any]] = None
    opt_in_data: Optional[Dict[str, Any]] = None
    funnel_step_reached: Optional[str] = None
    utm: Optional[Dict[str, Any]] = Field(
        None, description="UTM params from the landing URL: source, medium, campaign, term, content"
    )
    session_id: Optional[str] = Field(
        None, max_length=200, description="trackEvent session id; used to look up UTM when `utm` is omitted"
    )
    visitor_id: Optional[str] = Field(
        None, max_length=200, description="trackEvent visitor id; fallback UTM lookup key"
    )

    @model_validator(mode="after")
    def validate_prospect_payload_size(self):
        """Limit arbitrary JSON size for quiz/opt-in (DoS / prompt injection mitigation)."""
        try:
            blob = json.dumps(
                {"quiz_answers": self.quiz_answers, "opt_in_data": self.opt_in_data},
                default=str,
            )
        except Exception:
            raise ValueError("Invalid prospect data")
        if len(blob.encode("utf-8")) > MAX_PROSPECT_BYTES:
            raise ValueError(f"Prospect payload exceeds {MAX_PROSPECT_BYTES} bytes")
        return self


class FunnelLeadResponse(BaseModel):
    """Response after submitting a funnel lead."""
    client_id: UUID
    created: bool = True  # True if new client, False if existing client was updated
    message: str = "ok"


class FunnelLeadListItem(BaseModel):
    """One lead shown on the funnel Leads tab."""
    id: str  # Stable row key for delete (client_id or notification id)
    client_id: Optional[UUID] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    name: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    instagram: Optional[str] = None
    source: Optional[str] = None
    funnel_step_reached: Optional[str] = None
    lifecycle_state: Optional[str] = None
    is_new_client: Optional[bool] = None
    captured_at: Optional[datetime] = None
    # Quiz / opt-in / other prospect payload answers for the Answers column
    answers: Dict[str, Any] = {}


class FunnelLeadListResponse(BaseModel):
    funnel_id: UUID
    total: int
    leads: List[FunnelLeadListItem]


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
    count: int  # Event count (kept for backward compatibility)
    unique_visitors: int  # Unique visitor count
    conversions: int
    revenue_cents: int = 0

class ReferrerStats(BaseModel):
    referrer: str
    count: int  # Event count (kept for backward compatibility)
    unique_visitors: int  # Unique visitor count
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



# ---------------------------------------------------------------------------
# Funnels dashboard + weekly ad spend (PRD phase 8)
# ---------------------------------------------------------------------------


class FunnelAdSpendIn(BaseModel):
    """Set one week's spend. funnel_id omitted = unassigned (counts toward "All funnels" only)."""
    funnel_id: Optional[UUID] = None
    week_start: date = Field(..., description="Any day in the week; normalized to its Monday")
    amount_usd: float = Field(..., ge=0, le=10_000_000, description="0 with no counts clears the week")
    ads_deployed: Optional[int] = Field(None, ge=0, le=10_000, description="Sheet: New Ads Deployed")
    angles_deployed: Optional[int] = Field(None, ge=0, le=10_000, description="Sheet: New Angles Deployed")


class FunnelAdSpendRead(BaseModel):
    id: UUID
    funnel_id: Optional[UUID] = None
    week_start: date
    amount_usd: float
    ads_deployed: Optional[int] = None
    angles_deployed: Optional[int] = None


class FunnelScorecardMetric(BaseModel):
    key: str
    label: str
    group: str  # "ads" | "funnel" | "close" | "economics"
    format: str  # "int" | "usd" | "pct" (fraction) | "ratio"
    better: str  # "up" | "down" | "neutral"
    values: List[Optional[float]] = []  # one per FunnelScorecard.weeks entry
    benchmark: Optional[float] = None  # average of the complete weeks' values


class FunnelScorecardWeek(BaseModel):
    week_start: date  # Monday
    in_progress: bool = False  # current week: shown, but excluded from the benchmark


class FunnelScorecard(BaseModel):
    weeks: List[FunnelScorecardWeek] = []
    benchmark_weeks: int = 0  # complete weeks averaged into each benchmark
    benchmark_source: str = "range"  # "compare" when the benchmark is the compare range's average week
    metrics: List[FunnelScorecardMetric] = []


class FunnelDashboardTracking(BaseModel):
    status: str  # "live" | "silent" | "errors" | "no_funnels"
    last_event_at: Optional[datetime] = None
    errors_24h: int = 0


class FunnelDashboardMoney(BaseModel):
    has_spend: bool
    spend_usd: float
    paid_cash_usd: float
    cpl_usd: Optional[float] = None
    cac_usd: Optional[float] = None
    roas: Optional[float] = None
    profit_usd: Optional[float] = None


class FunnelDashboardWeek(BaseModel):
    week_start: date
    spend_usd: float
    cash_usd: float
    opt_ins: int
    closed: int
    cac_usd: Optional[float] = None


class FunnelDashboardSource(BaseModel):
    source: str
    opt_ins: int
    booked: int
    closed: int
    cash_usd: float


class FunnelDashboardResponse(BaseModel):
    window_start: date
    window_end: date
    channel: str
    funnel_id: Optional[UUID] = None
    tracking: FunnelDashboardTracking
    visitors: Optional[int] = None
    summary: "KpiFunnelSummaryResponse"
    money: Optional[FunnelDashboardMoney] = None
    weekly: List[FunnelDashboardWeek]
    sources: List[FunnelDashboardSource]
    scorecard: FunnelScorecard


from app.schemas.kpi import KpiFunnelSummaryResponse  # noqa: E402  (after models: avoids import cycles)

FunnelDashboardResponse.model_rebuild()

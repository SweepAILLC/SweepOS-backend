"""KPI Command Center schemas — daily entries, rollups, benchmarks, flags."""
from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, List, Literal, Optional
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator


# ---------------------------------------------------------------------------
# Calculated-field helpers (never persisted)
# ---------------------------------------------------------------------------

def safe_pct(numerator: Optional[float], denominator: Optional[float]) -> Optional[float]:
    """Return percentage (0–100) or None when denominator is missing/zero."""
    if numerator is None or denominator is None:
        return None
    try:
        d = float(denominator)
        if d == 0:
            return None
        return round((float(numerator) / d) * 100.0, 2)
    except (TypeError, ValueError):
        return None


def safe_avg(numerator: Optional[float], denominator: Optional[float]) -> Optional[float]:
    if numerator is None or denominator is None:
        return None
    try:
        d = float(denominator)
        if d == 0:
            return None
        return round(float(numerator) / d, 2)
    except (TypeError, ValueError):
        return None


def compute_rates(row: Dict[str, Any]) -> Dict[str, Optional[float]]:
    outreach = row.get("outreach_sent")
    respondents = row.get("respondents")
    booked = row.get("calls_booked")
    taken = row.get("calls_taken")
    closes = row.get("closes")
    revenue = row.get("revenue")
    return {
        "outreach_reply_pct": safe_pct(respondents, outreach),
        "convo_to_booking_pct": safe_pct(booked, respondents),
        "show_up_pct": safe_pct(taken, booked),
        "closing_rate_pct": safe_pct(closes, taken),
        "avg_order_value": safe_avg(
            float(revenue) if revenue is not None else None,
            float(closes) if closes is not None else None,
        ),
    }


# ---------------------------------------------------------------------------
# Daily entry
# ---------------------------------------------------------------------------

class KpiDailyEntryBase(BaseModel):
    total_followers: Optional[int] = None
    new_followers: Optional[int] = None
    content_posted: Optional[bool] = None
    best_content_type: Optional[str] = Field(default=None, max_length=512)
    inboxes_checked: Optional[int] = None
    outreach_sent: Optional[int] = None
    respondents: Optional[int] = None
    inbound_icp_leads: Optional[int] = None
    followups_sent: Optional[int] = None
    new_conversations: Optional[int] = None
    conversations_nurtured: Optional[int] = None
    calls_pitched: Optional[int] = None
    inbound_bookings: Optional[int] = None
    outbound_bookings: Optional[int] = None
    calls_booked: Optional[int] = None
    calls_taken: Optional[int] = None
    offers_made: Optional[int] = None
    no_shows: Optional[int] = None
    closes: Optional[int] = None
    cash_collected: Optional[Decimal] = None
    revenue: Optional[Decimal] = None
    setter_context: Optional[str] = None


class KpiDailyEntryCreate(KpiDailyEntryBase):
    entry_date: date


class KpiDailyEntryUpdate(KpiDailyEntryBase):
    """Partial update — only provided fields are written."""
    pass


class KpiBulkImportItem(KpiDailyEntryBase):
    entry_date: date


class KpiBulkImportRequest(BaseModel):
    entries: List[KpiBulkImportItem]


class KpiDailyEntryRead(KpiDailyEntryBase):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    org_id: UUID
    entry_date: date
    created_at: datetime
    updated_at: datetime

    # Calculated (never stored)
    outreach_reply_pct: Optional[float] = None
    convo_to_booking_pct: Optional[float] = None
    show_up_pct: Optional[float] = None
    closing_rate_pct: Optional[float] = None
    avg_order_value: Optional[float] = None

    @classmethod
    def from_orm_row(cls, row: Any) -> "KpiDailyEntryRead":
        data = {
            "id": row.id,
            "org_id": row.org_id,
            "entry_date": row.entry_date,
            "total_followers": row.total_followers,
            "new_followers": row.new_followers,
            "content_posted": row.content_posted,
            "best_content_type": row.best_content_type,
            "inboxes_checked": row.inboxes_checked,
            "outreach_sent": row.outreach_sent,
            "respondents": row.respondents,
            "inbound_icp_leads": row.inbound_icp_leads,
            "followups_sent": row.followups_sent,
            "new_conversations": getattr(row, "new_conversations", None),
            "conversations_nurtured": getattr(row, "conversations_nurtured", None),
            "calls_pitched": row.calls_pitched,
            "inbound_bookings": getattr(row, "inbound_bookings", None),
            "outbound_bookings": getattr(row, "outbound_bookings", None),
            "calls_booked": row.calls_booked,
            "calls_taken": row.calls_taken,
            "offers_made": row.offers_made,
            "no_shows": row.no_shows,
            "closes": row.closes,
            "cash_collected": row.cash_collected,
            "revenue": row.revenue,
            "setter_context": getattr(row, "setter_context", None),
            "created_at": row.created_at,
            "updated_at": row.updated_at,
        }
        data.update(compute_rates(data))
        return cls(**data)


class KpiBulkImportResponse(BaseModel):
    imported: int = 0
    entries: List[KpiDailyEntryRead] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Monthly rollups
# ---------------------------------------------------------------------------

class KpiMonthlyRollup(BaseModel):
    period_label: str  # e.g. "2026-07"
    period_start: date
    period_end: date
    days_with_data: int = 0

    # Sums (additive)
    new_followers: int = 0
    outreach_sent: int = 0
    respondents: int = 0
    inbound_icp_leads: int = 0
    followups_sent: int = 0
    new_conversations: int = 0
    conversations_nurtured: int = 0
    calls_pitched: int = 0
    inbound_bookings: int = 0
    outbound_bookings: int = 0
    calls_booked: int = 0
    calls_taken: int = 0
    offers_made: int = 0
    no_shows: int = 0
    closes: int = 0
    cash_collected: float = 0.0
    revenue: float = 0.0
    content_posted_days: int = 0

    # Averages of rates (only days with a defined rate)
    avg_outreach_reply_pct: Optional[float] = None
    avg_convo_to_booking_pct: Optional[float] = None
    avg_show_up_pct: Optional[float] = None
    avg_closing_rate_pct: Optional[float] = None
    # Also recompute from month totals (often more meaningful)
    outreach_reply_pct: Optional[float] = None
    convo_to_booking_pct: Optional[float] = None
    show_up_pct: Optional[float] = None
    closing_rate_pct: Optional[float] = None
    avg_order_value: Optional[float] = None


# ---------------------------------------------------------------------------
# Benchmarks
# ---------------------------------------------------------------------------

TierName = Literal["strong", "okay", "weak"]


class MetricThreshold(BaseModel):
    """Threshold bands. `okay_min`/`okay_max` define the okay band; below okay_min = weak, at/above strong_min = strong."""

    strong_min: float
    okay_min: float
    # Optional upper bound for okay (when okay is a closed band before strong).
    # If omitted, okay is okay_min <= x < strong_min.
    okay_max: Optional[float] = None
    # Unit hint for UI
    unit: Literal["count", "percent"] = "count"


# Target band = okay_min … strong_min. Below okay_min = weak; at/above strong_min = strong.
DEFAULT_KPI_THRESHOLDS: Dict[str, Dict[str, Any]] = {
    "daily_dm_reachouts": {"strong_min": 30, "okay_min": 20, "unit": "count"},
    "daily_followups": {"strong_min": 20, "okay_min": 10, "unit": "count"},
    "dm_response_rate": {"strong_min": 20, "okay_min": 3, "unit": "percent"},
    "convo_to_booking_rate": {"strong_min": 20, "okay_min": 10, "unit": "percent"},
    "show_up_rate": {"strong_min": 100, "okay_min": 70, "unit": "percent"},
    "closing_rate": {"strong_min": 60, "okay_min": 30, "unit": "percent"},
}

DEFAULT_CONTENT_TYPE_TAGS = ["Reel", "Carousel", "Story", "Static", "Live"]


class KpiBenchmarks(BaseModel):
    org_id: UUID
    thresholds: Dict[str, MetricThreshold]
    content_type_tags: List[str] = Field(default_factory=lambda: list(DEFAULT_CONTENT_TYPE_TAGS))
    updated_at: Optional[datetime] = None
    entry_form_token: Optional[str] = None


class KpiBenchmarksUpdate(BaseModel):
    thresholds: Optional[Dict[str, MetricThreshold]] = None
    content_type_tags: Optional[List[str]] = None

    @field_validator("content_type_tags")
    @classmethod
    def _non_empty_tags(cls, v: Optional[List[str]]) -> Optional[List[str]]:
        if v is None:
            return v
        cleaned = [t.strip() for t in v if t and t.strip()]
        return cleaned or list(DEFAULT_CONTENT_TYPE_TAGS)


# ---------------------------------------------------------------------------
# Bottleneck flags
# ---------------------------------------------------------------------------

RelatedFeature = Literal["content_studio", "automations", "call_library"]


class KpiFlag(BaseModel):
    id: str
    metric: str
    stage: str
    tier: TierName
    message: str
    comparison: Optional[str] = None
    related_feature: Optional[RelatedFeature] = None
    severity: Literal["info", "watch", "critical"] = "watch"
    window_start: Optional[date] = None
    window_end: Optional[date] = None


class KpiFlagsResponse(BaseModel):
    flags: List[KpiFlag]
    generated_at: datetime


class KpiEntryLinkResponse(BaseModel):
    token: str
    url: str


class KpiAutopopulateStatusResponse(BaseModel):
    calendar_available: bool = False
    payments_available: bool = False
    # Column keys that should be presented as AUTO in UI.
    autopopulated_columns: List[str] = Field(default_factory=list)



# ---------------------------------------------------------------------------
# Compact snapshot (owner dashboard / terminal / cross-tab)
# ---------------------------------------------------------------------------


class KpiSnapshotCard(BaseModel):
    key: str
    label: str
    value: Optional[float] = None
    kind: Literal["int", "pct", "currency"] = "int"
    aggregation: Literal["sum", "avg", "ratio"] = "sum"
    tier: Optional[TierName] = None


class KpiSnapshotSeriesPoint(BaseModel):
    date: date
    outreach_sent: Optional[int] = None
    calls_booked: Optional[int] = None
    calls_taken: Optional[int] = None
    closes: Optional[int] = None
    cash_collected: Optional[float] = None
    revenue: Optional[float] = None
    show_up_pct: Optional[float] = None
    closing_rate_pct: Optional[float] = None
    convo_to_booking_pct: Optional[float] = None
    outreach_reply_pct: Optional[float] = None


class KpiSnapshotResponse(BaseModel):
    """Essential KPI insights for dashboards and graphs outside the Command Center."""

    range_start: date
    range_end: date
    days: int
    generated_at: datetime
    days_with_data: int = 0
    cards: List[KpiSnapshotCard] = Field(default_factory=list)
    current_month: Optional[KpiMonthlyRollup] = None
    flags: List[KpiFlag] = Field(default_factory=list)
    flags_truncated: int = 0
    series: List[KpiSnapshotSeriesPoint] = Field(default_factory=list)
    calendar_available: bool = False
    payments_available: bool = False

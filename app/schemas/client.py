from pydantic import BaseModel, ConfigDict, Field, field_validator
from datetime import datetime, timezone
from typing import List, Optional, Union
from decimal import Decimal
import uuid
from app.models.client import LifecycleState


def _naive_utc_program(dt: datetime) -> datetime:
    """Program dates are stored/compared as naive UTC (matches utcnow in model helpers)."""
    if dt.tzinfo is None:
        return dt
    return dt.astimezone(timezone.utc).replace(tzinfo=None)


def parse_optional_program_datetime(v):
    """Parse program date fields from API (ISO-Z) or datetime; always naive UTC."""
    if v is None or v == "":
        return None
    if isinstance(v, datetime):
        return _naive_utc_program(v)
    if isinstance(v, str):
        try:
            s = v
            if s.endswith("Z"):
                s = s.replace("Z", "+00:00")
            if "T" not in s and len(s) == 10:
                s = s + "T00:00:00"
            return _naive_utc_program(datetime.fromisoformat(s))
        except Exception:
            raise ValueError(f"Invalid datetime format: {v}")
    return v


class ClientBase(BaseModel):
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    email: Optional[str] = None  # Primary email (backward compat)
    emails: Optional[List[str]] = None  # Additional emails; client can have multiple
    phone: Optional[str] = None
    instagram: Optional[str] = None
    lifecycle_state: LifecycleState = LifecycleState.COLD_LEAD
    stripe_customer_id: Optional[str] = None
    estimated_mrr: Optional[Union[float, Decimal]] = 0.0
    notes: Optional[str] = None
    # Program tracking fields
    program_start_date: Optional[datetime] = None
    program_duration_days: Optional[int] = None
    program_end_date: Optional[datetime] = None
    
    @field_validator('estimated_mrr', mode='before')
    @classmethod
    def convert_decimal_to_float(cls, v):
        """Convert Decimal to float for serialization"""
        if isinstance(v, Decimal):
            return float(v)
        return v if v is not None else 0.0
    
    @field_validator('email', mode='before')
    @classmethod
    def validate_email(cls, v):
        """Accept any string as email (including test emails like @stripe.test)"""
        # Just return the string as-is - no validation
        # This allows test emails that EmailStr would reject
        return str(v) if v is not None else None
    
    @field_validator('emails', mode='before')
    @classmethod
    def normalize_emails_list(cls, v):
        """Coerce DB JSON to a clean list of strings so email-only clients serialize reliably."""
        if v is None:
            return None
        if isinstance(v, list):
            out: List[str] = []
            for e in v:
                if e is None:
                    continue
                s = str(e).strip()
                if s:
                    out.append(s)
            return out if out else None
        return None

    @field_validator("program_start_date", "program_end_date", mode="before")
    @classmethod
    def parse_program_date(cls, v):
        return parse_optional_program_datetime(v)


class ClientCreate(ClientBase):
    pass


class MergeClientsRequest(BaseModel):
    """Request to merge multiple client records into one (e.g. same email)."""
    client_ids: List[uuid.UUID]


class ClientUpdate(BaseModel):
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    email: Optional[str] = None
    emails: Optional[List[str]] = None
    phone: Optional[str] = None
    instagram: Optional[str] = None
    lifecycle_state: Optional[LifecycleState] = None
    stripe_customer_id: Optional[str] = None
    estimated_mrr: Optional[float] = None
    notes: Optional[str] = None
    meta: Optional[dict] = None  # e.g. sort_orders: { cold_lead: 0, warm_lead: 1 } for column ordering
    # Program tracking fields
    program_start_date: Optional[datetime] = None
    program_duration_days: Optional[int] = None
    program_end_date: Optional[datetime] = None
    program_progress_percent: Optional[float] = None
    
    @field_validator("program_start_date", "program_end_date", mode="before")
    @classmethod
    def parse_program_dates_update(cls, v):
        return parse_optional_program_datetime(v)


class Client(ClientBase):
    id: uuid.UUID
    tenant_id: Optional[uuid.UUID] = None
    last_activity_at: Optional[datetime] = None
    lifetime_revenue_cents: Optional[int] = 0
    notes: Optional[str] = None
    meta: Optional[dict] = None
    # Program tracking fields (read-only, calculated)
    program_end_date: Optional[datetime] = None
    program_progress_percent: Optional[Union[float, Decimal]] = None
    created_at: datetime
    updated_at: datetime

    @field_validator('program_progress_percent', mode='before')
    @classmethod
    def coerce_program_progress_percent(cls, v):
        """SQLAlchemy may return Decimal; coerce so unnamed/email-only clients never fail validation."""
        if v is None:
            return None
        if isinstance(v, Decimal):
            return float(v)
        try:
            return float(v)
        except (TypeError, ValueError):
            return None

    @field_validator('meta', mode='before')
    @classmethod
    def coerce_meta_dict(cls, v):
        if v is None:
            return None
        if isinstance(v, dict):
            return v
        return None

    class Config:
        from_attributes = True
        json_encoders = {
            Decimal: lambda v: float(v) if v is not None else 0.0
        }


# Terminal dashboard summary (cash, MRR, top contributors) - precomputed to avoid N+1
class TerminalCashCollected(BaseModel):
    today: float = 0.0
    last_7_days: float = 0.0
    last_30_days: float = 0.0
    last_mtd: float = 0.0  # Month to date (1st of current month to now)


class TerminalCashSourceTotals(BaseModel):
    """Cash collected from one source (same window definitions as TerminalCashCollected)."""

    today: float = 0.0
    last_7_days: float = 0.0
    last_30_days: float = 0.0
    last_mtd: float = 0.0


class TerminalCashBySourceBreakdown(BaseModel):
    stripe: TerminalCashSourceTotals = Field(default_factory=TerminalCashSourceTotals)
    whop: TerminalCashSourceTotals = Field(default_factory=TerminalCashSourceTotals)
    manual: TerminalCashSourceTotals = Field(default_factory=TerminalCashSourceTotals)


class TerminalMRR(BaseModel):
    current_mrr: float = 0.0
    arr: float = 0.0


class TerminalTopContributor(BaseModel):
    client_id: str
    display_name: str
    revenue: float
    last_payment_date: Optional[str] = None
    merged_client_ids: Optional[List[str]] = None


class TerminalSummaryResponse(BaseModel):
    cash_collected: TerminalCashCollected
    mrr: TerminalMRR
    top_contributors_30d: List[TerminalTopContributor]
    top_contributors_90d: List[TerminalTopContributor]
    cash_by_source: Optional[TerminalCashBySourceBreakdown] = None


# Client health score (logic-based; AI-ready factors for future referral/testimonial/retention/upsell)
class ClientHealthFactor(BaseModel):
    key: str
    label: str
    value: Optional[Union[float, int]] = None
    raw: Optional[dict] = None
    unit: Optional[str] = None
    description: Optional[str] = None


class ClientHealthScoreResponse(BaseModel):
    client_id: str
    score: float
    grade: str
    factors: List[ClientHealthFactor]
    computed_at: Optional[str] = None
    source: Optional[str] = None  # "logic" | "ai"
    explanation: Optional[str] = None
    source_reason: Optional[str] = None  # e.g. ai_unavailable when AI requested but skipped


# AI recommendation checklist (modular actions; manual completion until call-insights pipeline)
class AIRecommendationActionOut(BaseModel):
    model_config = ConfigDict(extra="ignore")

    id: str
    title: str
    detail: Optional[str] = None
    category: Optional[str] = None
    priority: int = 0
    completed: bool = False
    completed_at: Optional[str] = None
    supports_email_draft: bool = False


class ClientAIRecommendationsResponse(BaseModel):
    client_id: str
    headline: Optional[str] = None
    actions: List[AIRecommendationActionOut]
    updated_at: Optional[str] = None


class AIRecommendationActionPatch(BaseModel):
    completed: bool


class AIRecommendationEmailDraftResponse(BaseModel):
    subject: str
    body_plain: str
    body_html: str
    source: str = "template"  # "llm" | "template"


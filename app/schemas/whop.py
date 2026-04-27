from pydantic import BaseModel, Field
from typing import Any, Dict, List, Optional


class WhopConnectRequest(BaseModel):
    api_key: str = Field(..., min_length=8)
    company_id: str = Field(..., description="Whop company id, e.g. biz_…")


class WhopConnectionStatus(BaseModel):
    connected: bool
    company_id: Optional[str] = None
    message: str = ""


class WhopPaymentOut(BaseModel):
    id: str
    whop_id: str
    amount_cents: int
    currency: str
    status: str
    client_id: Optional[str] = None
    payer_email: Optional[str] = None
    created_at: int  # unix seconds


class WhopSummaryOut(BaseModel):
    payment_count: int
    succeeded_cents_30d: int
    succeeded_cents_mtd: int


class WhopRevenueTimelinePoint(BaseModel):
    t: int  # unix day start or bucket — use date string for clarity
    label: str
    amount_cents: int


class WhopRevenueTimelineResponse(BaseModel):
    timeline: List[WhopRevenueTimelinePoint]
    group_by: str = "day"

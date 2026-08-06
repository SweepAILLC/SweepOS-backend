"""Schemas for the public post-sales close survey."""
from __future__ import annotations

from datetime import date, datetime
from typing import List, Literal, Optional
from uuid import UUID

from pydantic import BaseModel, Field


class CloseSurveyEntryLinkResponse(BaseModel):
    token: str
    url: str


class CloseSurveyClientOption(BaseModel):
    id: str
    name: str
    email: Optional[str] = None
    lifecycle_state: str


class CloseSurveyOfferOption(BaseModel):
    slot: str
    label: str
    suggested_total_cents: Optional[int] = None


class CloseSurveyMetaResponse(BaseModel):
    org_name: str
    clients: List[CloseSurveyClientOption]
    offers: List[CloseSurveyOfferOption]


PaymentSource = Literal["manual", "stripe", "whop", "none"]
DealOutcome = Literal["yes", "no", "no_show"]


class CloseSurveySubmitRequest(BaseModel):
    client_id: UUID
    closed: bool = False
    deal_outcome: Optional[DealOutcome] = None
    payment_source: PaymentSource = "none"
    cash_collected: Optional[float] = Field(None, ge=0, description="Dollars")
    offer_slot: Optional[str] = Field(None, max_length=48)
    offer_name: Optional[str] = Field(None, max_length=220)
    contract_amount: Optional[float] = Field(None, ge=0, description="Dollars → offer total")
    recording_url: Optional[str] = Field(None, max_length=2000)
    call_notes: Optional[str] = Field(None, max_length=8000)
    entry_date: Optional[date] = None


class CloseSurveySubmitResponse(BaseModel):
    ok: bool = True
    client_id: str
    closed: bool
    deal_outcome: DealOutcome
    payment_source: PaymentSource
    manual_payment_id: Optional[str] = None
    lifecycle_state: Optional[str] = None
    message: str = "Logged — pipeline / payments / KPI will refresh."
    submitted_at: datetime

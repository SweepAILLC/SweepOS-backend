"""Pydantic models for Performance tab snapshot and prescription APIs."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field


class FunnelStepDropOut(BaseModel):
    model_config = ConfigDict(extra="ignore")

    step_order: int
    label: Optional[str] = None
    event_name: str
    count: int
    conversion_rate_pct: Optional[float] = None


class FunnelPerformanceSummary(BaseModel):
    model_config = ConfigDict(extra="ignore")

    funnel_id: str
    name: str
    range_days: int
    total_visitors: int
    total_conversions: int
    overall_conversion_rate_pct: float
    step_drops: List[FunnelStepDropOut] = Field(default_factory=list)


class FailedPaymentSample(BaseModel):
    model_config = ConfigDict(extra="ignore")

    id: str
    amount_cents: int
    currency: str = "usd"
    created_at: Optional[str] = None
    client_id: Optional[str] = None


class PerformanceDiagnosisOut(BaseModel):
    model_config = ConfigDict(extra="ignore")

    traffic: str  # ok | watch | risk
    nurture: str
    conversion: str
    traffic_hint: str = ""
    nurture_hint: str = ""
    conversion_hint: str = ""
    pipeline_strip: Optional[Dict[str, Any]] = None
    revenue_compare: Optional[Dict[str, Any]] = None
    funnel_compare: Optional[Dict[str, Any]] = None
    insights: List[str] = Field(default_factory=list)


class PerformanceTaskOut(BaseModel):
    model_config = ConfigDict(extra="ignore")

    id: str
    title: str
    category: str
    impact_score: float
    confidence: float = 1.0
    evidence: Dict[str, Any] = Field(default_factory=dict)
    recommended_actions: List[str] = Field(default_factory=list)
    why: str = ""
    prescription: str = ""
    next_step: str = ""
    completed: bool = False


class PerformanceTaskEmailDraftOut(BaseModel):
    """Saved email draft for a Performance task (one per task id)."""

    model_config = ConfigDict(extra="ignore")

    task_id: str
    subject: str = ""
    body_plain: str = ""
    body_html: str = ""
    source: str = "llm"  # llm | template | placeholder
    generated_at: str = ""
    client_id: Optional[str] = None
    # Lets the UI link straight to Brevo for that contact when present.
    client_email: Optional[str] = None
    # Truthy when the draft is missing because the task isn't tied to a client.
    skipped_reason: Optional[str] = None


class PerformanceSnapshotResponse(BaseModel):
    model_config = ConfigDict(extra="ignore")

    generated_at: str
    pipeline: Dict[str, Any]
    revenue: Dict[str, Any]
    failed_payments: Dict[str, Any]
    funnels: List[FunnelPerformanceSummary]
    diagnosis: PerformanceDiagnosisOut
    tasks: List[PerformanceTaskOut]
    # Mirrors Intelligence tab pipeline_priorities (used to rank ROI + org tasks).
    pipeline_priorities: List[str] = Field(default_factory=list)
    # Persisted Performance email drafts (auto-generated + on-demand). Keyed by task_id on the client.
    drafts: List[PerformanceTaskEmailDraftOut] = Field(default_factory=list)


class PerformanceTasksPatchBody(BaseModel):
    completed_task_ids: List[str] = Field(default_factory=list, max_length=500)


class PerformanceTasksPatchResponse(BaseModel):
    model_config = ConfigDict(extra="ignore")

    completed_task_ids: List[str]
    updated_at: Optional[str] = None


class PerformancePrescriptionBody(BaseModel):
    """Optional subset of task IDs to enrich; empty = all non-completed top tasks."""

    task_ids: List[str] = Field(default_factory=list, max_length=50)


class PerformancePrescriptionTaskOut(BaseModel):
    model_config = ConfigDict(extra="ignore")

    id: str
    why: str = ""
    prescription: str = ""
    next_step: str = ""


class PerformancePrescriptionResponse(BaseModel):
    model_config = ConfigDict(extra="ignore")

    tasks: List[PerformancePrescriptionTaskOut]
    source: str = "deterministic"  # llm | deterministic


class PerformanceEmailDraftsBody(BaseModel):
    """Request body for POST /performance/email-drafts: which tasks to draft for."""

    task_ids: List[str] = Field(default_factory=list, max_length=20)
    # If true, regenerate even when a saved draft already exists.
    force: bool = False


class PerformanceEmailDraftsResponse(BaseModel):
    model_config = ConfigDict(extra="ignore")

    drafts: List[PerformanceTaskEmailDraftOut] = Field(default_factory=list)
    skipped: List[str] = Field(default_factory=list)
    source: str = "llm"


def performance_state_from_ai_profile(ai_profile: Any) -> Dict[str, Any]:
    if not ai_profile or not isinstance(ai_profile, dict):
        return {}
    raw = ai_profile.get("performance_state")
    return raw if isinstance(raw, dict) else {}

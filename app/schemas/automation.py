"""Pydantic schemas for the automation API (rules, jobs, dispatcher health, outreach inbox)."""
from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator

from app.models.automation import (
    CONTENT_MODE_VALUES,
    FLOW_VALUES,
    JOB_STATE_VALUES,
    NODE_KIND_VALUES,
    PLAYBOOK_VALUES,
    SCHEDULE_MODE_VALUES,
    TRIGGER_KIND_VALUES,
)


# -- Rules ---------------------------------------------------------------------

class HtmlTemplateRef(BaseModel):
    """Reference to a saved Intelligence writing-sample HTML template (single source of truth)."""
    kind: Literal["writing_samples_by_title", "writing_samples_by_kind"]
    title: Optional[str] = None
    sample_kind: Optional[str] = None


class AudienceFilter(BaseModel):
    lifecycle_in: Optional[List[str]] = None
    min_lifetime_revenue_cents: Optional[int] = None
    program_progress_min_percent: Optional[float] = None
    program_progress_max_percent: Optional[float] = None


class BookingTriggerConfig(BaseModel):
    """Trigger config for the pre_sale_post_booking playbook.

    The engine reads this to decide whether a freshly created Calendly/Cal.com booking
    should fire the rule. ``event_type_ids`` is a list of provider-native ids
    (Cal.com eventType.id as string, Calendly event_type URI). Set ``match_all_events``
    to True to fire on any *sales* booking from the chosen provider without picking
    specific events (off by default to prevent accidental blast-emails).

    Regardless of selection, the engine only enqueues when the booking is marked as a
    sales call (Calendar event-type sales toggle / per-booking override).
    """
    provider: Literal["calcom", "calendly", "any"] = "any"
    event_type_ids: Optional[List[str]] = None
    match_all_events: bool = False
    # Documented for API clarity; backend always enforces sales-only today.
    sales_calls_only: bool = True


_AUTOMATION_AI_PROMPT_MAX_LEN = 8000


class AutomationRuleBase(BaseModel):
    enabled: bool = False
    delay_seconds: int = Field(0, ge=0, le=60 * 60 * 24 * 14)  # up to 14 days
    content_mode: str = "ai_generated"
    subject_template: Optional[str] = None
    html_template_ref: Optional[HtmlTemplateRef] = None
    # When content_mode is ai_generated: appended to the base system prompt for this playbook.
    ai_content_system_prompt: Optional[str] = Field(None, max_length=_AUTOMATION_AI_PROMPT_MAX_LEN)
    audience_filter: Optional[AudienceFilter] = None
    trigger_config: Optional[BookingTriggerConfig] = None
    opportunity_priority: Optional[List[str]] = None
    # 0 = full LLM autonomy (up to 3 asks); 1–3 = hard cap on combined opportunities.
    combine_top_n: int = Field(1, ge=0, le=3)
    require_approval: bool = False
    approval_ttl_hours: Optional[int] = Field(None, ge=1, le=24 * 14)
    # Flow metadata (optional on update — preserve existing when omitted).
    flow: Optional[str] = None
    trigger_kind: Optional[str] = None
    schedule_mode: Optional[str] = None
    step_index: Optional[int] = Field(None, ge=0, le=10_000)
    node_kind: Optional[str] = None

    @field_validator("ai_content_system_prompt")
    @classmethod
    def _normalize_ai_prompt(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return None
        t = v.strip()
        return t if t else None

    @field_validator("content_mode")
    @classmethod
    def _validate_content_mode(cls, v: str) -> str:
        if v not in CONTENT_MODE_VALUES:
            raise ValueError(f"content_mode must be one of {CONTENT_MODE_VALUES}")
        return v

    @field_validator("flow")
    @classmethod
    def _validate_flow(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        if v not in FLOW_VALUES:
            raise ValueError(f"flow must be one of {FLOW_VALUES}")
        return v

    @field_validator("trigger_kind")
    @classmethod
    def _validate_trigger_kind(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        if v not in TRIGGER_KIND_VALUES:
            raise ValueError(f"trigger_kind must be one of {TRIGGER_KIND_VALUES}")
        return v

    @field_validator("schedule_mode")
    @classmethod
    def _validate_schedule_mode(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        if v not in SCHEDULE_MODE_VALUES:
            raise ValueError(f"schedule_mode must be one of {SCHEDULE_MODE_VALUES}")
        return v

    @field_validator("node_kind")
    @classmethod
    def _validate_node_kind(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        if v not in NODE_KIND_VALUES:
            raise ValueError(f"node_kind must be one of {NODE_KIND_VALUES}")
        return v

    @field_validator("opportunity_priority")
    @classmethod
    def _validate_priority(cls, v: Optional[List[str]]) -> Optional[List[str]]:
        if v is None:
            return v
        allowed = {"referral", "upsell", "testimonial"}
        for x in v:
            if x not in allowed:
                raise ValueError(f"opportunity_priority entries must be in {allowed}")
        return v


class AutomationRuleUpdate(AutomationRuleBase):
    pass


class AutomationRuleRead(AutomationRuleBase):
    model_config = ConfigDict(from_attributes=True)
    id: uuid.UUID
    org_id: uuid.UUID
    playbook: str
    flow: Optional[str] = None
    trigger_kind: Optional[str] = None
    schedule_mode: Optional[str] = None
    step_index: int = 0
    node_kind: str = "action"
    last_modified_by: Optional[uuid.UUID] = None
    created_at: datetime
    updated_at: datetime
    is_protected: bool = False

    def model_post_init(self, __context: Any) -> None:
        # Soft label only — canvas allows deleting any step.
        object.__setattr__(self, "is_protected", False)
        if not self.node_kind:
            object.__setattr__(self, "node_kind", "action")


class AutomationFlowStepCreate(BaseModel):
    """Add a wait or action node to a flow trigger chain."""
    trigger_kind: str
    node_kind: str = "action"
    schedule_mode: str = "after_previous"
    delay_seconds: int = Field(0, ge=0, le=60 * 60 * 24 * 14)
    subject_template: Optional[str] = "Quick follow-up, {{first_name}}"
    insert_before_playbook: Optional[str] = None

    @field_validator("trigger_kind")
    @classmethod
    def _validate_trigger_kind(cls, v: str) -> str:
        if v not in TRIGGER_KIND_VALUES:
            raise ValueError(f"trigger_kind must be one of {TRIGGER_KIND_VALUES}")
        return v

    @field_validator("schedule_mode")
    @classmethod
    def _validate_schedule_mode(cls, v: str) -> str:
        if v not in SCHEDULE_MODE_VALUES:
            raise ValueError(f"schedule_mode must be one of {SCHEDULE_MODE_VALUES}")
        return v

    @field_validator("node_kind")
    @classmethod
    def _validate_node_kind(cls, v: str) -> str:
        if v not in NODE_KIND_VALUES:
            raise ValueError(f"node_kind must be one of {NODE_KIND_VALUES}")
        return v


# -- Jobs ----------------------------------------------------------------------

class AutomationEmailJobRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    org_id: uuid.UUID
    rule_id: Optional[uuid.UUID] = None
    client_id: uuid.UUID
    playbook: str
    trigger_event: Optional[str] = None
    idempotency_key: str
    scheduled_at: datetime
    state: str
    payload_json: Optional[Dict[str, Any]] = None
    attempts: int
    last_attempt_at: Optional[datetime] = None
    dispatched_at: Optional[datetime] = None
    brevo_message_id: Optional[str] = None
    error_text: Optional[str] = None
    created_at: datetime
    updated_at: datetime


class AutomationEmailJobListResponse(BaseModel):
    items: List[AutomationEmailJobRead]
    total: int


class JobStateUpdate(BaseModel):
    state: str

    @field_validator("state")
    @classmethod
    def _validate(cls, v: str) -> str:
        if v not in JOB_STATE_VALUES:
            raise ValueError(f"state must be one of {JOB_STATE_VALUES}")
        return v


# -- Dispatcher health ---------------------------------------------------------

class DispatcherHealth(BaseModel):
    healthy: bool
    last_tick_at: Optional[datetime] = None
    seconds_since_tick: Optional[int] = None
    worker_pid: Optional[int] = None
    worker_host: Optional[str] = None
    queue_depth: int = 0
    in_flight: int = 0
    awaiting_approval: int = 0
    rq_enabled: bool = False
    notes: Optional[str] = None


# -- Preview / draft round-trip ------------------------------------------------

class AutomationPreviewRequest(BaseModel):
    playbook: str
    client_id: uuid.UUID
    content_mode: Optional[str] = None  # override rule.content_mode for preview
    subject_template: Optional[str] = None
    html_template_ref: Optional[HtmlTemplateRef] = None
    ai_content_system_prompt: Optional[str] = Field(None, max_length=_AUTOMATION_AI_PROMPT_MAX_LEN)

    @field_validator("ai_content_system_prompt")
    @classmethod
    def _normalize_preview_ai_prompt(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return None
        t = v.strip()
        return t if t else None

    @field_validator("playbook")
    @classmethod
    def _validate_playbook(cls, v: str) -> str:
        import re
        if not re.fullmatch(r"[a-z][a-z0-9_]{0,62}", v or ""):
            raise ValueError("playbook must be a lowercase snake_case id (max 63 chars)")
        return v


class AutomationPreviewResponse(BaseModel):
    subject: str
    body_plain: str
    html: str
    chosen_opportunities: List[str] = []
    merge_tags_resolved: Dict[str, str] = {}
    notes: List[str] = []


class AutomationFlowTestRequest(BaseModel):
    """Send lightweight test emails for one flow's enabled action steps."""
    email: str = Field(..., min_length=3, max_length=320)
    client_id: Optional[uuid.UUID] = None
    trigger_kind: Optional[str] = None

    @field_validator("email")
    @classmethod
    def _normalize_email(cls, v: str) -> str:
        t = (v or "").strip().lower()
        if "@" not in t or "." not in t.split("@")[-1]:
            raise ValueError("email must look like an address")
        return t

    @field_validator("trigger_kind")
    @classmethod
    def _validate_trigger_kind(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        if v not in TRIGGER_KIND_VALUES:
            raise ValueError(f"trigger_kind must be one of {TRIGGER_KIND_VALUES}")
        return v


class AutomationFlowTestStepResult(BaseModel):
    playbook: str
    node_kind: str
    trigger_kind: Optional[str] = None
    step_index: Optional[int] = None
    enabled: bool = False
    status: str
    detail: Optional[str] = None
    brevo_message_id: Optional[str] = None
    job_id: Optional[str] = None


class AutomationFlowTestResponse(BaseModel):
    ok: bool
    flow: str
    to_email: str
    client_id: Optional[str] = None
    client_label: Optional[str] = None
    sent_count: int = 0
    results: List[AutomationFlowTestStepResult] = []
    error: Optional[str] = None
    # Diagnostics (also useful when ok=false)
    dispatcher_healthy: bool = False
    brevo_connected: bool = False
    brevo_note: Optional[str] = None
    enabled_action_count: int = 0
    enabled_wait_count: int = 0
    enabled_playbooks: List[str] = []
    approval_gated_playbooks: List[str] = []
    booking_trigger_ready: bool = True
    booking_trigger_note: Optional[str] = None
    blockers: List[str] = []
    ready_for_live_sends: bool = False


# -- Outreach inbox ------------------------------------------------------------

class OutreachInboxItem(BaseModel):
    id: str
    source: Literal["performance_task", "automation_job"]
    client_id: Optional[uuid.UUID] = None
    client_name: Optional[str] = None
    playbook: Optional[str] = None
    title: str
    summary: Optional[str] = None
    state: Optional[str] = None
    scheduled_at: Optional[datetime] = None
    created_at: datetime
    requires_approval: bool = False
    # Performance-task detail fields (None for automation jobs). These let the
    # OutreachInbox UI render an expandable detail row with full prescription
    # context and surface "Mark complete" / "Generate email" actions without
    # the user having to bounce out to the full Performance tab.
    category: Optional[str] = None
    prescription: Optional[str] = None
    next_step: Optional[str] = None
    recommended_actions: Optional[List[str]] = None
    impact_score: Optional[float] = None
    has_email_draft: Optional[bool] = None


class OutreachInboxResponse(BaseModel):
    items: List[OutreachInboxItem]
    awaiting_approval_count: int = 0
    performance_task_count: int = 0

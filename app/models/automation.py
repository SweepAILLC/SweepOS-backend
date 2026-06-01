"""Automation engine models: rules (per playbook), jobs (durable queue + audit), worker heartbeat.

Triggers (Stripe webhook, Whop sync, Fathom call insight, lifecycle transition) only insert
AutomationEmailJob rows. A standalone worker process (`python -m app.worker`) materializes
drafts and sends via Brevo. UNIQUE(org_id, idempotency_key) prevents duplicate sends.
"""
from __future__ import annotations

import enum
import uuid
from datetime import datetime
from typing import Optional

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID

from app.db.session import Base


# -- Playbook identifiers (stored as text, validated at the application layer) ----------

class Playbook(str, enum.Enum):
    # Fires when a Calendly/Cal.com booking lands AND the client has no recorded sale yet.
    # Lets the operator nurture booked-but-not-yet-sold leads (e.g. discovery call ->
    # send a primer / agenda / pre-call value email before the actual sales call).
    PRE_SALE_POST_BOOKING = "pre_sale_post_booking"
    FIRST_PAYMENT_ONBOARDING = "first_payment_onboarding"
    FIRST_PAYMENT_REFERRAL = "first_payment_referral"
    WIN_COMBINED_ASK = "win_combined_ask"
    OFFBOARDING_RECAP_ASK = "offboarding_recap_ask"


PLAYBOOK_VALUES = tuple(p.value for p in Playbook)


class JobState(str, enum.Enum):
    SCHEDULED = "scheduled"
    AWAITING_APPROVAL = "awaiting_approval"
    READY = "ready"
    SENDING = "sending"
    SENT = "sent"
    SKIPPED = "skipped"
    FAILED = "failed"
    CANCELED = "canceled"


JOB_STATE_VALUES = tuple(s.value for s in JobState)


class ContentMode(str, enum.Enum):
    AI_GENERATED = "ai_generated"
    HTML_TEMPLATE = "html_template"


CONTENT_MODE_VALUES = tuple(m.value for m in ContentMode)


# -- Tables ------------------------------------------------------------------------------

class AutomationRule(Base):
    """Per-org config for one playbook. Auto-seeded as disabled on first list call."""

    __tablename__ = "automation_rules"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    org_id = Column(UUID(as_uuid=True), ForeignKey("organizations.id"), nullable=False, index=True)

    playbook = Column(String(64), nullable=False)
    enabled = Column(Boolean, nullable=False, default=False)

    delay_seconds = Column(Integer, nullable=False, default=0)
    content_mode = Column(String(32), nullable=False, default=ContentMode.AI_GENERATED.value)

    subject_template = Column(Text, nullable=True)
    # When content_mode == html_template: reference into ai_profile.writing_samples
    # Shape: {"kind": "writing_samples_by_title", "title": "..."}
    #     or {"kind": "writing_samples_by_kind", "sample_kind": "referral_campaign"}
    html_template_ref = Column(JSONB, nullable=True)

    # When content_mode == ai_generated: operator instructions merged into the LLM system prompt.
    ai_content_system_prompt = Column(Text, nullable=True)

    # {"lifecycle_in": ["active", "offboarding"], "min_lifetime_revenue_cents": 0}
    audience_filter = Column(JSONB, nullable=True)

    # Per-rule trigger options that don't fit the generic audience filter. Today only
    # the pre_sale_post_booking playbook reads this, where it carries:
    #   {"provider": "calendly" | "calcom" | "any",
    #    "event_type_ids": ["<id-or-uri>", ...],
    #    "match_all_events": false}
    # Stored as JSONB so we can grow the trigger taxonomy without another migration.
    trigger_config = Column(JSONB, nullable=True)

    # Combined-ask playbooks; ordered subset of ["referral", "upsell", "testimonial"].
    opportunity_priority = Column(JSONB, nullable=True)
    combine_top_n = Column(Integer, nullable=False, default=1)

    require_approval = Column(Boolean, nullable=False, default=False)
    approval_ttl_hours = Column(Integer, nullable=True)

    last_modified_by = Column(UUID(as_uuid=True), nullable=True)

    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("org_id", "playbook", name="uq_automation_rules_org_playbook"),
    )


class AutomationEmailJob(Base):
    """Durable queue + audit log for outbound automation emails."""

    __tablename__ = "automation_email_jobs"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    org_id = Column(UUID(as_uuid=True), ForeignKey("organizations.id"), nullable=False, index=True)
    rule_id = Column(
        UUID(as_uuid=True),
        ForeignKey("automation_rules.id", ondelete="SET NULL"),
        nullable=True,
    )
    client_id = Column(
        UUID(as_uuid=True),
        ForeignKey("clients.id", ondelete="CASCADE"),
        nullable=False,
    )

    playbook = Column(String(64), nullable=False)
    trigger_event = Column(Text, nullable=True)
    idempotency_key = Column(Text, nullable=False)

    scheduled_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    state = Column(String(32), nullable=False, default=JobState.SCHEDULED.value)

    payload_json = Column(JSONB, nullable=True)

    attempts = Column(Integer, nullable=False, default=0)
    last_attempt_at = Column(DateTime, nullable=True)
    dispatched_at = Column(DateTime, nullable=True)
    brevo_message_id = Column(Text, nullable=True)
    error_text = Column(Text, nullable=True)

    created_at = Column(DateTime, nullable=False, default=datetime.utcnow, index=True)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("org_id", "idempotency_key", name="uq_automation_email_jobs_org_idemp"),
        Index("ix_automation_email_jobs_state_scheduled", "org_id", "state", "scheduled_at"),
        Index("ix_automation_email_jobs_client_created", "client_id", "created_at"),
    )


class AutomationWorkerHeartbeat(Base):
    """Singleton heartbeat row used as a fallback when REDIS_URL is not configured."""

    __tablename__ = "automation_worker_heartbeat"

    id = Column(Integer, primary_key=True, default=1)
    last_tick_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    worker_pid = Column(Integer, nullable=True)
    worker_host = Column(String(255), nullable=True)
    queue_depth = Column(Integer, nullable=False, default=0)
    in_flight = Column(Integer, nullable=False, default=0)
    awaiting_approval = Column(Integer, nullable=False, default=0)

"""Durable inbox for inbound payment/calendar webhooks.

Persist first, process second. Worker retries failed rows so a crash or
provider timeout never silently drops a booking or payment.
"""
from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import Column, DateTime, Index, Integer, String, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB, UUID

from app.db.session import Base


class InboundWebhookEvent(Base):
    __tablename__ = "inbound_webhook_events"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    org_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    provider = Column(String(32), nullable=False)  # stripe | whop | calcom | calendly
    event_id = Column(String(255), nullable=False)
    event_type = Column(String(128), nullable=True)
    payload = Column(JSONB, nullable=False)
    status = Column(String(16), nullable=False, default="pending", index=True)
    attempts = Column(Integer, nullable=False, default=0)
    next_attempt_at = Column(DateTime, nullable=True, index=True)
    error_text = Column(Text, nullable=True)
    received_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    processed_at = Column(DateTime, nullable=True)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("org_id", "provider", "event_id", name="uq_inbound_webhook_org_provider_event"),
        Index(
            "ix_inbound_webhook_events_due",
            "status",
            "next_attempt_at",
        ),
    )

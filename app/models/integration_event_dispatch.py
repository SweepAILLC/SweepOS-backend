"""Idempotent claim rows for Discord / automation side effects.

Webhook + pull-sync can both observe the same booking/payment. One row per
(org, source, source_id, action) so each side effect fires at most once.
"""
from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import Column, DateTime, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID

from app.db.session import Base


class IntegrationEventDispatch(Base):
    __tablename__ = "integration_event_dispatches"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    org_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    source = Column(String(32), nullable=False)
    source_id = Column(String(255), nullable=False)
    action = Column(String(64), nullable=False)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint(
            "org_id",
            "source",
            "source_id",
            "action",
            name="uq_integration_event_dispatch",
        ),
    )

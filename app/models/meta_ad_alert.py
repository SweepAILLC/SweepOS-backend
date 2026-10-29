"""Open diagnostic on a synced Meta entity. Upserted by ads_signal_engine.

One non-resolved row per (org, entity, alert_type). Resolved rows stay for
history and free the partial unique index so the same condition can reopen.
"""
from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import Column, DateTime, ForeignKey, String, Text
from sqlalchemy.dialects.postgresql import JSONB, UUID

from app.db.session import Base


class MetaAdAlert(Base):
    __tablename__ = "meta_ad_alerts"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    org_id = Column(
        UUID(as_uuid=True),
        ForeignKey("organizations.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    level = Column(String(16), nullable=False)  # campaign | adset | ad
    entity_id = Column(String(64), nullable=False)  # Meta id at that level
    alert_type = Column(String(32), nullable=False)
    severity = Column(String(16), nullable=False)  # info | warn | critical
    metrics_snapshot = Column(JSONB, nullable=True)
    recommended_action = Column(Text, nullable=False)
    status = Column(String(16), nullable=False, default="open")  # open | acknowledged | resolved

    first_detected_at = Column(DateTime(timezone=True), nullable=False, default=datetime.utcnow)
    last_evaluated_at = Column(DateTime(timezone=True), nullable=False, default=datetime.utcnow)
    notified_at = Column(DateTime(timezone=True), nullable=True)

    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

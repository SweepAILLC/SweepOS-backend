"""Org-scoped outcome snapshots for self-learning (similar past cases retrieval)."""
from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import Column, String, DateTime, ForeignKey, Float
from sqlalchemy.dialects.postgresql import UUID, JSON

from app.db.session import Base


class HealthOutcomeSnapshot(Base):
    __tablename__ = "health_outcome_snapshots"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    org_id = Column(UUID(as_uuid=True), ForeignKey("organizations.id"), nullable=False, index=True)
    client_id = Column(UUID(as_uuid=True), ForeignKey("clients.id", ondelete="CASCADE"), nullable=False, index=True)

    score = Column(Float, nullable=False)
    grade = Column(String(4), nullable=False)
    lifecycle_phase = Column(String(32), nullable=False)

    # Bucketed features for SQL similarity (no vectors required)
    feature_bucket = Column(JSON, nullable=True)

    recorded_at = Column(DateTime(timezone=True), default=datetime.utcnow, nullable=False)

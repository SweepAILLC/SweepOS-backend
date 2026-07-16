"""Persisted per-org LLM API usage for owner cost visibility."""
from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import Column, DateTime, Float, ForeignKey, Index, Integer, String
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship

from app.db.session import Base


class LlmUsageEvent(Base):
    __tablename__ = "llm_usage_events"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    org_id = Column(
        UUID(as_uuid=True),
        ForeignKey("organizations.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    provider = Column(String(32), nullable=False)  # openai | gemini
    model = Column(String(128), nullable=True)
    feature = Column(String(64), nullable=False, default="unknown")
    prompt_tokens = Column(Integer, nullable=False, default=0)
    completion_tokens = Column(Integer, nullable=False, default=0)
    total_tokens = Column(Integer, nullable=False, default=0)
    estimated_cost_usd = Column(Float, nullable=True)
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow, nullable=False)

    organization = relationship("Organization", backref="llm_usage_events")

    __table_args__ = (
        Index("ix_llm_usage_events_org_created", "org_id", "created_at"),
        Index("ix_llm_usage_events_org_feature", "org_id", "feature"),
    )

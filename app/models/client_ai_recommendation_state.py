"""Per-client AI recommendation checklist (action items with manual completion). Modular JSON until full call-insights pipeline exists."""
from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import Column, String, DateTime, Text, ForeignKey
from sqlalchemy.dialects.postgresql import UUID, JSON

from app.db.session import Base


class ClientAIRecommendationState(Base):
    """
    One row per client. `actions` is a JSON array of:
    { id, title, detail?, category?, priority?, completed, completed_at? }
    """

    __tablename__ = "client_ai_recommendation_states"

    client_id = Column(UUID(as_uuid=True), ForeignKey("clients.id", ondelete="CASCADE"), primary_key=True)
    org_id = Column(UUID(as_uuid=True), ForeignKey("organizations.id"), nullable=False, index=True)

    headline = Column(Text, nullable=True)
    actions = Column(JSON, nullable=False)

    updated_at = Column(DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow, nullable=False)

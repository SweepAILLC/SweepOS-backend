"""Persisted client health score (logic and/or AI) with input hash for invalidation."""
from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import Column, String, DateTime, Text, ForeignKey, Float
from sqlalchemy.dialects.postgresql import UUID, JSON

from app.db.session import Base


class ClientHealthScoreCache(Base):
    __tablename__ = "client_health_score_cache"

    client_id = Column(UUID(as_uuid=True), ForeignKey("clients.id", ondelete="CASCADE"), primary_key=True)
    org_id = Column(UUID(as_uuid=True), ForeignKey("organizations.id"), nullable=False, index=True)

    score = Column(Float, nullable=False)
    grade = Column(String(4), nullable=False)
    source = Column(String(16), nullable=False, default="logic")  # logic | ai

    explanation = Column(Text, nullable=True)
    factors_json = Column(JSON, nullable=True)

    input_hash = Column(String(128), nullable=False)
    computed_at = Column(DateTime(timezone=True), nullable=False, default=datetime.utcnow)

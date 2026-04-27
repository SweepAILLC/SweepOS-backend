"""Fathom sales call records with required sentiment after LLM analysis."""
from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import Column, String, DateTime, Text, ForeignKey, Float, BigInteger, Index
from sqlalchemy.dialects.postgresql import UUID, JSON
from sqlalchemy.orm import relationship

from app.db.session import Base


class FathomCallRecord(Base):
    __tablename__ = "fathom_call_records"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    org_id = Column(UUID(as_uuid=True), ForeignKey("organizations.id"), nullable=False, index=True)
    client_id = Column(UUID(as_uuid=True), ForeignKey("clients.id"), nullable=True, index=True)

    fathom_recording_id = Column(BigInteger, nullable=False)
    summary_text = Column(Text, nullable=True)
    transcript_snippet = Column(Text, nullable=True)

    sentiment_status = Column(String(32), nullable=False, default="pending")  # pending, complete, failed
    sentiment_score = Column(Float, nullable=True)
    sentiment_label = Column(String(32), nullable=True)  # positive, neutral, negative
    sentiment_snippet = Column(String(512), nullable=True)

    meeting_at = Column(DateTime(timezone=True), nullable=True)

    recording_url = Column(Text, nullable=True)
    # Optional extra URLs from the meeting payload (webhook/list API) for embedding.
    share_url = Column(Text, nullable=True)
    video_url = Column(Text, nullable=True)
    attendees_json = Column(JSON, nullable=True)
    related_client_ids = Column(JSON, nullable=True)

    created_at = Column(DateTime(timezone=True), default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    organization = relationship("Organization", backref="fathom_call_records")
    client = relationship("Client", backref="fathom_call_records")

    __table_args__ = (
        Index("ix_fathom_call_org_recording", "org_id", "fathom_recording_id", unique=True),
    )

"""Persisted transcript reviews for conversion coaching (Content Studio)."""
from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import Column, String, Text, DateTime, ForeignKey, Index
from sqlalchemy.dialects.postgresql import UUID, JSON
from sqlalchemy.orm import relationship

from app.db.session import Base


class ContentStudioTranscriptAnalysis(Base):
    __tablename__ = "content_studio_transcript_analyses"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    org_id = Column(UUID(as_uuid=True), ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False, index=True)
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id", ondelete="SET NULL"), nullable=True, index=True)
    purpose = Column(String(16), nullable=False)  # TOF | MOF | BOF | mixed
    mixed_note = Column(Text, nullable=True)
    transcript_text = Column(Text, nullable=False)
    analysis_json = Column(JSON, nullable=False, default=dict)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    organization = relationship("Organization", backref="content_studio_transcript_analyses")
    user = relationship("User", foreign_keys=[user_id])

    __table_args__ = (Index("ix_cs_transcript_org_created", "org_id", "created_at"),)

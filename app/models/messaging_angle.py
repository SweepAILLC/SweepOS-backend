"""Messaging Matrix: org-scoped messaging angles proven (or disproven) by published content.

See docs/features/MESSAGING_MATRIX.md. Scores are computed in code from post/ad metrics
(app/services/messaging_matrix_scoring.py); the LLM only names angles and writes reasoning.
"""
from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import Column, DateTime, Float, ForeignKey, Index, Integer, String, Text, UniqueConstraint, func
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import relationship

from app.db.session import Base

AWARENESS_STAGES = ("unaware", "problem_aware", "solution_aware", "product_aware", "most_aware")
SOURCE_INSTAGRAM = "instagram"
SOURCE_META_AD = "meta_ad"


class MessagingAngle(Base):
    __tablename__ = "messaging_angles"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    org_id = Column(UUID(as_uuid=True), ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False)
    name = Column(String(120), nullable=False)
    description = Column(Text, nullable=True)
    awareness_stage = Column(String(24), nullable=True)
    persona = Column(String(120), nullable=True)
    promise = Column(Text, nullable=True)
    hook_mechanic = Column(String(32), nullable=True)
    organic_score = Column(Float, nullable=True)
    paid_score = Column(Float, nullable=True)
    overall_score = Column(Float, nullable=True)
    organic_samples = Column(Integer, nullable=False, default=0)
    paid_samples = Column(Integer, nullable=False, default=0)
    reasoning = Column(Text, nullable=True)
    # overall_score when the reasoning was last written; reasoning is only rewritten
    # when the score has moved far enough from this to change the story.
    reasoning_score = Column(Float, nullable=True)
    reasoning_prompt_version = Column(String(16), nullable=True)
    # Latest Recreate output: {"hooks", "outline", "evidence", "approved": False, ...}.
    # Drafts only; nothing is posted or launched from it.
    last_draft = Column(JSONB, nullable=True)
    status = Column(String(16), nullable=False, default="active")  # active | archived
    scored_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    sources = relationship(
        "MessagingAngleSource",
        back_populates="angle",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )

    __table_args__ = (
        Index("ix_messaging_angles_org_score", "org_id", "overall_score"),
        Index("uq_messaging_angles_org_name", "org_id", func.lower(name), unique=True),
    )


class MessagingAngleSource(Base):
    """One tagged content item (an Instagram post or a Meta ad) and the angle it uses."""

    __tablename__ = "messaging_angle_sources"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    org_id = Column(UUID(as_uuid=True), ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False)
    # NULL = the tag was rejected (confidence < 0.6 or quote not in transcript). The row
    # still caches transcript_hash so the same content is not re-sent to the LLM.
    angle_id = Column(UUID(as_uuid=True), ForeignKey("messaging_angles.id", ondelete="CASCADE"), nullable=True)
    source_type = Column(String(16), nullable=False)  # instagram | meta_ad
    source_id = Column(UUID(as_uuid=True), nullable=False)  # instagram_media.id | meta_ads.id
    quote = Column(Text, nullable=True)
    tag_confidence = Column(Float, nullable=True)
    item_score = Column(Float, nullable=True)
    metrics_snapshot = Column(JSONB, nullable=True)
    transcript_hash = Column(String(64), nullable=True)
    prompt_version = Column(String(16), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    angle = relationship("MessagingAngle", back_populates="sources")

    __table_args__ = (
        UniqueConstraint("org_id", "source_type", "source_id", name="uq_messaging_angle_source_item"),
        Index("ix_messaging_angle_sources_org_angle", "org_id", "angle_id"),
    )

"""Jev vs LLM answers side by side while a feature runs in shadow mode (Jev PRD, issue 3).

Answers and probabilities only — never the state (transcripts, client text) sent to either
model. Rows exist to measure agreement before a feature's JEV_*_MODE flips to "on", and are
dropped once it does.
"""
from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import Column, DateTime, ForeignKey, Index, String
from sqlalchemy.dialects.postgresql import JSON, UUID

from app.db.session import Base


class JevShadowResult(Base):
    __tablename__ = "jev_shadow_results"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    org_id = Column(
        UUID(as_uuid=True),
        ForeignKey("organizations.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    feature = Column(String(64), nullable=False)  # fathom_sentiment | call_insight | call_library | picker
    subject_type = Column(String(32), nullable=False)  # e.g. fathom_call_record, client_call_insight
    subject_id = Column(String(64), nullable=False)  # uuid or bigint id of the subject, as text
    question_set_version = Column(String(16), nullable=False)
    jev_json = Column(JSON, nullable=True)  # JevResult.to_json(), or null when Jev failed
    llm_json = Column(JSON, nullable=True)  # the LLM path's answers for the same keys
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow, nullable=False)

    __table_args__ = (Index("ix_jev_shadow_results_org_feature_created", "org_id", "feature", "created_at"),)

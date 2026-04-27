"""Per-call AI analysis report for the Call Library tab."""
from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import Column, String, DateTime, Text, ForeignKey, Index, Float
from sqlalchemy.dialects.postgresql import UUID, JSON
from sqlalchemy.orm import relationship

from app.db.session import Base


class CallLibraryReport(Base):
    __tablename__ = "call_library_reports"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    org_id = Column(UUID(as_uuid=True), ForeignKey("organizations.id"), nullable=False, index=True)
    fathom_call_record_id = Column(
        UUID(as_uuid=True),
        ForeignKey("fathom_call_records.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,
    )

    status = Column(String(32), nullable=False, default="pending")  # pending | complete | failed
    report_json = Column(JSON, nullable=True)  # 5-section structured AI analysis
    failure_reason = Column(Text, nullable=True)
    call_title = Column(Text, nullable=True)   # derived from meeting title / invitee names
    call_title_override = Column(Text, nullable=True)  # user-defined display name (wins in UI)

    call_score = Column(Float, nullable=True)  # 0-100 sales effectiveness (LLM)
    recording_url = Column(Text, nullable=True)  # snapshot; no video bytes stored
    share_url = Column(Text, nullable=True)  # share link URL (when present)
    video_url = Column(Text, nullable=True)  # direct/streaming video URL (when present)
    attendees_json = Column(JSON, nullable=True)  # [{email, name, ...}]

    computed_at = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    organization = relationship("Organization", backref="call_library_reports")
    fathom_call_record = relationship(
        "FathomCallRecord",
        backref="call_library_report",
        uselist=False,
    )

    __table_args__ = (
        Index("ix_call_library_reports_org_created", "org_id", "created_at"),
    )

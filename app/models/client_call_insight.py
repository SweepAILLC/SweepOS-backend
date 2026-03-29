"""Per–Fathom-call LLM insights (ROI, clips, tags)."""
from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import Column, String, DateTime, Text, ForeignKey, Index, Numeric
from sqlalchemy.dialects.postgresql import UUID, JSON
from sqlalchemy.orm import relationship, backref

from app.db.session import Base


class ClientCallInsight(Base):
    __tablename__ = "client_call_insights"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    org_id = Column(UUID(as_uuid=True), ForeignKey("organizations.id"), nullable=False, index=True)
    client_id = Column(UUID(as_uuid=True), ForeignKey("clients.id", ondelete="CASCADE"), nullable=False, index=True)
    fathom_call_record_id = Column(
        UUID(as_uuid=True),
        ForeignKey("fathom_call_records.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,
    )
    check_in_id = Column(
        UUID(as_uuid=True),
        ForeignKey("client_check_ins.id", ondelete="SET NULL"),
        nullable=True,
    )

    insight_json = Column(JSON, nullable=True)
    status = Column(String(32), nullable=False, default="complete")  # complete | failed | skipped
    failure_reason = Column(Text, nullable=True)
    computed_at = Column(DateTime(timezone=True), default=datetime.utcnow, nullable=False)
    model = Column(String(128), nullable=True)
    input_hash = Column(String(64), nullable=True)
    lifecycle_at_compute = Column(String(64), nullable=True)

    organization = relationship("Organization", backref="client_call_insights")
    client = relationship("Client", backref="call_insights")
    fathom_call_record = relationship(
        "FathomCallRecord",
        backref=backref("call_insight", uselist=False),
    )
    check_in = relationship("ClientCheckIn", backref="call_insights")

    __table_args__ = (Index("ix_client_call_insights_client_computed", "client_id", "computed_at"),)


class ClientInsightSummary(Base):
    """Denormalized rollup for cheap board tags + drawer headline."""

    __tablename__ = "client_insight_summaries"

    client_id = Column(UUID(as_uuid=True), ForeignKey("clients.id", ondelete="CASCADE"), primary_key=True)
    org_id = Column(UUID(as_uuid=True), ForeignKey("organizations.id"), nullable=False, index=True)
    headline = Column(Text, nullable=True)
    tags = Column(JSON, nullable=False, default=list)
    last_call_at = Column(DateTime(timezone=True), nullable=True)
    last_insight_at = Column(DateTime(timezone=True), nullable=True)
    last_lifecycle_state = Column(String(32), nullable=True)
    last_health_grade = Column(String(8), nullable=True)
    last_health_score = Column(Numeric(6, 2), nullable=True)

    client = relationship("Client", backref="insight_summary", uselist=False)
    organization = relationship("Organization", backref="client_insight_summaries")

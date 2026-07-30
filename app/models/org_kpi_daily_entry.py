"""Org-scoped daily KPI tracker rows (manual entry + computed rates on read)."""
from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import (
    Boolean,
    Column,
    Date,
    DateTime,
    ForeignKey,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import UUID

from app.db.session import Base


class OrgKpiDailyEntry(Base):
    __tablename__ = "org_kpi_daily_entries"
    __table_args__ = (
        UniqueConstraint("org_id", "entry_date", name="uq_org_kpi_daily_entries_org_date"),
    )

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    org_id = Column(
        UUID(as_uuid=True),
        ForeignKey("organizations.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    entry_date = Column(Date, nullable=False, index=True)

    total_followers = Column(Integer, nullable=True)
    new_followers = Column(Integer, nullable=True)
    content_posted = Column(Boolean, nullable=True)
    # UI label: "Content Attracting ICP"
    best_content_type = Column(String(512), nullable=True)
    inboxes_checked = Column(Integer, nullable=True)
    outreach_sent = Column(Integer, nullable=True)
    respondents = Column(Integer, nullable=True)
    inbound_icp_leads = Column(Integer, nullable=True)
    followups_sent = Column(Integer, nullable=True)
    new_conversations = Column(Integer, nullable=True)
    conversations_nurtured = Column(Integer, nullable=True)
    calls_pitched = Column(Integer, nullable=True)
    inbound_bookings = Column(Integer, nullable=True)
    outbound_bookings = Column(Integer, nullable=True)
    calls_booked = Column(Integer, nullable=True)
    calls_taken = Column(Integer, nullable=True)
    offers_made = Column(Integer, nullable=True)
    no_shows = Column(Integer, nullable=True)
    closes = Column(Integer, nullable=True)
    cash_collected = Column(Numeric(12, 2), nullable=True)
    revenue = Column(Numeric(12, 2), nullable=True)
    setter_context = Column(Text, nullable=True)

    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

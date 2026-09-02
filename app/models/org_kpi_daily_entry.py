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
    Index,
    Integer,
    Numeric,
    String,
    Text,
    text,
)
from sqlalchemy.dialects.postgresql import UUID

from app.db.session import Base


class OrgKpiDailyEntry(Base):
    """
    One row per (org_id, entry_date) when rep_user_id is NULL — the org-wide
    aggregate, unchanged from the original single-row-per-day design. One row
    per (org_id, entry_date, rep_user_id) when set — a single rep's entry for
    that day. Both shapes coexist; org-level reads must SUM/merge across rows
    for a date instead of assuming exactly one (see kpi_compute.py).
    """

    __tablename__ = "org_kpi_daily_entries"
    __table_args__ = (
        # At most one aggregate row per org/date (rep_user_id NULL).
        Index(
            "uq_org_kpi_daily_entries_org_date_agg",
            "org_id",
            "entry_date",
            unique=True,
            postgresql_where=text("rep_user_id IS NULL"),
        ),
        # At most one row per org/date/rep (rep_user_id set). A plain
        # UniqueConstraint on (org_id, entry_date, rep_user_id) would NOT
        # enforce the aggregate case above, since SQL treats NULL <> NULL —
        # hence two partial indexes instead of one constraint.
        Index(
            "uq_org_kpi_daily_entries_org_date_rep",
            "org_id",
            "entry_date",
            "rep_user_id",
            unique=True,
            postgresql_where=text("rep_user_id IS NOT NULL"),
        ),
    )

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    org_id = Column(
        UUID(as_uuid=True),
        ForeignKey("organizations.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    entry_date = Column(Date, nullable=False, index=True)
    rep_user_id = Column(
        UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="SET NULL"),
        nullable=True,
    )

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
    # Calls whose *booking action* (ClientCheckIn.created_at) happened on this
    # entry_date — distinct from calls_booked, which counts calls whose meeting
    # start_time falls on this date. A call booked today for next week counts
    # here today, and toward calls_booked on the meeting's actual date.
    calls_booked_activity = Column(Integer, nullable=True)
    calls_taken = Column(Integer, nullable=True)
    offers_made = Column(Integer, nullable=True)
    no_shows = Column(Integer, nullable=True)
    closes = Column(Integer, nullable=True)
    cash_collected = Column(Numeric(12, 2), nullable=True)
    revenue = Column(Numeric(12, 2), nullable=True)
    setter_context = Column(Text, nullable=True)

    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

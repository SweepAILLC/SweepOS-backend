"""Append-only per-rep sales activity log — feeds the by-rep KPI performance dashboard."""
from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import Boolean, Column, Date, DateTime, ForeignKey, Integer, String
from sqlalchemy.dialects.postgresql import UUID

from app.db.session import Base


class SalesActivityEvent(Base):
    """
    One immutable row per attributable sales event (currently: one per
    close-survey submission). Unlike org_kpi_daily_entries (upsert-in-place
    aggregates) or Client.meta.post_sales (overwritten per client), this is
    a log — never updated after insert — so per-rep totals over time are a
    straight SUM/COUNT GROUP BY rep_user_id, and history survives even if a
    client is later worked by a different rep.
    """

    __tablename__ = "sales_activity_events"

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
        index=True,
    )
    rep_role = Column(String(16), nullable=False)  # "setter" | "closer"
    client_id = Column(
        UUID(as_uuid=True),
        ForeignKey("clients.id", ondelete="SET NULL"),
        nullable=True,
    )
    cash_collected_cents = Column(Integer, nullable=True)
    is_closed = Column(Boolean, nullable=False, default=False)
    source = Column(String(32), nullable=False, default="close_survey")

    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

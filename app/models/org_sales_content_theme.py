"""Org-level recurring sales/objection themes derived from call insights (cross-client validation)."""
from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import Column, String, DateTime, Integer, ForeignKey, UniqueConstraint, Index
from sqlalchemy.dialects.postgresql import UUID, JSON
from sqlalchemy.orm import relationship

from app.db.session import Base


class OrgSalesContentTheme(Base):
    """
    Aggregated theme bucket per org. A theme becomes "validated" for content/email use when
    distinct_client_count and occurrence_count meet configured thresholds within lookback.
    """

    __tablename__ = "org_sales_content_themes"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    org_id = Column(UUID(as_uuid=True), ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False, index=True)
    theme_key = Column(String(32), nullable=False)
    # "check_in" (ClientCallInsight, post-sale) or "sales_call" (CallLibraryReport, prospect-
    # facing) — same theme_key text can occur in both pools, so they're kept as separate rows
    # rather than merged (see uq_org_sales_content_theme_org_key_source below).
    source = Column(String(16), nullable=False, default="check_in", server_default="check_in")
    # "objection" | "question" | "other" — the clip kind this theme was bucketed from. Lets
    # "common objection" and "common point of confusion" be queried separately even though
    # both flow through the same aggregation pipeline (see issue 6 in
    # docs/prd/content-generation-signal-upgrades/ISSUES.md). source="sales_call" rows are
    # always "objection" (objection_handling_audit has no other clip kind).
    kind = Column(String(16), nullable=True)
    label = Column(String(220), nullable=True)
    occurrence_count = Column(Integer, nullable=False, default=0)
    distinct_client_count = Column(Integer, nullable=False, default=0)
    # source="sales_call" only: how many occurrences came from a CallLibraryReport with
    # deal_closed=true — the "this objection, handled this way, actually closed" signal.
    closed_won_count = Column(Integer, nullable=False, default=0, server_default="0")
    contributing_client_ids = Column(JSON, nullable=False, default=list)
    sample_quotes = Column(JSON, nullable=False, default=list)
    first_seen_at = Column(DateTime(timezone=True), nullable=False)
    last_seen_at = Column(DateTime(timezone=True), nullable=False)

    organization = relationship("Organization", backref="sales_content_themes")

    __table_args__ = (
        UniqueConstraint(
            "org_id", "theme_key", "source", name="uq_org_sales_content_theme_org_key_source"
        ),
        Index("ix_org_sales_content_themes_org_last_seen", "org_id", "last_seen_at"),
    )

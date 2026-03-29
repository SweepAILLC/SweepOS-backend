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
    label = Column(String(220), nullable=True)
    occurrence_count = Column(Integer, nullable=False, default=0)
    distinct_client_count = Column(Integer, nullable=False, default=0)
    contributing_client_ids = Column(JSON, nullable=False, default=list)
    sample_quotes = Column(JSON, nullable=False, default=list)
    first_seen_at = Column(DateTime(timezone=True), nullable=False)
    last_seen_at = Column(DateTime(timezone=True), nullable=False)

    organization = relationship("Organization", backref="sales_content_themes")

    __table_args__ = (
        UniqueConstraint("org_id", "theme_key", name="uq_org_sales_content_theme_org_key"),
        Index("ix_org_sales_content_themes_org_last_seen", "org_id", "last_seen_at"),
    )

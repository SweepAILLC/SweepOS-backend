from datetime import datetime
import uuid

from sqlalchemy import Column, Date, DateTime, ForeignKey, Numeric, String
from sqlalchemy.dialects.postgresql import UUID

from app.db.session import Base


class FunnelScorecardOverride(Base):
    """
    A hand-entered value for one count row of the Funnels scorecard grid, for one
    week, in one view (funnel + channel). It replaces the computed weekly count;
    derived rows (rates, costs, ROAS) are recomputed from it.

    `funnel_id` NULL = the "All funnels" view. `channel` is "all", "paid" or
    "organic". One row per (org, funnel, channel, week, metric), enforced by a
    unique index over COALESCE(funnel_id) in migration 101.
    """

    __tablename__ = "funnel_scorecard_overrides"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    org_id = Column(UUID(as_uuid=True), ForeignKey("organizations.id"), nullable=False, index=True)
    funnel_id = Column(UUID(as_uuid=True), ForeignKey("funnels.id", ondelete="CASCADE"), nullable=True)
    channel = Column(String(16), nullable=False, default="all")
    week_start = Column(Date, nullable=False)  # always a Monday
    metric_key = Column(String(32), nullable=False)
    value = Column(Numeric(14, 2), nullable=False)
    updated_by_user_id = Column(UUID(as_uuid=True), ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

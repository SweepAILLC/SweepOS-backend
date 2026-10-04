from datetime import datetime
import uuid

from sqlalchemy import Column, Date, DateTime, ForeignKey, Integer
from sqlalchemy.dialects.postgresql import UUID

from app.db.session import Base


class FunnelAdSpend(Base):
    """
    Manually-entered weekly ad spend, per funnel (PRD phase 8). Feeds the paid-only
    money row on the Funnels dashboard: CPL, CAC, ROAS, profit.

    `week_start` is always a Monday. `funnel_id` NULL = spend that can't be split
    across funnels; it counts only toward the "All funnels" view. One row per
    (org, funnel, week) — enforced by two partial unique indexes in migration 095
    because Postgres treats NULL funnel_ids as distinct.
    """

    __tablename__ = "funnel_ad_spend"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    org_id = Column(UUID(as_uuid=True), ForeignKey("organizations.id"), nullable=False, index=True)
    funnel_id = Column(UUID(as_uuid=True), ForeignKey("funnels.id", ondelete="CASCADE"), nullable=True, index=True)
    week_start = Column(Date, nullable=False)
    amount_cents = Column(Integer, nullable=False, default=0)
    # Weekly creative-velocity counts (sheet: "New Ads Deployed" / "New Angles Deployed").
    ads_deployed = Column(Integer, nullable=True)
    angles_deployed = Column(Integer, nullable=True)
    entered_by_user_id = Column(UUID(as_uuid=True), ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

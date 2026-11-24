from datetime import datetime
import uuid

from sqlalchemy import Boolean, Column, DateTime, Float, ForeignKey, Integer, String, Text
from sqlalchemy.dialects.postgresql import UUID

from app.db.session import Base


class OrgBusinessEconomics(Base):
    """
    One row per org: the numbers the Scale Planner needs that no integration knows.

    Fixed costs and the variable-cost share feed break-even ROAS. Cash reserves,
    tax set-aside and owner living costs feed the scale-readiness verdict; they are
    personal finances, so they are Fernet-encrypted (ENCRYPTION_KEY) at rest and only
    org owners/admins (or Sweep system owners viewing the org) can read them.
    """

    __tablename__ = "org_business_economics"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    org_id = Column(
        UUID(as_uuid=True),
        ForeignKey("organizations.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,
        index=True,
    )
    # Software, team salaries, coaching, rent: costs that don't move with ad spend.
    fixed_monthly_costs_cents = Column(Integer, nullable=True)
    # Share of every revenue dollar that goes out again: commissions + processor + delivery (0-100).
    variable_cost_pct = Column(Float, nullable=True)
    # Count owner living costs as a fixed cost when computing break-even.
    include_living_in_breakeven = Column(Boolean, nullable=False, default=False, server_default="false")
    # Encrypted dollar amounts (stored as encrypted decimal strings).
    cash_reserves_enc = Column(Text, nullable=True)
    tax_reserve_enc = Column(Text, nullable=True)
    living_expenses_enc = Column(Text, nullable=True)
    # conservative | balanced | aggressive
    risk_tolerance = Column(String(16), nullable=False, default="balanced", server_default="balanced")
    # Kill rule and benchmark: what a booked call should cost.
    target_cost_per_call_cents = Column(Integer, nullable=True)
    # How fast ROAS falls as spend grows: ROAS(S) = ROAS0 * (S/S0)^-decay. 0.5 default.
    roas_decay = Column(Float, nullable=True)
    # Recurring renewals per month not captured as new cash (cash-only ROAS misses it).
    monthly_recurring_cash_cents = Column(Integer, nullable=True)
    updated_by_user_id = Column(UUID(as_uuid=True), ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

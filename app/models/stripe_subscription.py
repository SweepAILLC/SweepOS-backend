from sqlalchemy import Column, String, DateTime, Numeric, ForeignKey, JSON
from sqlalchemy.dialects.postgresql import UUID
import uuid
from datetime import datetime
from app.db.session import Base


class StripeSubscription(Base):
    __tablename__ = "stripe_subscriptions"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    org_id = Column(UUID(as_uuid=True), ForeignKey("organizations.id"), nullable=False, index=True)
    stripe_subscription_id = Column(String, nullable=False, index=True)  # Not unique across orgs
    client_id = Column(UUID(as_uuid=True), ForeignKey("clients.id"), nullable=True, index=True)
    status = Column(String, nullable=False, index=True)  # active, past_due, canceled, unpaid, incomplete
    current_period_start = Column(DateTime, nullable=True)
    current_period_end = Column(DateTime, nullable=True, index=True)
    plan_id = Column(String, nullable=True)
    mrr = Column(Numeric(10, 2), default=0, nullable=False)  # Monthly Recurring Revenue in dollars
    raw = Column(JSON, nullable=True)  # Store raw Stripe subscription data
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False, index=True)


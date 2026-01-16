from sqlalchemy import Column, String, DateTime, JSON, Boolean, ForeignKey
from sqlalchemy.dialects.postgresql import UUID
import uuid
from datetime import datetime
from app.db.session import Base


class StripeEvent(Base):
    __tablename__ = "stripe_events"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    org_id = Column(UUID(as_uuid=True), ForeignKey("organizations.id"), nullable=False, index=True)
    stripe_event_id = Column(String, nullable=False, index=True)  # Not unique across orgs
    type = Column(String, nullable=False, index=True)  # invoice.payment_succeeded, charge.succeeded, etc.
    payload = Column(JSON, nullable=False)  # Full event payload from Stripe
    processed = Column(Boolean, default=False, nullable=False, index=True)
    received_at = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)
    processed_at = Column(DateTime, nullable=True)


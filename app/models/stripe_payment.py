from sqlalchemy import Column, String, DateTime, Integer, ForeignKey, JSON, Text
from sqlalchemy.dialects.postgresql import UUID
import uuid
from datetime import datetime
from app.db.session import Base


class StripePayment(Base):
    __tablename__ = "stripe_payments"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    org_id = Column(UUID(as_uuid=True), ForeignKey("organizations.id"), nullable=False, index=True)
    stripe_id = Column(String, nullable=False, index=True)  # charge id or payment_intent id
    client_id = Column(UUID(as_uuid=True), ForeignKey("clients.id"), nullable=True, index=True)
    amount_cents = Column(Integer, nullable=False)  # Store in cents to avoid floating point issues
    currency = Column(String(3), default="usd", nullable=False)
    status = Column(String, nullable=False, index=True)  # succeeded, failed, refunded, pending
    type = Column(String, nullable=True)  # charge, payment_intent, invoice
    subscription_id = Column(String, nullable=True, index=True)  # stripe subscription id if available
    invoice_id = Column(String, nullable=True, index=True)  # stripe invoice id if available (for deduplication)
    receipt_url = Column(Text, nullable=True)
    raw_event = Column(JSON, nullable=True)  # Store raw Stripe event data
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)


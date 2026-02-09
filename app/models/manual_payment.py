from sqlalchemy import Column, String, Integer, DateTime, ForeignKey, Numeric, Text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
from app.db.session import Base
import uuid
from datetime import datetime


class ManualPayment(Base):
    """
    Manual payment transactions entered by users.
    These are separate from Stripe payments and should NOT appear in Stripe dashboard.
    They DO affect cash collected totals and revenue contributors.
    """
    __tablename__ = "manual_payments"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, index=True)
    org_id = Column(UUID(as_uuid=True), ForeignKey("organizations.id"), nullable=False, index=True)
    client_id = Column(UUID(as_uuid=True), ForeignKey("clients.id"), nullable=False, index=True)
    
    # Payment details
    amount_cents = Column(Integer, nullable=False)  # Amount in cents
    currency = Column(String(3), default="usd", nullable=False)
    payment_date = Column(DateTime(timezone=True), nullable=False, default=datetime.utcnow)
    
    # Optional fields
    description = Column(Text, nullable=True)  # Payment description/notes
    payment_method = Column(String(100), nullable=True)  # e.g., "cash", "check", "bank_transfer", "other"
    receipt_url = Column(String(500), nullable=True)  # Optional receipt/document URL
    
    # Metadata
    created_by = Column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=True)  # User who created the payment
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    
    # Relationships
    client = relationship("Client", backref="manual_payments")
    organization = relationship("Organization", backref="manual_payments")
    creator = relationship("User", foreign_keys=[created_by])


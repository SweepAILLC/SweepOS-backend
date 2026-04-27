from sqlalchemy import Column, String, DateTime, Integer, ForeignKey
from sqlalchemy.dialects.postgresql import UUID, JSON
import uuid
from datetime import datetime
from app.db.session import Base


class WhopPayment(Base):
    __tablename__ = "whop_payments"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    org_id = Column(UUID(as_uuid=True), ForeignKey("organizations.id"), nullable=False, index=True)
    whop_id = Column(String, nullable=False)  # pay_xxx
    amount_cents = Column(Integer, nullable=False, default=0)
    currency = Column(String(3), default="usd", nullable=False)
    status = Column(String, nullable=False, index=True)
    client_id = Column(UUID(as_uuid=True), ForeignKey("clients.id"), nullable=True, index=True)
    raw = Column(JSON, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

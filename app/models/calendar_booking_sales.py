"""
Calendar booking sales tracking: mark bookings as sales calls and track if sale closed.
Used in calendar tab to distinguish sales vs check-in calls and for close-rate reporting.
"""
from sqlalchemy import Column, String, Boolean, DateTime, ForeignKey
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
from app.db.session import Base
import uuid
from datetime import datetime


class CalendarBookingSales(Base):
    """
    Per-booking sales call designation and close status.
    Keyed by provider + event_id (Cal.com booking uid, or Calendly event uuid).
    """
    __tablename__ = "calendar_booking_sales"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, index=True)
    org_id = Column(UUID(as_uuid=True), ForeignKey("organizations.id"), nullable=False, index=True)
    provider = Column(String(20), nullable=False, index=True)  # "calcom" | "calendly"
    event_id = Column(String(255), nullable=False, index=True)  # Cal.com uid or Calendly event uuid
    event_uri = Column(String(512), nullable=True)  # Full Calendly event URI if needed
    is_sales_call = Column(Boolean, default=False, nullable=False)
    sale_closed = Column(Boolean, nullable=True)  # True=closed, False=open, None=not set
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    organization = relationship("Organization", backref="calendar_booking_sales")


class EventTypeSalesCall(Base):
    """
    Event types designated as sales calls (e.g. "Discovery Call", "Sales Demo").
    New bookings of these types are treated as sales calls by default.
    """
    __tablename__ = "event_type_sales_calls"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, index=True)
    org_id = Column(UUID(as_uuid=True), ForeignKey("organizations.id"), nullable=False, index=True)
    provider = Column(String(20), nullable=False, index=True)  # "calcom" | "calendly"
    event_type_id = Column(String(255), nullable=False, index=True)  # Cal.com event type id or Calendly event type URI
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow, nullable=False)

    organization = relationship("Organization", backref="event_type_sales_calls")

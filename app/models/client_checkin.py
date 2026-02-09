from sqlalchemy import Column, String, DateTime, ForeignKey, Text, Boolean
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
from app.db.session import Base
import uuid
from datetime import datetime


class ClientCheckIn(Base):
    """
    Check-in records synced from calendar events (Cal.com or Calendly).
    Matches calendar events with clients by email address.
    """
    __tablename__ = "client_check_ins"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, index=True)
    org_id = Column(UUID(as_uuid=True), ForeignKey("organizations.id"), nullable=False, index=True)
    client_id = Column(UUID(as_uuid=True), ForeignKey("clients.id"), nullable=False, index=True)
    
    # Calendar event details
    event_id = Column(String, nullable=False, index=True)  # Calendar event ID (from Cal.com or Calendly)
    event_uri = Column(String, nullable=True)  # Calendly event URI (if applicable)
    provider = Column(String, nullable=False)  # "calcom" or "calendly"
    title = Column(String, nullable=True)  # Event title
    start_time = Column(DateTime(timezone=True), nullable=False, index=True)
    end_time = Column(DateTime(timezone=True), nullable=True)
    location = Column(String, nullable=True)  # Meeting location/URL
    meeting_url = Column(String, nullable=True)  # Meeting link (Zoom, Google Meet, etc.)
    
    # Attendee information (for matching)
    attendee_email = Column(String, nullable=False, index=True)  # Email used to match with client
    attendee_name = Column(String, nullable=True)
    
    # Status
    completed = Column(Boolean, default=False, nullable=False)  # True if meeting has passed
    cancelled = Column(Boolean, default=False, nullable=False)  # True if event was cancelled
    
    # Metadata
    raw_event_data = Column(Text, nullable=True)  # JSON string of full event data for reference
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    
    # Relationships
    client = relationship("Client", backref="check_ins")
    organization = relationship("Organization", backref="check_ins")


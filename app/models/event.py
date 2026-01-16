from sqlalchemy import Column, String, DateTime, ForeignKey, JSON
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
import uuid
from datetime import datetime
from app.db.session import Base


class Event(Base):
    __tablename__ = "events"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    org_id = Column(UUID(as_uuid=True), ForeignKey("organizations.id"), nullable=False, index=True)
    funnel_id = Column(UUID(as_uuid=True), ForeignKey("funnels.id"), nullable=True, index=True)  # For funnel events
    client_id = Column(UUID(as_uuid=True), ForeignKey("clients.id"), nullable=True, index=True)
    type = Column(String, nullable=False)  # payment, checkin, message, funnel_event
    event_name = Column(String, nullable=True, index=True)  # For funnel events: page_view, form_submit, etc.
    visitor_id = Column(String, nullable=True, index=True)  # Anonymous visitor identifier
    session_id = Column(String, nullable=True, index=True)  # Session identifier
    payload = Column(JSON, nullable=True)  # Event payload (backward compatibility)
    event_metadata = Column(JSON, nullable=True)  # Additional event metadata (for funnel events) - renamed from 'metadata' to avoid SQLAlchemy reserved name
    occurred_at = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)
    received_at = Column(DateTime, default=datetime.utcnow, nullable=False)  # When event was received by API


from sqlalchemy import Column, String, DateTime, JSON, Text
from sqlalchemy.dialects.postgresql import UUID
import uuid
from datetime import datetime
from app.db.session import Base


class EventError(Base):
    __tablename__ = "event_errors"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    payload = Column(JSON, nullable=True)  # Original event payload that failed
    reason = Column(Text, nullable=True)  # Error reason/message
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)


from sqlalchemy import Column, String, DateTime, ForeignKey, JSON
from sqlalchemy.dialects.postgresql import UUID
import uuid
from datetime import datetime
from app.db.session import Base


class Session(Base):
    __tablename__ = "sessions"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    org_id = Column(UUID(as_uuid=True), ForeignKey("organizations.id"), nullable=False, index=True)
    visitor_id = Column(String, nullable=True, index=True)  # Anonymous visitor identifier
    session_id = Column(String, nullable=True, index=True)  # Session identifier
    first_seen = Column(DateTime, default=datetime.utcnow, nullable=False)
    last_seen = Column(DateTime, default=datetime.utcnow, nullable=False, onupdate=datetime.utcnow)
    utm = Column(JSON, nullable=True)  # UTM parameters: {source, medium, campaign, term, content}
    referrer = Column(String, nullable=True, index=True)  # HTTP referrer (where user came from)
    session_metadata = Column(JSON, nullable=True)  # Additional session metadata - renamed from 'metadata' to avoid SQLAlchemy reserved name


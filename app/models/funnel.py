from sqlalchemy import Column, String, DateTime, ForeignKey, Integer
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
import uuid
from datetime import datetime
from app.db.session import Base


class Funnel(Base):
    __tablename__ = "funnels"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    org_id = Column(UUID(as_uuid=True), ForeignKey("organizations.id"), nullable=False, index=True)
    client_id = Column(UUID(as_uuid=True), ForeignKey("clients.id"), nullable=True, index=True)
    name = Column(String, nullable=False)
    slug = Column(String, nullable=True, unique=True)  # Optional unique slug for URL matching
    domain = Column(String, nullable=True)  # Domain for URL-based funnel detection
    env = Column(String, nullable=True)  # Environment: production, staging, etc.
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)


class FunnelStep(Base):
    __tablename__ = "funnel_steps"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    org_id = Column(UUID(as_uuid=True), ForeignKey("organizations.id"), nullable=False, index=True)
    funnel_id = Column(UUID(as_uuid=True), ForeignKey("funnels.id", ondelete="CASCADE"), nullable=False, index=True)
    step_order = Column(Integer, nullable=False)  # Order of step in funnel (1, 2, 3, ...)
    event_name = Column(String, nullable=False)  # Event name that triggers this step
    label = Column(String, nullable=True)  # Human-readable label for the step
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)


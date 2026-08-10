"""Durable queue for batched funnel-lead digest emails to org admins."""
from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    text,
)
from sqlalchemy.dialects.postgresql import UUID
import uuid
from datetime import datetime
from app.db.session import Base


class FunnelLeadNotification(Base):
    """One row per funnel lead capture; worker flushes into digests per org."""

    __tablename__ = "funnel_lead_notifications"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    org_id = Column(
        UUID(as_uuid=True),
        ForeignKey("organizations.id"),
        nullable=False,
        index=True,
    )
    client_id = Column(
        UUID(as_uuid=True),
        ForeignKey("clients.id", ondelete="SET NULL"),
        nullable=True,
    )
    funnel_id = Column(UUID(as_uuid=True), nullable=True)
    funnel_name = Column(String(255), nullable=True)

    # Snapshot at capture time (email stays correct if client later changes)
    lead_name = Column(String(512), nullable=True)
    lead_email = Column(String(512), nullable=True)
    lead_phone = Column(String(64), nullable=True)
    lead_instagram = Column(String(255), nullable=True)
    source = Column(String(128), nullable=True)
    funnel_step_reached = Column(String(255), nullable=True)
    is_new_client = Column(Boolean, nullable=False, default=True)

    sent_at = Column(DateTime, nullable=True)
    attempts = Column(Integer, nullable=False, default=0)
    next_attempt_at = Column(DateTime, nullable=True)
    error_text = Column(Text, nullable=True)

    created_at = Column(DateTime, nullable=False, default=datetime.utcnow, index=True)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        Index(
            "ix_funnel_lead_notifications_unsent_org_created",
            "org_id",
            "created_at",
            postgresql_where=text("sent_at IS NULL"),
        ),
    )

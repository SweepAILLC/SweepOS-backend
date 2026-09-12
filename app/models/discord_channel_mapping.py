import uuid
from datetime import datetime

from sqlalchemy import Column, DateTime, ForeignKey, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID

from app.db.session import Base


class DiscordChannelMapping(Base):
    """
    Routes one Sweep event type (eod_form, post_call, general) to one Discord
    channel_id within the org's connected guild. One row per (org_id, event_type).
    """

    __tablename__ = "discord_channel_mappings"
    __table_args__ = (
        UniqueConstraint("org_id", "event_type", name="uq_discord_channel_mappings_org_event"),
    )

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    org_id = Column(UUID(as_uuid=True), ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False, index=True)
    event_type = Column(String(64), nullable=False)
    channel_id = Column(String(64), nullable=False)
    channel_name = Column(String(255), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

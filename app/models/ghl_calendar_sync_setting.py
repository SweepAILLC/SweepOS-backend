import uuid
from datetime import datetime

from sqlalchemy import Boolean, Column, DateTime, ForeignKey, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID

from app.db.session import Base


class GhlCalendarSyncSetting(Base):
    """
    Which GHL calendars an org has opted into syncing to Sweep, and whether bookings
    on that calendar count as sales calls. One row per (org_id, calendar_id).
    Absence of a row (or enabled=False) means the calendar's events are ignored —
    both by the manual calendar picker UI and the appointment webhook handler.
    """

    __tablename__ = "ghl_calendar_sync_settings"
    __table_args__ = (
        UniqueConstraint("org_id", "calendar_id", name="uq_ghl_calendar_sync_settings_org_calendar"),
    )

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    org_id = Column(UUID(as_uuid=True), ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False, index=True)
    calendar_id = Column(String(255), nullable=False)
    calendar_name = Column(String(255), nullable=True)
    enabled = Column(Boolean, default=True, nullable=False)
    is_sales_call = Column(Boolean, default=False, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

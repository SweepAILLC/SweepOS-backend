"""Team KPIs (docs/features/TEAM_KPIS_PRD.md): org settings, notification log."""
from datetime import datetime
import uuid

from sqlalchemy import Column, Date, DateTime, Float, ForeignKey, Index, String, text
from sqlalchemy.dialects.postgresql import JSONB, UUID

from app.db.session import Base

# Rep type for member-access users (Settings → Team role dropdown). Sales reps owe the
# daily EOD and appear in the Team view; marketing reps don't.
TEAM_ROLES = ("sales", "marketing")


class TeamKpiSettings(Base):
    """One row per org: EOD required weekdays, reminder + digest schedule/channels (JSON)."""

    __tablename__ = "team_kpi_settings"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    org_id = Column(
        UUID(as_uuid=True), ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False, unique=True
    )
    settings = Column(JSONB, nullable=False, default=dict)
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)


class TeamKpiNotification(Base):
    """Send log that makes reminders/digests idempotent across worker restarts and double runs."""

    __tablename__ = "team_kpi_notifications"
    __table_args__ = (
        Index(
            "uq_team_kpi_notifications_member",
            "org_id",
            "kind",
            "channel",
            "user_id",
            "period_key",
            unique=True,
            postgresql_where=text("user_id IS NOT NULL"),
        ),
        Index(
            "uq_team_kpi_notifications_team",
            "org_id",
            "kind",
            "channel",
            "period_key",
            unique=True,
            postgresql_where=text("user_id IS NULL"),
        ),
    )

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    org_id = Column(UUID(as_uuid=True), ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False)
    kind = Column(String(16), nullable=False)  # "reminder" | "digest"
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=True)
    period_key = Column(Date, nullable=False)
    channel = Column(String(16), nullable=False)  # "discord" | "email"
    sent_at = Column(DateTime(timezone=True), default=datetime.utcnow, nullable=False)

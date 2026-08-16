"""System-owner notices sent to consulting orgs (portal + toolkit)."""
from sqlalchemy import Column, DateTime, ForeignKey, String, Text
from sqlalchemy.dialects.postgresql import UUID
import uuid
from datetime import datetime
from app.db.session import Base


class OwnerOrgNotice(Base):
    __tablename__ = "owner_org_notices"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    org_id = Column(
        UUID(as_uuid=True),
        ForeignKey("organizations.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    title = Column(String(200), nullable=False)
    body = Column(Text, nullable=False)
    created_by = Column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)


class OwnerOrgNoticeRead(Base):
    __tablename__ = "owner_org_notice_reads"

    notice_id = Column(
        UUID(as_uuid=True),
        ForeignKey("owner_org_notices.id", ondelete="CASCADE"),
        primary_key=True,
    )
    user_id = Column(
        UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        primary_key=True,
    )
    read_at = Column(DateTime, default=datetime.utcnow, nullable=False)

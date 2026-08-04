"""Daily Instagram account-level insight snapshots for follower/growth trends."""
from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import Column, Date, DateTime, ForeignKey, Integer, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID

from app.db.session import Base


class InstagramAccountSnapshot(Base):
    __tablename__ = "instagram_account_snapshots"
    __table_args__ = (
        UniqueConstraint(
            "org_id",
            "snapshot_date",
            name="uq_instagram_account_snapshots_org_date",
        ),
    )

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    org_id = Column(
        UUID(as_uuid=True),
        ForeignKey("organizations.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    snapshot_date = Column(Date, nullable=False, index=True)

    followers_count = Column(Integer, nullable=True)
    reach = Column(Integer, nullable=True)
    views = Column(Integer, nullable=True)
    profile_views = Column(Integer, nullable=True)
    accounts_engaged = Column(Integer, nullable=True)
    follows = Column(Integer, nullable=True)
    unfollows = Column(Integer, nullable=True)

    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

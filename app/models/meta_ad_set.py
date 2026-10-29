"""One row per synced Meta ad set — see docs/features/META_ADS_PERFORMANCE_PRD.md."""
from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import BigInteger, Column, DateTime, ForeignKey, String
from sqlalchemy.dialects.postgresql import UUID

from app.db.session import Base


class MetaAdSet(Base):
    __tablename__ = "meta_ad_sets"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    org_id = Column(
        UUID(as_uuid=True),
        ForeignKey("organizations.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    campaign_id = Column(
        UUID(as_uuid=True),
        ForeignKey("meta_campaigns.id", ondelete="CASCADE"),
        nullable=False,
    )
    meta_adset_id = Column(String(64), nullable=False)
    name = Column(String(255), nullable=True)
    optimization_goal = Column(String(64), nullable=True)
    effective_status = Column(String(32), nullable=True)
    daily_budget_cents = Column(BigInteger, nullable=True)
    learning_stage = Column(String(32), nullable=True)  # LEARNING | LEARNING_LIMITED | SUCCESS | ...
    budget_updated_at = Column(DateTime(timezone=True), nullable=True)

    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

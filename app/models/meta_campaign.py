"""One row per synced Meta campaign — see docs/features/META_ADS_PERFORMANCE_PRD.md."""
from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import BigInteger, Boolean, Column, DateTime, ForeignKey, String
from sqlalchemy.dialects.postgresql import UUID

from app.db.session import Base


class MetaCampaign(Base):
    __tablename__ = "meta_campaigns"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    org_id = Column(
        UUID(as_uuid=True),
        ForeignKey("organizations.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    ad_account_id = Column(
        UUID(as_uuid=True),
        ForeignKey("meta_ad_accounts.id", ondelete="CASCADE"),
        nullable=False,
    )
    meta_campaign_id = Column(String(64), nullable=False)
    name = Column(String(255), nullable=True)
    objective = Column(String(64), nullable=True)
    status = Column(String(32), nullable=True)  # Meta's own status, synced
    effective_status = Column(String(32), nullable=True)
    daily_budget_cents = Column(BigInteger, nullable=True)
    # Stamped when a sync sees a different daily_budget than the stored one.
    budget_updated_at = Column(DateTime(timezone=True), nullable=True)
    # Sweep-side annotation only — never synced from Meta. See PRD "Foundational campaign tagging".
    sweep_status = Column(String(16), nullable=True)  # testing | foundational | paused
    # Many campaigns can feed one funnel (replaces Funnel.meta_campaign_id's 1:1
    # assumption — see migration 110). Null = unlinked.
    funnel_id = Column(UUID(as_uuid=True), ForeignKey("funnels.id", ondelete="SET NULL"), nullable=True, index=True)
    funnel_linked_at = Column(DateTime(timezone=True), nullable=True)
    funnel_linked_by_user_id = Column(UUID(as_uuid=True), ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    funnel_link_auto_suggested = Column(Boolean, nullable=False, default=False)

    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

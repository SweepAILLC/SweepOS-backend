"""Daily insights per entity level — see docs/features/META_ADS_PERFORMANCE_PRD.md.

Upsert target, not append-only: Meta revises insights for up to 28 days after
the report date (attribution settling), so a sync always re-pulls the trailing
28 days and upserts on (org_id, level, entity_id, date) rather than inserting
a new row per sync.
"""
from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import BigInteger, Column, Date, DateTime, Float, ForeignKey, Integer, String
from sqlalchemy.dialects.postgresql import UUID

from app.db.session import Base


class MetaAdInsightDaily(Base):
    __tablename__ = "meta_ad_insights_daily"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    org_id = Column(
        UUID(as_uuid=True),
        ForeignKey("organizations.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    level = Column(String(16), nullable=False)  # account | campaign | adset | ad
    # Meta id of the entity at that level (meta_campaign_id / meta_adset_id / meta_ad_id / meta_ad_account_id).
    entity_id = Column(String(64), nullable=False)
    date = Column(Date, nullable=False)
    spend_cents = Column(BigInteger, nullable=True)
    results = Column(Integer, nullable=True)
    # Nullable: Meta omits this when the campaign has no mapped optimization/result event.
    cost_per_result_cents = Column(BigInteger, nullable=True)
    impressions = Column(BigInteger, nullable=True)
    clicks = Column(BigInteger, nullable=True)
    cpm_cents = Column(BigInteger, nullable=True)
    ctr = Column(Float, nullable=True)
    reach = Column(BigInteger, nullable=True)
    frequency = Column(Float, nullable=True)
    link_ctr = Column(Float, nullable=True)
    video_3s_views = Column(BigInteger, nullable=True)
    thruplays = Column(BigInteger, nullable=True)

    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

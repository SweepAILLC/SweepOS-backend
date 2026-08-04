"""Cached Instagram media + insights for Marketing Intel Performance."""
from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID

from app.db.session import Base


class InstagramMedia(Base):
    __tablename__ = "instagram_media"
    __table_args__ = (
        UniqueConstraint("org_id", "ig_media_id", name="uq_instagram_media_org_ig_media"),
    )

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    org_id = Column(
        UUID(as_uuid=True),
        ForeignKey("organizations.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    ig_media_id = Column(String(64), nullable=False, index=True)
    permalink = Column(String(512), nullable=True)
    media_type = Column(String(32), nullable=True)  # IMAGE, VIDEO, CAROUSEL_ALBUM
    media_product_type = Column(String(32), nullable=True)  # FEED, REELS, STORY
    posted_at = Column(DateTime, nullable=True, index=True)
    caption = Column(Text, nullable=True)
    thumbnail_url = Column(String(1024), nullable=True)

    # Raw metrics
    views = Column(Integer, nullable=True)
    reach = Column(Integer, nullable=True)
    saved = Column(Integer, nullable=True)
    likes = Column(Integer, nullable=True)
    comments = Column(Integer, nullable=True)
    shares = Column(Integer, nullable=True)
    total_interactions = Column(Integer, nullable=True)
    reposts = Column(Integer, nullable=True)

    # Reels-specific
    avg_watch_time_sec = Column(Float, nullable=True)
    total_watch_time_sec = Column(Float, nullable=True)
    skip_rate_pct = Column(Float, nullable=True)

    # Derived
    engagement_rate_pct = Column(Float, nullable=True)
    save_rate_pct = Column(Float, nullable=True)
    share_rate_pct = Column(Float, nullable=True)
    reach_vs_followers_pct = Column(Float, nullable=True)

    # Classification
    format_bucket = Column(String(32), nullable=True)
    hook_text = Column(String(500), nullable=True)
    hook_pattern = Column(String(64), nullable=True)
    theme_keys = Column(JSONB, nullable=True)
    funnel_stage = Column(String(8), nullable=True)  # TOF | MOF | BOF
    caption_len = Column(Integer, nullable=True)
    posted_dow = Column(Integer, nullable=True)  # 0=Mon … 6=Sun
    posted_hour = Column(Integer, nullable=True)

    # Provenance
    insights_status = Column(String(32), nullable=False, default="unavailable")
    insights_error = Column(Text, nullable=True)
    metrics_settled = Column(Boolean, nullable=False, default=False)
    last_synced_at = Column(DateTime, nullable=True)
    linked_concept_id = Column(String(64), nullable=True)

    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

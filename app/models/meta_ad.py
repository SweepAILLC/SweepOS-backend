"""One row per synced Meta ad — see docs/features/META_ADS_PERFORMANCE_PRD.md."""
from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import Column, DateTime, ForeignKey, String, Text
from sqlalchemy.dialects.postgresql import UUID

from app.db.session import Base


class MetaAd(Base):
    __tablename__ = "meta_ads"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    org_id = Column(
        UUID(as_uuid=True),
        ForeignKey("organizations.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    ad_set_id = Column(
        UUID(as_uuid=True),
        ForeignKey("meta_ad_sets.id", ondelete="CASCADE"),
        nullable=False,
    )
    meta_ad_id = Column(String(64), nullable=False)
    name = Column(String(255), nullable=True)
    creative_id = Column(String(64), nullable=True)
    creative_thumbnail_url = Column(String(1024), nullable=True)
    effective_status = Column(String(32), nullable=True)
    # Meta's own ad creation timestamp (not this row's created_at) — used to
    # count "New Ads Deployed" per week for a funnel's mapped campaign.
    created_time = Column(DateTime(timezone=True), nullable=True)

    # Messaging Matrix inputs (see docs/features/MESSAGING_MATRIX.md). video_id and
    # instagram_media_id come from the ad creative; when the ad is a boosted reel that
    # already has a transcript, that transcript is reused instead of paying for Whisper.
    video_id = Column(String(64), nullable=True)
    instagram_media_id = Column(String(64), nullable=True)
    ad_copy = Column(Text, nullable=True)  # primary text + headline, used for image ads
    transcript = Column(Text, nullable=True)
    transcript_source = Column(String(16), nullable=True)  # instagram | whisper | ad_copy
    transcript_status = Column(String(16), nullable=False, default="unavailable")  # unavailable|ok|failed
    transcript_error = Column(Text, nullable=True)
    transcribed_at = Column(DateTime, nullable=True)

    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

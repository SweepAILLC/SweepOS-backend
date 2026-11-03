"""Instagram media: store the raw video media_url from each sync.

instagram_transcript_service.transcribe_media has always read media.media_url to
download the reel for Whisper, but no code ever persisted it — instagram_sync_service
only derived a still-image thumbnail from the raw media_url and discarded the original,
so every transcription attempt (on-demand, post-sync top-performer, post-sync
priority-reference) has been silently failing with AttributeError. Column is nullable
and refreshed on every sync (same lifecycle as thumbnail_url), so a stale/expired CDN
link self-heals on the next sync rather than needing a separate refetch mechanism.

Revision ID: 115
Revises: 114
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "115"
down_revision = "114"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("instagram_media", sa.Column("media_url", sa.Text(), nullable=True))


def downgrade() -> None:
    op.drop_column("instagram_media", "media_url")

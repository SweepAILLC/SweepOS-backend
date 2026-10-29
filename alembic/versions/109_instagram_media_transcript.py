"""Instagram reel transcripts (OpenAI Whisper) for top-performer angle ideation.

Additive only. Transcript text + status live on instagram_media so auto-transcription
of rank_score top_posts and on-demand per-post transcription share one row.

Revision ID: 109
Revises: 108
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "109"
down_revision = "108"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("instagram_media", sa.Column("transcript", sa.Text(), nullable=True))
    op.add_column(
        "instagram_media",
        sa.Column("transcript_status", sa.String(length=16), nullable=False, server_default="unavailable"),
    )
    op.add_column("instagram_media", sa.Column("transcript_error", sa.Text(), nullable=True))
    op.add_column("instagram_media", sa.Column("transcribed_at", sa.DateTime(), nullable=True))


def downgrade() -> None:
    op.drop_column("instagram_media", "transcribed_at")
    op.drop_column("instagram_media", "transcript_error")
    op.drop_column("instagram_media", "transcript_status")
    op.drop_column("instagram_media", "transcript")

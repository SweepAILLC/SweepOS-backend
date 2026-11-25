"""Messaging Matrix: ad transcripts, messaging angles, and the content items tagged to them.

Additive only: nullable columns on meta_ads and two new tables. No backfill.

Revision ID: 120
Revises: 119
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "120"
down_revision = "119"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("meta_ads", sa.Column("video_id", sa.String(64), nullable=True))
    op.add_column("meta_ads", sa.Column("instagram_media_id", sa.String(64), nullable=True))
    op.add_column("meta_ads", sa.Column("ad_copy", sa.Text(), nullable=True))
    op.add_column("meta_ads", sa.Column("transcript", sa.Text(), nullable=True))
    op.add_column("meta_ads", sa.Column("transcript_source", sa.String(16), nullable=True))
    op.add_column(
        "meta_ads",
        sa.Column("transcript_status", sa.String(16), nullable=False, server_default="unavailable"),
    )
    op.add_column("meta_ads", sa.Column("transcript_error", sa.Text(), nullable=True))
    op.add_column("meta_ads", sa.Column("transcribed_at", sa.DateTime(), nullable=True))

    op.create_table(
        "messaging_angles",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "org_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("organizations.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("name", sa.String(120), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("awareness_stage", sa.String(24), nullable=True),
        sa.Column("persona", sa.String(120), nullable=True),
        sa.Column("promise", sa.Text(), nullable=True),
        sa.Column("hook_mechanic", sa.String(32), nullable=True),
        sa.Column("organic_score", sa.Float(), nullable=True),
        sa.Column("paid_score", sa.Float(), nullable=True),
        sa.Column("overall_score", sa.Float(), nullable=True),
        sa.Column("organic_samples", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("paid_samples", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("reasoning", sa.Text(), nullable=True),
        sa.Column("reasoning_score", sa.Float(), nullable=True),
        sa.Column("reasoning_prompt_version", sa.String(16), nullable=True),
        sa.Column("last_draft", postgresql.JSONB(), nullable=True),
        sa.Column("status", sa.String(16), nullable=False, server_default="active"),
        sa.Column("scored_at", sa.DateTime(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
    )
    op.create_index("ix_messaging_angles_org_score", "messaging_angles", ["org_id", "overall_score"])
    op.create_index(
        "uq_messaging_angles_org_name",
        "messaging_angles",
        ["org_id", sa.text("lower(name)")],
        unique=True,
    )

    op.create_table(
        "messaging_angle_sources",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "org_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("organizations.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "angle_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("messaging_angles.id", ondelete="CASCADE"),
            nullable=True,  # NULL = tag rejected (low confidence / bad quote); row caches the hash
        ),
        sa.Column("source_type", sa.String(16), nullable=False),
        sa.Column("source_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("quote", sa.Text(), nullable=True),
        sa.Column("tag_confidence", sa.Float(), nullable=True),
        sa.Column("item_score", sa.Float(), nullable=True),
        sa.Column("metrics_snapshot", postgresql.JSONB(), nullable=True),
        sa.Column("transcript_hash", sa.String(64), nullable=True),
        sa.Column("prompt_version", sa.String(16), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint("org_id", "source_type", "source_id", name="uq_messaging_angle_source_item"),
    )
    op.create_index("ix_messaging_angle_sources_org_angle", "messaging_angle_sources", ["org_id", "angle_id"])


def downgrade() -> None:
    op.drop_index("ix_messaging_angle_sources_org_angle", table_name="messaging_angle_sources")
    op.drop_table("messaging_angle_sources")
    op.drop_index("uq_messaging_angles_org_name", table_name="messaging_angles")
    op.drop_index("ix_messaging_angles_org_score", table_name="messaging_angles")
    op.drop_table("messaging_angles")
    for col in (
        "transcribed_at",
        "transcript_error",
        "transcript_status",
        "transcript_source",
        "transcript",
        "ad_copy",
        "instagram_media_id",
        "video_id",
    ):
        op.drop_column("meta_ads", col)

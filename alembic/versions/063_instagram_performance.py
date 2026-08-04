"""Instagram Performance Intel: oauth provider + media/snapshot tables.

Revision ID: 063
Revises: 062
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "063"
down_revision = "062"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Idempotent enum add (PostgreSQL <15 has no IF NOT EXISTS on ADD VALUE).
    op.execute(
        """
        DO $$ BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_enum e
                JOIN pg_type t ON e.enumtypid = t.oid
                WHERE t.typname = 'oauthprovider' AND e.enumlabel = 'instagram'
            ) THEN
                ALTER TYPE oauthprovider ADD VALUE 'instagram';
            END IF;
        END $$;
        """
    )

    op.create_table(
        "instagram_media",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "org_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("organizations.id", ondelete="CASCADE"),
            nullable=False,
            index=True,
        ),
        sa.Column("ig_media_id", sa.String(length=64), nullable=False, index=True),
        sa.Column("permalink", sa.String(length=512), nullable=True),
        sa.Column("media_type", sa.String(length=32), nullable=True),
        sa.Column("media_product_type", sa.String(length=32), nullable=True),
        sa.Column("posted_at", sa.DateTime(), nullable=True, index=True),
        sa.Column("caption", sa.Text(), nullable=True),
        sa.Column("thumbnail_url", sa.String(length=1024), nullable=True),
        sa.Column("views", sa.Integer(), nullable=True),
        sa.Column("reach", sa.Integer(), nullable=True),
        sa.Column("saved", sa.Integer(), nullable=True),
        sa.Column("likes", sa.Integer(), nullable=True),
        sa.Column("comments", sa.Integer(), nullable=True),
        sa.Column("shares", sa.Integer(), nullable=True),
        sa.Column("total_interactions", sa.Integer(), nullable=True),
        sa.Column("reposts", sa.Integer(), nullable=True),
        sa.Column("avg_watch_time_sec", sa.Float(), nullable=True),
        sa.Column("total_watch_time_sec", sa.Float(), nullable=True),
        sa.Column("skip_rate_pct", sa.Float(), nullable=True),
        sa.Column("engagement_rate_pct", sa.Float(), nullable=True),
        sa.Column("save_rate_pct", sa.Float(), nullable=True),
        sa.Column("share_rate_pct", sa.Float(), nullable=True),
        sa.Column("reach_vs_followers_pct", sa.Float(), nullable=True),
        sa.Column("format_bucket", sa.String(length=32), nullable=True),
        sa.Column("hook_text", sa.String(length=500), nullable=True),
        sa.Column("hook_pattern", sa.String(length=64), nullable=True),
        sa.Column("theme_keys", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("funnel_stage", sa.String(length=8), nullable=True),
        sa.Column("caption_len", sa.Integer(), nullable=True),
        sa.Column("posted_dow", sa.Integer(), nullable=True),
        sa.Column("posted_hour", sa.Integer(), nullable=True),
        sa.Column(
            "insights_status",
            sa.String(length=32),
            nullable=False,
            server_default="unavailable",
        ),
        sa.Column("insights_error", sa.Text(), nullable=True),
        sa.Column(
            "metrics_settled",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
        sa.Column("last_synced_at", sa.DateTime(), nullable=True),
        sa.Column("linked_concept_id", sa.String(length=64), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.UniqueConstraint("org_id", "ig_media_id", name="uq_instagram_media_org_ig_media"),
    )

    op.create_table(
        "instagram_account_snapshots",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "org_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("organizations.id", ondelete="CASCADE"),
            nullable=False,
            index=True,
        ),
        sa.Column("snapshot_date", sa.Date(), nullable=False, index=True),
        sa.Column("followers_count", sa.Integer(), nullable=True),
        sa.Column("reach", sa.Integer(), nullable=True),
        sa.Column("views", sa.Integer(), nullable=True),
        sa.Column("profile_views", sa.Integer(), nullable=True),
        sa.Column("accounts_engaged", sa.Integer(), nullable=True),
        sa.Column("follows", sa.Integer(), nullable=True),
        sa.Column("unfollows", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.UniqueConstraint(
            "org_id",
            "snapshot_date",
            name="uq_instagram_account_snapshots_org_date",
        ),
    )


def downgrade() -> None:
    op.drop_table("instagram_account_snapshots")
    op.drop_table("instagram_media")
    # Enum values cannot be safely removed on PostgreSQL — leave 'instagram' in place.

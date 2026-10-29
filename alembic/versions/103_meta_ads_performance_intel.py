"""Meta Ads performance intel: meta_ads oauth provider + 5 tables.

Direct Meta Marketing API (no Composio) — see
docs/features/META_ADS_PERFORMANCE_PRD.md. Additive only, no backfill.
`meta_ad_insights_daily` is an upsert target (unique on org_id+level+entity_id+date)
rather than append-only — Meta revises insights for up to 28 days after the report
date, unlike an immutable event log.

Revision ID: 103
Revises: 102
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import UUID

revision = "103"
down_revision = "102"
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
                WHERE t.typname = 'oauthprovider' AND e.enumlabel = 'meta_ads'
            ) THEN
                ALTER TYPE oauthprovider ADD VALUE 'meta_ads';
            END IF;
        END $$;
        """
    )

    op.create_table(
        "meta_ad_accounts",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "org_id",
            UUID(as_uuid=True),
            sa.ForeignKey("organizations.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("meta_ad_account_id", sa.String(64), nullable=False),  # Meta's act_<id>
        sa.Column("account_name", sa.String(255), nullable=True),
        sa.Column("currency", sa.String(8), nullable=True),
        sa.Column("last_synced_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
    )
    op.create_index(
        "uq_meta_ad_accounts_org_account",
        "meta_ad_accounts",
        ["org_id", "meta_ad_account_id"],
        unique=True,
    )

    op.create_table(
        "meta_campaigns",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "org_id",
            UUID(as_uuid=True),
            sa.ForeignKey("organizations.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "ad_account_id",
            UUID(as_uuid=True),
            sa.ForeignKey("meta_ad_accounts.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("meta_campaign_id", sa.String(64), nullable=False),
        sa.Column("name", sa.String(255), nullable=True),
        sa.Column("objective", sa.String(64), nullable=True),
        sa.Column("status", sa.String(32), nullable=True),  # Meta's own status, synced
        # Sweep-side annotation only — never synced from Meta. See PRD "Foundational campaign tagging".
        sa.Column("sweep_status", sa.String(16), nullable=True),  # testing | foundational | paused
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
    )
    op.create_index(
        "uq_meta_campaigns_org_campaign",
        "meta_campaigns",
        ["org_id", "meta_campaign_id"],
        unique=True,
    )

    op.create_table(
        "meta_ad_sets",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "org_id",
            UUID(as_uuid=True),
            sa.ForeignKey("organizations.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "campaign_id",
            UUID(as_uuid=True),
            sa.ForeignKey("meta_campaigns.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("meta_adset_id", sa.String(64), nullable=False),
        sa.Column("name", sa.String(255), nullable=True),
        sa.Column("optimization_goal", sa.String(64), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
    )
    op.create_index(
        "uq_meta_ad_sets_org_adset",
        "meta_ad_sets",
        ["org_id", "meta_adset_id"],
        unique=True,
    )

    op.create_table(
        "meta_ads",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "org_id",
            UUID(as_uuid=True),
            sa.ForeignKey("organizations.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "ad_set_id",
            UUID(as_uuid=True),
            sa.ForeignKey("meta_ad_sets.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("meta_ad_id", sa.String(64), nullable=False),
        sa.Column("name", sa.String(255), nullable=True),
        sa.Column("creative_id", sa.String(64), nullable=True),
        sa.Column("creative_thumbnail_url", sa.String(1024), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
    )
    op.create_index(
        "uq_meta_ads_org_ad",
        "meta_ads",
        ["org_id", "meta_ad_id"],
        unique=True,
    )

    op.create_table(
        "meta_ad_insights_daily",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "org_id",
            UUID(as_uuid=True),
            sa.ForeignKey("organizations.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("level", sa.String(16), nullable=False),  # account | campaign | adset | ad
        # Meta id of the entity at that level (meta_campaign_id / meta_adset_id / meta_ad_id / meta_ad_account_id).
        sa.Column("entity_id", sa.String(64), nullable=False),
        sa.Column("date", sa.Date(), nullable=False),
        sa.Column("spend_cents", sa.BigInteger(), nullable=True),
        sa.Column("results", sa.Integer(), nullable=True),
        # Nullable: Meta omits this when the campaign has no mapped optimization/result event.
        sa.Column("cost_per_result_cents", sa.BigInteger(), nullable=True),
        sa.Column("impressions", sa.BigInteger(), nullable=True),
        sa.Column("clicks", sa.BigInteger(), nullable=True),
        sa.Column("cpm_cents", sa.BigInteger(), nullable=True),
        sa.Column("ctr", sa.Float(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
    )
    # Upsert target — insights get revised for up to 28 days after the report date.
    op.create_index(
        "uq_meta_ad_insights_daily_entity_date",
        "meta_ad_insights_daily",
        ["org_id", "level", "entity_id", "date"],
        unique=True,
    )
    op.create_index(
        "ix_meta_ad_insights_daily_org_date",
        "meta_ad_insights_daily",
        ["org_id", "date"],
    )


def downgrade() -> None:
    op.drop_index("ix_meta_ad_insights_daily_org_date", table_name="meta_ad_insights_daily")
    op.drop_index("uq_meta_ad_insights_daily_entity_date", table_name="meta_ad_insights_daily")
    op.drop_table("meta_ad_insights_daily")
    op.drop_index("uq_meta_ads_org_ad", table_name="meta_ads")
    op.drop_table("meta_ads")
    op.drop_index("uq_meta_ad_sets_org_adset", table_name="meta_ad_sets")
    op.drop_table("meta_ad_sets")
    op.drop_index("uq_meta_campaigns_org_campaign", table_name="meta_campaigns")
    op.drop_table("meta_campaigns")
    op.drop_index("uq_meta_ad_accounts_org_account", table_name="meta_ad_accounts")
    op.drop_table("meta_ad_accounts")
    # PostgreSQL cannot easily remove enum values; leave labels in place.

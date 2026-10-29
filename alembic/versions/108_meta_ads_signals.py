"""Ads dashboard signals: fatigue metrics, budgets, and alert rows.

Additive only. Insights gain reach/frequency/link CTR/video counts so the
rules engine can tell creative fatigue from a messaging ceiling. Ad sets and
campaigns store budget + delivery status. `meta_ad_alerts` holds open
diagnostics (partial-unique while not resolved).

Revision ID: 108
Revises: 107
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB, UUID

revision = "108"
down_revision = "107"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("meta_ad_insights_daily", sa.Column("reach", sa.BigInteger(), nullable=True))
    op.add_column("meta_ad_insights_daily", sa.Column("frequency", sa.Float(), nullable=True))
    op.add_column("meta_ad_insights_daily", sa.Column("link_ctr", sa.Float(), nullable=True))
    op.add_column("meta_ad_insights_daily", sa.Column("video_3s_views", sa.BigInteger(), nullable=True))
    op.add_column("meta_ad_insights_daily", sa.Column("thruplays", sa.BigInteger(), nullable=True))

    op.add_column("meta_campaigns", sa.Column("daily_budget_cents", sa.BigInteger(), nullable=True))
    op.add_column("meta_campaigns", sa.Column("effective_status", sa.String(length=32), nullable=True))
    op.add_column("meta_campaigns", sa.Column("budget_updated_at", sa.DateTime(timezone=True), nullable=True))

    op.add_column("meta_ad_sets", sa.Column("daily_budget_cents", sa.BigInteger(), nullable=True))
    op.add_column("meta_ad_sets", sa.Column("effective_status", sa.String(length=32), nullable=True))
    op.add_column("meta_ad_sets", sa.Column("learning_stage", sa.String(length=32), nullable=True))
    op.add_column("meta_ad_sets", sa.Column("budget_updated_at", sa.DateTime(timezone=True), nullable=True))

    op.add_column("meta_ads", sa.Column("effective_status", sa.String(length=32), nullable=True))

    op.add_column("meta_ad_accounts", sa.Column("target_cpr_cents", sa.BigInteger(), nullable=True))
    op.add_column("meta_ad_accounts", sa.Column("min_roas", sa.Float(), nullable=True))

    op.create_table(
        "meta_ad_alerts",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "org_id",
            UUID(as_uuid=True),
            sa.ForeignKey("organizations.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("level", sa.String(length=16), nullable=False),
        sa.Column("entity_id", sa.String(length=64), nullable=False),
        sa.Column("alert_type", sa.String(length=32), nullable=False),
        sa.Column("severity", sa.String(length=16), nullable=False),
        sa.Column("metrics_snapshot", JSONB(), nullable=True),
        sa.Column("recommended_action", sa.Text(), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False, server_default="open"),
        sa.Column("first_detected_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_evaluated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("notified_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
    )
    op.create_index("ix_meta_ad_alerts_org_status", "meta_ad_alerts", ["org_id", "status"])
    op.create_index(
        "uq_meta_ad_alerts_active",
        "meta_ad_alerts",
        ["org_id", "entity_id", "alert_type"],
        unique=True,
        postgresql_where=sa.text("status <> 'resolved'"),
    )


def downgrade() -> None:
    op.drop_index("uq_meta_ad_alerts_active", table_name="meta_ad_alerts")
    op.drop_index("ix_meta_ad_alerts_org_status", table_name="meta_ad_alerts")
    op.drop_table("meta_ad_alerts")

    op.drop_column("meta_ad_accounts", "min_roas")
    op.drop_column("meta_ad_accounts", "target_cpr_cents")
    op.drop_column("meta_ads", "effective_status")
    op.drop_column("meta_ad_sets", "budget_updated_at")
    op.drop_column("meta_ad_sets", "learning_stage")
    op.drop_column("meta_ad_sets", "effective_status")
    op.drop_column("meta_ad_sets", "daily_budget_cents")
    op.drop_column("meta_campaigns", "budget_updated_at")
    op.drop_column("meta_campaigns", "effective_status")
    op.drop_column("meta_campaigns", "daily_budget_cents")
    op.drop_column("meta_ad_insights_daily", "thruplays")
    op.drop_column("meta_ad_insights_daily", "video_3s_views")
    op.drop_column("meta_ad_insights_daily", "link_ctr")
    op.drop_column("meta_ad_insights_daily", "frequency")
    op.drop_column("meta_ad_insights_daily", "reach")

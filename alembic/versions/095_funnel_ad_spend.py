"""Add funnel_ad_spend: manually-entered weekly ad spend + ads/angles deployed per funnel (PRD phase 8).

Revision ID: 095
Revises: 094
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "095"
down_revision = "094"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "funnel_ad_spend",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("org_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("organizations.id"), nullable=False),
        sa.Column(
            "funnel_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("funnels.id", ondelete="CASCADE"),
            nullable=True,
        ),
        sa.Column("week_start", sa.Date(), nullable=False),
        sa.Column("amount_cents", sa.Integer(), nullable=False, server_default="0"),
        # Weekly creative-velocity counts from the sheet ("New Ads/Angles Deployed").
        sa.Column("ads_deployed", sa.Integer(), nullable=True),
        sa.Column("angles_deployed", sa.Integer(), nullable=True),
        sa.Column("entered_by_user_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("users.id"), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    )
    op.create_index("ix_funnel_ad_spend_org_id", "funnel_ad_spend", ["org_id"])
    op.create_index("ix_funnel_ad_spend_funnel_id", "funnel_ad_spend", ["funnel_id"])
    # One row per (org, funnel, week); Postgres treats NULLs as distinct, so the
    # unassigned (funnel_id IS NULL) bucket needs its own partial index.
    op.create_index(
        "uq_funnel_ad_spend_org_funnel_week",
        "funnel_ad_spend",
        ["org_id", "funnel_id", "week_start"],
        unique=True,
        postgresql_where=sa.text("funnel_id IS NOT NULL"),
    )
    op.create_index(
        "uq_funnel_ad_spend_org_unassigned_week",
        "funnel_ad_spend",
        ["org_id", "week_start"],
        unique=True,
        postgresql_where=sa.text("funnel_id IS NULL"),
    )


def downgrade() -> None:
    op.drop_index("uq_funnel_ad_spend_org_unassigned_week", table_name="funnel_ad_spend")
    op.drop_index("uq_funnel_ad_spend_org_funnel_week", table_name="funnel_ad_spend")
    op.drop_index("ix_funnel_ad_spend_funnel_id", table_name="funnel_ad_spend")
    op.drop_index("ix_funnel_ad_spend_org_id", table_name="funnel_ad_spend")
    op.drop_table("funnel_ad_spend")

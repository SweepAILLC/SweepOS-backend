"""Many-campaigns-to-one-funnel linking, replacing Funnel.meta_campaign_id's
1:1 assumption. A funnel can be fed by more than one campaign (confirmed real
usage, not hypothetical) — Funnel.meta_campaign_id (migration 106) can't
represent that, so the FK moves to the many side: MetaCampaign.funnel_id.
See docs/features/META_ADS_PERFORMANCE_PRD.md, "Funnel Scorecard integration".

Funnel.meta_campaign_id / meta_campaign_name (106) are left in place,
deprecated, not dropped or backfilled further — any org with a single
existing mapping gets it copied forward below so nothing regresses.
Additive only, no destructive change.

Revision ID: 110
Revises: 109
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "110"
down_revision = "109"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("meta_campaigns", sa.Column("funnel_id", postgresql.UUID(as_uuid=True), nullable=True))
    op.add_column("meta_campaigns", sa.Column("funnel_linked_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column(
        "meta_campaigns",
        sa.Column("funnel_linked_by_user_id", postgresql.UUID(as_uuid=True), nullable=True),
    )
    op.add_column(
        "meta_campaigns",
        sa.Column("funnel_link_auto_suggested", sa.Boolean(), nullable=False, server_default=sa.false()),
    )
    op.create_foreign_key(
        "fk_meta_campaigns_funnel_id",
        "meta_campaigns",
        "funnels",
        ["funnel_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_index("ix_meta_campaigns_funnel_id", "meta_campaigns", ["funnel_id"])

    # One-time copy: any funnel already singly-mapped via migration 106's columns
    # gets that same campaign's new funnel_id set, so existing orgs see no change.
    op.execute(
        """
        UPDATE meta_campaigns mc
        SET funnel_id = f.id,
            funnel_linked_at = now()
        FROM funnels f
        WHERE f.meta_campaign_id IS NOT NULL
          AND f.org_id = mc.org_id
          AND f.meta_campaign_id = mc.meta_campaign_id
          AND mc.funnel_id IS NULL
        """
    )


def downgrade() -> None:
    op.drop_index("ix_meta_campaigns_funnel_id", table_name="meta_campaigns")
    op.drop_constraint("fk_meta_campaigns_funnel_id", "meta_campaigns", type_="foreignkey")
    op.drop_column("meta_campaigns", "funnel_link_auto_suggested")
    op.drop_column("meta_campaigns", "funnel_linked_by_user_id")
    op.drop_column("meta_campaigns", "funnel_linked_at")
    op.drop_column("meta_campaigns", "funnel_id")

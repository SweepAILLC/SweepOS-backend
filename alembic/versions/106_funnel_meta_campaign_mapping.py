"""Map a funnel to a Meta Ads campaign so its weekly ad spend auto-populates
from already-synced meta_ad_insights_daily instead of manual entry. See
docs/features/META_ADS_PERFORMANCE_PRD.md. Additive only, no backfill.

Revision ID: 106
Revises: 105
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "106"
down_revision = "105"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("funnels", sa.Column("meta_campaign_id", sa.String(64), nullable=True))
    op.add_column("funnels", sa.Column("meta_campaign_name", sa.String(255), nullable=True))


def downgrade() -> None:
    op.drop_column("funnels", "meta_campaign_name")
    op.drop_column("funnels", "meta_campaign_id")

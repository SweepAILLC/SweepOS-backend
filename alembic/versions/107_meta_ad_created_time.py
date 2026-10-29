"""Add meta_ads.created_time (Meta's own ad creation timestamp, not our sync
row's created_at) so weekly "New Ads Deployed" can be counted per funnel's
mapped campaign. See docs/features/META_ADS_PERFORMANCE_PRD.md. Additive
only, no backfill — existing rows get created_time on their next full sync.

Revision ID: 107
Revises: 106
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "107"
down_revision = "106"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("meta_ads", sa.Column("created_time", sa.DateTime(timezone=True), nullable=True))


def downgrade() -> None:
    op.drop_column("meta_ads", "created_time")

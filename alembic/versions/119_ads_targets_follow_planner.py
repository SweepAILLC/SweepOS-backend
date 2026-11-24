"""Ads tab targets follow the Scale Planner by default.

Revision ID: 119
Revises: 118
"""
import sqlalchemy as sa
from alembic import op

revision = "119"
down_revision = "118"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "meta_ad_accounts",
        sa.Column("targets_follow_planner", sa.Boolean(), nullable=False, server_default=sa.text("true")),
    )


def downgrade():
    op.drop_column("meta_ad_accounts", "targets_follow_planner")

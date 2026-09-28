"""Add setter_booked_client_ids to org_kpi_daily_entries.

Revision ID: 092
Revises: 091
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "092"
down_revision = "091"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "org_kpi_daily_entries",
        sa.Column("setter_booked_client_ids", sa.JSON(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("org_kpi_daily_entries", "setter_booked_client_ids")

"""Widen org_kpi_daily_entries.best_content_type for free-text notes.

Revision ID: 061
Revises: 060
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "061"
down_revision = "060"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    insp = sa.inspect(conn)
    if "org_kpi_daily_entries" not in insp.get_table_names():
        return
    op.alter_column(
        "org_kpi_daily_entries",
        "best_content_type",
        existing_type=sa.String(length=64),
        type_=sa.String(length=512),
        existing_nullable=True,
    )


def downgrade() -> None:
    conn = op.get_bind()
    insp = sa.inspect(conn)
    if "org_kpi_daily_entries" not in insp.get_table_names():
        return
    op.alter_column(
        "org_kpi_daily_entries",
        "best_content_type",
        existing_type=sa.String(length=512),
        type_=sa.String(length=64),
        existing_nullable=True,
    )

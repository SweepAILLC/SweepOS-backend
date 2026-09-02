"""Add calls_booked_activity to org_kpi_daily_entries (booking-creation-date metric).

Revision ID: 072
Revises: 071
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "072"
down_revision = "071"
branch_labels = None
depends_on = None

TABLE = "org_kpi_daily_entries"
COLUMN = "calls_booked_activity"


def upgrade() -> None:
    conn = op.get_bind()
    insp = sa.inspect(conn)
    if TABLE not in insp.get_table_names():
        return
    existing = {c["name"] for c in insp.get_columns(TABLE)}
    if COLUMN not in existing:
        op.add_column(TABLE, sa.Column(COLUMN, sa.Integer(), nullable=True))


def downgrade() -> None:
    conn = op.get_bind()
    insp = sa.inspect(conn)
    if TABLE not in insp.get_table_names():
        return
    existing = {c["name"] for c in insp.get_columns(TABLE)}
    if COLUMN in existing:
        op.drop_column(TABLE, COLUMN)

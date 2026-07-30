"""Add setter KPI fields: booking split, convos, setter context.

Revision ID: 062
Revises: 061
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "062"
down_revision = "061"
branch_labels = None
depends_on = None

NEW_COLUMNS = (
    ("inbound_bookings", sa.Column("inbound_bookings", sa.Integer(), nullable=True)),
    ("outbound_bookings", sa.Column("outbound_bookings", sa.Integer(), nullable=True)),
    ("new_conversations", sa.Column("new_conversations", sa.Integer(), nullable=True)),
    ("conversations_nurtured", sa.Column("conversations_nurtured", sa.Integer(), nullable=True)),
    ("setter_context", sa.Column("setter_context", sa.Text(), nullable=True)),
)


def upgrade() -> None:
    conn = op.get_bind()
    insp = sa.inspect(conn)
    if "org_kpi_daily_entries" not in insp.get_table_names():
        return
    existing = {c["name"] for c in insp.get_columns("org_kpi_daily_entries")}
    for name, col in NEW_COLUMNS:
        if name not in existing:
            op.add_column("org_kpi_daily_entries", col)


def downgrade() -> None:
    conn = op.get_bind()
    insp = sa.inspect(conn)
    if "org_kpi_daily_entries" not in insp.get_table_names():
        return
    existing = {c["name"] for c in insp.get_columns("org_kpi_daily_entries")}
    for name, _ in reversed(NEW_COLUMNS):
        if name in existing:
            op.drop_column("org_kpi_daily_entries", name)

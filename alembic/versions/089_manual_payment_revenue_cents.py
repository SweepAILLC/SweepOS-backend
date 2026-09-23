"""Add revenue_cents to manual_payments.

Revision ID: 089
Revises: 088
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "089"
down_revision = "088"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    insp = sa.inspect(conn)
    if "manual_payments" not in set(insp.get_table_names()):
        return
    cols = {c["name"] for c in insp.get_columns("manual_payments")}
    if "revenue_cents" in cols:
        return
    op.add_column("manual_payments", sa.Column("revenue_cents", sa.Integer(), nullable=True))


def downgrade() -> None:
    conn = op.get_bind()
    insp = sa.inspect(conn)
    if "manual_payments" not in set(insp.get_table_names()):
        return
    cols = {c["name"] for c in insp.get_columns("manual_payments")}
    if "revenue_cents" not in cols:
        return
    op.drop_column("manual_payments", "revenue_cents")

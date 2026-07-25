"""Add node_kind (action|wait) to automation_rules.

Revision ID: 058
Revises: 057
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "058"
down_revision = "057"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    insp = sa.inspect(conn)
    if "automation_rules" not in insp.get_table_names():
        return
    cols = {c["name"] for c in insp.get_columns("automation_rules")}
    if "node_kind" not in cols:
        op.add_column(
            "automation_rules",
            sa.Column("node_kind", sa.String(length=16), nullable=False, server_default="action"),
        )


def downgrade() -> None:
    conn = op.get_bind()
    insp = sa.inspect(conn)
    if "automation_rules" not in insp.get_table_names():
        return
    cols = {c["name"] for c in insp.get_columns("automation_rules")}
    if "node_kind" in cols:
        op.drop_column("automation_rules", "node_kind")

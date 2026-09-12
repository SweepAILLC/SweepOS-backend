"""Add program start/end dates on organizations.

Revision ID: 080
Revises: 079
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "080"
down_revision = "079"
branch_labels = None
depends_on = None


def upgrade():
    conn = op.get_bind()
    insp = sa.inspect(conn)
    cols = {c["name"] for c in insp.get_columns("organizations")}
    if "program_start_date" not in cols:
        op.add_column("organizations", sa.Column("program_start_date", sa.Date(), nullable=True))
    if "program_end_date" not in cols:
        op.add_column("organizations", sa.Column("program_end_date", sa.Date(), nullable=True))


def downgrade():
    conn = op.get_bind()
    insp = sa.inspect(conn)
    cols = {c["name"] for c in insp.get_columns("organizations")}
    if "program_end_date" in cols:
        op.drop_column("organizations", "program_end_date")
    if "program_start_date" in cols:
        op.drop_column("organizations", "program_start_date")

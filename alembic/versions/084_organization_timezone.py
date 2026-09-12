"""Add organizations.timezone (IANA tz name) for localized notification formatting.

Revision ID: 084
Revises: 083
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "084"
down_revision = "083"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    insp = sa.inspect(conn)
    cols = [c["name"] for c in insp.get_columns("organizations")]
    if "timezone" not in cols:
        op.add_column(
            "organizations",
            sa.Column("timezone", sa.String(64), nullable=False, server_default="UTC"),
        )


def downgrade() -> None:
    op.drop_column("organizations", "timezone")

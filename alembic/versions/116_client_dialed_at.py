"""Remember when a pipeline lead was marked dialled.

Revision ID: 116
Revises: 115
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "116"
down_revision = "115"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("clients", sa.Column("dialed_at", sa.DateTime(), nullable=True))


def downgrade() -> None:
    op.drop_column("clients", "dialed_at")

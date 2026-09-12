"""Add multi_use flag for unlimited signup links.

Revision ID: 078
Revises: 077
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "078"
down_revision = "077"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "organization_invitations",
        sa.Column("multi_use", sa.Boolean(), nullable=False, server_default=sa.false()),
    )


def downgrade():
    op.drop_column("organization_invitations", "multi_use")

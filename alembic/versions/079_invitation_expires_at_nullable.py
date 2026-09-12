"""Allow invitations with no expiration.

Revision ID: 079
Revises: 078
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "079"
down_revision = "078"
branch_labels = None
depends_on = None


def upgrade():
    op.alter_column(
        "organization_invitations",
        "expires_at",
        existing_type=sa.DateTime(),
        nullable=True,
    )


def downgrade():
    op.execute(
        "UPDATE organization_invitations SET expires_at = NOW() + INTERVAL '7 days' "
        "WHERE expires_at IS NULL"
    )
    op.alter_column(
        "organization_invitations",
        "expires_at",
        existing_type=sa.DateTime(),
        nullable=False,
    )

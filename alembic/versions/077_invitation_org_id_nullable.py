"""Allow organization invitations without a pre-created org.

Revision ID: 077
Revises: 076

Open signup links create the organization when the recipient accepts.
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "077"
down_revision = "076"
branch_labels = None
depends_on = None


def upgrade():
    op.alter_column(
        "organization_invitations",
        "org_id",
        existing_type=postgresql.UUID(as_uuid=True),
        nullable=True,
    )


def downgrade():
    op.execute("DELETE FROM organization_invitations WHERE org_id IS NULL")
    op.alter_column(
        "organization_invitations",
        "org_id",
        existing_type=postgresql.UUID(as_uuid=True),
        nullable=False,
    )

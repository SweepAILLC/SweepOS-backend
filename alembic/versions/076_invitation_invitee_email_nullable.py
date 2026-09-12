"""Allow organization invitations without a preloaded invitee email.

Revision ID: 076
Revises: 075

Open (account-creation) invite links bind email on accept.
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "076"
down_revision = "075"
branch_labels = None
depends_on = None


def upgrade():
    op.alter_column(
        "organization_invitations",
        "invitee_email",
        existing_type=sa.String(255),
        nullable=True,
    )


def downgrade():
    op.execute(
        "UPDATE organization_invitations SET invitee_email = '' WHERE invitee_email IS NULL"
    )
    op.alter_column(
        "organization_invitations",
        "invitee_email",
        existing_type=sa.String(255),
        nullable=False,
    )

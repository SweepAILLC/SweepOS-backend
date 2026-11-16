"""Remember when a sales call was marked closed, for dial-to-close.

Existing closed calls use updated_at as the close moment.

Revision ID: 117
Revises: 116
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "117"
down_revision = "116"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "client_check_ins",
        sa.Column("sale_closed_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.execute(
        "UPDATE client_check_ins SET sale_closed_at = updated_at "
        "WHERE sale_closed IS TRUE AND sale_closed_at IS NULL"
    )


def downgrade() -> None:
    op.drop_column("client_check_ins", "sale_closed_at")

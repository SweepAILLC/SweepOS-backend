"""add no_show to client_check_ins

Revision ID: 028
Revises: 027
Create Date: 2026-02-25

Allow check-in history to track no-show events (from Cal.com or manual).
"""
from alembic import op
import sqlalchemy as sa


revision = "028"
down_revision = "027"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "client_check_ins",
        sa.Column("no_show", sa.Boolean(), nullable=False, server_default="false"),
    )


def downgrade():
    op.drop_column("client_check_ins", "no_show")

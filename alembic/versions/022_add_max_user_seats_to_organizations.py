"""add max_user_seats to organizations

Revision ID: 022
Revises: 021
Create Date: 2025-02-07 12:00:00.000000

"""
from alembic import op
import sqlalchemy as sa

revision = "022"
down_revision = "021"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("organizations", sa.Column("max_user_seats", sa.Integer(), nullable=True))


def downgrade():
    op.drop_column("organizations", "max_user_seats")

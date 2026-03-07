"""add fathom_api_key to users

Revision ID: 029
Revises: 028
Create Date: 2026-03-01

Store Fathom API key for intelligence (call summaries/transcripts).
"""
from alembic import op
import sqlalchemy as sa


revision = "029"
down_revision = "028"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "users",
        sa.Column("fathom_api_key", sa.String(), nullable=True),
    )


def downgrade():
    op.drop_column("users", "fathom_api_key")

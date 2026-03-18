"""add fathom_api_key to users

Revision ID: 030
Revises: 029
Create Date: 2026-03-01

Store Fathom API key for intelligence (call summaries/transcripts).
"""
from alembic import op
import sqlalchemy as sa


revision = "030"
down_revision = "029"
branch_labels = None
depends_on = None


def upgrade():
    conn = op.get_bind()
    r = conn.execute(
        sa.text(
            "SELECT 1 FROM information_schema.columns "
            "WHERE table_schema = 'public' AND table_name = 'users' AND column_name = 'fathom_api_key'"
        )
    )
    if r.fetchone() is None:
        op.add_column(
            "users",
            sa.Column("fathom_api_key", sa.String(), nullable=True),
        )


def downgrade():
    op.drop_column("users", "fathom_api_key")

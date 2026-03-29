"""Add ai_profile JSONB column to users table for AI personalization context

Revision ID: 034
Revises: 033
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "034"
down_revision = "033"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "users",
        sa.Column("ai_profile", postgresql.JSON(astext_type=sa.Text()), nullable=True),
    )


def downgrade():
    op.drop_column("users", "ai_profile")

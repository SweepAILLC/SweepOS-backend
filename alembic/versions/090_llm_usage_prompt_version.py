"""Add prompt_version to llm_usage_events.

Revision ID: 090
Revises: 089
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "090"
down_revision = "089"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "llm_usage_events",
        sa.Column("prompt_version", sa.String(length=16), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("llm_usage_events", "prompt_version")

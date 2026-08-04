"""Add composio to oauthprovider enum for per-org Composio API keys.

Revision ID: 064
Revises: 063
"""
from __future__ import annotations

from alembic import op

revision = "064"
down_revision = "063"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Idempotent enum add (PostgreSQL <15 has no IF NOT EXISTS on ADD VALUE).
    op.execute(
        """
        DO $$ BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_enum e
                JOIN pg_type t ON e.enumtypid = t.oid
                WHERE t.typname = 'oauthprovider' AND e.enumlabel = 'composio'
            ) THEN
                ALTER TYPE oauthprovider ADD VALUE 'composio';
            END IF;
        END $$;
        """
    )


def downgrade() -> None:
    # PostgreSQL cannot easily remove enum values; leave label in place.
    pass

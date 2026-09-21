"""Add ghl to oauthprovider enum.

Revision ID: 085
Revises: 084
"""
from __future__ import annotations

from alembic import op

revision = "085"
down_revision = "084"
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
                WHERE t.typname = 'oauthprovider' AND e.enumlabel = 'ghl'
            ) THEN
                ALTER TYPE oauthprovider ADD VALUE 'ghl';
            END IF;
        END $$;
        """
    )


def downgrade() -> None:
    # PostgreSQL cannot easily remove enum values; leave label in place.
    pass

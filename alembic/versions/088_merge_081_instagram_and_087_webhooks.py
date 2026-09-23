"""No-op after 087. Instagram DM migration 081 was never shipped.

Revision ID: 088
Revises: 087
"""
from __future__ import annotations

revision = "088"
down_revision = "087"
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass

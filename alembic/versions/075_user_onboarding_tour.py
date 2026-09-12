"""Add onboarding_tour_completed_at to users; grandfather existing users.

Revision ID: 075
Revises: 074

Product-tour completion is stored on the org-scoped user row so it
follows the account across devices (not localStorage). Existing users
are grandfathered so they are not forced through the tour on next login.
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "075"
down_revision = "074"
branch_labels = None
depends_on = None

TABLE = "users"
COL = "onboarding_tour_completed_at"


def upgrade() -> None:
    conn = op.get_bind()
    insp = sa.inspect(conn)
    if TABLE not in insp.get_table_names():
        return
    existing = {c["name"] for c in insp.get_columns(TABLE)}
    if COL not in existing:
        op.add_column(TABLE, sa.Column(COL, sa.DateTime(), nullable=True))
    op.execute(
        sa.text(
            f"UPDATE {TABLE} SET {COL} = COALESCE({COL}, created_at) WHERE {COL} IS NULL"
        )
    )


def downgrade() -> None:
    conn = op.get_bind()
    insp = sa.inspect(conn)
    if TABLE not in insp.get_table_names():
        return
    existing = {c["name"] for c in insp.get_columns(TABLE)}
    if COL in existing:
        op.drop_column(TABLE, COL)

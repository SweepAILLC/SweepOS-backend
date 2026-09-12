"""Add onboarding_call_booked_at to users; grandfather existing users.

Revision ID: 074
Revises: 073

Existing users are grandfathered (set to created_at) so they are not
forced to book a call on next login.  New users invited after this
migration will have NULL and must book before entering the app.
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "074"
down_revision = "073"
branch_labels = None
depends_on = None

TABLE = "users"
COL = "onboarding_call_booked_at"


def upgrade() -> None:
    conn = op.get_bind()
    insp = sa.inspect(conn)
    if TABLE not in insp.get_table_names():
        return
    existing = {c["name"] for c in insp.get_columns(TABLE)}
    if COL not in existing:
        op.add_column(TABLE, sa.Column(COL, sa.DateTime(), nullable=True))
    # Grandfather: anyone already in the DB skips the booking step.
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

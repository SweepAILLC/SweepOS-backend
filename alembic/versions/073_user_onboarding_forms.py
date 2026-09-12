"""Track first-login Tally CSA + intake form completion on users.

Revision ID: 073
Revises: 072

Existing users are grandfathered so they are not blocked on next login.
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "073"
down_revision = "072"
branch_labels = None
depends_on = None

TABLE = "users"
CSA = "onboarding_csa_completed_at"
INTAKE = "onboarding_intake_completed_at"


def upgrade() -> None:
    conn = op.get_bind()
    insp = sa.inspect(conn)
    if TABLE not in insp.get_table_names():
        return
    existing = {c["name"] for c in insp.get_columns(TABLE)}
    if CSA not in existing:
        op.add_column(TABLE, sa.Column(CSA, sa.DateTime(), nullable=True))
    if INTAKE not in existing:
        op.add_column(TABLE, sa.Column(INTAKE, sa.DateTime(), nullable=True))
    op.execute(
        sa.text(
            f"UPDATE {TABLE} SET {CSA} = COALESCE({CSA}, created_at), "
            f"{INTAKE} = COALESCE({INTAKE}, created_at) "
            f"WHERE {CSA} IS NULL OR {INTAKE} IS NULL"
        )
    )


def downgrade() -> None:
    conn = op.get_bind()
    insp = sa.inspect(conn)
    if TABLE not in insp.get_table_names():
        return
    existing = {c["name"] for c in insp.get_columns(TABLE)}
    if INTAKE in existing:
        op.drop_column(TABLE, INTAKE)
    if CSA in existing:
        op.drop_column(TABLE, CSA)

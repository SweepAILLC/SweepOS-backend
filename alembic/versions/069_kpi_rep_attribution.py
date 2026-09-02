"""Add rep_user_id to org_kpi_daily_entries for per-rep KPI attribution.

Revision ID: 069
Revises: 068
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import UUID

revision = "069"
down_revision = "068"
branch_labels = None
depends_on = None

TABLE = "org_kpi_daily_entries"
OLD_CONSTRAINT = "uq_org_kpi_daily_entries_org_date"
AGG_INDEX = "uq_org_kpi_daily_entries_org_date_agg"
REP_INDEX = "uq_org_kpi_daily_entries_org_date_rep"


def upgrade() -> None:
    conn = op.get_bind()
    insp = sa.inspect(conn)
    if TABLE not in insp.get_table_names():
        return

    existing_cols = {c["name"] for c in insp.get_columns(TABLE)}
    if "rep_user_id" not in existing_cols:
        op.add_column(
            TABLE,
            sa.Column(
                "rep_user_id",
                UUID(as_uuid=True),
                sa.ForeignKey("users.id", ondelete="SET NULL"),
                nullable=True,
            ),
        )

    # Replace the plain (org_id, entry_date) unique constraint with two
    # partial unique indexes: NULL rep_user_id is never distinct from itself
    # under a standard unique constraint, so a plain 3-column constraint would
    # silently allow duplicate aggregate rows once rep_user_id is nullable.
    existing_constraints = {c["name"] for c in insp.get_unique_constraints(TABLE)}
    if OLD_CONSTRAINT in existing_constraints:
        op.drop_constraint(OLD_CONSTRAINT, TABLE, type_="unique")

    existing_indexes = {i["name"] for i in insp.get_indexes(TABLE)}
    if AGG_INDEX not in existing_indexes:
        op.create_index(
            AGG_INDEX,
            TABLE,
            ["org_id", "entry_date"],
            unique=True,
            postgresql_where=sa.text("rep_user_id IS NULL"),
        )
    if REP_INDEX not in existing_indexes:
        op.create_index(
            REP_INDEX,
            TABLE,
            ["org_id", "entry_date", "rep_user_id"],
            unique=True,
            postgresql_where=sa.text("rep_user_id IS NOT NULL"),
        )


def downgrade() -> None:
    conn = op.get_bind()
    insp = sa.inspect(conn)
    if TABLE not in insp.get_table_names():
        return

    existing_indexes = {i["name"] for i in insp.get_indexes(TABLE)}
    if REP_INDEX in existing_indexes:
        op.drop_index(REP_INDEX, table_name=TABLE)
    if AGG_INDEX in existing_indexes:
        op.drop_index(AGG_INDEX, table_name=TABLE)

    existing_cols = {c["name"] for c in insp.get_columns(TABLE)}
    if "rep_user_id" in existing_cols:
        op.drop_column(TABLE, "rep_user_id")

    # Recreate the original constraint. Fails if any (org_id, entry_date)
    # pair now has duplicate rows — expected if per-rep rows were written
    # after upgrade; delete/collapse those rows before downgrading.
    existing_constraints = {c["name"] for c in insp.get_unique_constraints(TABLE)}
    if OLD_CONSTRAINT not in existing_constraints:
        op.create_unique_constraint(OLD_CONSTRAINT, TABLE, ["org_id", "entry_date"])

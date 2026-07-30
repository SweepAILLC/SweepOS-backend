"""Add entry_form_token to org_kpi_benchmarks.

Revision ID: 060
Revises: 059
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import UUID

revision = "060"
down_revision = "059"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    insp = sa.inspect(conn)
    if "org_kpi_benchmarks" not in insp.get_table_names():
        return
    cols = {c["name"] for c in insp.get_columns("org_kpi_benchmarks")}
    if "entry_form_token" not in cols:
        op.add_column(
            "org_kpi_benchmarks",
            sa.Column("entry_form_token", UUID(as_uuid=True), nullable=True),
        )
    idx = {i["name"] for i in insp.get_indexes("org_kpi_benchmarks")}
    if "ix_org_kpi_benchmarks_entry_form_token" not in idx:
        op.create_index(
            "ix_org_kpi_benchmarks_entry_form_token",
            "org_kpi_benchmarks",
            ["entry_form_token"],
            unique=True,
        )


def downgrade() -> None:
    conn = op.get_bind()
    insp = sa.inspect(conn)
    if "org_kpi_benchmarks" not in insp.get_table_names():
        return
    idx = {i["name"] for i in insp.get_indexes("org_kpi_benchmarks")}
    if "ix_org_kpi_benchmarks_entry_form_token" in idx:
        op.drop_index("ix_org_kpi_benchmarks_entry_form_token", table_name="org_kpi_benchmarks")
    cols = {c["name"] for c in insp.get_columns("org_kpi_benchmarks")}
    if "entry_form_token" in cols:
        op.drop_column("org_kpi_benchmarks", "entry_form_token")

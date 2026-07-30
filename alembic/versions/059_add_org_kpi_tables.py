"""Add org KPI daily entries and benchmark settings tables.

Revision ID: 059
Revises: 058
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB, UUID

revision = "059"
down_revision = "058"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    insp = sa.inspect(conn)
    tables = insp.get_table_names()

    if "org_kpi_daily_entries" not in tables:
        op.create_table(
            "org_kpi_daily_entries",
            sa.Column("id", UUID(as_uuid=True), primary_key=True, nullable=False),
            sa.Column(
                "org_id",
                UUID(as_uuid=True),
                sa.ForeignKey("organizations.id", ondelete="CASCADE"),
                nullable=False,
            ),
            sa.Column("entry_date", sa.Date(), nullable=False),
            sa.Column("total_followers", sa.Integer(), nullable=True),
            sa.Column("new_followers", sa.Integer(), nullable=True),
            sa.Column("content_posted", sa.Boolean(), nullable=True),
            sa.Column("best_content_type", sa.String(length=64), nullable=True),
            sa.Column("inboxes_checked", sa.Integer(), nullable=True),
            sa.Column("outreach_sent", sa.Integer(), nullable=True),
            sa.Column("respondents", sa.Integer(), nullable=True),
            sa.Column("inbound_icp_leads", sa.Integer(), nullable=True),
            sa.Column("followups_sent", sa.Integer(), nullable=True),
            sa.Column("calls_pitched", sa.Integer(), nullable=True),
            sa.Column("calls_booked", sa.Integer(), nullable=True),
            sa.Column("calls_taken", sa.Integer(), nullable=True),
            sa.Column("offers_made", sa.Integer(), nullable=True),
            sa.Column("no_shows", sa.Integer(), nullable=True),
            sa.Column("closes", sa.Integer(), nullable=True),
            sa.Column("cash_collected", sa.Numeric(12, 2), nullable=True),
            sa.Column("revenue", sa.Numeric(12, 2), nullable=True),
            sa.Column("created_at", sa.DateTime(), nullable=False),
            sa.Column("updated_at", sa.DateTime(), nullable=False),
            sa.UniqueConstraint("org_id", "entry_date", name="uq_org_kpi_daily_entries_org_date"),
        )
        op.create_index("ix_org_kpi_daily_entries_org_id", "org_kpi_daily_entries", ["org_id"])
        op.create_index("ix_org_kpi_daily_entries_entry_date", "org_kpi_daily_entries", ["entry_date"])

    if "org_kpi_benchmarks" not in tables:
        op.create_table(
            "org_kpi_benchmarks",
            sa.Column("id", UUID(as_uuid=True), primary_key=True, nullable=False),
            sa.Column(
                "org_id",
                UUID(as_uuid=True),
                sa.ForeignKey("organizations.id", ondelete="CASCADE"),
                nullable=False,
            ),
            sa.Column("thresholds", JSONB(), nullable=False),
            sa.Column("content_type_tags", JSONB(), nullable=True),
            sa.Column("created_at", sa.DateTime(), nullable=False),
            sa.Column("updated_at", sa.DateTime(), nullable=False),
            sa.UniqueConstraint("org_id", name="uq_org_kpi_benchmarks_org_id"),
        )
        op.create_index("ix_org_kpi_benchmarks_org_id", "org_kpi_benchmarks", ["org_id"])


def downgrade() -> None:
    conn = op.get_bind()
    insp = sa.inspect(conn)
    tables = insp.get_table_names()

    if "org_kpi_benchmarks" in tables:
        op.drop_index("ix_org_kpi_benchmarks_org_id", table_name="org_kpi_benchmarks")
        op.drop_table("org_kpi_benchmarks")

    if "org_kpi_daily_entries" in tables:
        op.drop_index("ix_org_kpi_daily_entries_entry_date", table_name="org_kpi_daily_entries")
        op.drop_index("ix_org_kpi_daily_entries_org_id", table_name="org_kpi_daily_entries")
        op.drop_table("org_kpi_daily_entries")

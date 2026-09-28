"""Add call-library auto-close fields to manual_payments and sales_activity_events.

Revision ID: 093
Revises: 092
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "093"
down_revision = "092"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "manual_payments",
        sa.Column("source", sa.String(length=32), nullable=True, server_default="manual"),
    )
    op.add_column(
        "manual_payments",
        sa.Column("call_library_report_id", postgresql.UUID(as_uuid=True), nullable=True),
    )
    op.add_column(
        "manual_payments",
        sa.Column("superseded_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_foreign_key(
        "fk_manual_payments_call_library_report_id",
        "manual_payments",
        "call_library_reports",
        ["call_library_report_id"],
        ["id"],
    )

    op.add_column(
        "sales_activity_events",
        sa.Column("call_library_report_id", postgresql.UUID(as_uuid=True), nullable=True),
    )
    op.create_foreign_key(
        "fk_sales_activity_events_call_library_report_id",
        "sales_activity_events",
        "call_library_reports",
        ["call_library_report_id"],
        ["id"],
    )


def downgrade() -> None:
    op.drop_constraint(
        "fk_sales_activity_events_call_library_report_id", "sales_activity_events", type_="foreignkey"
    )
    op.drop_column("sales_activity_events", "call_library_report_id")

    op.drop_constraint(
        "fk_manual_payments_call_library_report_id", "manual_payments", type_="foreignkey"
    )
    op.drop_column("manual_payments", "superseded_at")
    op.drop_column("manual_payments", "call_library_report_id")
    op.drop_column("manual_payments", "source")

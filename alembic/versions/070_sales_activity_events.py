"""Add sales_activity_events — append-only per-rep sales activity log.

Revision ID: 070
Revises: 069
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import UUID

revision = "070"
down_revision = "069"
branch_labels = None
depends_on = None

TABLE = "sales_activity_events"


def upgrade() -> None:
    conn = op.get_bind()
    insp = sa.inspect(conn)
    if TABLE in insp.get_table_names():
        return
    op.create_table(
        TABLE,
        sa.Column("id", UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column(
            "org_id",
            UUID(as_uuid=True),
            sa.ForeignKey("organizations.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("entry_date", sa.Date(), nullable=False),
        sa.Column(
            "rep_user_id",
            UUID(as_uuid=True),
            sa.ForeignKey("users.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("rep_role", sa.String(16), nullable=False),
        sa.Column(
            "client_id",
            UUID(as_uuid=True),
            sa.ForeignKey("clients.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("cash_collected_cents", sa.Integer(), nullable=True),
        sa.Column("is_closed", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("source", sa.String(32), nullable=False, server_default="close_survey"),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
    )
    op.create_index("ix_sales_activity_events_org_id", TABLE, ["org_id"])
    op.create_index("ix_sales_activity_events_entry_date", TABLE, ["entry_date"])
    op.create_index("ix_sales_activity_events_rep_user_id", TABLE, ["rep_user_id"])
    # Fast path for the by-rep monthly rollup query (Phase B).
    op.create_index(
        "ix_sales_activity_events_org_rep_date",
        TABLE,
        ["org_id", "rep_user_id", "entry_date"],
    )


def downgrade() -> None:
    conn = op.get_bind()
    insp = sa.inspect(conn)
    if TABLE not in insp.get_table_names():
        return
    op.drop_table(TABLE)

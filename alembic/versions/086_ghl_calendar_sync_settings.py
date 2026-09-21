"""Add ghl_calendar_sync_settings table.

Revision ID: 086
Revises: 085
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "086"
down_revision = "085"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    insp = sa.inspect(conn)
    if "ghl_calendar_sync_settings" not in insp.get_table_names():
        op.create_table(
            "ghl_calendar_sync_settings",
            sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
            sa.Column(
                "org_id",
                postgresql.UUID(as_uuid=True),
                sa.ForeignKey("organizations.id", ondelete="CASCADE"),
                nullable=False,
                index=True,
            ),
            sa.Column("calendar_id", sa.String(255), nullable=False),
            sa.Column("calendar_name", sa.String(255), nullable=True),
            sa.Column("enabled", sa.Boolean(), nullable=False, server_default=sa.true()),
            sa.Column("is_sales_call", sa.Boolean(), nullable=False, server_default=sa.false()),
            sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
            sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
            sa.UniqueConstraint("org_id", "calendar_id", name="uq_ghl_calendar_sync_settings_org_calendar"),
        )
        # org_id's Column(index=True) above already creates ix_ghl_calendar_sync_settings_org_id.


def downgrade() -> None:
    op.drop_table("ghl_calendar_sync_settings")

"""Add funnel_simulator_scenarios for consulting portal named models.

Revision ID: 068
Revises: 067
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB, UUID

revision = "068"
down_revision = "067"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    insp = sa.inspect(conn)
    if "funnel_simulator_scenarios" in insp.get_table_names():
        return
    op.create_table(
        "funnel_simulator_scenarios",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column(
            "org_id",
            UUID(as_uuid=True),
            sa.ForeignKey("organizations.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("name", sa.String(120), nullable=False),
        sa.Column("mode", sa.String(32), nullable=False, server_default="paid_vsl"),
        sa.Column(
            "funnel_id",
            UUID(as_uuid=True),
            sa.ForeignKey("funnels.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("lookback_days", sa.String(16), nullable=False, server_default="90"),
        sa.Column("inputs", JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("created_by", UUID(as_uuid=True), sa.ForeignKey("users.id"), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
    )
    op.create_index(
        "ix_funnel_simulator_scenarios_org_id",
        "funnel_simulator_scenarios",
        ["org_id"],
    )


def downgrade() -> None:
    conn = op.get_bind()
    insp = sa.inspect(conn)
    if "funnel_simulator_scenarios" not in insp.get_table_names():
        return
    op.drop_index("ix_funnel_simulator_scenarios_org_id", table_name="funnel_simulator_scenarios")
    op.drop_table("funnel_simulator_scenarios")

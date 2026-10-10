"""Add funnel_scorecard_overrides: hand-edited weekly counts in the Funnels scorecard grid.

New table only; nothing existing is touched.

Revision ID: 101
Revises: 100
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "101"
down_revision = "100"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "funnel_scorecard_overrides",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("org_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("organizations.id"), nullable=False),
        sa.Column(
            "funnel_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("funnels.id", ondelete="CASCADE"),
            nullable=True,
        ),
        sa.Column("channel", sa.String(16), nullable=False, server_default="all"),
        sa.Column("week_start", sa.Date(), nullable=False),
        sa.Column("metric_key", sa.String(32), nullable=False),
        sa.Column("value", sa.Numeric(14, 2), nullable=False),
        sa.Column(
            "updated_by_user_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("users.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    )
    op.create_index("ix_funnel_scorecard_overrides_org_id", "funnel_scorecard_overrides", ["org_id"])
    # One value per (org, view, week, metric). NULL funnel_id (= "All funnels") is
    # coalesced so it can't be duplicated (Postgres treats NULLs as distinct).
    op.create_index(
        "uq_funnel_scorecard_overrides_scope",
        "funnel_scorecard_overrides",
        [
            "org_id",
            sa.text("COALESCE(funnel_id, '00000000-0000-0000-0000-000000000000'::uuid)"),
            "channel",
            "week_start",
            "metric_key",
        ],
        unique=True,
    )


def downgrade() -> None:
    op.drop_index("uq_funnel_scorecard_overrides_scope", table_name="funnel_scorecard_overrides")
    op.drop_index("ix_funnel_scorecard_overrides_org_id", table_name="funnel_scorecard_overrides")
    op.drop_table("funnel_scorecard_overrides")

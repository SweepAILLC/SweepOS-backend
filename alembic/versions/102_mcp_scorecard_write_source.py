"""Add source attribution for scorecard writes + org opt-in for MCP agent writes.

Additive only: `source` on funnel_ad_spend / funnel_scorecard_overrides
distinguishes a human UI edit from an automated MCP-agent write (both default
to the existing human behavior); `organizations.mcp_scorecard_write_enabled`
gates whether an org accepts agent writes at all, off by default.

Revision ID: 102
Revises: 101
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "102"
down_revision = "101"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "funnel_ad_spend",
        sa.Column("source", sa.String(16), nullable=False, server_default="human_ui"),
    )
    op.add_column(
        "funnel_scorecard_overrides",
        sa.Column("source", sa.String(16), nullable=False, server_default="human_ui"),
    )
    op.add_column(
        "organizations",
        sa.Column(
            "mcp_scorecard_write_enabled",
            sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
        ),
    )


def downgrade() -> None:
    op.drop_column("organizations", "mcp_scorecard_write_enabled")
    op.drop_column("funnel_scorecard_overrides", "source")
    op.drop_column("funnel_ad_spend", "source")

"""Per-org business economics for the Scale Planner (break-even ROAS, readiness).

Revision ID: 118
Revises: 117
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "118"
down_revision = "117"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "org_business_economics",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "org_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("organizations.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("fixed_monthly_costs_cents", sa.Integer(), nullable=True),
        sa.Column("variable_cost_pct", sa.Float(), nullable=True),
        sa.Column(
            "include_living_in_breakeven", sa.Boolean(), nullable=False, server_default=sa.text("false")
        ),
        sa.Column("cash_reserves_enc", sa.Text(), nullable=True),
        sa.Column("tax_reserve_enc", sa.Text(), nullable=True),
        sa.Column("living_expenses_enc", sa.Text(), nullable=True),
        sa.Column("risk_tolerance", sa.String(16), nullable=False, server_default="balanced"),
        sa.Column("target_cost_per_call_cents", sa.Integer(), nullable=True),
        sa.Column("roas_decay", sa.Float(), nullable=True),
        sa.Column("monthly_recurring_cash_cents", sa.Integer(), nullable=True),
        sa.Column(
            "updated_by_user_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("users.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    )
    op.create_index(
        "ix_org_business_economics_org_id", "org_business_economics", ["org_id"], unique=True
    )


def downgrade() -> None:
    op.drop_index("ix_org_business_economics_org_id", table_name="org_business_economics")
    op.drop_table("org_business_economics")

"""GHL-backed funnels: funnels.source/ghl_config, clients.opted_in_at, lookup indexes.

Additive and nullable only (same posture as 091): no backfill, no server default
that rewrites a table. `funnels.source` NULL reads as "sweep" in application code.

Revision ID: 097
Revises: 096
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "097"
down_revision = "096"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("funnels", sa.Column("source", sa.String(16), nullable=True))
    op.add_column("funnels", sa.Column("ghl_config", postgresql.JSONB(), nullable=True))
    # One GHL funnel pairs with at most one Sweep funnel per org, so no lead is counted twice.
    op.create_index(
        "uq_funnels_org_ghl_funnel_id",
        "funnels",
        ["org_id", sa.text("(ghl_config->>'ghl_funnel_id')")],
        unique=True,
        postgresql_where=sa.text("source = 'ghl' AND ghl_config->>'ghl_funnel_id' IS NOT NULL"),
    )

    # Real opt-in time for leads tagged after the row was created (GHL sync, re-attribution).
    op.add_column("clients", sa.Column("opted_in_at", sa.DateTime(timezone=True), nullable=True))
    # clients.meta is json (not jsonb); ->> works on both. Partial: only GHL-linked rows.
    op.create_index(
        "ix_clients_org_ghl_contact_id",
        "clients",
        ["org_id", sa.text("(meta->>'ghl_contact_id')")],
        postgresql_where=sa.text("meta->>'ghl_contact_id' IS NOT NULL"),
    )


def downgrade() -> None:
    op.drop_index("ix_clients_org_ghl_contact_id", table_name="clients")
    op.drop_column("clients", "opted_in_at")
    op.drop_index("uq_funnels_org_ghl_funnel_id", table_name="funnels")
    op.drop_column("funnels", "ghl_config")
    op.drop_column("funnels", "source")

"""GHL-backed funnels: funnels.source/ghl_config, clients.opted_in_at, lookup indexes.

Additive and nullable only (same posture as 091): no backfill, no server default
that rewrites a table. `funnels.source` NULL reads as "sweep" in application code.

Idempotent: app startup (main._ensure_schema_columns_on_startup) adds the three
columns if they are missing, so production can run new code before this migration;
running it afterwards still succeeds and adds the indexes.

Revision ID: 097
Revises: 096
"""
from __future__ import annotations

from alembic import op

revision = "097"
down_revision = "096"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE funnels ADD COLUMN IF NOT EXISTS source VARCHAR(16)")
    op.execute("ALTER TABLE funnels ADD COLUMN IF NOT EXISTS ghl_config JSONB")
    # One GHL funnel pairs with at most one Sweep funnel per org, so no lead is counted twice.
    op.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_funnels_org_ghl_funnel_id "
        "ON funnels (org_id, (ghl_config->>'ghl_funnel_id')) "
        "WHERE source = 'ghl' AND ghl_config->>'ghl_funnel_id' IS NOT NULL"
    )

    # Real opt-in time for leads tagged after the row was created (GHL sync, re-attribution).
    op.execute("ALTER TABLE clients ADD COLUMN IF NOT EXISTS opted_in_at TIMESTAMP WITH TIME ZONE")
    # clients.meta is json (not jsonb); ->> works on both. Partial: only GHL-linked rows.
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_clients_org_ghl_contact_id "
        "ON clients (org_id, (meta->>'ghl_contact_id')) "
        "WHERE meta->>'ghl_contact_id' IS NOT NULL"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_clients_org_ghl_contact_id")
    op.execute("ALTER TABLE clients DROP COLUMN IF EXISTS opted_in_at")
    op.execute("DROP INDEX IF EXISTS uq_funnels_org_ghl_funnel_id")
    op.execute("ALTER TABLE funnels DROP COLUMN IF EXISTS ghl_config")
    op.execute("ALTER TABLE funnels DROP COLUMN IF EXISTS source")

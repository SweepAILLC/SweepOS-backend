"""Custom funnel webhooks: per-funnel secret URL + inbox listing index.

`funnels.webhook_token_hash` is the sha256 of the secret in the webhook URL; the
public endpoint looks funnels up by it. `funnels.webhook_config` holds the
Fernet-encrypted token (so admins can copy the URL again), its display prefix,
and the optional field map. Both nullable, no backfill.

The inbound_webhook_events indexes serve the per-org delivery log and the
retention prune of done funnel-webhook rows. Built CONCURRENTLY: the inbox is
written by every inbound webhook and must not be locked during deploy.

Revision ID: 099
Revises: 098
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "099"
down_revision = "098"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("funnels", sa.Column("webhook_token_hash", sa.String(64), nullable=True))
    op.add_column("funnels", sa.Column("webhook_config", postgresql.JSONB(), nullable=True))
    op.create_index(
        "uq_funnels_webhook_token_hash",
        "funnels",
        ["webhook_token_hash"],
        unique=True,
        postgresql_where=sa.text("webhook_token_hash IS NOT NULL"),
    )
    with op.get_context().autocommit_block():
        op.execute(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_inbound_webhook_events_org_provider_received "
            "ON inbound_webhook_events (org_id, provider, received_at DESC)"
        )
        op.execute(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_inbound_webhook_events_funnel_done_received "
            "ON inbound_webhook_events (received_at) "
            "WHERE provider = 'funnel_webhook' AND status = 'done'"
        )


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS ix_inbound_webhook_events_funnel_done_received")
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS ix_inbound_webhook_events_org_provider_received")
    op.drop_index("uq_funnels_webhook_token_hash", table_name="funnels")
    op.drop_column("funnels", "webhook_config")
    op.drop_column("funnels", "webhook_token_hash")

"""Durable inbound webhook inbox + idempotent side-effect claims.

Revision ID: 087
Revises: 086
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB, UUID

revision = "087"
down_revision = "086"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    insp = sa.inspect(conn)
    tables = set(insp.get_table_names())

    if "inbound_webhook_events" not in tables:
        op.create_table(
            "inbound_webhook_events",
            sa.Column("id", UUID(as_uuid=True), primary_key=True, nullable=False),
            sa.Column("org_id", UUID(as_uuid=True), nullable=False),
            sa.Column("provider", sa.String(length=32), nullable=False),
            sa.Column("event_id", sa.String(length=255), nullable=False),
            sa.Column("event_type", sa.String(length=128), nullable=True),
            sa.Column("payload", JSONB(), nullable=False),
            sa.Column("status", sa.String(length=16), nullable=False, server_default="pending"),
            sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
            sa.Column("next_attempt_at", sa.DateTime(), nullable=True),
            sa.Column("error_text", sa.Text(), nullable=True),
            sa.Column("received_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")),
            sa.Column("processed_at", sa.DateTime(), nullable=True),
            sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")),
            sa.UniqueConstraint(
                "org_id",
                "provider",
                "event_id",
                name="uq_inbound_webhook_org_provider_event",
            ),
        )
        op.create_index("ix_inbound_webhook_events_org_id", "inbound_webhook_events", ["org_id"])
        op.create_index("ix_inbound_webhook_events_status", "inbound_webhook_events", ["status"])
        op.create_index(
            "ix_inbound_webhook_events_due",
            "inbound_webhook_events",
            ["status", "next_attempt_at"],
        )

    if "integration_event_dispatches" not in tables:
        op.create_table(
            "integration_event_dispatches",
            sa.Column("id", UUID(as_uuid=True), primary_key=True, nullable=False),
            sa.Column("org_id", UUID(as_uuid=True), nullable=False),
            sa.Column("source", sa.String(length=32), nullable=False),
            sa.Column("source_id", sa.String(length=255), nullable=False),
            sa.Column("action", sa.String(length=64), nullable=False),
            sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")),
            sa.UniqueConstraint(
                "org_id",
                "source",
                "source_id",
                "action",
                name="uq_integration_event_dispatch",
            ),
        )
        op.create_index(
            "ix_integration_event_dispatches_org_id",
            "integration_event_dispatches",
            ["org_id"],
        )


def downgrade() -> None:
    conn = op.get_bind()
    insp = sa.inspect(conn)
    tables = set(insp.get_table_names())
    if "integration_event_dispatches" in tables:
        op.drop_index("ix_integration_event_dispatches_org_id", table_name="integration_event_dispatches")
        op.drop_table("integration_event_dispatches")
    if "inbound_webhook_events" in tables:
        op.drop_index("ix_inbound_webhook_events_due", table_name="inbound_webhook_events")
        op.drop_index("ix_inbound_webhook_events_status", table_name="inbound_webhook_events")
        op.drop_index("ix_inbound_webhook_events_org_id", table_name="inbound_webhook_events")
        op.drop_table("inbound_webhook_events")

"""Add org notification_settings and funnel_lead_notifications queue.

Revision ID: 066
Revises: 065
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB, UUID

revision = "066"
down_revision = "065"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    insp = sa.inspect(conn)

    if "organizations" in insp.get_table_names():
        cols = {c["name"] for c in insp.get_columns("organizations")}
        if "notification_settings" not in cols:
            op.add_column(
                "organizations",
                sa.Column("notification_settings", JSONB(), nullable=True),
            )

    tables = set(insp.get_table_names())
    if "funnel_lead_notifications" not in tables:
        op.create_table(
            "funnel_lead_notifications",
            sa.Column("id", UUID(as_uuid=True), primary_key=True, nullable=False),
            sa.Column(
                "org_id",
                UUID(as_uuid=True),
                sa.ForeignKey("organizations.id"),
                nullable=False,
            ),
            sa.Column(
                "client_id",
                UUID(as_uuid=True),
                sa.ForeignKey("clients.id", ondelete="SET NULL"),
                nullable=True,
            ),
            sa.Column("funnel_id", UUID(as_uuid=True), nullable=True),
            sa.Column("funnel_name", sa.String(255), nullable=True),
            sa.Column("lead_name", sa.String(512), nullable=True),
            sa.Column("lead_email", sa.String(512), nullable=True),
            sa.Column("lead_phone", sa.String(64), nullable=True),
            sa.Column("lead_instagram", sa.String(255), nullable=True),
            sa.Column("source", sa.String(128), nullable=True),
            sa.Column("funnel_step_reached", sa.String(255), nullable=True),
            sa.Column("is_new_client", sa.Boolean(), nullable=False, server_default=sa.text("true")),
            sa.Column("sent_at", sa.DateTime(), nullable=True),
            sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
            sa.Column("next_attempt_at", sa.DateTime(), nullable=True),
            sa.Column("error_text", sa.Text(), nullable=True),
            sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")),
            sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")),
        )
        op.create_index(
            "ix_funnel_lead_notifications_org_id",
            "funnel_lead_notifications",
            ["org_id"],
        )
        op.create_index(
            "ix_funnel_lead_notifications_created_at",
            "funnel_lead_notifications",
            ["created_at"],
        )
        op.create_index(
            "ix_funnel_lead_notifications_unsent_org_created",
            "funnel_lead_notifications",
            ["org_id", "created_at"],
            postgresql_where=sa.text("sent_at IS NULL"),
        )


def downgrade() -> None:
    conn = op.get_bind()
    insp = sa.inspect(conn)
    tables = set(insp.get_table_names())

    if "funnel_lead_notifications" in tables:
        idx = {i["name"] for i in insp.get_indexes("funnel_lead_notifications")}
        for name in (
            "ix_funnel_lead_notifications_unsent_org_created",
            "ix_funnel_lead_notifications_created_at",
            "ix_funnel_lead_notifications_org_id",
        ):
            if name in idx:
                op.drop_index(name, table_name="funnel_lead_notifications")
        op.drop_table("funnel_lead_notifications")

    if "organizations" in tables:
        cols = {c["name"] for c in insp.get_columns("organizations")}
        if "notification_settings" in cols:
            op.drop_column("organizations", "notification_settings")

"""Add discord to oauthprovider enum + discord_channel_mappings table.

Revision ID: 083
Revises: 082
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "083"
down_revision = "082"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Idempotent enum add (PostgreSQL <15 has no IF NOT EXISTS on ADD VALUE).
    op.execute(
        """
        DO $$ BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_enum e
                JOIN pg_type t ON e.enumtypid = t.oid
                WHERE t.typname = 'oauthprovider' AND e.enumlabel = 'discord'
            ) THEN
                ALTER TYPE oauthprovider ADD VALUE 'discord';
            END IF;
        END $$;
        """
    )

    conn = op.get_bind()
    insp = sa.inspect(conn)
    if "discord_channel_mappings" not in insp.get_table_names():
        op.create_table(
            "discord_channel_mappings",
            sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
            sa.Column(
                "org_id",
                postgresql.UUID(as_uuid=True),
                sa.ForeignKey("organizations.id", ondelete="CASCADE"),
                nullable=False,
                index=True,
            ),
            sa.Column("event_type", sa.String(64), nullable=False),
            sa.Column("channel_id", sa.String(64), nullable=False),
            sa.Column("channel_name", sa.String(255), nullable=True),
            sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
            sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
            sa.UniqueConstraint("org_id", "event_type", name="uq_discord_channel_mappings_org_event"),
        )


def downgrade() -> None:
    op.drop_table("discord_channel_mappings")
    # PostgreSQL cannot easily remove enum values; leave label in place.

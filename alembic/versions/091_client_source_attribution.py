"""Add source_channel/source_funnel_id to clients.

Revision ID: 091
Revises: 090
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "091"
down_revision = "090"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("clients", sa.Column("source_channel", sa.String(), nullable=True))
    op.add_column(
        "clients",
        sa.Column("source_funnel_id", postgresql.UUID(as_uuid=True), nullable=True),
    )
    op.create_foreign_key(
        "fk_clients_source_funnel_id_funnels",
        "clients",
        "funnels",
        ["source_funnel_id"],
        ["id"],
    )


def downgrade() -> None:
    op.drop_constraint("fk_clients_source_funnel_id_funnels", "clients", type_="foreignkey")
    op.drop_column("clients", "source_funnel_id")
    op.drop_column("clients", "source_channel")

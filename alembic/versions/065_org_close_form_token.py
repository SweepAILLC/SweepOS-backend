"""Add close_form_token to organizations for post-sales survey links.

Revision ID: 065
Revises: 064
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import UUID

revision = "065"
down_revision = "064"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    insp = sa.inspect(conn)
    if "organizations" not in insp.get_table_names():
        return
    cols = {c["name"] for c in insp.get_columns("organizations")}
    if "close_form_token" not in cols:
        op.add_column(
            "organizations",
            sa.Column("close_form_token", UUID(as_uuid=True), nullable=True),
        )
    idx = {i["name"] for i in insp.get_indexes("organizations")}
    if "ix_organizations_close_form_token" not in idx:
        op.create_index(
            "ix_organizations_close_form_token",
            "organizations",
            ["close_form_token"],
            unique=True,
        )


def downgrade() -> None:
    conn = op.get_bind()
    insp = sa.inspect(conn)
    if "organizations" not in insp.get_table_names():
        return
    idx = {i["name"] for i in insp.get_indexes("organizations")}
    if "ix_organizations_close_form_token" in idx:
        op.drop_index("ix_organizations_close_form_token", table_name="organizations")
    cols = {c["name"] for c in insp.get_columns("organizations")}
    if "close_form_token" in cols:
        op.drop_column("organizations", "close_form_token")

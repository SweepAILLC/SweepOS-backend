"""Widen instagram_media.thumbnail_url so CDN URLs are not truncated.

Revision ID: 067
Revises: 066
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "067"
down_revision = "066"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    insp = sa.inspect(conn)
    if "instagram_media" not in insp.get_table_names():
        return
    cols = {c["name"]: c for c in insp.get_columns("instagram_media")}
    if "thumbnail_url" not in cols:
        return
    op.alter_column(
        "instagram_media",
        "thumbnail_url",
        existing_type=sa.String(length=1024),
        type_=sa.Text(),
        existing_nullable=True,
    )


def downgrade() -> None:
    conn = op.get_bind()
    insp = sa.inspect(conn)
    if "instagram_media" not in insp.get_table_names():
        return
    cols = {c["name"] for c in insp.get_columns("instagram_media")}
    if "thumbnail_url" not in cols:
        return
    op.alter_column(
        "instagram_media",
        "thumbnail_url",
        existing_type=sa.Text(),
        type_=sa.String(length=1024),
        existing_nullable=True,
    )

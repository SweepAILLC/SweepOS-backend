"""Add host_user_id / host_email to client_check_ins for calendar-host rep attribution.

Revision ID: 071
Revises: 070
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import UUID

revision = "071"
down_revision = "070"
branch_labels = None
depends_on = None

TABLE = "client_check_ins"
NEW_COLUMNS = (
    (
        "host_user_id",
        sa.Column(
            "host_user_id",
            UUID(as_uuid=True),
            sa.ForeignKey("users.id", ondelete="SET NULL"),
            nullable=True,
        ),
    ),
    ("host_email", sa.Column("host_email", sa.String(), nullable=True)),
)


def upgrade() -> None:
    conn = op.get_bind()
    insp = sa.inspect(conn)
    if TABLE not in insp.get_table_names():
        return
    existing = {c["name"] for c in insp.get_columns(TABLE)}
    for name, col in NEW_COLUMNS:
        if name not in existing:
            op.add_column(TABLE, col)


def downgrade() -> None:
    conn = op.get_bind()
    insp = sa.inspect(conn)
    if TABLE not in insp.get_table_names():
        return
    existing = {c["name"] for c in insp.get_columns(TABLE)}
    for name, _ in reversed(NEW_COLUMNS):
        if name in existing:
            op.drop_column(TABLE, name)

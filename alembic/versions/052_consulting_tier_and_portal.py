"""Add consulting_tier / booking_url on organizations and portal_todos table.

Revision ID: 052
Revises: 051
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID

revision = "052"
down_revision = "051"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    insp = sa.inspect(conn)
    tables = insp.get_table_names()

    if "organizations" in tables:
        cols = {c["name"] for c in insp.get_columns("organizations")}
        if "consulting_tier" not in cols:
            op.add_column("organizations", sa.Column("consulting_tier", sa.String(), nullable=True))
        if "booking_url" not in cols:
            op.add_column("organizations", sa.Column("booking_url", sa.Text(), nullable=True))

    if "portal_todos" not in tables:
        op.create_table(
            "portal_todos",
            sa.Column("id", UUID(as_uuid=True), primary_key=True, nullable=False),
            sa.Column("org_id", UUID(as_uuid=True), sa.ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False),
            sa.Column("title", sa.String(length=500), nullable=False),
            sa.Column("description", sa.Text(), nullable=True),
            sa.Column("completed", sa.Boolean(), nullable=False, server_default=sa.text("false")),
            sa.Column("due_date", sa.Date(), nullable=True),
            sa.Column("created_by", UUID(as_uuid=True), sa.ForeignKey("users.id"), nullable=True),
            sa.Column("created_at", sa.DateTime(), nullable=False),
            sa.Column("updated_at", sa.DateTime(), nullable=False),
        )
        op.create_index("ix_portal_todos_org_id", "portal_todos", ["org_id"])


def downgrade() -> None:
    conn = op.get_bind()
    insp = sa.inspect(conn)
    tables = insp.get_table_names()

    if "portal_todos" in tables:
        op.drop_index("ix_portal_todos_org_id", table_name="portal_todos")
        op.drop_table("portal_todos")

    if "organizations" in tables:
        cols = {c["name"] for c in insp.get_columns("organizations")}
        if "booking_url" in cols:
            op.drop_column("organizations", "booking_url")
        if "consulting_tier" in cols:
            op.drop_column("organizations", "consulting_tier")

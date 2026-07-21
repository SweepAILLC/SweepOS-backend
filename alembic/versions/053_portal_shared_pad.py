"""Add portal_shared_pads for live consulting notepad.

Revision ID: 053
Revises: 052
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID

revision = "053"
down_revision = "052"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    insp = sa.inspect(conn)
    tables = insp.get_table_names()

    if "portal_shared_pads" not in tables:
        op.create_table(
            "portal_shared_pads",
            sa.Column("id", UUID(as_uuid=True), primary_key=True, nullable=False),
            sa.Column(
                "org_id",
                UUID(as_uuid=True),
                sa.ForeignKey("organizations.id", ondelete="CASCADE"),
                nullable=False,
            ),
            sa.Column("content", sa.Text(), nullable=False, server_default=""),
            sa.Column("revision", sa.Integer(), nullable=False, server_default="1"),
            sa.Column("updated_by", UUID(as_uuid=True), sa.ForeignKey("users.id"), nullable=True),
            sa.Column("updated_by_name", sa.String(length=255), nullable=True),
            sa.Column("created_at", sa.DateTime(), nullable=False),
            sa.Column("updated_at", sa.DateTime(), nullable=False),
            sa.UniqueConstraint("org_id", name="uq_portal_shared_pads_org_id"),
        )
        op.create_index("ix_portal_shared_pads_org_id", "portal_shared_pads", ["org_id"])


def downgrade() -> None:
    conn = op.get_bind()
    insp = sa.inspect(conn)
    if "portal_shared_pads" in insp.get_table_names():
        op.drop_index("ix_portal_shared_pads_org_id", table_name="portal_shared_pads")
        op.drop_table("portal_shared_pads")

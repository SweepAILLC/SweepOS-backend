"""add organization_invitations

Revision ID: 020
Revises: 019
Create Date: 2025-02-06 12:00:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "020"
down_revision = "019"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "organization_invitations",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("org_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False),
        sa.Column("invitee_email", sa.String(255), nullable=False),
        sa.Column("invitation_type", sa.String(50), nullable=False, server_default="USER"),
        sa.Column("role", sa.String(50), nullable=False, server_default="member"),
        sa.Column("token", sa.String(255), nullable=False),
        sa.Column("expires_at", sa.DateTime(), nullable=False),
        sa.Column("used_at", sa.DateTime(), nullable=True),
        sa.Column("created_by", postgresql.UUID(as_uuid=True), sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
    )
    op.create_index("ix_organization_invitations_token", "organization_invitations", ["token"], unique=True)
    op.create_index("ix_organization_invitations_invitee_email", "organization_invitations", ["invitee_email"])
    op.create_index("ix_organization_invitations_org_id", "organization_invitations", ["org_id"])
    op.create_index("ix_organization_invitations_invitation_type", "organization_invitations", ["invitation_type"])


def downgrade():
    op.drop_index("ix_organization_invitations_invitation_type", table_name="organization_invitations")
    op.drop_index("ix_organization_invitations_org_id", table_name="organization_invitations")
    op.drop_index("ix_organization_invitations_invitee_email", table_name="organization_invitations")
    op.drop_index("ix_organization_invitations_token", table_name="organization_invitations")
    op.drop_table("organization_invitations")

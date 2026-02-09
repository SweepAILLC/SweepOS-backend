"""add email_templates table

Revision ID: 023
Revises: 022
Create Date: 2026-02-06

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID

revision = "023"
down_revision = "022"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "email_templates",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("org_id", UUID(as_uuid=True), nullable=False, index=True),
        sa.Column("template_key", sa.String(64), nullable=False),
        sa.Column("subject", sa.String(512), nullable=False, server_default=""),
        sa.Column("html_content", sa.Text(), nullable=False, server_default=""),
        sa.Column("text_content", sa.Text(), nullable=False, server_default=""),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint("org_id", "template_key", name="uq_email_templates_org_key"),
    )


def downgrade():
    op.drop_table("email_templates")

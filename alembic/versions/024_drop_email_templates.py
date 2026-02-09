"""drop email_templates table

Revision ID: 024
Revises: 023
Create Date: 2026-02-06

"""
from alembic import op

revision = "024"
down_revision = "023"
branch_labels = None
depends_on = None


def upgrade():
    op.drop_table("email_templates")


def downgrade():
    op.create_table(
        "email_templates",
        op.Column("id", op.dialects.postgresql.UUID(as_uuid=True), primary_key=True),
        op.Column("org_id", op.dialects.postgresql.UUID(as_uuid=True), nullable=False, index=True),
        op.Column("template_key", op.String(64), nullable=False),
        op.Column("subject", op.String(512), nullable=False, server_default=""),
        op.Column("html_content", op.Text(), nullable=False, server_default=""),
        op.Column("text_content", op.Text(), nullable=False, server_default=""),
        op.Column("created_at", op.DateTime(), nullable=False, server_default=op.func.now()),
        op.Column("updated_at", op.DateTime(), nullable=False, server_default=op.func.now()),
        op.UniqueConstraint("org_id", "template_key", name="uq_email_templates_org_key"),
    )

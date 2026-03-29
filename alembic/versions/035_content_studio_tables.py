"""Content Studio: playbook items, latest generation per org, transcript analyses

Revision ID: 035
Revises: 034
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "035"
down_revision = "034"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "content_studio_knowledge_items",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("org_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("kind", sa.String(length=32), nullable=False),
        sa.Column("body", sa.Text(), nullable=False),
        sa.Column("sort_order", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["org_id"], ["organizations.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_content_studio_knowledge_items_org_id", "content_studio_knowledge_items", ["org_id"])
    op.create_index("ix_cs_knowledge_org_kind", "content_studio_knowledge_items", ["org_id", "kind", "sort_order"])

    op.create_table(
        "content_studio_generations",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("org_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("batch_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("ideas_json", postgresql.JSON(astext_type=sa.Text()), nullable=False),
        sa.Column("created_by_user_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["created_by_user_id"], ["users.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["org_id"], ["organizations.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("org_id", name="uq_content_studio_generation_org"),
    )

    op.create_table(
        "content_studio_transcript_analyses",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("org_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("user_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("purpose", sa.String(length=16), nullable=False),
        sa.Column("mixed_note", sa.Text(), nullable=True),
        sa.Column("transcript_text", sa.Text(), nullable=False),
        sa.Column("analysis_json", postgresql.JSON(astext_type=sa.Text()), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["org_id"], ["organizations.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_content_studio_transcript_analyses_org_id", "content_studio_transcript_analyses", ["org_id"])
    op.create_index("ix_content_studio_transcript_analyses_user_id", "content_studio_transcript_analyses", ["user_id"])
    op.create_index("ix_cs_transcript_org_created", "content_studio_transcript_analyses", ["org_id", "created_at"])


def downgrade():
    op.drop_index("ix_cs_transcript_org_created", table_name="content_studio_transcript_analyses")
    op.drop_index("ix_content_studio_transcript_analyses_user_id", table_name="content_studio_transcript_analyses")
    op.drop_index("ix_content_studio_transcript_analyses_org_id", table_name="content_studio_transcript_analyses")
    op.drop_table("content_studio_transcript_analyses")
    op.drop_table("content_studio_generations")
    op.drop_index("ix_cs_knowledge_org_kind", table_name="content_studio_knowledge_items")
    op.drop_index("ix_content_studio_knowledge_items_org_id", table_name="content_studio_knowledge_items")
    op.drop_table("content_studio_knowledge_items")

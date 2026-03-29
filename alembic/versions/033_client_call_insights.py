"""Client call insights (LLM per Fathom recording) + rollup summary for board tags

Revision ID: 033
Revises: 032
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "033"
down_revision = "032"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "client_call_insights",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("org_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("client_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("fathom_call_record_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("check_in_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("insight_json", postgresql.JSON(astext_type=sa.Text()), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False, server_default="complete"),
        sa.Column("failure_reason", sa.Text(), nullable=True),
        sa.Column("computed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("model", sa.String(length=128), nullable=True),
        sa.Column("input_hash", sa.String(length=64), nullable=True),
        sa.Column("lifecycle_at_compute", sa.String(length=64), nullable=True),
        sa.ForeignKeyConstraint(["org_id"], ["organizations.id"]),
        sa.ForeignKeyConstraint(["client_id"], ["clients.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(
            ["fathom_call_record_id"],
            ["fathom_call_records.id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(["check_in_id"], ["client_check_ins.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("fathom_call_record_id", name="uq_client_call_insights_fathom_record"),
    )
    op.create_index("ix_client_call_insights_org_id", "client_call_insights", ["org_id"], unique=False)
    op.create_index("ix_client_call_insights_client_id", "client_call_insights", ["client_id"], unique=False)
    op.create_index(
        "ix_client_call_insights_client_computed",
        "client_call_insights",
        ["client_id", "computed_at"],
        unique=False,
    )

    op.create_table(
        "client_insight_summaries",
        sa.Column("client_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("org_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("headline", sa.Text(), nullable=True),
        sa.Column(
            "tags",
            postgresql.JSON(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'[]'::json"),
        ),
        sa.Column("last_call_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_insight_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_lifecycle_state", sa.String(length=32), nullable=True),
        sa.Column("last_health_grade", sa.String(length=8), nullable=True),
        sa.Column("last_health_score", sa.Numeric(precision=6, scale=2), nullable=True),
        sa.ForeignKeyConstraint(["client_id"], ["clients.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["org_id"], ["organizations.id"]),
        sa.PrimaryKeyConstraint("client_id"),
    )
    op.create_index(
        "ix_client_insight_summaries_org_id",
        "client_insight_summaries",
        ["org_id"],
        unique=False,
    )


def downgrade():
    op.drop_index("ix_client_insight_summaries_org_id", table_name="client_insight_summaries")
    op.drop_table("client_insight_summaries")
    op.drop_index("ix_client_call_insights_client_computed", table_name="client_call_insights")
    op.drop_index("ix_client_call_insights_client_id", table_name="client_call_insights")
    op.drop_index("ix_client_call_insights_org_id", table_name="client_call_insights")
    op.drop_table("client_call_insights")

"""LLM usage events table + call_library attempt_count.

Revision ID: 049
Revises: 048
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "049"
down_revision = "048"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    insp = sa.inspect(conn)
    tables = insp.get_table_names()

    if "call_library_reports" in tables:
        cols = {c["name"] for c in insp.get_columns("call_library_reports")}
        if "attempt_count" not in cols:
            op.add_column(
                "call_library_reports",
                sa.Column(
                    "attempt_count",
                    sa.BigInteger(),
                    nullable=False,
                    server_default="0",
                ),
            )

    if "llm_usage_events" not in tables:
        op.create_table(
            "llm_usage_events",
            sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
            sa.Column(
                "org_id",
                postgresql.UUID(as_uuid=True),
                sa.ForeignKey("organizations.id", ondelete="CASCADE"),
                nullable=False,
            ),
            sa.Column("provider", sa.String(length=32), nullable=False),
            sa.Column("model", sa.String(length=128), nullable=True),
            sa.Column("feature", sa.String(length=64), nullable=False, server_default="unknown"),
            sa.Column("prompt_tokens", sa.Integer(), nullable=False, server_default="0"),
            sa.Column("completion_tokens", sa.Integer(), nullable=False, server_default="0"),
            sa.Column("total_tokens", sa.Integer(), nullable=False, server_default="0"),
            sa.Column("estimated_cost_usd", sa.Float(), nullable=True),
            sa.Column(
                "created_at",
                sa.DateTime(timezone=True),
                nullable=False,
                server_default=sa.text("now()"),
            ),
        )
        op.create_index(
            "ix_llm_usage_events_org_created",
            "llm_usage_events",
            ["org_id", "created_at"],
        )
        op.create_index(
            "ix_llm_usage_events_org_feature",
            "llm_usage_events",
            ["org_id", "feature"],
        )


def downgrade() -> None:
    conn = op.get_bind()
    insp = sa.inspect(conn)
    tables = insp.get_table_names()
    if "llm_usage_events" in tables:
        op.drop_index("ix_llm_usage_events_org_feature", table_name="llm_usage_events")
        op.drop_index("ix_llm_usage_events_org_created", table_name="llm_usage_events")
        op.drop_table("llm_usage_events")
    if "call_library_reports" in tables:
        cols = {c["name"] for c in insp.get_columns("call_library_reports")}
        if "attempt_count" in cols:
            op.drop_column("call_library_reports", "attempt_count")

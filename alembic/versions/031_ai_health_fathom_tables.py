"""AI health score: Fathom call records, health cache, outcome snapshots

Revision ID: 031
Revises: 030
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "031"
down_revision = "030"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "fathom_call_records",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("org_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("client_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("fathom_recording_id", sa.BigInteger(), nullable=False),
        sa.Column("summary_text", sa.Text(), nullable=True),
        sa.Column("transcript_snippet", sa.Text(), nullable=True),
        sa.Column("sentiment_status", sa.String(length=32), nullable=False, server_default="pending"),
        sa.Column("sentiment_score", sa.Float(), nullable=True),
        sa.Column("sentiment_label", sa.String(length=32), nullable=True),
        sa.Column("sentiment_snippet", sa.String(length=512), nullable=True),
        sa.Column("meeting_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["client_id"], ["clients.id"], ),
        sa.ForeignKeyConstraint(["org_id"], ["organizations.id"], ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_fathom_call_records_org_id", "fathom_call_records", ["org_id"], unique=False)
    op.create_index("ix_fathom_call_records_client_id", "fathom_call_records", ["client_id"], unique=False)
    op.create_index("ix_fathom_call_org_recording", "fathom_call_records", ["org_id", "fathom_recording_id"], unique=True)

    op.create_table(
        "client_health_score_cache",
        sa.Column("client_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("org_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("score", sa.Float(), nullable=False),
        sa.Column("grade", sa.String(length=4), nullable=False),
        sa.Column("source", sa.String(length=16), nullable=False, server_default="logic"),
        sa.Column("explanation", sa.Text(), nullable=True),
        sa.Column("factors_json", postgresql.JSON(astext_type=sa.Text()), nullable=True),
        sa.Column("input_hash", sa.String(length=128), nullable=False),
        sa.Column("computed_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["client_id"], ["clients.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["org_id"], ["organizations.id"], ),
        sa.PrimaryKeyConstraint("client_id"),
    )
    op.create_index("ix_client_health_score_cache_org_id", "client_health_score_cache", ["org_id"], unique=False)

    op.create_table(
        "health_outcome_snapshots",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("org_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("client_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("score", sa.Float(), nullable=False),
        sa.Column("grade", sa.String(length=4), nullable=False),
        sa.Column("lifecycle_phase", sa.String(length=32), nullable=False),
        sa.Column("feature_bucket", postgresql.JSON(astext_type=sa.Text()), nullable=True),
        sa.Column("recorded_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["client_id"], ["clients.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["org_id"], ["organizations.id"], ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_health_outcome_snapshots_org_id", "health_outcome_snapshots", ["org_id"], unique=False)
    op.create_index("ix_health_outcome_snapshots_client_id", "health_outcome_snapshots", ["client_id"], unique=False)


def downgrade():
    op.drop_index("ix_health_outcome_snapshots_client_id", table_name="health_outcome_snapshots")
    op.drop_index("ix_health_outcome_snapshots_org_id", table_name="health_outcome_snapshots")
    op.drop_table("health_outcome_snapshots")

    op.drop_index("ix_client_health_score_cache_org_id", table_name="client_health_score_cache")
    op.drop_table("client_health_score_cache")

    op.drop_index("ix_fathom_call_org_recording", table_name="fathom_call_records")
    op.drop_index("ix_fathom_call_records_client_id", table_name="fathom_call_records")
    op.drop_index("ix_fathom_call_records_org_id", table_name="fathom_call_records")
    op.drop_table("fathom_call_records")

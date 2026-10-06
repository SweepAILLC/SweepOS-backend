"""Jev decision layer: jev_shadow_results table + fathom_call_records.sentiment_confidence.

Additive only: a new table and one nullable column, no defaults, no backfill.

Revision ID: 099
Revises: 098
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "099"
down_revision = "098"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "jev_shadow_results",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "org_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("organizations.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("feature", sa.String(length=64), nullable=False),
        sa.Column("subject_type", sa.String(length=32), nullable=False),
        sa.Column("subject_id", sa.String(length=64), nullable=False),
        sa.Column("question_set_version", sa.String(length=16), nullable=False),
        sa.Column("jev_json", sa.JSON(), nullable=True),
        sa.Column("llm_json", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_jev_shadow_results_org_id", "jev_shadow_results", ["org_id"])
    op.create_index(
        "ix_jev_shadow_results_org_feature_created",
        "jev_shadow_results",
        ["org_id", "feature", "created_at"],
    )
    op.add_column("fathom_call_records", sa.Column("sentiment_confidence", sa.Float(), nullable=True))


def downgrade() -> None:
    op.drop_column("fathom_call_records", "sentiment_confidence")
    op.drop_index("ix_jev_shadow_results_org_feature_created", table_name="jev_shadow_results")
    op.drop_index("ix_jev_shadow_results_org_id", table_name="jev_shadow_results")
    op.drop_table("jev_shadow_results")

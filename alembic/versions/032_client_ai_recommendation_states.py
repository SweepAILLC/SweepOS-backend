"""Client AI recommendation action checklist (manual completion)

Revision ID: 032
Revises: 031
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "032"
down_revision = "031"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "client_ai_recommendation_states",
        sa.Column("client_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("org_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("headline", sa.Text(), nullable=True),
        sa.Column(
            "actions",
            postgresql.JSON(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'[]'::json"),
        ),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["client_id"], ["clients.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["org_id"], ["organizations.id"]),
        sa.PrimaryKeyConstraint("client_id"),
    )
    op.create_index(
        "ix_client_ai_recommendation_states_org_id",
        "client_ai_recommendation_states",
        ["org_id"],
        unique=False,
    )


def downgrade():
    op.drop_index("ix_client_ai_recommendation_states_org_id", table_name="client_ai_recommendation_states")
    op.drop_table("client_ai_recommendation_states")

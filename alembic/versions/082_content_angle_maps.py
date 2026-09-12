"""Content Angle Map per consulting org.

Revision ID: 082
Revises: 081
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "082"
down_revision = "081"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    insp = sa.inspect(conn)
    if "content_angle_maps" in set(insp.get_table_names()):
        return
    op.create_table(
        "content_angle_maps",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "org_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("organizations.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("icp_angles", postgresql.JSON(astext_type=sa.Text()), nullable=False),
        sa.Column("personal_brand_angles", postgresql.JSON(astext_type=sa.Text()), nullable=False),
        sa.Column("format_pills_tof", postgresql.JSON(astext_type=sa.Text()), nullable=False),
        sa.Column("format_pills_mof", postgresql.JSON(astext_type=sa.Text()), nullable=False),
        sa.Column("format_pills_bof", postgresql.JSON(astext_type=sa.Text()), nullable=False),
        sa.Column("last_generated_at", sa.DateTime(), nullable=True),
        sa.Column("last_generated_icp_at", sa.DateTime(), nullable=True),
        sa.Column("last_generated_brand_at", sa.DateTime(), nullable=True),
        sa.Column("calls_seen_at_generation", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("input_fingerprint", sa.String(length=64), nullable=True),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.UniqueConstraint("org_id", name="uq_content_angle_map_org"),
    )


def downgrade() -> None:
    conn = op.get_bind()
    insp = sa.inspect(conn)
    if "content_angle_maps" in set(insp.get_table_names()):
        op.drop_table("content_angle_maps")

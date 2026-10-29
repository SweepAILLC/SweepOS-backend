"""Drop meta_pages and meta_ad_leads — lead capture use case removed from
Meta Ads Performance Intel (performance measurement and create/manage ads
are unaffected). See docs/features/META_ADS_PERFORMANCE_PRD.md.

Revision ID: 105
Revises: 104
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB, UUID

revision = "105"
down_revision = "104"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_index("ix_meta_ad_leads_org_created", table_name="meta_ad_leads")
    op.drop_index("uq_meta_ad_leads_org_leadgen", table_name="meta_ad_leads")
    op.drop_table("meta_ad_leads")
    op.drop_index("ix_meta_pages_page_id", table_name="meta_pages")
    op.drop_index("uq_meta_pages_org_page", table_name="meta_pages")
    op.drop_table("meta_pages")


def downgrade() -> None:
    op.create_table(
        "meta_pages",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "org_id",
            UUID(as_uuid=True),
            sa.ForeignKey("organizations.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("page_id", sa.String(64), nullable=False),
        sa.Column("page_name", sa.String(255), nullable=True),
        sa.Column("page_access_token", sa.String(2048), nullable=False),
        sa.Column("subscribed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
    )
    op.create_index("uq_meta_pages_org_page", "meta_pages", ["org_id", "page_id"], unique=True)
    op.create_index("ix_meta_pages_page_id", "meta_pages", ["page_id"])

    op.create_table(
        "meta_ad_leads",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "org_id",
            UUID(as_uuid=True),
            sa.ForeignKey("organizations.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("leadgen_id", sa.String(64), nullable=False),
        sa.Column("page_id", sa.String(64), nullable=False),
        sa.Column("form_id", sa.String(64), nullable=True),
        sa.Column("ad_id", sa.String(64), nullable=True),
        sa.Column("lead_created_time", sa.DateTime(timezone=True), nullable=True),
        sa.Column("field_data", JSONB, nullable=True),
        sa.Column("fetched_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("fetch_error", sa.String(500), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
    )
    op.create_index("uq_meta_ad_leads_org_leadgen", "meta_ad_leads", ["org_id", "leadgen_id"], unique=True)
    op.create_index("ix_meta_ad_leads_org_created", "meta_ad_leads", ["org_id", "created_at"])

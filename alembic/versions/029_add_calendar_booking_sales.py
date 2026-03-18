"""add calendar_booking_sales and event_type_sales_calls; is_sales_call/sale_closed on client_check_ins

Revision ID: 029
Revises: 028
Create Date: 2026-02-25

Track sales vs check-in calls and whether sale closed (for close rate).
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "029"
down_revision = "028"
branch_labels = None
depends_on = None


def upgrade():
    # New tables for calendar sales tracking
    op.create_table(
        "calendar_booking_sales",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("org_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False),
        sa.Column("provider", sa.String(20), nullable=False),
        sa.Column("event_id", sa.String(255), nullable=False),
        sa.Column("event_uri", sa.String(512), nullable=True),
        sa.Column("is_sales_call", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column("sale_closed", sa.Boolean(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    )
    op.create_index("ix_calendar_booking_sales_org_id", "calendar_booking_sales", ["org_id"])
    op.create_index("ix_calendar_booking_sales_provider_event_id", "calendar_booking_sales", ["org_id", "provider", "event_id"], unique=True)

    op.create_table(
        "event_type_sales_calls",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("org_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False),
        sa.Column("provider", sa.String(20), nullable=False),
        sa.Column("event_type_id", sa.String(255), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    )
    op.create_index("ix_event_type_sales_calls_org_provider", "event_type_sales_calls", ["org_id", "provider", "event_type_id"], unique=True)

    # client_check_ins: sales call designation and close status
    op.add_column(
        "client_check_ins",
        sa.Column("is_sales_call", sa.Boolean(), nullable=False, server_default="false"),
    )
    op.add_column(
        "client_check_ins",
        sa.Column("sale_closed", sa.Boolean(), nullable=True),
    )


def downgrade():
    op.drop_column("client_check_ins", "sale_closed")
    op.drop_column("client_check_ins", "is_sales_call")
    op.drop_index("ix_event_type_sales_calls_org_provider", table_name="event_type_sales_calls")
    op.drop_table("event_type_sales_calls")
    op.drop_index("ix_calendar_booking_sales_provider_event_id", table_name="calendar_booking_sales")
    op.drop_index("ix_calendar_booking_sales_org_id", table_name="calendar_booking_sales")
    op.drop_table("calendar_booking_sales")

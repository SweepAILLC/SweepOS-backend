"""Add Whop OAuth provider enum value and whop_payments table."""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "040"
down_revision = "039"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Idempotent on PostgreSQL <15 (no IF NOT EXISTS on ADD VALUE) and when re-run.
    op.execute(
        """
        DO $$ BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_enum e
                JOIN pg_type t ON e.enumtypid = t.oid
                WHERE t.typname = 'oauthprovider' AND e.enumlabel = 'whop'
            ) THEN
                ALTER TYPE oauthprovider ADD VALUE 'whop';
            END IF;
        END $$;
        """
    )

    op.create_table(
        "whop_payments",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("org_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("organizations.id"), nullable=False, index=True),
        sa.Column("whop_id", sa.String(), nullable=False),
        sa.Column("amount_cents", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("currency", sa.String(length=3), nullable=False, server_default="usd"),
        sa.Column("status", sa.String(), nullable=False, index=True),
        sa.Column("client_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("clients.id"), nullable=True, index=True),
        sa.Column("raw", postgresql.JSON(astext_type=sa.Text()), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
    )
    op.create_index("ix_whop_payments_org_created", "whop_payments", ["org_id", "created_at"])
    op.create_unique_constraint(
        "uq_whop_payments_whop_id_org_id",
        "whop_payments",
        ["whop_id", "org_id"],
    )


def downgrade() -> None:
    op.drop_constraint("uq_whop_payments_whop_id_org_id", "whop_payments", type_="unique")
    op.drop_index("ix_whop_payments_org_created", table_name="whop_payments")
    op.drop_table("whop_payments")
    # Enum value 'whop' is left in place (PostgreSQL cannot drop enum values easily)

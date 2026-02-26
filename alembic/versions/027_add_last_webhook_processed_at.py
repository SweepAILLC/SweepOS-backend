"""add last_webhook_processed_at to oauth_tokens for terminal refetch-on-webhook

Revision ID: 027
Revises: 026
Create Date: 2026-02-06

When a Stripe webhook is processed for an org, we set this timestamp so the terminal
tab can skip refetching Stripe data on tab focus unless a webhook has fired.
"""
from alembic import op
import sqlalchemy as sa


revision = "027"
down_revision = "026"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "oauth_tokens",
        sa.Column("last_webhook_processed_at", sa.DateTime(), nullable=True),
    )


def downgrade():
    op.drop_column("oauth_tokens", "last_webhook_processed_at")

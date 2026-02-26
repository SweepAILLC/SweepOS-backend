"""add stripe webhook_secret and webhook_endpoint_id to oauth_tokens

Revision ID: 026
Revises: 025
Create Date: 2026-02-06

Stores per-org Stripe webhook endpoint details when created via API key connect.
"""
from alembic import op
import sqlalchemy as sa


revision = "026"
down_revision = "025"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("oauth_tokens", sa.Column("webhook_secret", sa.String(255), nullable=True))
    op.add_column("oauth_tokens", sa.Column("webhook_endpoint_id", sa.String(64), nullable=True))


def downgrade():
    op.drop_column("oauth_tokens", "webhook_endpoint_id")
    op.drop_column("oauth_tokens", "webhook_secret")

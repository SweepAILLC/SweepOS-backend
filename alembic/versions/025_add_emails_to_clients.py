"""add emails (JSON array) to clients for multiple emails per client

Revision ID: 025
Revises: 024
Create Date: 2026-02-06

"""
from alembic import op
import sqlalchemy as sa


revision = "025"
down_revision = "024"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("clients", sa.Column("emails", sa.JSON, nullable=True))


def downgrade():
    op.drop_column("clients", "emails")

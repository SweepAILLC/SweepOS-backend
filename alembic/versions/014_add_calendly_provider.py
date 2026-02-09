"""add calendly provider

Revision ID: 014
Revises: 013
Create Date: 2025-01-20 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '014'
down_revision = '013'
branch_labels = None
depends_on = None


def upgrade():
    # Add 'calendly' value to oauthprovider enum
    # Note: PostgreSQL requires adding enum values in a separate transaction
    op.execute("ALTER TYPE oauthprovider ADD VALUE IF NOT EXISTS 'calendly'")


def downgrade():
    # Note: PostgreSQL does not support removing enum values directly
    # This would require recreating the enum type, which is complex
    # For now, we'll leave the calendly value in place
    # If you need to remove it, you would need to:
    # 1. Create a new enum without 'calendly'
    # 2. Update all columns to use the new enum
    # 3. Drop the old enum
    # 4. Rename the new enum to the original name
    pass



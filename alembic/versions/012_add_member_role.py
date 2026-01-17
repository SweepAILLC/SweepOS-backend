"""add_member_role

Revision ID: 012
Revises: 011
Create Date: 2026-01-16 17:30:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '012'
down_revision = '011'
branch_labels = None
depends_on = None


def upgrade():
    # Add 'member' value to userrole enum
    # Note: PostgreSQL requires adding enum values in a separate transaction
    op.execute("ALTER TYPE userrole ADD VALUE IF NOT EXISTS 'member'")


def downgrade():
    # Note: PostgreSQL does not support removing enum values directly
    # This would require recreating the enum type, which is complex
    # For now, we'll leave the member value in place
    # If you need to remove it, you would need to:
    # 1. Create a new enum without 'member'
    # 2. Update all columns to use the new enum
    # 3. Drop the old enum
    # 4. Rename the new enum to the original name
    pass


"""add client notes field

Revision ID: 003
Revises: 002
Create Date: 2024-01-15 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '003'
down_revision = '002'
branch_labels = None
depends_on = None


def upgrade():
    # Add notes field to clients table
    op.add_column('clients', sa.Column('notes', sa.Text(), nullable=True))


def downgrade():
    op.drop_column('clients', 'notes')


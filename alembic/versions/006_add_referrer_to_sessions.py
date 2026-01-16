"""add referrer to sessions

Revision ID: 006
Revises: 005
Create Date: 2024-01-15 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '006'
down_revision = '005'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Add referrer column to sessions table
    op.add_column('sessions', sa.Column('referrer', sa.String(), nullable=True))
    op.create_index('ix_sessions_referrer', 'sessions', ['referrer'])


def downgrade() -> None:
    op.drop_index('ix_sessions_referrer', table_name='sessions')
    op.drop_column('sessions', 'referrer')


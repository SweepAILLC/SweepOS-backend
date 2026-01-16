"""add_program_tracking_to_clients

Revision ID: 011
Revises: 010
Create Date: 2026-01-15 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '011'
down_revision = '010'
branch_labels = None
depends_on = None


def upgrade():
    # Add program tracking fields to clients table
    op.add_column('clients', sa.Column('program_start_date', sa.DateTime(), nullable=True))
    op.add_column('clients', sa.Column('program_duration_days', sa.Integer(), nullable=True))
    op.add_column('clients', sa.Column('program_end_date', sa.DateTime(), nullable=True))
    op.add_column('clients', sa.Column('program_progress_percent', sa.Numeric(5, 2), nullable=True))
    
    # Create index on program_end_date for efficient queries
    op.create_index('ix_clients_program_end_date', 'clients', ['program_end_date'])


def downgrade():
    op.drop_index('ix_clients_program_end_date', table_name='clients')
    op.drop_column('clients', 'program_progress_percent')
    op.drop_column('clients', 'program_end_date')
    op.drop_column('clients', 'program_duration_days')
    op.drop_column('clients', 'program_start_date')


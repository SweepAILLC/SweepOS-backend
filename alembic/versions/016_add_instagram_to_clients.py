"""add instagram to clients

Revision ID: 016
Revises: 015
Create Date: 2025-02-01 12:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '016'
down_revision = '015'
branch_labels = None
depends_on = None


def upgrade():
    # Add instagram field to clients table
    op.add_column('clients', sa.Column('instagram', sa.String(), nullable=True))


def downgrade():
    op.drop_column('clients', 'instagram')



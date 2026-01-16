"""add_invoice_id_to_payments

Revision ID: 010
Revises: 009
Create Date: 2026-01-14 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '010'
down_revision = '009'
branch_labels = None
depends_on = None


def upgrade():
    # Add invoice_id column to stripe_payments table
    op.add_column('stripe_payments', sa.Column('invoice_id', sa.String(), nullable=True))
    op.create_index('ix_stripe_payments_invoice_id', 'stripe_payments', ['invoice_id'])


def downgrade():
    op.drop_index('ix_stripe_payments_invoice_id', table_name='stripe_payments')
    op.drop_column('stripe_payments', 'invoice_id')


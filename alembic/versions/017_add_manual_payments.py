"""add manual payments

Revision ID: 017
Revises: 016
Create Date: 2024-01-15 12:00:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '017'
down_revision = '016'
branch_labels = None
depends_on = None


def upgrade():
    # Create manual_payments table
    op.create_table(
        'manual_payments',
        sa.Column('id', postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column('org_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('client_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('amount_cents', sa.Integer(), nullable=False),
        sa.Column('currency', sa.String(length=3), nullable=False, server_default='usd'),
        sa.Column('payment_date', sa.DateTime(timezone=True), nullable=False),
        sa.Column('description', sa.Text(), nullable=True),
        sa.Column('payment_method', sa.String(length=100), nullable=True),
        sa.Column('receipt_url', sa.String(length=500), nullable=True),
        sa.Column('created_by', postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(['org_id'], ['organizations.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['client_id'], ['clients.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['created_by'], ['users.id'], ondelete='SET NULL'),
    )
    
    # Create indexes
    op.create_index('ix_manual_payments_id', 'manual_payments', ['id'])
    op.create_index('ix_manual_payments_org_id', 'manual_payments', ['org_id'])
    op.create_index('ix_manual_payments_client_id', 'manual_payments', ['client_id'])
    op.create_index('ix_manual_payments_payment_date', 'manual_payments', ['payment_date'])


def downgrade():
    op.drop_index('ix_manual_payments_payment_date', table_name='manual_payments')
    op.drop_index('ix_manual_payments_client_id', table_name='manual_payments')
    op.drop_index('ix_manual_payments_org_id', table_name='manual_payments')
    op.drop_index('ix_manual_payments_id', table_name='manual_payments')
    op.drop_table('manual_payments')



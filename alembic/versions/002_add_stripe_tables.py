"""Add Stripe tables and lifetime_revenue_cents

Revision ID: 002
Revises: 001
Create Date: 2024-01-15 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '002'
down_revision = '001'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Add lifetime_revenue_cents to clients table
    op.add_column('clients', sa.Column('lifetime_revenue_cents', sa.Integer(), nullable=False, server_default='0'))
    op.create_index('ix_clients_stripe_customer_id', 'clients', ['stripe_customer_id'])
    
    # Stripe payments table
    op.create_table(
        'stripe_payments',
        sa.Column('id', postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column('stripe_id', sa.String(), nullable=False, unique=True),
        sa.Column('client_id', postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column('amount_cents', sa.Integer(), nullable=False),
        sa.Column('currency', sa.String(3), nullable=False, server_default='usd'),
        sa.Column('status', sa.String(), nullable=False),
        sa.Column('type', sa.String(), nullable=True),
        sa.Column('subscription_id', sa.String(), nullable=True),
        sa.Column('receipt_url', sa.Text(), nullable=True),
        sa.Column('raw_event', postgresql.JSON(astext_type=sa.Text()), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.Column('updated_at', sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(['client_id'], ['clients.id'], ),
    )
    op.create_index('ix_stripe_payments_stripe_id', 'stripe_payments', ['stripe_id'])
    op.create_index('ix_stripe_payments_client_id', 'stripe_payments', ['client_id'])
    op.create_index('ix_stripe_payments_status', 'stripe_payments', ['status'])
    op.create_index('ix_stripe_payments_subscription_id', 'stripe_payments', ['subscription_id'])
    op.create_index('ix_stripe_payments_created_at', 'stripe_payments', ['created_at'])
    
    # Stripe subscriptions table
    op.create_table(
        'stripe_subscriptions',
        sa.Column('id', postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column('stripe_subscription_id', sa.String(), nullable=False, unique=True),
        sa.Column('client_id', postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column('status', sa.String(), nullable=False),
        sa.Column('current_period_start', sa.DateTime(), nullable=True),
        sa.Column('current_period_end', sa.DateTime(), nullable=True),
        sa.Column('plan_id', sa.String(), nullable=True),
        sa.Column('mrr', sa.Numeric(10, 2), nullable=False, server_default='0'),
        sa.Column('raw', postgresql.JSON(astext_type=sa.Text()), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.Column('updated_at', sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(['client_id'], ['clients.id'], ),
    )
    op.create_index('ix_stripe_subscriptions_stripe_subscription_id', 'stripe_subscriptions', ['stripe_subscription_id'])
    op.create_index('ix_stripe_subscriptions_client_id', 'stripe_subscriptions', ['client_id'])
    op.create_index('ix_stripe_subscriptions_status', 'stripe_subscriptions', ['status'])
    op.create_index('ix_stripe_subscriptions_current_period_end', 'stripe_subscriptions', ['current_period_end'])
    op.create_index('ix_stripe_subscriptions_updated_at', 'stripe_subscriptions', ['updated_at'])
    
    # Stripe events table
    op.create_table(
        'stripe_events',
        sa.Column('id', postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column('stripe_event_id', sa.String(), nullable=False, unique=True),
        sa.Column('type', sa.String(), nullable=False),
        sa.Column('payload', postgresql.JSON(astext_type=sa.Text()), nullable=False),
        sa.Column('processed', sa.Boolean(), nullable=False, server_default='false'),
        sa.Column('received_at', sa.DateTime(), nullable=False),
        sa.Column('processed_at', sa.DateTime(), nullable=True),
    )
    op.create_index('ix_stripe_events_stripe_event_id', 'stripe_events', ['stripe_event_id'])
    op.create_index('ix_stripe_events_type', 'stripe_events', ['type'])
    op.create_index('ix_stripe_events_processed', 'stripe_events', ['processed'])
    op.create_index('ix_stripe_events_received_at', 'stripe_events', ['received_at'])


def downgrade() -> None:
    op.drop_table('stripe_events')
    op.drop_table('stripe_subscriptions')
    op.drop_table('stripe_payments')
    op.drop_index('ix_clients_stripe_customer_id', table_name='clients')
    op.drop_column('clients', 'lifetime_revenue_cents')


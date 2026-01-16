"""add sync tracking and unique constraints

Revision ID: 009
Revises: 008
Create Date: 2025-01-14 12:00:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '009'
down_revision = '008'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Add last_sync_at to oauth_tokens
    op.add_column('oauth_tokens', sa.Column('last_sync_at', sa.DateTime(), nullable=True))
    
    # Add unique constraint on stripe_id + org_id for stripe_payments to prevent duplicates
    # First, remove any existing duplicates (keep the most recent one)
    op.execute("""
        DELETE FROM stripe_payments
        WHERE id NOT IN (
            SELECT DISTINCT ON (stripe_id, org_id) id
            FROM stripe_payments
            ORDER BY stripe_id, org_id, updated_at DESC
        )
    """)
    
    # Create unique constraint for payments
    op.create_unique_constraint(
        'uq_stripe_payments_stripe_id_org_id',
        'stripe_payments',
        ['stripe_id', 'org_id']
    )
    
    # Add unique constraint on stripe_subscription_id + org_id for stripe_subscriptions
    # First, remove any existing duplicates (keep the most recent one)
    op.execute("""
        DELETE FROM stripe_subscriptions
        WHERE id NOT IN (
            SELECT DISTINCT ON (stripe_subscription_id, org_id) id
            FROM stripe_subscriptions
            ORDER BY stripe_subscription_id, org_id, updated_at DESC
        )
    """)
    
    # Drop old unique constraint on stripe_subscription_id if it exists
    try:
        op.drop_constraint('stripe_subscriptions_stripe_subscription_id_key', 'stripe_subscriptions', type_='unique')
    except:
        pass  # Constraint might not exist
    
    # Create new unique constraint for subscriptions
    op.create_unique_constraint(
        'uq_stripe_subscriptions_stripe_subscription_id_org_id',
        'stripe_subscriptions',
        ['stripe_subscription_id', 'org_id']
    )


def downgrade() -> None:
    op.drop_constraint('uq_stripe_subscriptions_stripe_subscription_id_org_id', 'stripe_subscriptions', type_='unique')
    op.drop_constraint('uq_stripe_payments_stripe_id_org_id', 'stripe_payments', type_='unique')
    op.drop_column('oauth_tokens', 'last_sync_at')


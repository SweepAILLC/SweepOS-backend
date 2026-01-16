"""add multi-tenant structure

Revision ID: 004
Revises: 003
Create Date: 2024-01-15 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '004'
down_revision = '003'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Create UserRole enum type first (before using it)
    # PostgreSQL requires enum types to be created before they can be used in columns
    op.execute("CREATE TYPE userrole AS ENUM ('OWNER', 'ADMIN')")
    
    # Create organizations table
    op.create_table(
        'organizations',
        sa.Column('id', postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column('name', sa.String(), nullable=False),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.Column('updated_at', sa.DateTime(), nullable=False),
    )
    
    # Create default organization for existing data
    # This will be the "Sweep Internal" org
    default_org_id = '00000000-0000-0000-0000-000000000001'
    op.execute(f"""
        INSERT INTO organizations (id, name, created_at, updated_at)
        VALUES ('{default_org_id}', 'Sweep Internal', NOW(), NOW())
    """)
    
    # Add org_id to users table
    op.add_column('users', sa.Column('org_id', postgresql.UUID(as_uuid=True), nullable=True))
    op.add_column('users', sa.Column('role', postgresql.ENUM('OWNER', 'ADMIN', name='userrole', create_type=False), nullable=True))
    
    # Set all existing users to default org and OWNER role
    op.execute(f"""
        UPDATE users SET org_id = '{default_org_id}', role = 'OWNER' WHERE org_id IS NULL
    """)
    
    # Make org_id required
    op.alter_column('users', 'org_id', nullable=False)
    op.alter_column('users', 'role', nullable=False, server_default='ADMIN')
    
    # Add foreign key
    op.create_foreign_key('fk_users_org_id', 'users', 'organizations', ['org_id'], ['id'])
    op.create_index('ix_users_org_id', 'users', ['org_id'])
    
    # Remove unique constraint on email (emails can be reused across orgs)
    op.drop_index('ix_users_email', table_name='users')
    op.create_index('ix_users_email', 'users', ['email'], unique=False)
    
    # Add composite unique constraint: email must be unique per org
    op.create_unique_constraint('uq_users_email_org', 'users', ['email', 'org_id'])
    
    # Add org_id to clients table (rename tenant_id to org_id conceptually, but keep both for migration)
    op.add_column('clients', sa.Column('org_id', postgresql.UUID(as_uuid=True), nullable=True))
    op.execute(f"""
        UPDATE clients SET org_id = COALESCE(tenant_id, '{default_org_id}')
    """)
    op.execute(f"""
        UPDATE clients SET org_id = '{default_org_id}' WHERE org_id IS NULL
    """)
    op.alter_column('clients', 'org_id', nullable=False)
    op.create_foreign_key('fk_clients_org_id', 'clients', 'organizations', ['org_id'], ['id'])
    op.create_index('ix_clients_org_id', 'clients', ['org_id'])
    
    # Add org_id to oauth_tokens
    op.add_column('oauth_tokens', sa.Column('org_id', postgresql.UUID(as_uuid=True), nullable=True))
    op.execute(f"""
        UPDATE oauth_tokens SET org_id = '{default_org_id}'
    """)
    op.alter_column('oauth_tokens', 'org_id', nullable=False)
    op.create_foreign_key('fk_oauth_tokens_org_id', 'oauth_tokens', 'organizations', ['org_id'], ['id'])
    op.create_index('ix_oauth_tokens_org_id', 'oauth_tokens', ['org_id'])
    
    # Remove unique constraint on provider (can have multiple tokens per provider across orgs)
    # Add composite unique: one token per provider per org
    op.create_unique_constraint('uq_oauth_tokens_provider_org', 'oauth_tokens', ['provider', 'org_id'])
    
    # Add org_id to events
    op.add_column('events', sa.Column('org_id', postgresql.UUID(as_uuid=True), nullable=True))
    op.execute(f"""
        UPDATE events SET org_id = (
            SELECT org_id FROM clients WHERE clients.id = events.client_id LIMIT 1
        )
    """)
    op.execute(f"""
        UPDATE events SET org_id = '{default_org_id}' WHERE org_id IS NULL
    """)
    op.alter_column('events', 'org_id', nullable=False)
    op.create_foreign_key('fk_events_org_id', 'events', 'organizations', ['org_id'], ['id'])
    op.create_index('ix_events_org_id', 'events', ['org_id'])
    
    # Add org_id to campaigns
    op.add_column('campaigns', sa.Column('org_id', postgresql.UUID(as_uuid=True), nullable=True))
    op.execute(f"""
        UPDATE campaigns SET org_id = COALESCE(tenant_id, '{default_org_id}')
    """)
    op.execute(f"""
        UPDATE campaigns SET org_id = '{default_org_id}' WHERE org_id IS NULL
    """)
    op.alter_column('campaigns', 'org_id', nullable=False)
    op.create_foreign_key('fk_campaigns_org_id', 'campaigns', 'organizations', ['org_id'], ['id'])
    op.create_index('ix_campaigns_org_id', 'campaigns', ['org_id'])
    
    # Add org_id to recommendations
    op.add_column('recommendations', sa.Column('org_id', postgresql.UUID(as_uuid=True), nullable=True))
    op.execute(f"""
        UPDATE recommendations SET org_id = COALESCE(tenant_id, (
            SELECT org_id FROM clients WHERE clients.id = recommendations.client_id LIMIT 1
        ), '{default_org_id}')
    """)
    op.execute(f"""
        UPDATE recommendations SET org_id = '{default_org_id}' WHERE org_id IS NULL
    """)
    op.alter_column('recommendations', 'org_id', nullable=False)
    op.create_foreign_key('fk_recommendations_org_id', 'recommendations', 'organizations', ['org_id'], ['id'])
    op.create_index('ix_recommendations_org_id', 'recommendations', ['org_id'])
    
    # Add org_id to stripe_payments
    op.add_column('stripe_payments', sa.Column('org_id', postgresql.UUID(as_uuid=True), nullable=True))
    op.execute(f"""
        UPDATE stripe_payments SET org_id = (
            SELECT org_id FROM clients WHERE clients.id = stripe_payments.client_id LIMIT 1
        )
    """)
    op.execute(f"""
        UPDATE stripe_payments SET org_id = '{default_org_id}' WHERE org_id IS NULL
    """)
    op.alter_column('stripe_payments', 'org_id', nullable=False)
    op.create_foreign_key('fk_stripe_payments_org_id', 'stripe_payments', 'organizations', ['org_id'], ['id'])
    op.create_index('ix_stripe_payments_org_id', 'stripe_payments', ['org_id'])
    
    # Remove unique constraint on stripe_id (can be reused across orgs)
    op.drop_index('ix_stripe_payments_stripe_id', table_name='stripe_payments')
    op.create_index('ix_stripe_payments_stripe_id', 'stripe_payments', ['stripe_id'], unique=False)
    
    # Add org_id to stripe_subscriptions
    op.add_column('stripe_subscriptions', sa.Column('org_id', postgresql.UUID(as_uuid=True), nullable=True))
    op.execute(f"""
        UPDATE stripe_subscriptions SET org_id = (
            SELECT org_id FROM clients WHERE clients.id = stripe_subscriptions.client_id LIMIT 1
        )
    """)
    op.execute(f"""
        UPDATE stripe_subscriptions SET org_id = '{default_org_id}' WHERE org_id IS NULL
    """)
    op.alter_column('stripe_subscriptions', 'org_id', nullable=False)
    op.create_foreign_key('fk_stripe_subscriptions_org_id', 'stripe_subscriptions', 'organizations', ['org_id'], ['id'])
    op.create_index('ix_stripe_subscriptions_org_id', 'stripe_subscriptions', ['org_id'])
    
    # Remove unique constraint on stripe_subscription_id
    op.drop_index('ix_stripe_subscriptions_stripe_subscription_id', table_name='stripe_subscriptions')
    op.create_index('ix_stripe_subscriptions_stripe_subscription_id', 'stripe_subscriptions', ['stripe_subscription_id'], unique=False)
    
    # Add org_id to stripe_events
    op.add_column('stripe_events', sa.Column('org_id', postgresql.UUID(as_uuid=True), nullable=True))
    op.execute(f"""
        UPDATE stripe_events SET org_id = '{default_org_id}'
    """)
    op.alter_column('stripe_events', 'org_id', nullable=False)
    op.create_foreign_key('fk_stripe_events_org_id', 'stripe_events', 'organizations', ['org_id'], ['id'])
    op.create_index('ix_stripe_events_org_id', 'stripe_events', ['org_id'])
    
    # Remove unique constraint on stripe_event_id
    op.drop_index('ix_stripe_events_stripe_event_id', table_name='stripe_events')
    op.create_index('ix_stripe_events_stripe_event_id', 'stripe_events', ['stripe_event_id'], unique=False)
    
    # Create features table for feature flags
    op.create_table(
        'features',
        sa.Column('id', postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column('org_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('key', sa.String(), nullable=False),
        sa.Column('enabled', sa.Boolean(), nullable=False, server_default='false'),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.Column('updated_at', sa.DateTime(), nullable=False),
    )
    op.create_foreign_key('fk_features_org_id', 'features', 'organizations', ['org_id'], ['id'])
    op.create_index('ix_features_org_id', 'features', ['org_id'])
    op.create_unique_constraint('uq_features_key_org', 'features', ['key', 'org_id'])


def downgrade() -> None:
    # Drop features table
    op.drop_table('features')
    
    # Revert stripe_events
    op.drop_index('ix_stripe_events_org_id', table_name='stripe_events')
    op.drop_constraint('fk_stripe_events_org_id', 'stripe_events', type_='foreignkey')
    op.drop_column('stripe_events', 'org_id')
    op.drop_index('ix_stripe_events_stripe_event_id', table_name='stripe_events')
    op.create_index('ix_stripe_events_stripe_event_id', 'stripe_events', ['stripe_event_id'], unique=True)
    
    # Revert stripe_subscriptions
    op.drop_index('ix_stripe_subscriptions_org_id', table_name='stripe_subscriptions')
    op.drop_constraint('fk_stripe_subscriptions_org_id', 'stripe_subscriptions', type_='foreignkey')
    op.drop_column('stripe_subscriptions', 'org_id')
    op.drop_index('ix_stripe_subscriptions_stripe_subscription_id', table_name='stripe_subscriptions')
    op.create_index('ix_stripe_subscriptions_stripe_subscription_id', 'stripe_subscriptions', ['stripe_subscription_id'], unique=True)
    
    # Revert stripe_payments
    op.drop_index('ix_stripe_payments_org_id', table_name='stripe_payments')
    op.drop_constraint('fk_stripe_payments_org_id', 'stripe_payments', type_='foreignkey')
    op.drop_column('stripe_payments', 'org_id')
    op.drop_index('ix_stripe_payments_stripe_id', table_name='stripe_payments')
    op.create_index('ix_stripe_payments_stripe_id', 'stripe_payments', ['stripe_id'], unique=True)
    
    # Revert recommendations
    op.drop_index('ix_recommendations_org_id', table_name='recommendations')
    op.drop_constraint('fk_recommendations_org_id', 'recommendations', type_='foreignkey')
    op.drop_column('recommendations', 'org_id')
    
    # Revert campaigns
    op.drop_index('ix_campaigns_org_id', table_name='campaigns')
    op.drop_constraint('fk_campaigns_org_id', 'campaigns', type_='foreignkey')
    op.drop_column('campaigns', 'org_id')
    
    # Revert events
    op.drop_index('ix_events_org_id', table_name='events')
    op.drop_constraint('fk_events_org_id', 'events', type_='foreignkey')
    op.drop_column('events', 'org_id')
    
    # Revert oauth_tokens
    op.drop_constraint('uq_oauth_tokens_provider_org', 'oauth_tokens', type_='unique')
    op.drop_index('ix_oauth_tokens_org_id', table_name='oauth_tokens')
    op.drop_constraint('fk_oauth_tokens_org_id', 'oauth_tokens', type_='foreignkey')
    op.drop_column('oauth_tokens', 'org_id')
    
    # Revert clients
    op.drop_index('ix_clients_org_id', table_name='clients')
    op.drop_constraint('fk_clients_org_id', 'clients', type_='foreignkey')
    op.drop_column('clients', 'org_id')
    
    # Revert users
    op.drop_constraint('uq_users_email_org', 'users', type_='unique')
    op.drop_index('ix_users_email', table_name='users')
    op.create_index('ix_users_email', 'users', ['email'], unique=True)
    op.drop_index('ix_users_org_id', table_name='users')
    op.drop_constraint('fk_users_org_id', 'users', type_='foreignkey')
    op.drop_column('users', 'role')
    op.drop_column('users', 'org_id')
    
    # Drop organizations table
    op.drop_table('organizations')
    
    # Drop enum type
    op.execute('DROP TYPE IF EXISTS userrole')


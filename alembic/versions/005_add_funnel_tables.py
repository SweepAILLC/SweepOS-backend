"""add funnel tables

Revision ID: 005
Revises: 004
Create Date: 2024-01-15 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '005'
down_revision = '004'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Create funnels table
    op.create_table(
        'funnels',
        sa.Column('id', postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column('org_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('client_id', postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column('name', sa.String(), nullable=False),
        sa.Column('slug', sa.String(), nullable=True),
        sa.Column('domain', sa.String(), nullable=True),
        sa.Column('env', sa.String(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.Column('updated_at', sa.DateTime(), nullable=False),
    )
    op.create_foreign_key('fk_funnels_org_id', 'funnels', 'organizations', ['org_id'], ['id'])
    op.create_foreign_key('fk_funnels_client_id', 'funnels', 'clients', ['client_id'], ['id'])
    op.create_index('ix_funnels_org_id', 'funnels', ['org_id'])
    op.create_index('ix_funnels_client_id', 'funnels', ['client_id'])
    op.create_unique_constraint('uq_funnels_slug', 'funnels', ['slug'])
    
    # Create funnel_steps table
    op.create_table(
        'funnel_steps',
        sa.Column('id', postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column('org_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('funnel_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('step_order', sa.Integer(), nullable=False),
        sa.Column('event_name', sa.String(), nullable=False),
        sa.Column('label', sa.String(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.Column('updated_at', sa.DateTime(), nullable=False),
    )
    op.create_foreign_key('fk_funnel_steps_org_id', 'funnel_steps', 'organizations', ['org_id'], ['id'])
    op.create_foreign_key('fk_funnel_steps_funnel_id', 'funnel_steps', 'funnels', ['funnel_id'], ['id'], ondelete='CASCADE')
    op.create_index('ix_funnel_steps_org_id', 'funnel_steps', ['org_id'])
    op.create_index('ix_funnel_steps_funnel_id', 'funnel_steps', ['funnel_id'])
    
    # Create sessions table
    op.create_table(
        'sessions',
        sa.Column('id', postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column('org_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('visitor_id', sa.String(), nullable=True),
        sa.Column('session_id', sa.String(), nullable=True),
        sa.Column('first_seen', sa.DateTime(), nullable=False),
        sa.Column('last_seen', sa.DateTime(), nullable=False),
        sa.Column('utm', postgresql.JSON(astext_type=sa.Text()), nullable=True),
        sa.Column('session_metadata', postgresql.JSON(astext_type=sa.Text()), nullable=True),  # Renamed from 'metadata' to avoid SQLAlchemy reserved name
    )
    op.create_foreign_key('fk_sessions_org_id', 'sessions', 'organizations', ['org_id'], ['id'])
    op.create_index('ix_sessions_org_id', 'sessions', ['org_id'])
    op.create_index('ix_sessions_visitor_id', 'sessions', ['visitor_id'])
    op.create_index('ix_sessions_session_id', 'sessions', ['session_id'])
    
    # Create event_errors table
    op.create_table(
        'event_errors',
        sa.Column('id', postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column('payload', postgresql.JSON(astext_type=sa.Text()), nullable=True),
        sa.Column('reason', sa.Text(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False),
    )
    
    # Extend events table with funnel fields
    op.add_column('events', sa.Column('funnel_id', postgresql.UUID(as_uuid=True), nullable=True))
    op.add_column('events', sa.Column('event_name', sa.String(), nullable=True))
    op.add_column('events', sa.Column('visitor_id', sa.String(), nullable=True))
    op.add_column('events', sa.Column('session_id', sa.String(), nullable=True))
    op.add_column('events', sa.Column('event_metadata', postgresql.JSON(astext_type=sa.Text()), nullable=True))  # Renamed from 'metadata' to avoid SQLAlchemy reserved name
    op.add_column('events', sa.Column('received_at', sa.DateTime(), nullable=True))
    
    # Set received_at to occurred_at for existing events
    op.execute("UPDATE events SET received_at = occurred_at WHERE received_at IS NULL")
    op.alter_column('events', 'received_at', nullable=False)
    
    # Add foreign key and indexes
    op.create_foreign_key('fk_events_funnel_id', 'events', 'funnels', ['funnel_id'], ['id'])
    op.create_index('ix_events_funnel_id', 'events', ['funnel_id'])
    op.create_index('ix_events_event_name', 'events', ['event_name'])
    op.create_index('ix_events_visitor_id', 'events', ['visitor_id'])
    op.create_index('ix_events_session_id', 'events', ['session_id'])
    op.create_index('ix_events_occurred_at', 'events', ['occurred_at'])


def downgrade() -> None:
    # Remove indexes and foreign keys from events
    op.drop_index('ix_events_occurred_at', table_name='events')
    op.drop_index('ix_events_session_id', table_name='events')
    op.drop_index('ix_events_visitor_id', table_name='events')
    op.drop_index('ix_events_event_name', table_name='events')
    op.drop_index('ix_events_funnel_id', table_name='events')
    op.drop_constraint('fk_events_funnel_id', 'events', type_='foreignkey')
    
    # Remove columns from events
    op.drop_column('events', 'received_at')
    op.drop_column('events', 'event_metadata')
    op.drop_column('events', 'session_id')
    op.drop_column('events', 'visitor_id')
    op.drop_column('events', 'event_name')
    op.drop_column('events', 'funnel_id')
    
    # Drop tables
    op.drop_table('event_errors')
    op.drop_table('sessions')
    op.drop_table('funnel_steps')
    op.drop_table('funnels')


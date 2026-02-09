"""add client check ins

Revision ID: 018
Revises: 017
Create Date: 2025-02-02 12:00:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '018'
down_revision = '017'
branch_labels = None
depends_on = None


def upgrade():
    # Create client_check_ins table
    op.create_table(
        'client_check_ins',
        sa.Column('id', postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column('org_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('client_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('event_id', sa.String(), nullable=False),
        sa.Column('event_uri', sa.String(), nullable=True),
        sa.Column('provider', sa.String(), nullable=False),
        sa.Column('title', sa.String(), nullable=True),
        sa.Column('start_time', sa.DateTime(timezone=True), nullable=False),
        sa.Column('end_time', sa.DateTime(timezone=True), nullable=True),
        sa.Column('location', sa.String(), nullable=True),
        sa.Column('meeting_url', sa.String(), nullable=True),
        sa.Column('attendee_email', sa.String(), nullable=False),
        sa.Column('attendee_name', sa.String(), nullable=True),
        sa.Column('completed', sa.Boolean(), nullable=False, server_default='false'),
        sa.Column('cancelled', sa.Boolean(), nullable=False, server_default='false'),
        sa.Column('raw_event_data', sa.Text(), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(['org_id'], ['organizations.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['client_id'], ['clients.id'], ondelete='CASCADE'),
    )
    
    # Create indexes
    op.create_index('ix_client_check_ins_id', 'client_check_ins', ['id'])
    op.create_index('ix_client_check_ins_org_id', 'client_check_ins', ['org_id'])
    op.create_index('ix_client_check_ins_client_id', 'client_check_ins', ['client_id'])
    op.create_index('ix_client_check_ins_event_id', 'client_check_ins', ['event_id'])
    op.create_index('ix_client_check_ins_start_time', 'client_check_ins', ['start_time'])
    op.create_index('ix_client_check_ins_attendee_email', 'client_check_ins', ['attendee_email'])
    
    # Create unique constraint: one check-in per event_id per org
    op.create_unique_constraint('uq_client_check_ins_event_org', 'client_check_ins', ['event_id', 'org_id'])


def downgrade():
    op.drop_constraint('uq_client_check_ins_event_org', 'client_check_ins', type_='unique')
    op.drop_index('ix_client_check_ins_attendee_email', table_name='client_check_ins')
    op.drop_index('ix_client_check_ins_start_time', table_name='client_check_ins')
    op.drop_index('ix_client_check_ins_event_id', table_name='client_check_ins')
    op.drop_index('ix_client_check_ins_client_id', table_name='client_check_ins')
    op.drop_index('ix_client_check_ins_org_id', table_name='client_check_ins')
    op.drop_index('ix_client_check_ins_id', table_name='client_check_ins')
    op.drop_table('client_check_ins')


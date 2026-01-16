"""add tab permissions

Revision ID: 007
Revises: 006
Create Date: 2024-01-20 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '007'
down_revision = '006'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Create organization_tab_permissions table
    op.create_table(
        'organization_tab_permissions',
        sa.Column('id', postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column('org_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('tab_name', sa.String(), nullable=False),
        sa.Column('enabled', sa.Boolean(), nullable=False, server_default='true'),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.Column('updated_at', sa.DateTime(), nullable=False),
    )
    op.create_foreign_key(
        'fk_org_tab_permissions_org_id',
        'organization_tab_permissions', 'organizations',
        ['org_id'], ['id'],
        ondelete='CASCADE'
    )
    op.create_index('ix_organization_tab_permissions_org_id', 'organization_tab_permissions', ['org_id'])
    op.create_unique_constraint(
        'uq_org_tab_permissions_org_tab',
        'organization_tab_permissions',
        ['org_id', 'tab_name']
    )

    # Create user_tab_permissions table
    op.create_table(
        'user_tab_permissions',
        sa.Column('id', postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column('user_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('tab_name', sa.String(), nullable=False),
        sa.Column('enabled', sa.Boolean(), nullable=False, server_default='true'),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.Column('updated_at', sa.DateTime(), nullable=False),
    )
    op.create_foreign_key(
        'fk_user_tab_permissions_user_id',
        'user_tab_permissions', 'users',
        ['user_id'], ['id'],
        ondelete='CASCADE'
    )
    op.create_index('ix_user_tab_permissions_user_id', 'user_tab_permissions', ['user_id'])
    op.create_unique_constraint(
        'uq_user_tab_permissions_user_tab',
        'user_tab_permissions',
        ['user_id', 'tab_name']
    )

    # Set default permissions for existing orgs (all tabs enabled)
    # This will be done in application code on first access


def downgrade() -> None:
    op.drop_table('user_tab_permissions')
    op.drop_table('organization_tab_permissions')


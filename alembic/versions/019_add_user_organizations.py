"""add user organizations

Revision ID: 019
Revises: 018
Create Date: 2025-02-06 12:00:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = '019'
down_revision = '018'
branch_labels = None
depends_on = None


def upgrade():
    # Check if table already exists
    from sqlalchemy import inspect, text
    conn = op.get_bind()
    inspector = inspect(conn)
    tables = inspector.get_table_names()
    
    table_exists = 'user_organizations' in tables
    
    if not table_exists:
        # Create user_organizations table
        op.create_table(
            'user_organizations',
            sa.Column('id', postgresql.UUID(as_uuid=True), primary_key=True),
            sa.Column('user_id', postgresql.UUID(as_uuid=True), sa.ForeignKey('users.id', ondelete='CASCADE'), nullable=False),
            sa.Column('org_id', postgresql.UUID(as_uuid=True), sa.ForeignKey('organizations.id', ondelete='CASCADE'), nullable=False),
            sa.Column('is_primary', sa.Boolean(), nullable=False, server_default='false'),
            sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.func.now()),
            sa.PrimaryKeyConstraint('id'),
            sa.UniqueConstraint('user_id', 'org_id', name='uq_user_organizations_user_org')
        )
    
    # Create indexes if they don't exist (using raw SQL with IF NOT EXISTS)
    conn.execute(text("""
        CREATE INDEX IF NOT EXISTS ix_user_organizations_user_id 
        ON user_organizations (user_id)
    """))
    
    conn.execute(text("""
        CREATE INDEX IF NOT EXISTS ix_user_organizations_org_id 
        ON user_organizations (org_id)
    """))
    
    # Migrate existing data: create user_organization records for all existing users
    # Only if table was just created or if we need to backfill
    if not table_exists:
        op.execute(text("""
            INSERT INTO user_organizations (id, user_id, org_id, is_primary, created_at)
            SELECT 
                gen_random_uuid(),
                id as user_id,
                org_id,
                true as is_primary,
                created_at
            FROM users
            ON CONFLICT DO NOTHING
        """))
    else:
        # Table exists, but check if we need to backfill data
        result = conn.execute(text("SELECT COUNT(*) FROM user_organizations"))
        count = result.scalar()
        if count == 0:
            # Backfill existing users
            op.execute(text("""
                INSERT INTO user_organizations (id, user_id, org_id, is_primary, created_at)
                SELECT 
                    gen_random_uuid(),
                    id as user_id,
                    org_id,
                    true as is_primary,
                    created_at
                FROM users
                WHERE NOT EXISTS (
                    SELECT 1 FROM user_organizations uo WHERE uo.user_id = users.id AND uo.org_id = users.org_id
                )
            """))


def downgrade():
    op.drop_index('ix_user_organizations_org_id', table_name='user_organizations')
    op.drop_index('ix_user_organizations_user_id', table_name='user_organizations')
    op.drop_table('user_organizations')


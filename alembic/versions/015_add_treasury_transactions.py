"""add treasury transactions

Revision ID: 015_add_treasury_transactions
Revises: 014_add_calendly_provider
Create Date: 2024-01-01 12:00:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '015'
down_revision = '014'
branch_labels = None
depends_on = None


def upgrade():
    # Drop enum types if they exist (to handle previous failed migrations)
    # We'll recreate them below
    op.execute("DROP TYPE IF EXISTS treasurytransactionflowtype CASCADE")
    op.execute("DROP TYPE IF EXISTS treasurytransactionstatus CASCADE")
    
    # Create TreasuryTransactionStatus enum
    op.execute("CREATE TYPE treasurytransactionstatus AS ENUM ('open', 'posted', 'void')")
    
    # Create TreasuryTransactionFlowType enum
    op.execute("""
        CREATE TYPE treasurytransactionflowtype AS ENUM (
            'credit_reversal',
            'debit_reversal',
            'inbound_transfer',
            'issuing_authorization',
            'outbound_payment',
            'outbound_transfer',
            'received_credit',
            'received_debit'
        )
    """)
    
    # Check if table already exists (in case of previous failed migration)
    conn = op.get_bind()
    inspector = sa.inspect(conn)
    if 'stripe_treasury_transactions' in inspector.get_table_names():
        print("[MIGRATION] Table stripe_treasury_transactions already exists, skipping creation")
        return
    
    # Create stripe_treasury_transactions table
    # Use postgresql.ENUM with create_type=False to prevent SQLAlchemy from trying to create the enum
    treasury_status_enum = postgresql.ENUM('open', 'posted', 'void', name='treasurytransactionstatus', create_type=False)
    treasury_flow_type_enum = postgresql.ENUM('credit_reversal', 'debit_reversal', 'inbound_transfer', 'issuing_authorization', 'outbound_payment', 'outbound_transfer', 'received_credit', 'received_debit', name='treasurytransactionflowtype', create_type=False)
    
    op.create_table(
        'stripe_treasury_transactions',
        sa.Column('id', postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column('org_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('stripe_transaction_id', sa.String(), nullable=False),
        sa.Column('financial_account_id', sa.String(), nullable=True),
        sa.Column('flow_id', sa.String(), nullable=True),
        sa.Column('flow_type', treasury_flow_type_enum, nullable=True),
        sa.Column('amount', sa.Integer(), nullable=False),
        sa.Column('currency', sa.String(length=3), nullable=False, server_default='usd'),
        sa.Column('status', treasury_status_enum, nullable=False),
        sa.Column('balance_impact_cash', sa.Integer(), nullable=True),
        sa.Column('balance_impact_inbound_pending', sa.Integer(), nullable=True),
        sa.Column('balance_impact_outbound_pending', sa.Integer(), nullable=True),
        sa.Column('created', sa.DateTime(), nullable=False),
        sa.Column('posted_at', sa.DateTime(), nullable=True),
        sa.Column('void_at', sa.DateTime(), nullable=True),
        sa.Column('description', sa.Text(), nullable=True),
        sa.Column('customer_email', sa.String(), nullable=True),
        sa.Column('customer_id', sa.String(), nullable=True),
        sa.Column('client_id', postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column('raw_data', postgresql.JSON(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.text('now()')),
        sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.text('now()')),
        sa.ForeignKeyConstraint(['org_id'], ['organizations.id'], ),
        sa.ForeignKeyConstraint(['client_id'], ['clients.id'], ),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('stripe_transaction_id')
    )
    
    # Create indexes
    op.create_index('ix_stripe_treasury_transactions_org_id', 'stripe_treasury_transactions', ['org_id'])
    op.create_index('ix_stripe_treasury_transactions_stripe_transaction_id', 'stripe_treasury_transactions', ['stripe_transaction_id'], unique=True)
    op.create_index('ix_stripe_treasury_transactions_financial_account_id', 'stripe_treasury_transactions', ['financial_account_id'])
    op.create_index('ix_stripe_treasury_transactions_flow_id', 'stripe_treasury_transactions', ['flow_id'])
    op.create_index('ix_stripe_treasury_transactions_status', 'stripe_treasury_transactions', ['status'])
    op.create_index('ix_stripe_treasury_transactions_customer_email', 'stripe_treasury_transactions', ['customer_email'])
    op.create_index('ix_stripe_treasury_transactions_customer_id', 'stripe_treasury_transactions', ['customer_id'])
    op.create_index('ix_stripe_treasury_transactions_client_id', 'stripe_treasury_transactions', ['client_id'])
    op.create_index('ix_stripe_treasury_transactions_created', 'stripe_treasury_transactions', ['created'])
    op.create_index('ix_stripe_treasury_transactions_posted_at', 'stripe_treasury_transactions', ['posted_at'])


def downgrade():
    op.drop_index('ix_stripe_treasury_transactions_posted_at', table_name='stripe_treasury_transactions')
    op.drop_index('ix_stripe_treasury_transactions_created', table_name='stripe_treasury_transactions')
    op.drop_index('ix_stripe_treasury_transactions_client_id', table_name='stripe_treasury_transactions')
    op.drop_index('ix_stripe_treasury_transactions_customer_id', table_name='stripe_treasury_transactions')
    op.drop_index('ix_stripe_treasury_transactions_customer_email', table_name='stripe_treasury_transactions')
    op.drop_index('ix_stripe_treasury_transactions_status', table_name='stripe_treasury_transactions')
    op.drop_index('ix_stripe_treasury_transactions_flow_id', table_name='stripe_treasury_transactions')
    op.drop_index('ix_stripe_treasury_transactions_financial_account_id', table_name='stripe_treasury_transactions')
    op.drop_index('ix_stripe_treasury_transactions_stripe_transaction_id', table_name='stripe_treasury_transactions')
    op.drop_index('ix_stripe_treasury_transactions_org_id', table_name='stripe_treasury_transactions')
    op.drop_table('stripe_treasury_transactions')
    
    # Drop enums
    op.execute("DROP TYPE IF EXISTS treasurytransactionflowtype")
    op.execute("DROP TYPE IF EXISTS treasurytransactionstatus")


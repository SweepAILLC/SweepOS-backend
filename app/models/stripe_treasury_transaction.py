from sqlalchemy import Column, String, DateTime, Integer, ForeignKey, JSON, Text, Enum as SQLEnum
from sqlalchemy.dialects.postgresql import UUID
import uuid
from datetime import datetime
import enum
from app.db.session import Base


class TreasuryTransactionStatus(str, enum.Enum):
    OPEN = "open"
    POSTED = "posted"
    VOID = "void"


class TreasuryTransactionFlowType(str, enum.Enum):
    CREDIT_REVERSAL = "credit_reversal"
    DEBIT_REVERSAL = "debit_reversal"
    INBOUND_TRANSFER = "inbound_transfer"
    ISSUING_AUTHORIZATION = "issuing_authorization"
    OUTBOUND_PAYMENT = "outbound_payment"
    OUTBOUND_TRANSFER = "outbound_transfer"
    RECEIVED_CREDIT = "received_credit"
    RECEIVED_DEBIT = "received_debit"


class StripeTreasuryTransaction(Base):
    __tablename__ = "stripe_treasury_transactions"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    org_id = Column(UUID(as_uuid=True), ForeignKey("organizations.id"), nullable=False, index=True)
    stripe_transaction_id = Column(String, nullable=False, unique=True, index=True)  # trxn_xxx
    financial_account_id = Column(String, nullable=True, index=True)  # fa_xxx
    flow_id = Column(String, nullable=True, index=True)  # Flow ID (obt_xxx, ic_xxx, etc.)
    # Flow type - use native_enum=False to store enum values as strings
    flow_type = Column(SQLEnum(TreasuryTransactionFlowType, native_enum=False), nullable=True)
    
    # Amount and currency
    amount = Column(Integer, nullable=False)  # Amount in cents (can be negative for outbound)
    currency = Column(String(3), default="usd", nullable=False)
    
    # Status - use native_enum=False to store enum values as strings (not enum names)
    status = Column(SQLEnum(TreasuryTransactionStatus, native_enum=False), nullable=False, index=True)
    
    # Balance impact
    balance_impact_cash = Column(Integer, nullable=True)  # Cash balance impact in cents
    balance_impact_inbound_pending = Column(Integer, nullable=True)
    balance_impact_outbound_pending = Column(Integer, nullable=True)
    
    # Timestamps
    created = Column(DateTime, nullable=False, index=True)  # Transaction created timestamp
    posted_at = Column(DateTime, nullable=True, index=True)  # When transaction posted
    void_at = Column(DateTime, nullable=True)  # When transaction was voided
    
    # Description and metadata
    description = Column(Text, nullable=True)
    customer_email = Column(String, nullable=True, index=True)  # Extracted from description or flow
    customer_id = Column(String, nullable=True, index=True)  # Stripe customer ID if available
    
    # Client relationship
    client_id = Column(UUID(as_uuid=True), ForeignKey("clients.id"), nullable=True, index=True)
    
    # Raw data
    raw_data = Column(JSON, nullable=True)  # Store full Stripe transaction object
    
    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)


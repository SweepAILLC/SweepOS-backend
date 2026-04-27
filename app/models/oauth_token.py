from sqlalchemy import Column, String, DateTime, Enum as SQLEnum, ForeignKey
from sqlalchemy.dialects.postgresql import UUID
import uuid
from datetime import datetime
import enum
from app.db.session import Base


# Values must match PostgreSQL enum `oauthprovider` labels exactly (mixed-case in this schema).
class OAuthProvider(str, enum.Enum):
    STRIPE = "STRIPE"
    BREVO = "BREVO"
    CALCOM = "calcom"
    CALENDLY = "calendly"
    WHOP = "whop"


class OAuthToken(Base):
    __tablename__ = "oauth_tokens"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    org_id = Column(UUID(as_uuid=True), ForeignKey("organizations.id"), nullable=False, index=True)
    # SQLAlchemy otherwise binds member *names* (e.g. WHOP); values must match pg enum labels.
    provider = Column(
        SQLEnum(OAuthProvider, values_callable=lambda obj: [m.value for m in obj]),
        nullable=False,
    )
    account_id = Column(String, nullable=True)  # e.g., stripe_user_id
    access_token = Column(String, nullable=False)  # encrypted
    refresh_token = Column(String, nullable=True)  # encrypted
    scope = Column(String, nullable=True)
    expires_at = Column(DateTime, nullable=True)
    last_sync_at = Column(DateTime, nullable=True)  # Last successful incremental sync timestamp
    webhook_secret = Column(String(255), nullable=True)  # Stripe webhook signing secret (encrypted)
    webhook_endpoint_id = Column(String(64), nullable=True)  # Stripe we_xxx for deletion on disconnect
    last_webhook_processed_at = Column(DateTime, nullable=True)  # Set when a webhook is processed; terminal refetches only if this changed
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    
    # Composite unique constraint: one token per provider per org
    __table_args__ = (
        {"schema": None},  # Use default schema
    )


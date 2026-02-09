from sqlalchemy import Column, String, DateTime, Enum as SQLEnum, ForeignKey
from sqlalchemy.dialects.postgresql import UUID
import uuid
from datetime import datetime
import enum
from app.db.session import Base


class OAuthProvider(str, enum.Enum):
    STRIPE = "stripe"
    BREVO = "brevo"
    CALCOM = "calcom"
    CALENDLY = "calendly"


class OAuthToken(Base):
    __tablename__ = "oauth_tokens"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    org_id = Column(UUID(as_uuid=True), ForeignKey("organizations.id"), nullable=False, index=True)
    provider = Column(SQLEnum(OAuthProvider), nullable=False)
    account_id = Column(String, nullable=True)  # e.g., stripe_user_id
    access_token = Column(String, nullable=False)  # encrypted
    refresh_token = Column(String, nullable=True)  # encrypted
    scope = Column(String, nullable=True)
    expires_at = Column(DateTime, nullable=True)
    last_sync_at = Column(DateTime, nullable=True)  # Last successful incremental sync timestamp
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    
    # Composite unique constraint: one token per provider per org
    __table_args__ = (
        {"schema": None},  # Use default schema
    )


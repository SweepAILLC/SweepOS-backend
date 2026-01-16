from sqlalchemy import Column, String, DateTime, Enum as SQLEnum, Text, ForeignKey
from sqlalchemy.dialects.postgresql import UUID
import uuid
from datetime import datetime
import enum
from app.db.session import Base


class AuditEventType(str, enum.Enum):
    """Types of security events to audit"""
    API_KEY_CONNECTED = "api_key_connected"
    API_KEY_DISCONNECTED = "api_key_disconnected"
    OAUTH_CONNECTED = "oauth_connected"
    OAUTH_DISCONNECTED = "oauth_disconnected"
    TOKEN_ACCESSED = "token_accessed"
    TOKEN_DECRYPTED = "token_decrypted"
    RATE_LIMIT_EXCEEDED = "rate_limit_exceeded"
    UNAUTHORIZED_ACCESS = "unauthorized_access"


class AuditLog(Base):
    __tablename__ = "audit_logs"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    org_id = Column(UUID(as_uuid=True), ForeignKey("organizations.id"), nullable=False, index=True)
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=True, index=True)
    event_type = Column(SQLEnum(AuditEventType), nullable=False, index=True)
    resource_type = Column(String, nullable=True)  # e.g., "stripe", "oauth_token"
    resource_id = Column(String, nullable=True)  # e.g., account_id, token_id
    ip_address = Column(String, nullable=True)
    user_agent = Column(String, nullable=True)
    details = Column(Text, nullable=True)  # JSON string with additional details
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)
    
    __table_args__ = (
        {"schema": None},
    )


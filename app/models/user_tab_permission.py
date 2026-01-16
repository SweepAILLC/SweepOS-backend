from sqlalchemy import Column, String, Boolean, DateTime, ForeignKey
from sqlalchemy.dialects.postgresql import UUID
import uuid
from datetime import datetime
from app.db.session import Base


class UserTabPermission(Base):
    """Controls which tabs a specific user has access to (overrides org permissions)"""
    __tablename__ = "user_tab_permissions"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    tab_name = Column(String, nullable=False)  # 'brevo', 'clients', 'stripe', 'funnels', 'users'
    enabled = Column(Boolean, default=True, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    # Unique constraint: one permission per tab per user
    __table_args__ = (
        {"schema": None},
    )


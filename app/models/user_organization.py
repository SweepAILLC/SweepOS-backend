from sqlalchemy import Column, ForeignKey, DateTime, Boolean
from sqlalchemy.dialects.postgresql import UUID
import uuid
from datetime import datetime
from app.db.session import Base


class UserOrganization(Base):
    """
    Many-to-many relationship between users and organizations.
    Allows users to belong to multiple organizations.
    """
    __tablename__ = "user_organizations"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    org_id = Column(UUID(as_uuid=True), ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False, index=True)
    is_primary = Column(Boolean, default=False, nullable=False)  # Primary org for backward compatibility
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    
    # Composite unique constraint: one record per user-org pair
    __table_args__ = (
        {"schema": None},  # Use default schema
    )


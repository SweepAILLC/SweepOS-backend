"""Latest generated content ideas per org (Content Studio)."""
from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import Column, DateTime, ForeignKey, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID, JSON
from sqlalchemy.orm import relationship

from app.db.session import Base


class ContentStudioGeneration(Base):
    __tablename__ = "content_studio_generations"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    org_id = Column(UUID(as_uuid=True), ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False)
    batch_id = Column(UUID(as_uuid=True), nullable=False)
    ideas_json = Column(JSON, nullable=False, default=list)
    created_by_user_id = Column(UUID(as_uuid=True), ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    organization = relationship("Organization", backref="content_studio_generation_row")
    creator = relationship("User", foreign_keys=[created_by_user_id])

    __table_args__ = (UniqueConstraint("org_id", name="uq_content_studio_generation_org"),)

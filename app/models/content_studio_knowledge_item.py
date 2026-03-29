"""Per-org sales playbook lines for Content Studio (objections, closing, reframes)."""
from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import Column, String, Text, Integer, DateTime, ForeignKey, Index
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship

from app.db.session import Base


class ContentStudioKnowledgeItem(Base):
    __tablename__ = "content_studio_knowledge_items"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    org_id = Column(UUID(as_uuid=True), ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False, index=True)
    kind = Column(String(32), nullable=False)  # objection | closing | reframe
    body = Column(Text, nullable=False)
    sort_order = Column(Integer, nullable=False, default=0)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    organization = relationship("Organization", backref="content_studio_knowledge_items")

    __table_args__ = (Index("ix_cs_knowledge_org_kind", "org_id", "kind", "sort_order"),)

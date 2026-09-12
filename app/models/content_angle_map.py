"""Org-scoped Content Angle Map (portal deliverable)."""
from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import Column, DateTime, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSON, UUID
from sqlalchemy.orm import relationship

from app.db.session import Base


class ContentAngleMap(Base):
    __tablename__ = "content_angle_maps"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    org_id = Column(
        UUID(as_uuid=True),
        ForeignKey("organizations.id", ondelete="CASCADE"),
        nullable=False,
    )
    icp_angles = Column(JSON, nullable=False, default=list)
    personal_brand_angles = Column(JSON, nullable=False, default=list)
    format_pills_tof = Column(JSON, nullable=False, default=list)
    format_pills_mof = Column(JSON, nullable=False, default=list)
    format_pills_bof = Column(JSON, nullable=False, default=list)
    last_generated_at = Column(DateTime, nullable=True)
    last_generated_icp_at = Column(DateTime, nullable=True)
    last_generated_brand_at = Column(DateTime, nullable=True)
    calls_seen_at_generation = Column(Integer, nullable=False, default=0)
    input_fingerprint = Column(String(64), nullable=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    organization = relationship(
        "Organization",
        backref="content_angle_map_row",
        passive_deletes=True,
    )

    __table_args__ = (UniqueConstraint("org_id", name="uq_content_angle_map_org"),)

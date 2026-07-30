"""Per-org KPI benchmark thresholds (JSONB, seeded from product defaults)."""
from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import Column, DateTime, ForeignKey
from sqlalchemy.dialects.postgresql import JSONB, UUID

from app.db.session import Base


class OrgKpiBenchmark(Base):
    __tablename__ = "org_kpi_benchmarks"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    org_id = Column(
        UUID(as_uuid=True),
        ForeignKey("organizations.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,
        index=True,
    )
    # Typed on read/write via Pydantic (KpiBenchmarksThresholds).
    thresholds = Column(JSONB, nullable=False, default=dict)

    # Optional user-configurable content-type tags for best_content_type.
    content_type_tags = Column(JSONB, nullable=True)
    # Private per-org token for external form entry links.
    entry_form_token = Column(UUID(as_uuid=True), unique=True, nullable=True, index=True)

    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

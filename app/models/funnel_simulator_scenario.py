"""Org-saved Funnel Simulator scenarios (consulting portal)."""
from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import Column, DateTime, ForeignKey, String
from sqlalchemy.dialects.postgresql import JSONB, UUID

from app.db.session import Base

MAX_FUNNEL_SIMULATOR_SCENARIOS_PER_ORG = 50


class FunnelSimulatorScenario(Base):
    __tablename__ = "funnel_simulator_scenarios"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    org_id = Column(
        UUID(as_uuid=True),
        ForeignKey("organizations.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    name = Column(String(120), nullable=False)
    mode = Column(String(32), nullable=False, default="paid_vsl")
    funnel_id = Column(UUID(as_uuid=True), ForeignKey("funnels.id", ondelete="SET NULL"), nullable=True)
    lookback_days = Column(String(16), nullable=False, default="90")
    inputs = Column(JSONB, nullable=False, default=dict)
    created_by = Column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

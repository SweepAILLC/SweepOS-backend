"""One row per connected Meta ad account — see docs/features/META_ADS_PERFORMANCE_PRD.md."""
from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import BigInteger, Column, DateTime, Float, ForeignKey, String
from sqlalchemy.dialects.postgresql import UUID

from app.db.session import Base


class MetaAdAccount(Base):
    __tablename__ = "meta_ad_accounts"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    org_id = Column(
        UUID(as_uuid=True),
        ForeignKey("organizations.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    meta_ad_account_id = Column(String(64), nullable=False)  # Meta's act_<id>
    account_name = Column(String(255), nullable=True)
    currency = Column(String(8), nullable=True)
    last_synced_at = Column(DateTime(timezone=True), nullable=True)
    # Coach-set guardrails for the scale / ceiling rules. Not synced from Meta.
    target_cpr_cents = Column(BigInteger, nullable=True)
    min_roas = Column(Float, nullable=True)

    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

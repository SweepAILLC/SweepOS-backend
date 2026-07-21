from sqlalchemy import Column, String, DateTime, Text, Integer, ForeignKey
from sqlalchemy.dialects.postgresql import UUID
import uuid
from datetime import datetime
from app.db.session import Base

MAX_SHARED_PADS_PER_ORG = 10

# Default body for the first org pad (consulting onboarding checklist).
DEFAULT_SHARED_PAD_CONTENT = """Onboarding Checklist:
□ Complete Tally Forms Client Service Agreement (https://tally.so/r/mJyDAX)
□ Fill out Tally Forms Onboarding Form (https://tally.so/r/KY0yqg)
□ Join Discord — Join the Sweep Team Discord Server! (https://discord.gg/7BAPM45R7y)
"""

DEFAULT_SHARED_PAD_TITLE = "Onboarding"


class PortalSharedPad(Base):
    """Named shared live notepad tab for an org (consultant ↔ client consulting portal)."""

    __tablename__ = "portal_shared_pads"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    org_id = Column(
        UUID(as_uuid=True),
        ForeignKey("organizations.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    title = Column(String(120), nullable=False, default=DEFAULT_SHARED_PAD_TITLE)
    sort_order = Column(Integer, nullable=False, default=0)
    content = Column(Text, nullable=False, default=DEFAULT_SHARED_PAD_CONTENT)
    revision = Column(Integer, nullable=False, default=1)
    updated_by = Column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=True)
    updated_by_name = Column(String(255), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

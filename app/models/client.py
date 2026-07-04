from sqlalchemy import Column, String, DateTime, Numeric, JSON, Integer, Text, ForeignKey, Index, or_, TypeDecorator
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import UUID
import uuid
import re
from datetime import datetime, timedelta, timezone
from typing import Optional
import enum
from app.db.session import Base


def _as_naive_utc(dt: Optional[datetime]) -> Optional[datetime]:
    """Avoid TypeError comparing tz-aware program dates with naive utcnow()."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt
    return dt.astimezone(timezone.utc).replace(tzinfo=None)


class LifecycleState(str, enum.Enum):
    COLD_LEAD = "cold_lead"
    NURTURING = "nurturing"
    QUALIFIED = "qualified"
    BOOKED = "booked"
    ACTIVE = "active"
    OFFBOARDING = "offboarding"
    DEAD = "dead"


def parse_lifecycle_state_from_db(raw: object) -> LifecycleState:
    """Map DB lifecyclestate (any legacy casing) to LifecycleState."""
    if raw is None:
        return LifecycleState.QUALIFIED
    if isinstance(raw, LifecycleState):
        return raw
    key = str(raw).strip().lower()
    if key == "warm_lead":
        return LifecycleState.BOOKED
    try:
        return LifecycleState(key)
    except ValueError:
        return LifecycleState.QUALIFIED


def lifecyclestate_bind_value(state: LifecycleState | str) -> str:
    """
    PostgreSQL lifecyclestate label for raw SQL / legacy DBs.

    Older production DBs store UPPERCASE labels (DEAD, COLD_LEAD); migration 047 adds
    lowercase. Prefer uppercase on bind so column moves work before migrations run.
    """
    if isinstance(state, LifecycleState):
        key = state.value
    else:
        key = str(state).strip().lower()
    if key == "warm_lead":
        key = "booked"
    return key.upper()


class PgLifecycleState(TypeDecorator):
    """Bind lifecycle values using labels that exist on legacy PostgreSQL enums."""

    impl = String(32)
    cache_ok = True

    def process_bind_param(self, value, dialect):
        if value is None:
            return None
        if isinstance(value, LifecycleState):
            return lifecyclestate_bind_value(value)
        return lifecyclestate_bind_value(str(value))

    def process_result_value(self, value, dialect):
        if value is None:
            return None
        return parse_lifecycle_state_from_db(value)


# Pre-payment pipeline stages (funnel → qualified → booked → active).
PRE_PAYMENT_LIFECYCLE_STATES = frozenset(
    {
        LifecycleState.COLD_LEAD,
        LifecycleState.NURTURING,
        LifecycleState.QUALIFIED,
        LifecycleState.BOOKED,
    }
)

# Stages that show follow-up urgency bars and receive lead call-insight rules.
LEAD_PIPELINE_LIFECYCLE_STATES = PRE_PAYMENT_LIFECYCLE_STATES


class Client(Base):
    __tablename__ = "clients"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    org_id = Column(UUID(as_uuid=True), ForeignKey("organizations.id"), nullable=False, index=True)
    tenant_id = Column(UUID(as_uuid=True), nullable=True)  # Deprecated: use org_id instead, kept for backward compatibility
    first_name = Column(String, nullable=True)
    last_name = Column(String, nullable=True)
    email = Column(String, index=True, nullable=True)  # Primary email (backward compat)
    emails = Column(JSON, nullable=True)  # Additional emails: list of strings, e.g. ["a@x.com", "b@x.com"]
    phone = Column(String, nullable=True)
    instagram = Column(String, nullable=True)
    lifecycle_state = Column(
        PgLifecycleState(),
        default=LifecycleState.QUALIFIED,
        nullable=False,
    )
    last_activity_at = Column(DateTime, nullable=True)
    stripe_customer_id = Column(String, nullable=True, index=True)
    estimated_mrr = Column(Numeric(10, 2), default=0, nullable=False)
    lifetime_revenue_cents = Column(Integer, default=0, nullable=False)  # Lifetime revenue in cents
    notes = Column(Text, nullable=True)  # Client notes
    meta = Column(JSON, nullable=True)
    # Intelligence offer ladder slot + payment-plan tracking (total/paid in cents)
    offer_enrollment = Column(JSON, nullable=True)
    
    # Program tracking fields
    program_start_date = Column(DateTime, nullable=True)  # When the program started
    program_duration_days = Column(Integer, nullable=True)  # Program duration in days
    program_end_date = Column(DateTime, nullable=True, index=True)  # Calculated: start_date + duration_days
    program_progress_percent = Column(Numeric(5, 2), nullable=True)  # Calculated progress percentage (0-100)
    
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    
    def calculate_progress(self) -> Optional[float]:
        """
        Calculate program progress percentage based on current date.
        Returns 0-100, or None if program not set.
        """
        start = _as_naive_utc(self.program_start_date)
        if not start or not self.program_duration_days:
            return None

        now = datetime.utcnow()
        end_date = _as_naive_utc(self.program_end_date)
        if end_date is None:
            end_date = start + timedelta(days=self.program_duration_days)

        if now < start:
            return 0.0
        if now >= end_date:
            return 100.0

        total_duration = (end_date - start).total_seconds()
        elapsed = (now - start).total_seconds()
        if total_duration <= 0:
            return None
        progress = (elapsed / total_duration) * 100.0

        return min(100.0, max(0.0, progress))
    
    def update_program_dates(self):
        """Update program dates based on start_date and end_date or duration."""
        if self.program_start_date is not None:
            self.program_start_date = _as_naive_utc(self.program_start_date)
        if self.program_end_date is not None:
            self.program_end_date = _as_naive_utc(self.program_end_date)

        if self.program_start_date and self.program_end_date:
            # Calculate duration from start and end dates
            duration = (self.program_end_date - self.program_start_date).days
            if duration > 0:
                self.program_duration_days = duration
            else:
                self.program_duration_days = None
                self.program_end_date = None
        elif self.program_start_date and self.program_duration_days:
            # Calculate end date from start date and duration
            self.program_end_date = self.program_start_date + timedelta(days=self.program_duration_days)
        else:
            self.program_end_date = None
            if not self.program_start_date:
                self.program_duration_days = None

    def get_all_emails_normalized(self) -> set:
        """Return set of normalized (lowercase, no whitespace) emails for this client: primary email + emails list."""
        out = set()
        if self.email:
            out.add(re.sub(r'\s+', '', self.email.lower().strip()))
        if self.emails and isinstance(self.emails, list):
            for e in self.emails:
                if e and isinstance(e, str):
                    out.add(re.sub(r'\s+', '', e.lower().strip()))
        return out


def find_client_by_email(db: Session, org_id: uuid.UUID, email: str) -> Optional[Client]:
    """Find a client in the org by any of their emails (primary or emails list)."""
    if not email:
        return None
    normalized = re.sub(r'\s+', '', email.lower().strip())
    candidates = db.query(Client).filter(
        Client.org_id == org_id,
        or_(Client.email.isnot(None), Client.emails.isnot(None))
    ).all()
    for c in candidates:
        if normalized in c.get_all_emails_normalized():
            return c
    return None


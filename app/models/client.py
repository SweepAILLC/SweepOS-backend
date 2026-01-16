from sqlalchemy import Column, String, DateTime, Numeric, JSON, Integer, Enum as SQLEnum, Text, ForeignKey, Index
from sqlalchemy.dialects.postgresql import UUID
import uuid
from datetime import datetime, timedelta
import enum
from app.db.session import Base


class LifecycleState(str, enum.Enum):
    COLD_LEAD = "cold_lead"
    WARM_LEAD = "warm_lead"
    ACTIVE = "active"
    OFFBOARDING = "offboarding"
    DEAD = "dead"


class Client(Base):
    __tablename__ = "clients"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    org_id = Column(UUID(as_uuid=True), ForeignKey("organizations.id"), nullable=False, index=True)
    tenant_id = Column(UUID(as_uuid=True), nullable=True)  # Deprecated: use org_id instead, kept for backward compatibility
    first_name = Column(String, nullable=True)
    last_name = Column(String, nullable=True)
    email = Column(String, index=True, nullable=True)
    phone = Column(String, nullable=True)
    lifecycle_state = Column(SQLEnum(LifecycleState), default=LifecycleState.COLD_LEAD, nullable=False)
    last_activity_at = Column(DateTime, nullable=True)
    stripe_customer_id = Column(String, nullable=True, index=True)
    estimated_mrr = Column(Numeric(10, 2), default=0, nullable=False)
    lifetime_revenue_cents = Column(Integer, default=0, nullable=False)  # Lifetime revenue in cents
    notes = Column(Text, nullable=True)  # Client notes
    meta = Column(JSON, nullable=True)
    
    # Program tracking fields
    program_start_date = Column(DateTime, nullable=True)  # When the program started
    program_duration_days = Column(Integer, nullable=True)  # Program duration in days
    program_end_date = Column(DateTime, nullable=True, index=True)  # Calculated: start_date + duration_days
    program_progress_percent = Column(Numeric(5, 2), nullable=True)  # Calculated progress percentage (0-100)
    
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    
    def calculate_progress(self) -> float:
        """
        Calculate program progress percentage based on current date.
        Returns 0-100, or None if program not set.
        """
        if not self.program_start_date or not self.program_duration_days:
            return None
        
        now = datetime.utcnow()
        if self.program_end_date:
            end_date = self.program_end_date
        else:
            end_date = self.program_start_date + timedelta(days=self.program_duration_days)
        
        if now < self.program_start_date:
            return 0.0
        if now >= end_date:
            return 100.0
        
        total_duration = (end_date - self.program_start_date).total_seconds()
        elapsed = (now - self.program_start_date).total_seconds()
        progress = (elapsed / total_duration) * 100.0
        
        return min(100.0, max(0.0, progress))
    
    def update_program_dates(self):
        """Update program_end_date when start_date or duration changes."""
        if self.program_start_date and self.program_duration_days:
            self.program_end_date = self.program_start_date + timedelta(days=self.program_duration_days)
        else:
            self.program_end_date = None


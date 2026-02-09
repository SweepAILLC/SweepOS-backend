from pydantic import BaseModel, field_validator
from datetime import datetime
from typing import Optional, Union
from decimal import Decimal
import uuid
from app.models.client import LifecycleState


class ClientBase(BaseModel):
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    email: Optional[str] = None  # Changed from EmailStr to str to avoid validation issues with None/invalid emails
    phone: Optional[str] = None
    instagram: Optional[str] = None
    lifecycle_state: LifecycleState = LifecycleState.COLD_LEAD
    stripe_customer_id: Optional[str] = None
    estimated_mrr: Optional[Union[float, Decimal]] = 0.0
    notes: Optional[str] = None
    # Program tracking fields
    program_start_date: Optional[datetime] = None
    program_duration_days: Optional[int] = None
    program_end_date: Optional[datetime] = None
    
    @field_validator('estimated_mrr', mode='before')
    @classmethod
    def convert_decimal_to_float(cls, v):
        """Convert Decimal to float for serialization"""
        if isinstance(v, Decimal):
            return float(v)
        return v if v is not None else 0.0
    
    @field_validator('email', mode='before')
    @classmethod
    def validate_email(cls, v):
        """Accept any string as email (including test emails like @stripe.test)"""
        # Just return the string as-is - no validation
        # This allows test emails that EmailStr would reject
        return str(v) if v is not None else None
    
    @field_validator('program_start_date', 'program_end_date', mode='before')
    @classmethod
    def parse_program_date(cls, v):
        """Parse program dates from string or datetime"""
        if v is None or v == '':
            return None
        if isinstance(v, datetime):
            return v
        if isinstance(v, str):
            try:
                # Handle ISO format strings
                if v.endswith('Z'):
                    v = v.replace('Z', '+00:00')
                # Handle date-only strings (YYYY-MM-DD)
                if 'T' not in v and len(v) == 10:
                    v = v + 'T00:00:00'
                return datetime.fromisoformat(v)
            except Exception as e:
                raise ValueError(f"Invalid datetime format: {v}")
        return v


class ClientCreate(ClientBase):
    pass


class ClientUpdate(BaseModel):
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    email: Optional[str] = None  # Changed from EmailStr to str
    phone: Optional[str] = None
    instagram: Optional[str] = None
    lifecycle_state: Optional[LifecycleState] = None
    stripe_customer_id: Optional[str] = None
    estimated_mrr: Optional[float] = None
    notes: Optional[str] = None
    # Program tracking fields
    program_start_date: Optional[datetime] = None
    program_duration_days: Optional[int] = None
    program_end_date: Optional[datetime] = None
    program_progress_percent: Optional[float] = None
    
    @field_validator('program_start_date', mode='before')
    @classmethod
    def parse_program_start_date(cls, v):
        """Parse program_start_date from string or datetime"""
        if v is None or v == '':
            return None
        if isinstance(v, datetime):
            return v
        if isinstance(v, str):
            try:
                # Handle ISO format strings
                if v.endswith('Z'):
                    v = v.replace('Z', '+00:00')
                return datetime.fromisoformat(v)
            except Exception as e:
                raise ValueError(f"Invalid datetime format: {v}")
        return v


class Client(ClientBase):
    id: uuid.UUID
    tenant_id: Optional[uuid.UUID] = None
    last_activity_at: Optional[datetime] = None
    lifetime_revenue_cents: Optional[int] = 0
    notes: Optional[str] = None
    meta: Optional[dict] = None
    # Program tracking fields (read-only, calculated)
    program_end_date: Optional[datetime] = None
    program_progress_percent: Optional[Union[float, Decimal]] = None
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True
        json_encoders = {
            Decimal: lambda v: float(v) if v is not None else 0.0
        }


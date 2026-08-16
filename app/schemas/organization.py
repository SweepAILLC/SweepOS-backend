from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime
from uuid import UUID


class OrganizationBase(BaseModel):
    name: str


class OrganizationCreate(OrganizationBase):
    admin_email: Optional[str] = None  # Optional: email for the initial admin user
    admin_password: Optional[str] = None  # Optional: password for the initial admin user


class OrganizationUpdate(BaseModel):
    name: Optional[str] = None
    max_user_seats: Optional[int] = None  # null = unlimited
    # Empty string clears tier; omit field to leave unchanged
    consulting_tier: Optional[str] = None  # pro_consulting | core_consulting | ""
    booking_url: Optional[str] = None


class Organization(OrganizationBase):
    id: UUID
    max_user_seats: Optional[int] = None
    consulting_tier: Optional[str] = None
    booking_url: Optional[str] = None
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class OrganizationCreateResponse(Organization):
    """Response when creating an organization - includes admin user credentials"""
    admin_email: Optional[str] = None
    admin_password: Optional[str] = None


class OrganizationWithStats(Organization):
    user_count: int = 0
    client_count: int = 0
    funnel_count: int = 0
    cash_collected_30d_usd: float = 0.0
    cash_collected_prev_30d_usd: float = 0.0
    cash_collected_all_time_usd: float = 0.0
    mrr_usd: float = 0.0
    active_seconds_7d: int = 0
    active_seconds_30d: int = 0
    last_seen_at: Optional[datetime] = None
    currently_online: bool = False


class UserOrganizationResponse(BaseModel):
    """Organization info for user organization selection"""
    id: UUID
    name: str
    is_primary: bool
    created_at: datetime
    
    class Config:
        from_attributes = True


class OrganizationSwitchRequest(BaseModel):
    """Request to switch to a different organization"""
    org_id: UUID


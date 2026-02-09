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


class Organization(OrganizationBase):
    id: UUID
    max_user_seats: Optional[int] = None
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


from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from datetime import datetime
from uuid import UUID


class UserLogin(BaseModel):
    email: str
    password: str
    org_id: Optional[UUID] = None  # Optional organization ID for multi-org users


class UserBase(BaseModel):
    email: str  # Changed from EmailStr to str to allow .local domains for test orgs


class UserCreate(UserBase):
    password: Optional[str] = None  # Optional - will be auto-generated if not provided
    org_id: Optional[UUID] = None  # Optional - will be set from current user's org
    role: Optional[str] = None  # User role: 'owner', 'admin', or 'member'


class UserUpdate(BaseModel):
    email: Optional[str] = None  # Changed from EmailStr to str to allow .local domains
    password: Optional[str] = None
    role: Optional[str] = None  # User role: 'owner', 'admin', or 'member'


class UserPasswordChange(BaseModel):
    current_password: str
    new_password: str


class UserSettingsUpdate(BaseModel):
    """User settings including privacy and data preferences"""
    email: Optional[str] = None  # Changed from EmailStr to str to allow .local domains
    current_password: Optional[str] = None  # Required if changing password
    new_password: Optional[str] = None
    # Privacy settings
    data_sharing_enabled: Optional[bool] = None
    analytics_enabled: Optional[bool] = None


class User(UserBase):
    id: UUID
    org_id: UUID
    role: str
    is_admin: bool
    created_at: datetime

    class Config:
        from_attributes = True


class Token(BaseModel):
    access_token: str
    token_type: str


class LoginResponse(BaseModel):
    """Response from login - either token or org selection required"""
    requires_org_selection: bool = False
    access_token: Optional[str] = None
    token_type: Optional[str] = None
    organizations: Optional[List[Dict[str, Any]]] = None  # List of organizations if selection required

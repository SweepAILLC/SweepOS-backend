from pydantic import BaseModel
from typing import Optional
from datetime import datetime
from uuid import UUID


class InviteOrgAdminRequest(BaseModel):
    """System owner: create org and invite admin."""
    name: str
    admin_email: str


class InviteUserRequest(BaseModel):
    """Org admin: invite user to org."""
    email: str
    role: Optional[str] = "member"  # owner | admin | member


class InvitationResponse(BaseModel):
    id: UUID
    org_id: UUID
    invitee_email: str
    invitation_type: str
    role: str
    expires_at: datetime
    used_at: Optional[datetime] = None
    created_at: datetime

    class Config:
        from_attributes = True


class InviteValidateResponse(BaseModel):
    """Public: token validation response."""
    valid: bool
    org_name: Optional[str] = None
    invitation_type: Optional[str] = None
    role: Optional[str] = None
    expires_at: Optional[datetime] = None
    message: Optional[str] = None


class InviteAcceptRequest(BaseModel):
    """Accept invitation (password required for new users)."""
    token: str
    password: Optional[str] = None  # Required when creating new account


class InviteAcceptResponse(BaseModel):
    """Response after accepting invitation."""
    access_token: Optional[str] = None  # Present when new user created and logged in
    token_type: Optional[str] = "bearer"
    org_id: Optional[UUID] = None
    user_id: Optional[UUID] = None
    existing_user: bool = False
    message: Optional[str] = None

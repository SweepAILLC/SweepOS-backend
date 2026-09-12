from pydantic import BaseModel
from typing import Optional
from datetime import datetime
from uuid import UUID


class InviteOrgAdminRequest(BaseModel):
    """System owner: mint a signup link. Recipient supplies org name and email on accept."""
    name: Optional[str] = None
    consulting_tier: Optional[str] = None
    multi_use: bool = False
    never_expires: bool = False
    expires_at: Optional[datetime] = None
    expires_in_days: Optional[int] = None


class InviteUserRequest(BaseModel):
    """Invite user to org (email and/or copyable single-use link)."""
    email: str
    role: Optional[str] = "member"  # owner | admin | member
    send_email: bool = True


class InvitationResponse(BaseModel):
    id: UUID
    org_id: Optional[UUID] = None
    invitee_email: Optional[str] = None
    invitation_type: str
    role: str
    expires_at: Optional[datetime] = None
    used_at: Optional[datetime] = None
    created_at: datetime
    invitation_link: Optional[str] = None
    email_sent: Optional[bool] = None
    multi_use: bool = False

    class Config:
        from_attributes = True


class InvitationExpiryUpdate(BaseModel):
    never_expires: bool = False
    expires_at: Optional[datetime] = None
    expires_in_days: Optional[int] = None


class InviteValidateResponse(BaseModel):
    """Public: token validation response."""
    valid: bool
    org_name: Optional[str] = None
    invitation_type: Optional[str] = None
    role: Optional[str] = None
    expires_at: Optional[datetime] = None
    message: Optional[str] = None
    needs_email: bool = False
    needs_org_name: bool = False
    invitee_email: Optional[str] = None


class InviteAcceptRequest(BaseModel):
    """Accept invitation (password required for new users)."""
    token: str
    password: Optional[str] = None  # Required when creating new account
    email: Optional[str] = None  # Required when invitation has no preloaded email
    org_name: Optional[str] = None  # Required when invitation has no pre-created org


class InviteAcceptResponse(BaseModel):
    """Response after accepting invitation."""
    access_token: Optional[str] = None  # Present when new user created and logged in
    token_type: Optional[str] = "bearer"
    org_id: Optional[UUID] = None
    user_id: Optional[UUID] = None
    existing_user: bool = False
    message: Optional[str] = None

"""
Organization-scoped endpoints: invite users, list invitations, add system owner.
Requires current user to have access to the org and be admin/owner.
"""
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from sqlalchemy import func
from datetime import datetime, timedelta
from typing import List
from uuid import UUID
import secrets
from app.db.session import get_db
from app.api.deps import get_current_user
from app.models.user import User, UserRole
from app.models.organization import Organization
from app.models.organization_invitation import OrganizationInvitation
from app.models.user_organization import UserOrganization
from app.core.config import settings
from app.core.security import get_password_hash
from app.core.rate_limit import rate_limit
from app.schemas.invitation import InviteUserRequest, InvitationResponse

router = APIRouter()

# Default expiration for invitation links (days)
INVITATION_EXPIRES_DAYS = 7


def _user_has_org_access(db: Session, user: User, org_id: UUID) -> bool:
    """Check if user has access to org (via user_organizations or primary org_id)."""
    if str(user.org_id) == str(org_id):
        return True
    uo = db.query(UserOrganization).filter(
        UserOrganization.user_id == user.id,
        UserOrganization.org_id == org_id,
    ).first()
    return uo is not None


def _require_org_admin(db: Session, user: User, org_id: UUID) -> None:
    """Raise 403 if user is not admin/owner of the org."""
    if user.role not in (UserRole.ADMIN, UserRole.OWNER) and not user.is_admin:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only admins can perform this action",
        )
    if not _user_has_org_access(db, user, org_id):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You do not have access to this organization",
        )


def _normalize_role(role: str) -> str:
    r = (role or "member").strip().lower()
    if r not in ("owner", "admin", "member"):
        return "member"
    return r


@router.post("/{org_id}/invite-user", response_model=InvitationResponse, status_code=status.HTTP_201_CREATED)
@rate_limit(max_requests=20, window_seconds=900)  # 20 user invites per 15 min per user
def invite_user_to_org(
    org_id: UUID,
    body: InviteUserRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Invite a user to join this organization. Requires org admin/owner."""
    _require_org_admin(db, current_user, org_id)
    org = db.query(Organization).filter(Organization.id == org_id).first()
    if not org:
        raise HTTPException(status_code=404, detail="Organization not found")

    if org.max_user_seats is not None:
        current_count = db.query(func.count(User.id)).filter(User.org_id == org_id).scalar() or 0
        if current_count >= org.max_user_seats:
            raise HTTPException(
                status_code=403,
                detail=f"Organization user limit reached ({org.max_user_seats} seats). Contact your system owner to increase the limit.",
            )

    email = body.email.strip().lower()
    if not email:
        raise HTTPException(status_code=400, detail="Email is required")
    role = _normalize_role(body.role or "member")
    if role == "owner" and current_user.role != UserRole.OWNER:
        raise HTTPException(status_code=403, detail="Only owners can invite as owner")

    # Already in org?
    existing_in_org = db.query(User).filter(
        func.lower(User.email) == email,
        User.org_id == org_id,
    ).first()
    if existing_in_org:
        raise HTTPException(
            status_code=400,
            detail="A user with this email is already in this organization",
        )

    # Pending invitation for same email+org?
    existing_inv = db.query(OrganizationInvitation).filter(
        OrganizationInvitation.org_id == org_id,
        func.lower(OrganizationInvitation.invitee_email) == email,
        OrganizationInvitation.used_at.is_(None),
        OrganizationInvitation.expires_at > datetime.utcnow(),
    ).first()
    if existing_inv:
        raise HTTPException(
            status_code=400,
            detail="An invitation for this email is already pending",
        )

    token = secrets.token_urlsafe(32)
    expires_at = datetime.utcnow() + timedelta(days=INVITATION_EXPIRES_DAYS)
    inv = OrganizationInvitation(
        org_id=org_id,
        invitee_email=email,
        invitation_type="USER",
        role=role,
        token=token,
        expires_at=expires_at,
        created_by=current_user.id,
    )
    db.add(inv)
    db.commit()
    db.refresh(inv)

    # Send email via BREVO_API_KEY (onboarding only)
    from app.services.onboarding_email import send_user_invitation_email
    frontend_url = getattr(settings, "FRONTEND_URL", "") or "http://localhost:3002"
    link = f"{frontend_url.rstrip('/')}/invite/accept?token={token}"
    inviter_name = current_user.email
    send_user_invitation_email(
        to_email=email,
        org_name=org.name,
        invitation_link=link,
        role=role,
        inviter_name=inviter_name,
        existing_user=False,  # We don't know yet; email text works for both
    )

    return InvitationResponse(
        id=inv.id,
        org_id=inv.org_id,
        invitee_email=inv.invitee_email,
        invitation_type=inv.invitation_type,
        role=inv.role,
        expires_at=inv.expires_at,
        used_at=inv.used_at,
        created_at=inv.created_at,
    )


@router.get("/{org_id}/invitations", response_model=List[InvitationResponse])
def list_org_invitations(
    org_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """List pending invitations for this organization."""
    _require_org_admin(db, current_user, org_id)
    invs = (
        db.query(OrganizationInvitation)
        .filter(
            OrganizationInvitation.org_id == org_id,
            OrganizationInvitation.used_at.is_(None),
            OrganizationInvitation.expires_at > datetime.utcnow(),
        )
        .order_by(OrganizationInvitation.created_at.desc())
        .all()
    )
    return [
        InvitationResponse(
            id=i.id,
            org_id=i.org_id,
            invitee_email=i.invitee_email,
            invitation_type=i.invitation_type,
            role=i.role,
            expires_at=i.expires_at,
            used_at=i.used_at,
            created_at=i.created_at,
        )
        for i in invs
    ]


@router.post("/{org_id}/invitations/{invitation_id}/resend", response_model=InvitationResponse)
def resend_org_invitation(
    org_id: UUID,
    invitation_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Resend invitation email."""
    _require_org_admin(db, current_user, org_id)
    inv = db.query(OrganizationInvitation).filter(
        OrganizationInvitation.id == invitation_id,
        OrganizationInvitation.org_id == org_id,
        OrganizationInvitation.used_at.is_(None),
    ).first()
    if not inv:
        raise HTTPException(status_code=404, detail="Invitation not found or already used")
    if inv.expires_at <= datetime.utcnow():
        raise HTTPException(status_code=400, detail="Invitation has expired")
    org = db.query(Organization).filter(Organization.id == org_id).first()
    if not org:
        raise HTTPException(status_code=404, detail="Organization not found")
    frontend_url = getattr(settings, "FRONTEND_URL", "") or "http://localhost:3002"
    link = f"{frontend_url.rstrip('/')}/invite/accept?token={inv.token}"
    from app.services.onboarding_email import send_user_invitation_email
    send_user_invitation_email(
        to_email=inv.invitee_email,
        org_name=org.name,
        invitation_link=link,
        role=inv.role,
        existing_user=False,
    )
    inv.updated_at = datetime.utcnow()
    db.commit()
    db.refresh(inv)
    return InvitationResponse(
        id=inv.id,
        org_id=inv.org_id,
        invitee_email=inv.invitee_email,
        invitation_type=inv.invitation_type,
        role=inv.role,
        expires_at=inv.expires_at,
        used_at=inv.used_at,
        created_at=inv.created_at,
    )


@router.delete("/{org_id}/invitations/{invitation_id}", status_code=status.HTTP_204_NO_CONTENT)
def cancel_org_invitation(
    org_id: UUID,
    invitation_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Cancel a pending invitation."""
    _require_org_admin(db, current_user, org_id)
    inv = db.query(OrganizationInvitation).filter(
        OrganizationInvitation.id == invitation_id,
        OrganizationInvitation.org_id == org_id,
        OrganizationInvitation.used_at.is_(None),
    ).first()
    if not inv:
        raise HTTPException(status_code=404, detail="Invitation not found or already used")
    db.delete(inv)
    db.commit()
    return None


@router.post("/{org_id}/add-system-owner", status_code=status.HTTP_200_OK)
def add_system_owner_to_org(
    org_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Add the system owner (SUDO_ADMIN_EMAIL) to this organization as admin. Org admin/owner only."""
    _require_org_admin(db, current_user, org_id)
    org = db.query(Organization).filter(Organization.id == org_id).first()
    if not org:
        raise HTTPException(status_code=404, detail="Organization not found")
    sudo_email = (getattr(settings, "SUDO_ADMIN_EMAIL", None) or "").strip().lower()
    if not sudo_email:
        raise HTTPException(status_code=500, detail="System owner email not configured")

    # Find system owner user (they may exist in main org)
    sudo_user = db.query(User).filter(func.lower(User.email) == sudo_email).first()
    if not sudo_user:
        raise HTTPException(
            status_code=404,
            detail="System owner user not found. They must have an account first.",
        )

    existing_uo = db.query(UserOrganization).filter(
        UserOrganization.user_id == sudo_user.id,
        UserOrganization.org_id == org_id,
    ).first()
    if existing_uo:
        return {"message": "System owner is already in this organization"}

    uo = UserOrganization(
        user_id=sudo_user.id,
        org_id=org_id,
        is_primary=False,
    )
    db.add(uo)
    db.commit()
    return {"message": "System owner has been added to your organization"}

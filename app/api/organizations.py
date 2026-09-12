"""
Organization-scoped endpoints: invite users, list invitations, add system owner.
Requires current user to have access to the org and be admin/owner.
"""
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from sqlalchemy import func
from datetime import datetime
from typing import List
from uuid import UUID
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
from app.schemas.notification_settings import (
    NotificationSettingsResponse,
    NotificationSettingsUpdate,
    NotificationTestResponse,
    FunnelLeadNotificationSettings,
)
from app.services.funnel_lead_notifications import (
    ensure_funnel_lead_notifications_schema,
    get_funnel_lead_settings,
    merge_funnel_lead_settings,
    send_test_digest,
)

router = APIRouter()


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
    from app.services.org_user_context import user_can_manage_org_integrations

    scoped = user
    scoped.selected_org_id = org_id
    if not user_can_manage_org_integrations(scoped, db):
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

    role = _normalize_role(body.role or "member")
    if role == "owner" and current_user.role != UserRole.OWNER:
        raise HTTPException(status_code=403, detail="Only owners can invite as owner")

    from app.services.organization_invitations import (
        create_user_invitation,
        invitation_accept_link,
        invitation_response,
    )
    from app.services.onboarding_email import send_user_invitation_email

    inv = create_user_invitation(
        db,
        org=org,
        email=body.email,
        role=role,
        created_by=current_user.id,
    )
    email_sent = False
    if body.send_email:
        email_sent = bool(
            send_user_invitation_email(
                to_email=inv.invitee_email,
                org_name=org.name,
                invitation_link=invitation_accept_link(inv.token),
                role=inv.role,
                inviter_name=current_user.email,
                existing_user=False,
            )
        )
    return invitation_response(inv, email_sent=email_sent)


@router.get("/{org_id}/invitations", response_model=List[InvitationResponse])
def list_org_invitations(
    org_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """List pending invitations for this organization."""
    from app.services.organization_invitations import invitation_response, pending_invitation_filters

    _require_org_admin(db, current_user, org_id)
    invs = (
        db.query(OrganizationInvitation)
        .filter(
            OrganizationInvitation.org_id == org_id,
            *pending_invitation_filters(),
        )
        .order_by(OrganizationInvitation.created_at.desc())
        .all()
    )
    return [invitation_response(i) for i in invs]


@router.post("/{org_id}/invitations/{invitation_id}/resend", response_model=InvitationResponse)
@rate_limit(max_requests=10, window_seconds=900)  # 10 resends per 15 min per user — avoid inbox-bombing an invitee
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
    from app.services.organization_invitations import invitation_accept_link, invitation_response
    from app.services.onboarding_email import send_user_invitation_email

    send_user_invitation_email(
        to_email=inv.invitee_email,
        org_name=org.name,
        invitation_link=invitation_accept_link(inv.token),
        role=inv.role,
        existing_user=False,
    )
    inv.updated_at = datetime.utcnow()
    db.commit()
    db.refresh(inv)
    return invitation_response(inv, email_sent=True)


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


@router.get(
    "/{org_id}/notification-settings",
    response_model=NotificationSettingsResponse,
)
def get_notification_settings(
    org_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Return effective org notification settings (merged with defaults)."""
    _require_org_admin(db, current_user, org_id)
    ensure_funnel_lead_notifications_schema(db)
    try:
        org = db.query(Organization).filter(Organization.id == org_id).first()
    except Exception:
        db.rollback()
        ensure_funnel_lead_notifications_schema(db)
        org = db.query(Organization).filter(Organization.id == org_id).first()
    if not org:
        raise HTTPException(status_code=404, detail="Organization not found")
    cfg = get_funnel_lead_settings(org)
    return NotificationSettingsResponse(
        funnel_leads=FunnelLeadNotificationSettings(**cfg),
    )


@router.patch(
    "/{org_id}/notification-settings",
    response_model=NotificationSettingsResponse,
)
def update_notification_settings(
    org_id: UUID,
    body: NotificationSettingsUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Update org notification settings (admin/owner only)."""
    _require_org_admin(db, current_user, org_id)
    ensure_funnel_lead_notifications_schema(db)
    try:
        org = db.query(Organization).filter(Organization.id == org_id).first()
    except Exception:
        db.rollback()
        ensure_funnel_lead_notifications_schema(db)
        org = db.query(Organization).filter(Organization.id == org_id).first()
    if not org:
        raise HTTPException(status_code=404, detail="Organization not found")
    patch = {}
    if body.funnel_leads is not None:
        patch = body.funnel_leads.model_dump(exclude_unset=True)
    try:
        cfg = merge_funnel_lead_settings(org, patch) if patch else get_funnel_lead_settings(org)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    db.commit()
    db.refresh(org)
    return NotificationSettingsResponse(
        funnel_leads=FunnelLeadNotificationSettings(**cfg),
    )


@router.post(
    "/{org_id}/notification-settings/test",
    response_model=NotificationTestResponse,
)
@rate_limit(max_requests=5, window_seconds=900)
def send_notification_settings_test(
    org_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Send a sample funnel-lead digest to resolved recipients."""
    _require_org_admin(db, current_user, org_id)
    org = db.query(Organization).filter(Organization.id == org_id).first()
    if not org:
        raise HTTPException(status_code=404, detail="Organization not found")
    result = send_test_digest(db, org)
    return NotificationTestResponse(**result)


@router.get("/{org_id}/timezone")
def get_org_timezone(
    org_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """IANA timezone used to localize notification timestamps (e.g. Discord bookings)."""
    if not _user_has_org_access(db, current_user, org_id):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="You do not have access to this organization")
    org = db.query(Organization).filter(Organization.id == org_id).first()
    if not org:
        raise HTTPException(status_code=404, detail="Organization not found")
    return {"timezone": org.timezone or "UTC"}


@router.patch("/{org_id}/timezone")
def set_org_timezone(
    org_id: UUID,
    body: dict,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Set the org's IANA timezone (admin/owner only). Body: {"timezone": "America/New_York"}."""
    _require_org_admin(db, current_user, org_id)
    tz_name = str(body.get("timezone") or "").strip()
    if not tz_name:
        raise HTTPException(status_code=400, detail="timezone is required")
    from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

    try:
        ZoneInfo(tz_name)
    except ZoneInfoNotFoundError:
        raise HTTPException(status_code=400, detail=f"Unknown timezone: {tz_name}")
    org = db.query(Organization).filter(Organization.id == org_id).first()
    if not org:
        raise HTTPException(status_code=404, detail="Organization not found")
    org.timezone = tz_name
    org.updated_at = datetime.utcnow()
    db.commit()
    return {"timezone": org.timezone}

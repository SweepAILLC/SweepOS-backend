"""Create and serialize organization invitations (email or copyable link)."""
from __future__ import annotations

import secrets
from datetime import datetime, timedelta
from typing import Optional
from uuid import UUID

from fastapi import HTTPException, status
from sqlalchemy import func
from sqlalchemy.orm import Session

from app.core.config import settings
from app.models.organization import Organization
from app.models.organization_invitation import OrganizationInvitation
from app.models.user import User
from app.schemas.invitation import InvitationResponse
from app.services.onboarding_email import INVITATION_EXPIRES_DAYS


def invitation_is_expired(inv: OrganizationInvitation) -> bool:
    exp = getattr(inv, "expires_at", None)
    return bool(exp and exp <= datetime.utcnow())


def pending_invitation_filters():
    return (
        OrganizationInvitation.used_at.is_(None),
        (
            OrganizationInvitation.expires_at.is_(None)
            | (OrganizationInvitation.expires_at > datetime.utcnow())
        ),
    )


def resolve_invitation_expires_at(
    *,
    never_expires: bool = False,
    expires_at: Optional[datetime] = None,
    expires_in_days: Optional[int] = None,
    default_days: int = INVITATION_EXPIRES_DAYS,
) -> Optional[datetime]:
    if never_expires:
        return None
    if expires_at is not None:
        exp = expires_at.replace(tzinfo=None) if getattr(expires_at, "tzinfo", None) else expires_at
        if exp <= datetime.utcnow():
            raise HTTPException(status_code=400, detail="Expiration must be in the future")
        return exp
    if expires_in_days is not None:
        if expires_in_days <= 0:
            raise HTTPException(status_code=400, detail="expires_in_days must be positive")
        return datetime.utcnow() + timedelta(days=expires_in_days)
    return datetime.utcnow() + timedelta(days=default_days)


def is_multi_use(inv: OrganizationInvitation) -> bool:
    return bool(getattr(inv, "multi_use", False))


def consume_invitation(inv: OrganizationInvitation) -> None:
    if is_multi_use(inv):
        return
    inv.used_at = datetime.utcnow()


def bound_invitee_email(inv: OrganizationInvitation) -> Optional[str]:
    raw = (getattr(inv, "invitee_email", None) or "").strip().lower()
    return raw or None


def resolve_invitation_email(
    inv: OrganizationInvitation,
    provided: Optional[str] = None,
) -> str:
    """Bound invite email, or the address the acceptor supplies for an open link."""
    bound = bound_invitee_email(inv)
    given = (provided or "").strip().lower()
    if bound:
        if given and given != bound:
            raise HTTPException(status_code=400, detail="Email does not match this invitation")
        return bound
    if not given:
        raise HTTPException(status_code=400, detail="Email is required to create your account")
    return given


def ensure_invite_organization(
    db: Session,
    inv: OrganizationInvitation,
    org_name: Optional[str] = None,
    consulting_tier: Optional[str] = None,
) -> Organization:
    """Return the invite's org, creating it on accept when the link had no org."""
    if inv.org_id:
        org = db.query(Organization).filter(Organization.id == inv.org_id).first()
        if not org:
            raise HTTPException(status_code=400, detail="Organization not found")
        return org
    name = (org_name or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="Organization name is required")
    tier = (consulting_tier or "").strip() or None
    if tier not in ("pro_consulting", "core_consulting"):
        tier = None
    org = Organization(name=name[:255], consulting_tier=tier)
    db.add(org)
    db.flush()
    if not is_multi_use(inv):
        inv.org_id = org.id
    return org


def invitation_accept_link(token: str) -> str:
    frontend_url = (getattr(settings, "FRONTEND_URL", None) or "http://localhost:3003").rstrip("/")
    return f"{frontend_url}/invite/accept?token={token}"


def invitation_response(
    inv: OrganizationInvitation,
    *,
    email_sent: Optional[bool] = None,
) -> InvitationResponse:
    return InvitationResponse(
        id=inv.id,
        org_id=inv.org_id,
        invitee_email=inv.invitee_email,
        invitation_type=inv.invitation_type,
        role=inv.role,
        expires_at=inv.expires_at,
        used_at=inv.used_at,
        created_at=inv.created_at,
        invitation_link=invitation_accept_link(inv.token),
        email_sent=email_sent,
        multi_use=is_multi_use(inv),
    )


def create_user_invitation(
    db: Session,
    *,
    org: Organization,
    email: str,
    role: str,
    created_by: UUID,
) -> OrganizationInvitation:
    """Create a pending USER invitation. Does not send email."""
    email_normalized = (email or "").strip().lower()
    if not email_normalized:
        raise HTTPException(status_code=400, detail="Email is required")
    role_normalized = (role or "member").strip().lower()
    if role_normalized not in ("owner", "admin", "member"):
        role_normalized = "member"

    if org.max_user_seats is not None:
        current_count = db.query(func.count(User.id)).filter(User.org_id == org.id).scalar() or 0
        if current_count >= org.max_user_seats:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=(
                    f"Organization user limit reached ({org.max_user_seats} seats). "
                    "Contact your system owner to increase the limit."
                ),
            )

    existing_in_org = (
        db.query(User)
        .filter(func.lower(User.email) == email_normalized, User.org_id == org.id)
        .first()
    )
    if existing_in_org:
        raise HTTPException(
            status_code=400,
            detail="A user with this email is already in this organization",
        )

    existing_inv = (
        db.query(OrganizationInvitation)
        .filter(
            OrganizationInvitation.org_id == org.id,
            func.lower(OrganizationInvitation.invitee_email) == email_normalized,
            OrganizationInvitation.used_at.is_(None),
            OrganizationInvitation.expires_at > datetime.utcnow(),
        )
        .first()
    )
    if existing_inv:
        raise HTTPException(
            status_code=400,
            detail="An invitation for this email is already pending",
        )

    token = secrets.token_urlsafe(32)
    now = datetime.utcnow()
    inv = OrganizationInvitation(
        org_id=org.id,
        invitee_email=email_normalized,
        invitation_type="USER",
        role=role_normalized,
        token=token,
        expires_at=now + timedelta(days=INVITATION_EXPIRES_DAYS),
        created_by=created_by,
        created_at=now,
        updated_at=now,
    )
    db.add(inv)
    db.commit()
    db.refresh(inv)
    return inv

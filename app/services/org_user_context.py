"""Resolve per-org user rows (role/id) when one email has multiple users rows."""

from __future__ import annotations

import uuid
from typing import Any, Optional, Tuple

from sqlalchemy import func, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.models.user import UserRole, parse_user_role_from_api, parse_user_role_from_db


def fetch_user_row_for_org(
    db: Session,
    email: str,
    org_id: uuid.UUID,
) -> Optional[Tuple[Any, ...]]:
    """Return the users-table row for this email in the given org, if one exists."""
    if not email or not org_id:
        return None
    return db.execute(
        text(
            """
            SELECT id, org_id, email, hashed_password, role, is_admin, created_at, fathom_api_key
            FROM users
            WHERE LOWER(email) = LOWER(:email) AND org_id = :org_id
            LIMIT 1
            """
        ),
        {"email": email.strip(), "org_id": str(org_id)},
    ).fetchone()


def user_has_email_org_access(db: Session, email: str, org_id: uuid.UUID) -> bool:
    """True when any users row for this email is linked to the org."""
    if not email or not org_id:
        return False
    row = db.execute(
        text(
            """
            SELECT 1
            FROM user_organizations uo
            INNER JOIN users u ON u.id = uo.user_id
            WHERE LOWER(u.email) = LOWER(:email) AND uo.org_id = :org_id
            LIMIT 1
            """
        ),
        {"email": email.strip(), "org_id": str(org_id)},
    ).fetchone()
    return row is not None


def _latest_used_invitation_role(db: Session, email: str, org_id: uuid.UUID) -> Optional[str]:
    from app.models.organization_invitation import OrganizationInvitation

    inv = (
        db.query(OrganizationInvitation)
        .filter(
            OrganizationInvitation.org_id == org_id,
            func.lower(OrganizationInvitation.invitee_email) == email.strip().lower(),
            OrganizationInvitation.used_at.isnot(None),
        )
        .order_by(OrganizationInvitation.used_at.desc())
        .first()
    )
    if not inv:
        return None
    role = (inv.role or "member").strip().lower()
    return role if role in ("owner", "admin", "member") else "member"


def _role_from_invitation(db: Session, email: str, org_id: uuid.UUID) -> Optional[UserRole]:
    role_str = _latest_used_invitation_role(db, email, org_id)
    if not role_str:
        return None
    return parse_user_role_from_api(role_str)


def materialize_org_user_row_if_missing(
    db: Session,
    email: str,
    org_id: uuid.UUID,
) -> Optional[Tuple[Any, ...]]:
    """
    Backfill a per-org users row when legacy invites only created UserOrganization links.
    Only call from explicit user actions (org switch, invite accept) — not per request.
    """
    row = fetch_user_row_for_org(db, email, org_id)
    if row or not user_has_email_org_access(db, email, org_id):
        return row

    pwd_row = db.execute(
        text("SELECT hashed_password FROM users WHERE LOWER(email) = LOWER(:email) LIMIT 1"),
        {"email": email.strip()},
    ).fetchone()
    if not pwd_row:
        return None

    role_str = _latest_used_invitation_role(db, email, org_id) or "member"
    user_role = parse_user_role_from_api(role_str)
    new_user_id = uuid.uuid4()
    try:
        db.execute(
            text(
                """
                INSERT INTO users (id, org_id, email, hashed_password, role, is_admin, created_at)
                VALUES (:id, :org_id, :email, :hashed_password, CAST(:role AS userrole), :is_admin, NOW())
                """
            ),
            {
                "id": new_user_id,
                "org_id": str(org_id),
                "email": email.strip().lower(),
                "hashed_password": pwd_row[0],
                "role": user_role.value,
                "is_admin": user_role in (UserRole.ADMIN, UserRole.OWNER),
            },
        )
        db.flush()
    except IntegrityError:
        db.rollback()
    return fetch_user_row_for_org(db, email, org_id)


def apply_selected_org_user_context(user: Any, db: Session, selected_org_id: uuid.UUID) -> Any:
    """
    When JWT org_id differs from the loaded user row's org_id, swap id/role/is_admin
    to the users row for (email, selected_org_id). Keeps integration permissions org-scoped.
    """
    if not selected_org_id:
        return user

    # Fast path: token user_id already belongs to the selected org (most requests).
    if str(getattr(user, "org_id", None)) == str(selected_org_id):
        return user

    row = fetch_user_row_for_org(db, user.email, selected_org_id)
    if row:
        user.id = row[0]
        user.org_id = row[1]
        user.role_str = row[4]
        user.role = parse_user_role_from_db(row[4])
        user.is_admin = row[5]
        if hasattr(user, "fathom_api_key"):
            user.fathom_api_key = row[7] if len(row) > 7 else None
        return user

    if user_has_email_org_access(db, user.email, selected_org_id):
        invited_role = _role_from_invitation(db, user.email, selected_org_id)
        if invited_role is not None:
            user.role = invited_role
            user.is_admin = invited_role in (UserRole.ADMIN, UserRole.OWNER)
    return user


def user_can_manage_org_integrations(user: Any, db: Session) -> bool:
    """True when the user is admin/owner for their currently selected org."""
    from app.core.config import settings

    if user.email == settings.SUDO_ADMIN_EMAIL:
        return True

    selected_org_id = getattr(user, "selected_org_id", None) or user.org_id
    if not isinstance(selected_org_id, uuid.UUID):
        selected_org_id = uuid.UUID(str(selected_org_id))

    if str(getattr(user, "org_id", None)) == str(selected_org_id):
        role = user.role
        is_admin = bool(getattr(user, "is_admin", False))
    else:
        row = fetch_user_row_for_org(db, user.email, selected_org_id)
        if row:
            role = parse_user_role_from_db(row[4])
            is_admin = bool(row[5])
        elif user_has_email_org_access(db, user.email, selected_org_id):
            invited_role = _role_from_invitation(db, user.email, selected_org_id)
            role = invited_role or user.role
            is_admin = role in (UserRole.ADMIN, UserRole.OWNER)
        else:
            role = user.role
            is_admin = bool(getattr(user, "is_admin", False))

    return role in (UserRole.ADMIN, UserRole.OWNER) or is_admin

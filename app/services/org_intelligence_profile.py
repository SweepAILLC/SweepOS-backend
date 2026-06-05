"""Org-scoped Intelligence bank (ai_profile) for multi-org users."""
from __future__ import annotations

import uuid
from typing import Any, Dict, Optional, Tuple

from sqlalchemy import func
from sqlalchemy.orm import Session
from sqlalchemy.orm.attributes import flag_modified

from app.models.user import User
from app.models.user_organization import UserOrganization


def _selected_org_id(user: Any) -> uuid.UUID:
    raw = getattr(user, "selected_org_id", None) or getattr(user, "org_id", None)
    if isinstance(raw, uuid.UUID):
        return raw
    return uuid.UUID(str(raw))


def _normalized_email(user: Any) -> str:
    return str(getattr(user, "email", "") or "").strip().lower()


def _user_row_in_org(db: Session, email: str, org_id: uuid.UUID) -> Optional[User]:
    if not email:
        return None
    return (
        db.query(User)
        .filter(func.lower(User.email) == email, User.org_id == org_id)
        .order_by(User.created_at.asc())
        .first()
    )


def _user_org_link(
    db: Session,
    user_id: uuid.UUID,
    org_id: uuid.UUID,
) -> Optional[UserOrganization]:
    return (
        db.query(UserOrganization)
        .filter(
            UserOrganization.user_id == user_id,
            UserOrganization.org_id == org_id,
        )
        .first()
    )


def resolve_org_intelligence_target(
    db: Session,
    user: Any,
    *,
    selected_org_id: Optional[uuid.UUID] = None,
) -> Tuple[str, Any]:
    """
    Return (kind, row) where kind is 'user' or 'user_org' and row holds ai_profile.

    Prefer a users row in the selected org (same email). Fall back to the
    user_organizations junction when the account was invited without a per-org user row.
    """
    org_id = selected_org_id or _selected_org_id(user)
    email = _normalized_email(user)
    uid = user.id if isinstance(user.id, uuid.UUID) else uuid.UUID(str(user.id))

    org_user = _user_row_in_org(db, email, org_id)
    if org_user is not None:
        return "user", org_user

    link = _user_org_link(db, uid, org_id)
    if link is not None:
        return "user_org", link

    canonical = db.query(User).filter(User.id == uid).first()
    if canonical is not None and canonical.org_id == org_id:
        return "user", canonical

    raise ValueError("No intelligence profile target for user/org")


def get_org_ai_profile(
    db: Session,
    user: Any,
    *,
    selected_org_id: Optional[uuid.UUID] = None,
) -> Optional[Dict[str, Any]]:
    try:
        kind, row = resolve_org_intelligence_target(db, user, selected_org_id=selected_org_id)
    except ValueError:
        return None
    raw = getattr(row, "ai_profile", None)
    return raw if isinstance(raw, dict) else None


def set_org_ai_profile(
    db: Session,
    user: Any,
    profile: Dict[str, Any],
    *,
    selected_org_id: Optional[uuid.UUID] = None,
) -> None:
    kind, row = resolve_org_intelligence_target(db, user, selected_org_id=selected_org_id)
    row.ai_profile = profile
    if hasattr(row, "_sa_instance_state"):
        flag_modified(row, "ai_profile")


def resolve_org_intelligence_user_row(
    db: Session,
    user: Any,
    *,
    selected_org_id: Optional[uuid.UUID] = None,
) -> User:
    """
    Return a User ORM row whose ai_profile reflects the selected org.

    When the profile lives on user_organizations, overlay it on the canonical user row
    for read paths that expect a User instance (content studio, performance).
    """
    org_id = selected_org_id or _selected_org_id(user)
    uid = user.id if isinstance(user.id, uuid.UUID) else uuid.UUID(str(user.id))
    email = _normalized_email(user)

    org_user = _user_row_in_org(db, email, org_id)
    if org_user is not None:
        return org_user

    canonical = db.query(User).filter(User.id == uid).first()
    if canonical is None:
        raise ValueError("User not found")

    link = _user_org_link(db, uid, org_id)
    if link is not None:
        canonical.ai_profile = link.ai_profile if isinstance(link.ai_profile, dict) else None
        return canonical

    if canonical.org_id == org_id:
        return canonical

    canonical.ai_profile = None
    return canonical

"""Generic org-member option list, for pickers that need to attribute an action to a real user
(e.g. the KPI entry-form rep picker). Same logic as close_survey_service._list_org_closers —
kept as an independent implementation rather than a cross-import so the close-survey flow's
behavior can't be affected by changes made here, or vice versa."""
from __future__ import annotations

import uuid
from typing import Dict, List

from sqlalchemy.orm import Session

from app.models.user import User, role_to_api
from app.models.user_organization import UserOrganization
from app.schemas.kpi import KpiRepOption


def user_display_name(user: User) -> str:
    email = (getattr(user, "email", None) or "").strip()
    if email and "@" in email:
        return email.split("@")[0]
    return email or "Member"


def list_org_member_options(db: Session, org_id: uuid.UUID) -> List[KpiRepOption]:
    """Union of users.home-org + UserOrganization members — any org member is a valid rep."""
    by_id: Dict[str, User] = {}
    for u in db.query(User).filter(User.org_id == org_id).all():
        by_id[str(u.id)] = u
    for u in (
        db.query(User)
        .join(UserOrganization, UserOrganization.user_id == User.id)
        .filter(UserOrganization.org_id == org_id)
        .all()
    ):
        by_id[str(u.id)] = u

    opts: List[KpiRepOption] = []
    for u in by_id.values():
        try:
            role = role_to_api(u.role) if u.role is not None else "member"
        except Exception:
            role = "member"
        opts.append(
            KpiRepOption(
                id=str(u.id),
                name=user_display_name(u),
                email=(u.email or None),
                role=role,
            )
        )
    opts.sort(key=lambda o: (o.name or "").lower())
    return opts

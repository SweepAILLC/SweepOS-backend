"""Team KPIs API (docs/features/TEAM_KPIS_PRD.md) — roster, team overview, settings."""
from __future__ import annotations

import uuid
from datetime import timedelta
from typing import List, Literal, Optional

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.api.deps import get_current_user, require_admin_or_owner
from app.db.session import get_db
from app.models.user import User
from app.services import team_kpis

router = APIRouter()

TeamRole = Literal["sales", "marketing"]


def _org_id(user: User) -> uuid.UUID:
    raw = getattr(user, "selected_org_id", None) or user.org_id
    return raw if isinstance(raw, uuid.UUID) else uuid.UUID(str(raw))


class TeamMember(BaseModel):
    user_id: uuid.UUID
    name: str
    email: Optional[str] = None
    access_role: str
    team_role: Optional[TeamRole] = None
    owes_eod: bool = False


class TeamRoleUpdate(BaseModel):
    team_role: Optional[TeamRole] = None  # null = no rep type


@router.get("/members", response_model=List[TeamMember])
def get_team_members(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    return [TeamMember(**m) for m in team_kpis.list_team_members(db, _org_id(current_user))]


@router.put("/members/{user_id}/role", response_model=TeamMember)
def put_team_member_role(
    user_id: uuid.UUID,
    body: TeamRoleUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin_or_owner),
):
    try:
        member = team_kpis.set_team_role(db, _org_id(current_user), user_id, body.team_role)
    except team_kpis.TeamMemberNotFound:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Not a member of this organization")
    return TeamMember(**member)


# --- Team overview (accountability + performance in one view) ----------------


@router.get("/overview")
def get_team_overview(
    period: str = "month",
    anchor: Optional[str] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Per sales-team member for a week or month: EOD (setters), closer activity,
    and each metric vs the same span of the previous period (+ best month).
    `anchor` = any YYYY-MM-DD inside the period; defaults to today in org time."""
    from datetime import date as date_type

    if period not in ("week", "month"):
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="period must be week or month")
    org_id = _org_id(current_user)
    if end:
        # Shared date-range filter: explicit inclusive local dates (start omitted = 1 year back).
        from app.services.date_window import parse_ymd

        end_day = parse_ymd(end, "end")
        start_day = parse_ymd(start, "start") or (end_day - timedelta(days=364))
        if start_day > end_day:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="start must be on or before end")
        return team_kpis.compute_team_overview(db, org_id, "range", end_day, bounds=(start_day, end_day))
    if anchor:
        try:
            anchor_day = date_type.fromisoformat(anchor)
        except ValueError:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="anchor must be YYYY-MM-DD")
    else:
        anchor_day = team_kpis.org_today(db, org_id)
    return team_kpis.compute_team_overview(db, org_id, period, anchor_day)


# --- Settings -----------------------------------------------------------------


class TeamSettings(BaseModel):
    eod_required_weekdays: List[int]
    reminder_enabled: bool
    reminder_local_time: str
    reminder_channels: List[str]
    digest_enabled: bool
    digest_local_time: str
    timezone: str


def _settings_response(db: Session, org_id: uuid.UUID) -> TeamSettings:
    from app.models.organization import Organization

    tz = db.query(Organization.timezone).filter(Organization.id == org_id).scalar() or "UTC"
    return TeamSettings(**team_kpis.get_team_settings(db, org_id), timezone=tz)


@router.get("/settings", response_model=TeamSettings)
def get_team_kpi_settings(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    return _settings_response(db, _org_id(current_user))


@router.put("/settings", response_model=TeamSettings)
def put_team_kpi_settings(
    body: dict,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin_or_owner),
):
    """Partial update. Reminders and the digest send to the real team, so both default off."""
    org_id = _org_id(current_user)
    body = {k: v for k, v in body.items() if k != "timezone"}  # org timezone is edited on the org, not here
    try:
        team_kpis.update_team_settings(db, org_id, body)
    except ValueError as e:
        db.rollback()
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    return _settings_response(db, org_id)

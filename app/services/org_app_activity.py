"""In-app activity heartbeats and org time-on-app rollups."""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from uuid import UUID

from sqlalchemy import func
from sqlalchemy.orm import Session

from app.models.org_app_session import OrgAppSession
from app.models.organization import Organization

HEARTBEAT_GAP = timedelta(seconds=150)
ONLINE_WINDOW = timedelta(seconds=150)


def record_heartbeat(db: Session, *, org_id: UUID, user_id: UUID, now: Optional[datetime] = None) -> None:
    now = now or datetime.utcnow()
    row = (
        db.query(OrgAppSession)
        .filter(OrgAppSession.org_id == org_id, OrgAppSession.user_id == user_id)
        .order_by(OrgAppSession.last_seen_at.desc())
        .first()
    )
    if row and (now - (row.last_seen_at or now)) <= HEARTBEAT_GAP:
        row.last_seen_at = now
        return
    db.add(OrgAppSession(org_id=org_id, user_id=user_id, started_at=now, last_seen_at=now))


def _seconds_by_org(db: Session, window_start: datetime, now: datetime) -> Dict[UUID, int]:
    overlap_start = func.greatest(OrgAppSession.started_at, window_start)
    overlap_end = func.least(OrgAppSession.last_seen_at, now)
    seconds = func.extract("epoch", overlap_end - overlap_start)
    rows = (
        db.query(OrgAppSession.org_id, func.coalesce(func.sum(seconds), 0))
        .filter(OrgAppSession.last_seen_at >= window_start, OrgAppSession.started_at <= now)
        .group_by(OrgAppSession.org_id)
        .all()
    )
    out: Dict[UUID, int] = {}
    for org_id, total in rows:
        out[org_id] = max(int(total or 0), 0)
    return out


def _last_seen_by_org(db: Session) -> Dict[UUID, datetime]:
    rows = (
        db.query(OrgAppSession.org_id, func.max(OrgAppSession.last_seen_at))
        .group_by(OrgAppSession.org_id)
        .all()
    )
    return {org_id: ts for org_id, ts in rows if ts}


def _online_users_by_org(db: Session, now: datetime) -> Dict[UUID, int]:
    cutoff = now - ONLINE_WINDOW
    rows = (
        db.query(OrgAppSession.org_id, func.count(func.distinct(OrgAppSession.user_id)))
        .filter(OrgAppSession.last_seen_at >= cutoff)
        .group_by(OrgAppSession.org_id)
        .all()
    )
    return {org_id: int(n or 0) for org_id, n in rows}


def activity_maps(db: Session, now: Optional[datetime] = None) -> Tuple[Dict[UUID, int], Dict[UUID, int], Dict[UUID, datetime], Dict[UUID, int]]:
    now = now or datetime.utcnow()
    sec_7 = _seconds_by_org(db, now - timedelta(days=7), now)
    sec_30 = _seconds_by_org(db, now - timedelta(days=30), now)
    last_seen = _last_seen_by_org(db)
    online = _online_users_by_org(db, now)
    return sec_7, sec_30, last_seen, online


def health_activity_rows(db: Session, now: Optional[datetime] = None) -> Tuple[list, int, int, int, int]:
    now = now or datetime.utcnow()
    sec_7, sec_30, last_seen, online = activity_maps(db, now)
    orgs = db.query(Organization.id, Organization.name, Organization.consulting_tier).all()
    rows: List[dict] = []
    online_orgs = 0
    online_users = 0
    for org_id, name, tier in orgs:
        n_online = online.get(org_id, 0)
        if n_online:
            online_orgs += 1
            online_users += n_online
        rows.append(
            {
                "org_id": str(org_id),
                "organization_name": name,
                "consulting_tier": tier,
                "active_seconds_7d": sec_7.get(org_id, 0),
                "active_seconds_30d": sec_30.get(org_id, 0),
                "last_seen_at": last_seen.get(org_id),
                "currently_online": n_online > 0,
                "online_users": n_online,
            }
        )
    rows.sort(key=lambda r: r["active_seconds_7d"], reverse=True)
    total_7 = sum(r["active_seconds_7d"] for r in rows)
    total_30 = sum(r["active_seconds_30d"] for r in rows)
    return rows, online_orgs, online_users, total_7, total_30

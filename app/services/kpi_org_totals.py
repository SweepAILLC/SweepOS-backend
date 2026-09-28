"""One daily ledger (docs/features/TEAM_KPIS_PRD.md → "one daily ledger").

The org day is folded from that day's rows the same way everywhere:
- activity fields (what setters log on their EOD) = org row + every per-rep row;
- org-wide fields (calendar counts, cash, revenue, followers, content) = org row only.

Calendar sync writes org-wide counts to the org row *and* per-host counts to each
closer's row, so summing every row double-counts calls/closes; ignoring per-rep
rows drops setters' EOD activity. Every org-level reader goes through here.
"""
from __future__ import annotations

import uuid
from datetime import date, datetime
from typing import Any, Dict, Iterable, List, Optional

ACTIVITY_FIELDS = (
    "inboxes_checked",
    "outreach_sent",
    "respondents",
    "inbound_icp_leads",
    "followups_sent",
    "new_conversations",
    "conversations_nurtured",
    "calls_pitched",
    "inbound_bookings",
    "outbound_bookings",
    "offers_made",
)

_ORG_ONLY_FIELDS = (
    "total_followers",
    "new_followers",
    "content_posted",
    "best_content_type",
    "calls_booked",
    "calls_booked_activity",
    "calls_taken",
    "no_shows",
    "closes",
    "cash_collected",
    "revenue",
    "setter_context",
    "setter_booked_client_ids",
    "submitted_at",
)

_NAMESPACE = uuid.UUID("5b1f8a2e-9c3d-4e7a-8f10-2d6c4b9e7a11")


class OrgDay:
    """Row-shaped org day: exposes the same attributes as OrgKpiDailyEntry."""

    def __init__(self, org_id: uuid.UUID, entry_date: date):
        self.org_id = org_id
        self.entry_date = entry_date
        self.rep_user_id = None
        # Synthetic days (team EODs but no org row) get a stable id per org+date.
        self.id = uuid.uuid5(_NAMESPACE, f"{org_id}:{entry_date.isoformat()}")
        self.created_at: Optional[datetime] = None
        self.updated_at: Optional[datetime] = None
        self.has_org_row = False
        # Per-rep contribution to each activity field (drives "+N from team EODs").
        self.team_eod_totals: Dict[str, int] = {}
        for f in ACTIVITY_FIELDS + _ORG_ONLY_FIELDS:
            setattr(self, f, None)


def fold_org_daily_totals(rows: Iterable[Any]) -> List[OrgDay]:
    """Fold any mix of org + per-rep rows into one OrgDay per (org, date), oldest first."""
    days: Dict[tuple, OrgDay] = {}
    for r in rows:
        key = (r.org_id, r.entry_date)
        day = days.get(key)
        if day is None:
            day = days[key] = OrgDay(r.org_id, r.entry_date)
        if getattr(r, "rep_user_id", None) is None:
            day.has_org_row = True
            day.id = r.id
            for f in _ORG_ONLY_FIELDS + ACTIVITY_FIELDS:
                setattr(day, f, getattr(r, f, None))
            day.created_at, day.updated_at = getattr(r, "created_at", None), getattr(r, "updated_at", None)
        else:
            for f in ACTIVITY_FIELDS:
                v = getattr(r, f, None)
                if v is not None:
                    day.team_eod_totals[f] = day.team_eod_totals.get(f, 0) + int(v)
            if not day.has_org_row:
                for ts in ("created_at", "updated_at"):
                    v = getattr(r, ts, None)
                    cur = getattr(day, ts)
                    if v is not None and (cur is None or (ts == "created_at" and v < cur) or (ts == "updated_at" and v > cur)):
                        setattr(day, ts, v)
    for day in days.values():
        for f, team in day.team_eod_totals.items():
            base = getattr(day, f)
            setattr(day, f, (int(base) if base is not None else 0) + team)
        now = datetime.utcnow()
        day.created_at = day.created_at or now
        day.updated_at = day.updated_at or day.created_at
    return sorted(days.values(), key=lambda d: (str(d.org_id), d.entry_date))

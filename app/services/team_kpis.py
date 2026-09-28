"""Team KPIs service (docs/features/TEAM_KPIS_PRD.md): roster + roles.

Team members are org members (list_org_member_options — the same list the EOD
form's rep picker uses); the sales role lives on user_organizations.team_role.
"""
from __future__ import annotations

import uuid
from datetime import date, datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

from sqlalchemy.orm import Session

from app.models.team_kpi import TEAM_ROLES
from app.models.user import User
from app.models.user_organization import UserOrganization
from app.services.org_members import list_org_member_options

# Roles that owe a daily EOD (PRD decision: setters only; closers' numbers come from the calendar).
EOD_REQUIRED_ROLES = ("sales",)


class TeamMemberNotFound(ValueError):
    """user_id is not a member of the org."""


def _roles_by_user(db: Session, org_id: uuid.UUID) -> Dict[uuid.UUID, Optional[str]]:
    return {
        user_id: team_role
        for user_id, team_role in db.query(UserOrganization.user_id, UserOrganization.team_role)
        .filter(UserOrganization.org_id == org_id)
        .all()
    }


def list_team_members(db: Session, org_id: uuid.UUID) -> List[Dict[str, object]]:
    """Every org member with their access role and sales role (None = not on the sales team)."""
    roles = _roles_by_user(db, org_id)
    out: List[Dict[str, object]] = []
    for opt in list_org_member_options(db, org_id):
        uid = uuid.UUID(opt.id)
        team_role = roles.get(uid)
        out.append(
            {
                "user_id": uid,
                "name": opt.name,
                "email": opt.email,
                "access_role": opt.role,
                "team_role": team_role,
                "owes_eod": team_role in EOD_REQUIRED_ROLES,
            }
        )
    return out


def set_team_role(db: Session, org_id: uuid.UUID, user_id: uuid.UUID, team_role: Optional[str]) -> Dict[str, object]:
    """
    Set (or clear with None) a member's sales role. Members who belong to the org
    only via users.org_id get their membership row created here — the same repair
    login already performs (app/api/auth.py) — since team_role lives on that row.
    """
    if team_role is not None and team_role not in TEAM_ROLES:
        raise ValueError(f"team_role must be one of {', '.join(TEAM_ROLES)} or null")
    members = {m["user_id"]: m for m in list_team_members(db, org_id)}
    if user_id not in members:
        raise TeamMemberNotFound(str(user_id))

    row = (
        db.query(UserOrganization)
        .filter(UserOrganization.org_id == org_id, UserOrganization.user_id == user_id)
        .first()
    )
    if row is None:
        home_org = db.query(User.org_id).filter(User.id == user_id).scalar()
        row = UserOrganization(user_id=user_id, org_id=org_id, is_primary=(home_org == org_id))
        db.add(row)
    row.team_role = team_role
    db.commit()
    member = dict(members[user_id])
    member["team_role"] = team_role
    member["owes_eod"] = team_role in EOD_REQUIRED_ROLES
    return member


# ---------------------------------------------------------------------------
# Settings (read side; writes in the settings API)
# ---------------------------------------------------------------------------

DEFAULT_SETTINGS: Dict[str, object] = {
    "eod_required_weekdays": [0, 1, 2, 3, 4],  # Mon–Fri (Python weekday numbers)
    "reminder_enabled": False,
    "reminder_local_time": "18:00",
    "reminder_channels": ["discord"],
    "digest_enabled": False,
    "digest_local_time": "09:00",  # Mondays
}


def get_team_settings(db: Session, org_id: uuid.UUID) -> Dict[str, object]:
    from app.models.team_kpi import TeamKpiSettings

    stored = db.query(TeamKpiSettings.settings).filter(TeamKpiSettings.org_id == org_id).scalar()
    merged = dict(DEFAULT_SETTINGS)
    if isinstance(stored, dict):
        merged.update({k: v for k, v in stored.items() if k in DEFAULT_SETTINGS})
    return merged


def org_today(db: Session, org_id: uuid.UUID, now: Optional[datetime] = None) -> date:
    """Today's date in the org's timezone (EOD days are local days)."""
    from zoneinfo import ZoneInfo

    from app.models.organization import Organization

    tz_name = db.query(Organization.timezone).filter(Organization.id == org_id).scalar() or "UTC"
    try:
        tz = ZoneInfo(tz_name)
    except Exception:
        tz = ZoneInfo("UTC")
    return (now or datetime.now(timezone.utc)).astimezone(tz).date()


# ---------------------------------------------------------------------------
# Accountability: EOD tracker + activity vs previous span
# ---------------------------------------------------------------------------

# Fields a person types on the EOD form. Calendar sync fills calls/closes/cash and
# new_followers, so those never count as "the rep submitted" for legacy rows.
_EOD_MANUAL_FIELDS = (
    "total_followers", "content_posted", "best_content_type", "inboxes_checked", "outreach_sent",
    "respondents", "inbound_icp_leads", "followups_sent", "new_conversations", "conversations_nurtured",
    "calls_pitched", "inbound_bookings", "outbound_bookings", "offers_made", "revenue", "setter_context",
)
_STREAK_LOOKBACK_DAYS = 120


def _has_manual_fields(row: object) -> bool:
    for f in _EOD_MANUAL_FIELDS:
        v = getattr(row, f, None)
        if v not in (None, "", False):
            return True
    return False


def submitted_days(rows: List[object], stamp_cutoff: Optional[date]) -> Dict[uuid.UUID, set]:
    """
    rep_user_id -> set of dates with a submitted EOD. Stamped rows are authoritative.
    Before the org's first stamped submission (stamp_cutoff), a row counts as
    submitted if it carries any manually entered field — otherwise every member
    would start with a zero streak the day this ships.
    """
    out: Dict[uuid.UUID, set] = {}
    for r in rows:
        rep = getattr(r, "rep_user_id", None)
        if rep is None:
            continue
        stamped = getattr(r, "submitted_at", None) is not None
        legacy = stamp_cutoff is None or r.entry_date < stamp_cutoff
        if stamped or (legacy and _has_manual_fields(r)):
            out.setdefault(rep, set()).add(r.entry_date)
    return out


def period_bounds(period: str, anchor: date) -> "tuple[date, date]":
    """Mon–Sun week or calendar month containing `anchor`."""
    if period == "month":
        start = anchor.replace(day=1)
        nxt = (start.replace(day=28) + timedelta(days=4)).replace(day=1)
        return start, nxt - timedelta(days=1)
    start = anchor - timedelta(days=anchor.weekday())
    return start, start + timedelta(days=6)


def _required_days(start: date, end: date, required_weekdays: List[int]) -> List[date]:
    req = set(required_weekdays) or {0, 1, 2, 3, 4}
    days, d = [], start
    while d <= end:
        if d.weekday() in req:
            days.append(d)
        d += timedelta(days=1)
    return days


def eod_summary(
    submitted: set, today: date, period_start: date, period_end: date, required_weekdays: List[int]
) -> Dict[str, object]:
    """
    Submitted today, last submitted, streak, and — for the period — required days
    elapsed, submitted and missed. Today never counts as missed (the day isn't over).
    """
    req = set(required_weekdays) or {0, 1, 2, 3, 4}
    streak = 0
    d = today if today in submitted else today - timedelta(days=1)
    for _ in range(_STREAK_LOOKBACK_DAYS):
        if d.weekday() in req:
            if d not in submitted:
                break
            streak += 1
        d -= timedelta(days=1)

    required = _required_days(period_start, period_end, required_weekdays)
    due = [d for d in required if d < today]
    done = [d for d in due if d in submitted]

    def _day_status(d: date) -> str:
        if d in submitted:
            return "submitted"
        if d < today:
            return "missed"
        return "today" if d == today else "upcoming"

    return {
        # One entry per required day in the period, for the dot strip.
        "days": [{"date": d, "status": _day_status(d)} for d in required],
        "submitted_today": today in submitted,
        "last_submitted": max(submitted) if submitted else None,
        "streak": streak,
        "required_days": len(due),
        "submitted_days": len(done),
        "missed": len(due) - len(done),
    }


def period_pace(period_start: date, period_end: date, today: date, required_weekdays: List[int]) -> float:
    """Share of the period's required days already elapsed (1.0 for past periods)."""
    required = _required_days(period_start, period_end, required_weekdays)
    if not required:
        return 1.0
    return len([d for d in required if d <= today]) / len(required)


# Metrics shown per role in the Team view: (key, label, format, rep-performance field).
SETTER_VIEW_METRICS = [
    ("outreach_sent", "Outreach", "int", "outreach_sent"),
    ("respondents", "Replies", "int", "respondents"),
    ("reply_rate", "Reply %", "pct", "reply_rate_pct"),
    ("booked", "Booked", "int", "calls_booked_activity"),
    ("convo_to_book_rate", "Convo→Book %", "pct", "convo_to_booking_pct"),
]
CLOSER_VIEW_METRICS = [
    ("calls_taken", "Calls taken", "int", "calls_taken"),
    ("show_up_rate", "Show %", "pct", "show_up_pct"),
    ("closes", "Closes", "int", "closes"),
    ("close_rate", "Close %", "pct", "closing_rate_pct"),
    ("cash_collected", "Cash", "usd", "cash_collected_cents"),
]


def _metric_value(m: object, field: str) -> Optional[float]:
    if m is None:
        return None
    v = getattr(m, field, None)
    if v is None:
        return None
    return v / 100.0 if field == "cash_collected_cents" else float(v)


def _content_counts(notes: List[Dict[str, object]]) -> List[Dict[str, object]]:
    """How often each "content attracting ICP" answer came up in the period, most common first."""
    counts: Dict[str, int] = {}
    labels: Dict[str, str] = {}
    for n in notes:
        raw = n.get("content_type")
        if not raw:
            continue
        key = str(raw).strip().lower()
        counts[key] = counts.get(key, 0) + 1
        labels.setdefault(key, str(raw).strip())
    return [{"content_type": labels[k], "count": c} for k, c in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))]


def compute_team_overview(
    db: Session,
    org_id: uuid.UUID,
    period: str,
    anchor: date,
    *,
    today: Optional[date] = None,
    bounds: Optional[Tuple[date, date]] = None,
) -> Dict[str, object]:
    """
    One decision view per person for a month (week still supported by the API):
    EOD accountability (setters) with a per-day status strip, closer activity, and
    each metric vs the same elapsed span of the
    previous period and — for months — the person's best month.
    """
    from app.models.org_kpi_daily_entry import OrgKpiDailyEntry
    from app.services.kpi_rep_performance import build_rep_performance

    today = today or org_today(db, org_id)
    if bounds is not None:
        # Shared date-range filter: any inclusive range; no "best month" for custom spans.
        period = "range"
        start, end = bounds
    else:
        period = "month" if period == "month" else "week"
        start, end = period_bounds(period, anchor)
    settings = get_team_settings(db, org_id)
    weekdays = list(settings["eod_required_weekdays"])  # type: ignore[arg-type]
    pace = period_pace(start, end, today, weekdays)
    base: Dict[str, object] = {
        "period": period, "period_start": start, "period_end": end, "today": today,
        "pace": round(pace, 3), "required_weekdays": weekdays, "members": [],
    }

    # Only sales reps are on the Team view (marketing reps owe no EOD and have no sales activity).
    members = [m for m in list_team_members(db, org_id) if m["team_role"] == "sales"]
    if not members:
        return base

    # Current = the period up to today; previous = the same elapsed span of the
    # prior period (Sep 1–26 vs Aug 1–26), so ▲▼ compares like with like.
    cur_end = min(end, today)
    elapsed = cur_end - start
    if period == "range":
        prev_end = start - timedelta(days=1)
        prev_start = prev_end - elapsed
    else:
        prev_start = period_bounds(period, start - timedelta(days=1))[0]
        prev_end = min(prev_start + elapsed, start - timedelta(days=1))
    perf = build_rep_performance(db, org_id, range_start=start, range_end=cur_end)
    prev_perf = build_rep_performance(db, org_id, range_start=prev_start, range_end=prev_end)
    perf_by_rep = {row.rep_user_id: row for row in perf.reps}
    prev_by_rep = {row.rep_user_id: row.current for row in prev_perf.reps}

    eod_ids = [m["user_id"] for m in members if m["owes_eod"]]
    submitted: Dict[uuid.UUID, set] = {}
    notes_by_rep: Dict[uuid.UUID, List[Dict[str, object]]] = {}
    if eod_ids:
        lookback = min(today - timedelta(days=_STREAK_LOOKBACK_DAYS), start)
        rows = (
            db.query(OrgKpiDailyEntry)
            .filter(
                OrgKpiDailyEntry.org_id == org_id,
                OrgKpiDailyEntry.rep_user_id.in_(eod_ids),
                OrgKpiDailyEntry.entry_date >= lookback,
                OrgKpiDailyEntry.entry_date <= today,
            )
            .all()
        )
        first_stamp = (
            db.query(OrgKpiDailyEntry.submitted_at)
            .filter(OrgKpiDailyEntry.org_id == org_id, OrgKpiDailyEntry.submitted_at.isnot(None))
            .order_by(OrgKpiDailyEntry.submitted_at.asc())
            .limit(1)
            .scalar()
        )
        submitted = submitted_days(rows, first_stamp.date() if first_stamp else None)
        # Setter context + content attracting ICP from this period's EODs.
        for r in rows:
            if not (start <= r.entry_date <= end):
                continue
            context = (getattr(r, "setter_context", None) or "").strip()
            content = (getattr(r, "best_content_type", None) or "").strip()
            if context or content:
                notes_by_rep.setdefault(r.rep_user_id, []).append(
                    {"date": r.entry_date, "setter_context": context or None, "content_type": content or None}
                )

    out: List[Dict[str, object]] = []
    for m in members:
        uid, role = m["user_id"], m["team_role"]
        perf_row = perf_by_rep.get(uid)
        cur = perf_row.current if perf_row else None
        best = perf_row.personal_best if perf_row else None
        prev = prev_by_rep.get(uid)

        def metric_rows(defs: List[tuple]) -> List[Dict[str, object]]:
            rows_out: List[Dict[str, object]] = []
            for key, label, fmt, field in defs:
                value = _metric_value(cur, field)
                previous = _metric_value(prev, field)
                if fmt in ("int", "usd"):
                    # No activity in a period is a real zero for counts.
                    value = 0.0 if value is None else value
                    previous = 0.0 if previous is None else previous
                row: Dict[str, object] = {
                    "key": key,
                    "label": label,
                    "format": fmt,
                    "value": value,
                    "previous": previous,
                    "best": _metric_value(best, field) if period == "month" else None,
                }
                rows_out.append(row)
            return rows_out

        out.append(
            {
                "user_id": uid,
                "name": m["name"],
                "team_role": role,
                "eod": eod_summary(submitted.get(uid, set()), today, start, end, weekdays) if m["owes_eod"] else None,
                "notes": sorted(notes_by_rep.get(uid, []), key=lambda n: n["date"], reverse=True),
                "content_counts": _content_counts(notes_by_rep.get(uid, [])),
                # A sales rep both sets and closes: DM activity and calls/cash side by side.
                "setter_metrics": metric_rows(SETTER_VIEW_METRICS),
                "closer_metrics": metric_rows(CLOSER_VIEW_METRICS),
            }
        )
    out.sort(key=lambda r: str(r["name"]).lower())
    base["members"] = out
    return base


_TIME_RE = __import__("re").compile(r"^([01]\d|2[0-3]):[0-5]\d$")


def validate_settings_patch(patch: Dict[str, object]) -> Dict[str, object]:
    """Validate a partial settings update; unknown keys are rejected."""
    clean: Dict[str, object] = {}
    for key, value in patch.items():
        if key not in DEFAULT_SETTINGS:
            raise ValueError(f"Unknown setting: {key}")
        if key == "eod_required_weekdays":
            if not isinstance(value, list) or not value or not all(isinstance(d, int) and 0 <= d <= 6 for d in value):
                raise ValueError("eod_required_weekdays must be a non-empty list of 0–6 (Mon=0)")
            clean[key] = sorted(set(value))
        elif key in ("reminder_enabled", "digest_enabled"):
            if not isinstance(value, bool):
                raise ValueError(f"{key} must be true or false")
            clean[key] = value
        elif key in ("reminder_local_time", "digest_local_time"):
            if not isinstance(value, str) or not _TIME_RE.match(value):
                raise ValueError(f"{key} must be HH:MM (24h)")
            clean[key] = value
        elif key == "reminder_channels":
            if not isinstance(value, list) or not value or not set(value) <= {"discord", "email"}:
                raise ValueError("reminder_channels must include 'discord' and/or 'email'")
            clean[key] = sorted(set(value))
    return clean


def update_team_settings(db: Session, org_id: uuid.UUID, patch: Dict[str, object]) -> Dict[str, object]:
    from sqlalchemy.orm.attributes import flag_modified

    from app.models.team_kpi import TeamKpiSettings

    clean = validate_settings_patch(patch)
    row = db.query(TeamKpiSettings).filter(TeamKpiSettings.org_id == org_id).first()
    if row is None:
        row = TeamKpiSettings(org_id=org_id, settings={})
        db.add(row)
    row.settings = {**(row.settings or {}), **clean}
    flag_modified(row, "settings")
    db.commit()
    return get_team_settings(db, org_id)

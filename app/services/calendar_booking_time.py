"""UTC helpers for calendar check-in upcoming/past classification."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import func

from app.models.client_checkin import ClientCheckIn


def ensure_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def parse_utc_instant(iso: str) -> datetime:
    raw = iso.replace("Z", "+00:00") if iso.endswith("Z") else iso
    return ensure_utc(datetime.fromisoformat(raw))


def effective_end_time(ci: ClientCheckIn) -> Optional[datetime]:
    if ci.end_time is not None:
        return ensure_utc(ci.end_time)
    if ci.start_time is not None:
        return ensure_utc(ci.start_time)
    return None


def check_in_is_upcoming(ci: ClientCheckIn, now_utc: datetime) -> bool:
    boundary = effective_end_time(ci)
    if boundary is None:
        return False
    return boundary >= ensure_utc(now_utc)


def check_in_is_past(ci: ClientCheckIn, now_utc: datetime) -> bool:
    boundary = effective_end_time(ci)
    if boundary is None:
        return False
    return boundary < ensure_utc(now_utc)


def effective_end_sql_expression():
    """SQL: coalesce(end_time, start_time) for DB-side upcoming/past filters."""
    return func.coalesce(ClientCheckIn.end_time, ClientCheckIn.start_time)


def format_calendly_api_time(dt: datetime) -> str:
    """
    Calendly requires microsecond precision on paginated scheduled_events requests.
    First-page calls with coarse Z timestamps often work; page_token follow-ups 400 without this.
    """
    return ensure_utc(dt).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def format_cal_api_time(dt: datetime) -> str:
    """ISO-8601 UTC with millisecond precision for Cal.com v2 date filters."""
    ms = ensure_utc(dt).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
    return f"{ms}Z"


def booking_boundary_from_iso(
    start_iso: Optional[str],
    end_iso: Optional[str],
) -> Optional[datetime]:
    """Effective end instant from ISO strings (end when present, else start)."""
    raw = end_iso or start_iso
    if not raw:
        return None
    try:
        return ensure_utc(datetime.fromisoformat(raw.replace("Z", "+00:00")))
    except (ValueError, TypeError):
        return None


def classify_booking_window(
    start_iso: Optional[str],
    end_iso: Optional[str],
    *,
    now: Optional[datetime] = None,
) -> Optional[str]:
    """Return 'upcoming', 'past', or None when times are missing."""
    boundary = booking_boundary_from_iso(start_iso, end_iso)
    if boundary is None:
        return None
    now_utc = ensure_utc(now or datetime.now(timezone.utc))
    return "upcoming" if boundary >= now_utc else "past"

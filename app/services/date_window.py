"""Explicit date windows for the shared date-range filter (docs/features/DATE_RANGE_FILTER_PRD.md).

The UI sends inclusive local dates (`start` / `end`, YYYY-MM-DD in the org's timezone).
Endpoints store timestamps as naive UTC, so each window becomes a naive-UTC half-open
interval [start 00:00 local, day after end 00:00 local).
"""
from __future__ import annotations

import uuid
from datetime import date, datetime, time, timedelta, timezone
from typing import Optional, Tuple

from fastapi import HTTPException, status
from sqlalchemy.orm import Session

MAX_SPAN_DAYS = 3660
EPOCH = datetime(1970, 1, 1)


def parse_ymd(value: Optional[str], name: str) -> Optional[date]:
    # Non-strings include FastAPI `Query(None)` defaults when an endpoint is called directly
    # from Python (e.g. the terminal bundle); treat them as "not provided".
    if not isinstance(value, str) or value == "":
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"{name} must be YYYY-MM-DD")


def org_tz(db: Session, org_id: uuid.UUID):
    from zoneinfo import ZoneInfo

    from app.models.organization import Organization

    name = db.query(Organization.timezone).filter(Organization.id == org_id).scalar() or "UTC"
    try:
        return ZoneInfo(name)
    except Exception:
        return ZoneInfo("UTC")


def local_day_start_utc(day: date, tz) -> datetime:
    """Local midnight of `day` as a naive UTC datetime."""
    return datetime.combine(day, time.min, tzinfo=tz).astimezone(timezone.utc).replace(tzinfo=None)


def window_from_dates(start: Optional[date], end: date, tz) -> Tuple[datetime, datetime]:
    """Inclusive local dates -> naive-UTC [start, end). start=None means all history."""
    if start is not None and start > end:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="start must be on or before end")
    if start is not None and (end - start).days > MAX_SPAN_DAYS:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Date range is too long (max 10 years)")
    lo = EPOCH if start is None else local_day_start_utc(start, tz)
    return lo, local_day_start_utc(end + timedelta(days=1), tz)


def explicit_window(
    db: Session,
    org_id: uuid.UUID,
    start: Optional[str],
    end: Optional[str],
    *,
    name: str = "",
) -> Optional[Tuple[datetime, datetime]]:
    """
    Window for request params `start` / `end` (prefix `name`, e.g. "compare_"), or None
    when `end` is absent (caller falls back to its legacy range/scope params). A missing
    `start` with an `end` means all history up to `end`.
    """
    end_d = parse_ymd(end, f"{name}end")
    start_d = parse_ymd(start, f"{name}start")
    if end_d is None:
        if start_d is not None:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"{name}end is required with {name}start")
        return None
    return window_from_dates(start_d, end_d, org_tz(db, org_id))

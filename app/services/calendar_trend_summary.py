"""Scoped calendar trend windows and show-up / close-rate aggregates (UTC-aligned with Stripe MTD)."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple
from uuid import UUID

from sqlalchemy.orm import Session

from app.models.client_checkin import ClientCheckIn


@dataclass(frozen=True)
class CalendarTrendActivityWindow:
    past_start: datetime
    past_end: datetime
    upcoming_start: datetime
    upcoming_end: datetime


def calendar_trend_activity_window(
    *,
    scope: Optional[str] = None,
    range_days: Optional[int] = None,
    now: Optional[datetime] = None,
) -> CalendarTrendActivityWindow:
    """
    Activity window for calendar KPI cards (UTC, matches Stripe `scope=mtd` semantics).
    - mtd: 1st of current UTC month through now; upcoming through end of UTC month.
    - all: all past through now; upcoming through +10y.
    - range_days: rolling UTC-day window (past: now-Nd..now, upcoming: now..now+Nd).
    """
    now_utc = now or datetime.now(timezone.utc)
    if now_utc.tzinfo is None:
        now_utc = now_utc.replace(tzinfo=timezone.utc)
    else:
        now_utc = now_utc.astimezone(timezone.utc)

    if scope == "all":
        return CalendarTrendActivityWindow(
            past_start=datetime(1970, 1, 1, tzinfo=timezone.utc),
            past_end=now_utc,
            upcoming_start=now_utc,
            upcoming_end=now_utc + timedelta(days=3650),
        )

    if scope == "mtd":
        past_start = now_utc.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        if now_utc.month == 12:
            next_month = now_utc.replace(
                year=now_utc.year + 1, month=1, day=1, hour=0, minute=0, second=0, microsecond=0
            )
        else:
            next_month = now_utc.replace(
                month=now_utc.month + 1, day=1, hour=0, minute=0, second=0, microsecond=0
            )
        upcoming_end = next_month - timedelta(microseconds=1)
        return CalendarTrendActivityWindow(
            past_start=past_start,
            past_end=now_utc,
            upcoming_start=now_utc,
            upcoming_end=upcoming_end,
        )

    days = range_days if range_days is not None else 30
    utc_midnight = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
    past_start = utc_midnight - timedelta(days=days)
    return CalendarTrendActivityWindow(
        past_start=past_start,
        past_end=now_utc,
        upcoming_start=now_utc,
        upcoming_end=now_utc + timedelta(days=days),
    )


def _past_sales_calls_in_window(
    db: Session,
    org_id: UUID,
    past_start: datetime,
    past_end: datetime,
) -> list[ClientCheckIn]:
    """Past non-cancelled sales calls with start_time in [past_start, past_end)."""
    ps = past_start.replace(tzinfo=None) if past_start.tzinfo else past_start
    pe = past_end.replace(tzinfo=None) if past_end.tzinfo else past_end
    return (
        db.query(ClientCheckIn)
        .filter(
            ClientCheckIn.org_id == org_id,
            ClientCheckIn.provider.in_(["calcom", "calendly"]),
            ClientCheckIn.is_sales_call == True,  # noqa: E712
            ClientCheckIn.cancelled == False,  # noqa: E712
            ClientCheckIn.start_time >= ps,
            ClientCheckIn.start_time < pe,
        )
        .all()
    )


def _count_meetings_in_window(
    db: Session,
    org_id: UUID,
    window_start: datetime,
    window_end: datetime,
    *,
    upcoming: bool,
    now_utc: datetime,
) -> int:
    from app.services.calendar_booking_time import effective_end_sql_expression, ensure_utc

    ws = ensure_utc(window_start)
    we = ensure_utc(window_end)
    now = ensure_utc(now_utc)
    effective_end = effective_end_sql_expression()

    q = db.query(ClientCheckIn).filter(
        ClientCheckIn.org_id == org_id,
        ClientCheckIn.provider.in_(["calcom", "calendly"]),
    )
    if upcoming:
        q = q.filter(
            effective_end >= now,
            ClientCheckIn.start_time >= ws,
            ClientCheckIn.start_time <= we,
        )
    else:
        q = q.filter(
            ClientCheckIn.start_time >= ws,
            effective_end < now,
        )
    return q.count()


def compute_calendar_trend_summary(
    db: Session,
    org_id: UUID,
    *,
    scope: Optional[str] = None,
    range_days: Optional[int] = None,
    now: Optional[datetime] = None,
) -> dict:
    """
    Show-up rate: past sales calls in window that are not no-shows / all past sales calls.
    Matches calendar UI (past + not cancelled + not no_show = attended).
    Close rate: share with sale_closed=True among past sales calls in window.
    """
    now_utc = now or datetime.now(timezone.utc)
    if now_utc.tzinfo is None:
        now_utc = now_utc.replace(tzinfo=timezone.utc)

    w = calendar_trend_activity_window(scope=scope, range_days=range_days, now=now_utc)

    past_count = _count_meetings_in_window(
        db, org_id, w.past_start, w.past_end, upcoming=False, now_utc=now_utc
    )
    upcoming_count = _count_meetings_in_window(
        db, org_id, w.upcoming_start, w.upcoming_end, upcoming=True, now_utc=now_utc
    )

    sales_rows = _past_sales_calls_in_window(db, org_id, w.past_start, w.past_end)
    sales_total = len(sales_rows)
    showed_up = sum(1 for c in sales_rows if not getattr(c, "no_show", False))
    closed = sum(1 for c in sales_rows if getattr(c, "sale_closed", None) is True)

    show_up_rate_pct = round((showed_up / sales_total) * 100.0) if sales_total else None
    close_rate_pct = round((closed / sales_total) * 100.0) if sales_total else None

    return {
        "upcoming_count": upcoming_count,
        "past_count": past_count,
        "close_rate_pct": close_rate_pct,
        "sales_calls_in_range": sales_total,
        "closed_sales_count": closed,
        "show_up_rate_pct": show_up_rate_pct,
        "attendance_eligible_past": sales_total,
        "showed_up_count": showed_up,
    }


def show_up_rate_for_period(
    rows: list[ClientCheckIn],
) -> Optional[float]:
    """Past sales-call rows only: attended = not no_show."""
    if not rows:
        return None
    attended = sum(1 for c in rows if not getattr(c, "no_show", False))
    return round((attended / len(rows)) * 100.0, 1)

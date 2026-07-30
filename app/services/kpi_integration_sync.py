"""Live sync of KPI auto fields from calendar check-ins and payment sources.

Force-refreshes: calls_booked, calls_taken, closes, no_shows, cash_collected.
Never writes revenue (manual-only).
"""
from __future__ import annotations

import uuid
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

from sqlalchemy.orm import Session

from app.models.client_checkin import ClientCheckIn
from app.models.manual_payment import ManualPayment
from app.models.oauth_token import OAuthProvider, OAuthToken
from app.models.org_kpi_daily_entry import OrgKpiDailyEntry
from app.models.stripe_payment import StripePayment
from app.models.whop_payment import WhopPayment

LIVE_CALENDAR_FIELDS = ("calls_booked", "calls_taken", "closes", "no_shows")
LIVE_PAYMENT_FIELDS = ("cash_collected",)


def _ensure_utc(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def day_bounds_utc(entry_day: date) -> Tuple[datetime, datetime]:
    start = datetime.combine(entry_day, time.min, tzinfo=timezone.utc)
    end = datetime.combine(entry_day, time.max, tzinfo=timezone.utc)
    return start, end


def has_calendar_source(db: Session, org_id: uuid.UUID) -> bool:
    token = (
        db.query(OAuthToken)
        .filter(
            OAuthToken.org_id == org_id,
            OAuthToken.provider.in_([OAuthProvider.CALENDLY, OAuthProvider.CALCOM]),
        )
        .first()
    )
    if token is not None:
        return True
    return (
        db.query(ClientCheckIn.id)
        .filter(ClientCheckIn.org_id == org_id)
        .first()
        is not None
    )


def has_payment_source(db: Session, org_id: uuid.UUID) -> bool:
    return (
        db.query(StripePayment.id).filter(StripePayment.org_id == org_id).first() is not None
        or db.query(WhopPayment.id).filter(WhopPayment.org_id == org_id).first() is not None
        or db.query(ManualPayment.id).filter(ManualPayment.org_id == org_id).first() is not None
    )


def _payment_cash_by_day(
    db: Session,
    org_id: uuid.UUID,
    start: date,
    end: date,
) -> Dict[date, int]:
    """Prefetch cash (cents) per day for a date window — one scan per payment source."""
    range_start, _ = day_bounds_utc(start)
    _, range_end = day_bounds_utc(end)
    by_day: Dict[date, int] = {}

    def add(ts: Optional[datetime], cents: int) -> None:
        if ts is None or cents == 0:
            return
        if ts < range_start or ts > range_end:
            return
        d = ts.date()
        by_day[d] = by_day.get(d, 0) + cents

    for p in (
        db.query(StripePayment)
        .filter(StripePayment.org_id == org_id, StripePayment.status == "succeeded")
        .all()
    ):
        add(_ensure_utc(p.created_at), int(p.amount_cents or 0))
    for p in db.query(WhopPayment).filter(WhopPayment.org_id == org_id).all():
        if (p.status or "").lower() not in ("paid", "succeeded", "completed", "successful"):
            continue
        add(_ensure_utc(p.created_at), int(p.amount_cents or 0))
    for p in db.query(ManualPayment).filter(ManualPayment.org_id == org_id).all():
        add(_ensure_utc(p.payment_date or p.created_at), int(p.amount_cents or 0))
    return by_day


def compute_live_fields_for_day(
    db: Session,
    org_id: uuid.UUID,
    entry_day: date,
    *,
    calendar_available: Optional[bool] = None,
    payments_available: Optional[bool] = None,
    checkins: Optional[Iterable[ClientCheckIn]] = None,
    cash_by_day: Optional[Dict[date, int]] = None,
) -> Dict[str, Any]:
    """Compute live auto field values for a single day. Does not include revenue."""
    out: Dict[str, Any] = {}
    start, end = day_bounds_utc(entry_day)
    cal = has_calendar_source(db, org_id) if calendar_available is None else calendar_available
    pay = has_payment_source(db, org_id) if payments_available is None else payments_available

    if cal:
        if checkins is None:
            checkins = db.query(ClientCheckIn).filter(ClientCheckIn.org_id == org_id).all()
        scoped: List[ClientCheckIn] = []
        for ci in checkins:
            st = _ensure_utc(ci.start_time)
            if not st or st < start or st > end:
                continue
            scoped.append(ci)
        out["calls_taken"] = sum(
            1
            for ci in scoped
            if ci.is_sales_call and ci.completed and not ci.cancelled and not ci.no_show
        )
        out["calls_booked"] = sum(1 for ci in scoped if ci.is_sales_call and not ci.cancelled)
        out["closes"] = sum(1 for ci in scoped if ci.is_sales_call and ci.sale_closed is True)
        out["no_shows"] = sum(1 for ci in scoped if ci.is_sales_call and ci.no_show)

    if pay:
        if cash_by_day is None:
            cash_by_day = _payment_cash_by_day(db, org_id, entry_day, entry_day)
        out["cash_collected"] = round(cash_by_day.get(entry_day, 0) / 100.0, 2)

    return out


def sync_kpi_day_from_integrations(
    db: Session,
    org_id: uuid.UUID,
    entry_day: date,
    *,
    commit: bool = False,
    force_create: bool = True,
    checkins: Optional[Iterable[ClientCheckIn]] = None,
    calendar_available: Optional[bool] = None,
    payments_available: Optional[bool] = None,
    cash_by_day: Optional[Dict[date, int]] = None,
) -> Optional[OrgKpiDailyEntry]:
    """
    Force-refresh live auto KPI fields for a day from integrations.
    Creates a row when force_create or when there is non-zero live activity.
    Never modifies revenue or manual fields.
    """
    cal = has_calendar_source(db, org_id) if calendar_available is None else calendar_available
    pay = has_payment_source(db, org_id) if payments_available is None else payments_available
    if not cal and not pay:
        return None

    values = compute_live_fields_for_day(
        db,
        org_id,
        entry_day,
        calendar_available=cal,
        payments_available=pay,
        checkins=checkins,
        cash_by_day=cash_by_day,
    )
    if not values:
        return None

    has_activity = any(
        (isinstance(v, (int, float)) and v != 0) for v in values.values()
    )
    row = (
        db.query(OrgKpiDailyEntry)
        .filter(OrgKpiDailyEntry.org_id == org_id, OrgKpiDailyEntry.entry_date == entry_day)
        .first()
    )
    if row is None:
        if not force_create and not has_activity:
            return None
        row = OrgKpiDailyEntry(org_id=org_id, entry_date=entry_day)
        db.add(row)
        if entry_day <= date.today() and row.new_followers is None:
            row.new_followers = 0

    for field, value in values.items():
        setattr(row, field, value)
    row.updated_at = datetime.utcnow()

    if commit:
        db.commit()
        db.refresh(row)
    else:
        db.flush()
    return row


def sync_kpi_for_datetime(
    db: Session,
    org_id: uuid.UUID,
    when: Optional[datetime],
    *,
    commit: bool = True,
) -> None:
    """Sync the KPI day corresponding to a check-in/payment timestamp."""
    if when is None:
        return
    ts = _ensure_utc(when)
    if ts is None:
        return
    try:
        sync_kpi_day_from_integrations(
            db, org_id, ts.date(), commit=commit, force_create=True
        )
    except Exception:
        # Never break calendar/payment pipelines on KPI sync failure.
        if commit:
            try:
                db.rollback()
            except Exception:
                pass


def refresh_kpi_live_fields_for_range(
    db: Session,
    org_id: uuid.UUID,
    start: date,
    end: date,
) -> None:
    """Recompute live auto fields for each day in [start, end] (inclusive).

    Prefetches check-ins and payments once so month switches / 2-month compare
    do not re-scan payment tables per day.
    """
    if end < start:
        return
    cal = has_calendar_source(db, org_id)
    pay = has_payment_source(db, org_id)
    if not cal and not pay:
        return

    checkins = None
    if cal:
        checkins = db.query(ClientCheckIn).filter(ClientCheckIn.org_id == org_id).all()

    cash_by_day: Optional[Dict[date, int]] = None
    if pay:
        cash_by_day = _payment_cash_by_day(db, org_id, start, end)

    # Also refresh any existing rows in range even with zero activity
    existing_dates: Set[date] = {
        r.entry_date
        for r in db.query(OrgKpiDailyEntry.entry_date)
        .filter(
            OrgKpiDailyEntry.org_id == org_id,
            OrgKpiDailyEntry.entry_date >= start,
            OrgKpiDailyEntry.entry_date <= end,
        )
        .all()
    }

    cur = start
    dirty = False
    while cur <= end:
        force = cur in existing_dates
        row = sync_kpi_day_from_integrations(
            db,
            org_id,
            cur,
            commit=False,
            force_create=force,
            checkins=checkins,
            calendar_available=cal,
            payments_available=pay,
            cash_by_day=cash_by_day,
        )
        if row is not None:
            dirty = True
        cur += timedelta(days=1)
    if dirty:
        db.commit()

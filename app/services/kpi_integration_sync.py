"""Live sync of KPI auto fields from calendar check-ins and payment sources.

Force-refreshes: calls_booked, calls_taken, closes, no_shows, cash_collected.
Never writes revenue (manual-only).
"""
from __future__ import annotations

import uuid
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

from sqlalchemy.orm import Session

from app.models.client import Client
from app.models.client_checkin import ClientCheckIn
from app.models.manual_payment import ManualPayment
from app.models.oauth_token import OAuthProvider, OAuthToken
from app.models.org_kpi_daily_entry import OrgKpiDailyEntry
from app.models.stripe_payment import StripePayment
from app.models.whop_payment import WhopPayment
from app.schemas.kpi import KpiRevenueContributor

LIVE_CALENDAR_FIELDS = ("calls_booked", "calls_booked_activity", "calls_taken", "closes", "no_shows")
LIVE_PAYMENT_FIELDS = ("cash_collected",)

def _sales_call_unique_key(ci: ClientCheckIn) -> str:
    """Stable key so duplicate check-in rows for the same calendar event count once."""
    event_id = (getattr(ci, "event_id", None) or "").strip()
    if event_id:
        provider = (getattr(ci, "provider", None) or "").strip()
        return f"{provider}:{event_id}"
    return f"id:{ci.id}"


def _count_unique_sales_calls(rows: Iterable[ClientCheckIn], predicate) -> int:
    seen: Set[str] = set()
    n = 0
    for ci in rows:
        if not getattr(ci, "is_sales_call", False):
            continue
        if not predicate(ci):
            continue
        key = _sales_call_unique_key(ci)
        if key in seen:
            continue
        seen.add(key)
        n += 1
    return n



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


def _client_display_name(client: Optional[Client]) -> str:
    if client is None:
        return "Unknown client"
    name = " ".join(part for part in (client.first_name, client.last_name) if part).strip()
    return name or (client.email or "Unknown client")


def get_revenue_contributors_for_day(
    db: Session,
    org_id: uuid.UUID,
    entry_day: date,
) -> List[KpiRevenueContributor]:
    """Which clients' payments made up this day's cash_collected — same three
    sources _payment_cash_by_day sums, but itemized instead of totaled."""
    range_start, range_end = day_bounds_utc(entry_day)
    out: List[KpiRevenueContributor] = []

    def add(payment_id: str, client_id: Optional[uuid.UUID], amount_cents: int, source: str) -> None:
        if amount_cents == 0:
            return
        client = db.query(Client).filter(Client.id == client_id).first() if client_id else None
        out.append(
            KpiRevenueContributor(
                client_id=client_id,
                client_name=_client_display_name(client),
                amount_cents=amount_cents,
                source=source,
                payment_id=payment_id,
            )
        )

    for p in (
        db.query(StripePayment)
        .filter(StripePayment.org_id == org_id, StripePayment.status == "succeeded")
        .all()
    ):
        ts = _ensure_utc(p.created_at)
        if ts and range_start <= ts <= range_end:
            add(str(p.id), p.client_id, int(p.amount_cents or 0), "stripe")

    for p in db.query(WhopPayment).filter(WhopPayment.org_id == org_id).all():
        if (p.status or "").lower() not in ("paid", "succeeded", "completed", "successful"):
            continue
        ts = _ensure_utc(p.created_at)
        if ts and range_start <= ts <= range_end:
            add(str(p.id), p.client_id, int(p.amount_cents or 0), "whop")

    for p in db.query(ManualPayment).filter(ManualPayment.org_id == org_id).all():
        ts = _ensure_utc(p.payment_date or p.created_at)
        if ts and range_start <= ts <= range_end:
            add(str(p.id), p.client_id, int(p.amount_cents or 0), "manual")

    out.sort(key=lambda c: c.amount_cents, reverse=True)
    return out


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
        booked_on_day: List[ClientCheckIn] = []
        for ci in checkins:
            st = _ensure_utc(ci.start_time)
            if st and start <= st <= end:
                scoped.append(ci)
            created = _ensure_utc(ci.created_at)
            if created and start <= created <= end:
                booked_on_day.append(ci)
        out["calls_taken"] = _count_unique_sales_calls(
            scoped,
            lambda ci: bool(ci.completed) and not ci.cancelled and not ci.no_show,
        )
        out["calls_booked"] = _count_unique_sales_calls(
            scoped,
            lambda ci: not ci.cancelled,
        )
        # Unique sales calls marked closed / no-show for this calendar day.
        out["closes"] = _count_unique_sales_calls(
            scoped,
            lambda ci: ci.sale_closed is True,
        )
        out["no_shows"] = _count_unique_sales_calls(
            scoped,
            lambda ci: bool(ci.no_show),
        )
        # Booking *activity* — when the booking was made, not when the meeting is
        # scheduled for. A call booked today for next week counts here today.
        out["calls_booked_activity"] = _count_unique_sales_calls(
            booked_on_day,
            lambda ci: not ci.cancelled,
        )

    if pay:
        if cash_by_day is None:
            cash_by_day = _payment_cash_by_day(db, org_id, entry_day, entry_day)
        out["cash_collected"] = round(cash_by_day.get(entry_day, 0) / 100.0, 2)

    return out


def _compute_host_breakdown_for_day(
    entry_day: date,
    checkins: Iterable[ClientCheckIn],
) -> Dict[uuid.UUID, Dict[str, int]]:
    """Same calls_booked/calls_taken/closes/no_shows logic as compute_live_fields_for_day,
    scoped per host_user_id — feeds the per-rep org_kpi_daily_entries rows the By Rep
    view reads. Only meaningful for orgs whose calendar setup assigns different hosts;
    checkins with no resolved host_user_id contribute nothing here (they still count
    toward the org-aggregate row as before)."""
    start, end = day_bounds_utc(entry_day)
    by_host: Dict[uuid.UUID, List[ClientCheckIn]] = {}
    booked_by_host: Dict[uuid.UUID, List[ClientCheckIn]] = {}
    for ci in checkins:
        host_id = getattr(ci, "host_user_id", None)
        if not host_id:
            continue
        st = _ensure_utc(ci.start_time)
        if st and start <= st <= end:
            by_host.setdefault(host_id, []).append(ci)
        created = _ensure_utc(ci.created_at)
        if created and start <= created <= end:
            booked_by_host.setdefault(host_id, []).append(ci)

    out: Dict[uuid.UUID, Dict[str, int]] = {}
    for host_id in set(by_host.keys()) | set(booked_by_host.keys()):
        rows = by_host.get(host_id, [])
        booked_rows = booked_by_host.get(host_id, [])
        out[host_id] = {
            "calls_taken": _count_unique_sales_calls(
                rows,
                lambda ci: bool(ci.completed) and not ci.cancelled and not ci.no_show,
            ),
            "calls_booked": _count_unique_sales_calls(rows, lambda ci: not ci.cancelled),
            "calls_booked_activity": _count_unique_sales_calls(
                booked_rows, lambda ci: not ci.cancelled
            ),
            "closes": _count_unique_sales_calls(rows, lambda ci: ci.sale_closed is True),
            "no_shows": _count_unique_sales_calls(rows, lambda ci: bool(ci.no_show)),
        }
    return out


def _sync_host_kpi_rows_for_day(
    db: Session,
    org_id: uuid.UUID,
    entry_day: date,
    checkins: Iterable[ClientCheckIn],
) -> None:
    """Upsert one org_kpi_daily_entries row per host_user_id with calendar-derived
    metrics for entry_day. Never touches cash_collected/revenue/manual fields —
    those come from close-survey attribution (sales_activity_events), not calendars."""
    breakdown = _compute_host_breakdown_for_day(entry_day, checkins)
    for host_id, values in breakdown.items():
        row = (
            db.query(OrgKpiDailyEntry)
            .filter(
                OrgKpiDailyEntry.org_id == org_id,
                OrgKpiDailyEntry.entry_date == entry_day,
                OrgKpiDailyEntry.rep_user_id == host_id,
            )
            .first()
        )
        if row is None:
            row = OrgKpiDailyEntry(org_id=org_id, entry_date=entry_day, rep_user_id=host_id)
            db.add(row)
        for field, value in values.items():
            setattr(row, field, value)
        row.updated_at = datetime.utcnow()


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

    if cal and checkins is None:
        checkins = db.query(ClientCheckIn).filter(ClientCheckIn.org_id == org_id).all()

    values = compute_live_fields_for_day(
        db,
        org_id,
        entry_day,
        calendar_available=cal,
        payments_available=pay,
        checkins=checkins,
        cash_by_day=cash_by_day,
    )

    if cal and checkins is not None:
        _sync_host_kpi_rows_for_day(db, org_id, entry_day, checkins)

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

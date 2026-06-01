"""
Org-scoped terminal and calendar trend metrics.

Centralizes admin dashboard date/rate helpers so client API routes do not import app.api.admin.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from threading import Lock as ThreadingLock
from typing import List, Optional
from uuid import UUID

from sqlalchemy import func
from sqlalchemy.orm import Session

from app.models.client import Client, LifecycleState
from app.models.client_checkin import ClientCheckIn
from app.models.organization import Organization
from app.models.stripe_treasury_transaction import StripeTreasuryTransaction
from app.models.whop_payment import WhopPayment
from app.schemas.admin import HealthTrendPeriod
from app.schemas.calendar_metrics import CalendarMonthlyRateRow, CalendarMonthlyCoachingResponse
from app.schemas.calendar_metrics import TerminalMonthlyTrendsResponse

_WHOP_SUCCEEDED_STATUSES = ("paid", "succeeded", "completed", "successful")

_terminal_monthly_trends_cache: dict[str, tuple[float, object]] = {}
_terminal_monthly_trends_lock = ThreadingLock()
TERMINAL_MONTHLY_TRENDS_TTL_SEC = 300


def _admin():
    from app.api import admin as admin_api

    return admin_api


def org_whop_cash_usd_window(
    db: Session,
    org_id: UUID,
    org_created_naive: datetime,
    ps_naive: datetime,
    pe_exclusive_naive: datetime,
) -> float:
    """Whop cash in [ps, pe); clips start to onboarding."""
    effective_start = max(
        ps_naive.replace(tzinfo=None) if ps_naive.tzinfo else ps_naive,
        org_created_naive,
    )
    if effective_start >= pe_exclusive_naive:
        return 0.0
    ce = pe_exclusive_naive.replace(tzinfo=None) if pe_exclusive_naive.tzinfo else pe_exclusive_naive
    if effective_start >= ce:
        return 0.0
    cents = (
        db.query(func.coalesce(func.sum(WhopPayment.amount_cents), 0))
        .filter(
            WhopPayment.org_id == org_id,
            WhopPayment.status.in_(_WHOP_SUCCEEDED_STATUSES),
            WhopPayment.created_at >= effective_start,
            WhopPayment.created_at < ce,
            WhopPayment.created_at >= org_created_naive,
        )
        .scalar()
    )
    return float(cents or 0) / 100.0


def build_calendar_monthly_coaching_periods(db: Session, org: Organization) -> CalendarMonthlyCoachingResponse:
    admin_api = _admin()
    org_id = org.id
    now_utc_dash = datetime.now(timezone.utc)

    if org.created_at:
        oc_anchor = (
            org.created_at.replace(tzinfo=timezone.utc)
            if org.created_at.tzinfo is None
            else org.created_at.astimezone(timezone.utc)
        )
    else:
        oc_anchor = now_utc_dash

    month_cursor = admin_api._utc_month_start(oc_anchor)
    cap_month = admin_api._first_of_month_n_months_ago(now_utc_dash, 35)
    if month_cursor < cap_month:
        month_cursor = cap_month

    periods_out: List[CalendarMonthlyRateRow] = []

    while month_cursor <= now_utc_dash:
        month_end_exclusive = min(admin_api._add_one_calendar_month_first(month_cursor), now_utc_dash)

        sup = admin_api._org_show_up_rate_pct(db, org_id, month_cursor, month_end_exclusive, now_utc_dash)
        cr = admin_api._org_close_rate_pct(db, org_id, month_cursor, month_end_exclusive, now_utc_dash)

        periods_out.append(
            CalendarMonthlyRateRow(
                period_label=month_cursor.strftime("%b %Y"),
                period_start=month_cursor.isoformat(),
                period_end=month_end_exclusive.isoformat(),
                show_up_rate_pct=sup,
                close_rate_pct=cr,
            )
        )

        if month_end_exclusive >= now_utc_dash:
            break
        month_cursor = admin_api._add_one_calendar_month_first(month_cursor)

    return CalendarMonthlyCoachingResponse(periods=periods_out)


def terminal_monthly_trends_cache_get(org_id: UUID) -> Optional[TerminalMonthlyTrendsResponse]:
    cache_key = str(org_id)
    now_ts = datetime.utcnow().timestamp()
    with _terminal_monthly_trends_lock:
        hit = _terminal_monthly_trends_cache.get(cache_key)
        if hit and now_ts - hit[0] < TERMINAL_MONTHLY_TRENDS_TTL_SEC:
            return hit[1]
    return None


def terminal_monthly_trends_cache_set(org_id: UUID, response: TerminalMonthlyTrendsResponse) -> None:
    cache_key = str(org_id)
    now_ts = datetime.utcnow().timestamp()
    with _terminal_monthly_trends_lock:
        _terminal_monthly_trends_cache[cache_key] = (now_ts, response)


def build_terminal_monthly_trends(db: Session, org: Organization) -> TerminalMonthlyTrendsResponse:
    admin_api = _admin()
    org_id = org.id
    org_created_naive = org.created_at.replace(tzinfo=None) if org.created_at else datetime.utcnow()
    now_naive = datetime.utcnow()
    now_utc_dash = datetime.now(timezone.utc)

    treasury_count = (
        db.query(StripeTreasuryTransaction.id)
        .filter(StripeTreasuryTransaction.org_id == org_id)
        .limit(1)
        .scalar()
    )
    uses_treasury = treasury_count is not None

    if org.created_at:
        oc_anchor = (
            org.created_at.replace(tzinfo=timezone.utc)
            if org.created_at.tzinfo is None
            else org.created_at.astimezone(timezone.utc)
        )
    else:
        oc_anchor = now_utc_dash

    month_cursor = admin_api._utc_month_start(oc_anchor)
    cap_month = admin_api._first_of_month_n_months_ago(now_utc_dash, 35)
    if month_cursor < cap_month:
        month_cursor = cap_month

    periods_out: List[HealthTrendPeriod] = []

    while month_cursor <= now_utc_dash:
        month_end_exclusive = min(admin_api._add_one_calendar_month_first(month_cursor), now_utc_dash)
        ps_naive = admin_api._utc_naive(month_cursor)
        pe_naive_exclusive = admin_api._utc_naive(month_end_exclusive)

        stripe_cash = admin_api._org_cash_usd_window(
            db,
            org_id,
            org_created_naive,
            ps_naive,
            pe_naive_exclusive,
            uses_treasury,
            now_naive,
        )
        whop_cash = org_whop_cash_usd_window(db, org_id, org_created_naive, ps_naive, pe_naive_exclusive)
        combined_cash = stripe_cash + whop_cash

        calls_ct = (
            db.query(func.count(ClientCheckIn.id))
            .filter(
                ClientCheckIn.org_id == org_id,
                ClientCheckIn.is_sales_call == True,
                ClientCheckIn.cancelled == False,
                ClientCheckIn.start_time >= month_cursor,
                ClientCheckIn.start_time < month_end_exclusive,
            )
            .scalar()
            or 0
        )

        cum_clients = (
            db.query(func.count(Client.id))
            .filter(Client.org_id == org_id, Client.created_at < pe_naive_exclusive)
            .scalar()
            or 0
        )
        active_cohort = (
            db.query(func.count(Client.id))
            .filter(
                Client.org_id == org_id,
                Client.lifecycle_state == LifecycleState.ACTIVE,
                Client.created_at < pe_naive_exclusive,
            )
            .scalar()
            or 0
        )

        sup = admin_api._org_show_up_rate_pct(db, org_id, month_cursor, month_end_exclusive, now_utc_dash)
        cr = admin_api._org_close_rate_pct(db, org_id, month_cursor, month_end_exclusive, now_utc_dash)

        periods_out.append(
            HealthTrendPeriod(
                period_label=month_cursor.strftime("%b %Y"),
                period_start=month_cursor.isoformat(),
                period_end=month_end_exclusive.isoformat(),
                show_up_rate_pct=sup,
                close_rate_pct=cr,
                stripe_revenue_usd=stripe_cash,
                combined_revenue_usd=combined_cash,
                calls_booked_count=calls_ct,
                cumulative_total_clients=cum_clients,
                active_clients_cohort=active_cohort,
            )
        )

        if month_end_exclusive >= now_utc_dash:
            break
        month_cursor = admin_api._add_one_calendar_month_first(month_cursor)

    return TerminalMonthlyTrendsResponse(periods=periods_out)

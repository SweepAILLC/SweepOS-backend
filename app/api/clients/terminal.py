"""Clients API — terminal routes."""
from __future__ import annotations

import logging
import re
import uuid
from datetime import datetime, timedelta, timezone
from threading import Lock as ThreadingLock
from typing import List, Optional, Tuple
from uuid import UUID

import httpx
from fastapi import APIRouter, BackgroundTasks, Body, Depends, HTTPException, Query, Request, status
from fastapi.security import HTTPAuthorizationCredentials
from sqlalchemy import and_, desc, func, or_
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, defer
from sqlalchemy.orm.attributes import flag_modified
from starlette.concurrency import run_in_threadpool

from app.api.deps import get_current_user, security
from app.api.clients.helpers import (
    LOG,
    WHOP_PAID_STATUSES,
    effective_org_id,
    merge_client_meta_from_duplicates,
    normalize_email,
    client_created_sort_key,
    load_whop_payments,
    org_checkin_sync_lock,
    refresh_call_insights_after_checkin_sync,
    scope_org_id,
    sync_check_ins_in_worker,
    user_pipeline_priorities,
    brevo_merged_stats_for_client,
    fetch_brevo_email_stats,
    merge_brevo_stats,
)
from app.core.config import settings
from app.core.rate_limit import check_sliding_window
from app.db.session import get_db, SessionLocal
from app.long_jobs import schedule_background_work
from app.models.calendar_booking_sales import CalendarBookingSales
from app.models.client import Client, LifecycleState
from app.models.client_checkin import ClientCheckIn
from app.models.manual_payment import ManualPayment
from app.models.organization import Organization
from app.models.stripe_payment import StripePayment
from app.models.stripe_subscription import StripeSubscription
from app.models.stripe_treasury_transaction import StripeTreasuryTransaction, TreasuryTransactionStatus
from app.models.user import User
from app.models.whop_payment import WhopPayment
from app.utils.stripe_helpers import extract_email_from_payment_raw
from app.utils.stripe_ids import normalize_stripe_id_for_dedup

router = APIRouter()


from threading import Lock as ThreadingLock
from app.models.organization import Organization
from app.schemas.calendar_metrics import CalendarMonthlyRateRow, CalendarMonthlyCoachingResponse, TerminalMonthlyTrendsResponse
from app.schemas.admin import HealthTrendPeriod
from app.schemas.client import (
    TerminalSummaryResponse,
    TerminalCashCollected,
    TerminalCashBySourceBreakdown,
    TerminalCashSourceTotals,
    TerminalMRR,
    TerminalTopContributor,
)
from app.services.terminal_metrics_service import (
    build_calendar_monthly_coaching_periods,
    build_terminal_monthly_trends,
    org_whop_cash_usd_window,
    terminal_monthly_trends_cache_get,
    terminal_monthly_trends_cache_set,
)
@router.get(
    "/calendar/monthly-coaching-metrics",
    response_model=CalendarMonthlyCoachingResponse,
)
def get_calendar_monthly_coaching_metrics(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Monthly show-up % vs sales close % for the current org."""
    org_id = scope_org_id(current_user)
    org = db.query(Organization).filter(Organization.id == org_id).first()
    if not org:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Organization not found")
    return build_calendar_monthly_coaching_periods(db, org)


@router.get(
    "/terminal/monthly-trends",
    response_model=TerminalMonthlyTrendsResponse,
)
def get_terminal_monthly_trends(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Monthly combined cash, calendar rates, and sales calls booked since onboarding."""
    org_id = scope_org_id(current_user)
    cached = terminal_monthly_trends_cache_get(org_id)
    if cached is not None:
        return cached
    org = db.query(Organization).filter(Organization.id == org_id).first()
    if not org:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Organization not found")
    response = build_terminal_monthly_trends(db, org)
    terminal_monthly_trends_cache_set(org_id, response)
    return response


@router.get("/terminal-summary", response_model=TerminalSummaryResponse)
def get_terminal_summary(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Precomputed terminal dashboard summary: cash collected (today/7d/30d), MRR/ARR,
    and top 5 revenue contributors for 30d and 90d. One query instead of N+1.
    """
    org_id = getattr(current_user, "selected_org_id", current_user.org_id)

    now = datetime.utcnow()
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    seven_days_ago = today_start - timedelta(days=7)
    thirty_days_ago = today_start - timedelta(days=30)
    mtd_start = today_start.replace(day=1)  # First of current month

    # --- Cash collected: Stripe (dedupe by stripe_id) + Whop (paid) + Manual ---
    def _add_amount(tot: dict, ts: datetime, amount: float) -> None:
        if ts >= today_start:
            tot["today"] += amount
        if ts >= seven_days_ago:
            tot["last_7"] += amount
        if ts >= thirty_days_ago:
            tot["last_30"] += amount
        if ts >= mtd_start:
            tot["mtd"] += amount

    stripe_tot = {"today": 0.0, "last_7": 0.0, "last_30": 0.0, "mtd": 0.0}
    # Bound scan to cash windows (avoids loading full payment history per org).
    cash_since = min(thirty_days_ago, mtd_start)
    stripe_payments = (
        db.query(StripePayment)
        .filter(
            StripePayment.org_id == org_id,
            StripePayment.status == "succeeded",
            StripePayment.created_at >= cash_since,
        )
        .all()
    )
    seen_stripe_ids = set()
    for p in stripe_payments:
        if p.stripe_id and p.stripe_id in seen_stripe_ids:
            continue
        if p.stripe_id:
            seen_stripe_ids.add(p.stripe_id)
        ts = p.created_at
        if not ts:
            continue
        amount = (p.amount_cents or 0) / 100.0
        _add_amount(stripe_tot, ts, amount)

    manual_tot = {"today": 0.0, "last_7": 0.0, "last_30": 0.0, "mtd": 0.0}
    manual_payments = (
        db.query(ManualPayment)
        .filter(ManualPayment.org_id == org_id)
        .all()
    )
    for p in manual_payments:
        ts = p.payment_date or p.created_at
        if not ts:
            continue
        if getattr(ts, "tzinfo", None):
            ts = ts.astimezone(timezone.utc).replace(tzinfo=None)
        amount = (p.amount_cents or 0) / 100.0
        _add_amount(manual_tot, ts, amount)

    whop_tot = {"today": 0.0, "last_7": 0.0, "last_30": 0.0, "mtd": 0.0}
    for p in load_whop_payments(db, org_id):
        if (p.status or "").lower() not in WHOP_PAID_STATUSES:
            continue
        ts = p.created_at
        if not ts:
            continue
        amount = (p.amount_cents or 0) / 100.0
        _add_amount(whop_tot, ts, amount)

    today_cash = stripe_tot["today"] + manual_tot["today"] + whop_tot["today"]
    last_7_cash = stripe_tot["last_7"] + manual_tot["last_7"] + whop_tot["last_7"]
    last_30_cash = stripe_tot["last_30"] + manual_tot["last_30"] + whop_tot["last_30"]
    mtd_cash = stripe_tot["mtd"] + manual_tot["mtd"] + whop_tot["mtd"]

    cash_collected = TerminalCashCollected(
        today=today_cash,
        last_7_days=last_7_cash,
        last_30_days=last_30_cash,
        last_mtd=mtd_cash,
    )
    cash_by_source = TerminalCashBySourceBreakdown(
        stripe=TerminalCashSourceTotals(
            today=stripe_tot["today"],
            last_7_days=stripe_tot["last_7"],
            last_30_days=stripe_tot["last_30"],
            last_mtd=stripe_tot["mtd"],
        ),
        whop=TerminalCashSourceTotals(
            today=whop_tot["today"],
            last_7_days=whop_tot["last_7"],
            last_30_days=whop_tot["last_30"],
            last_mtd=whop_tot["mtd"],
        ),
        manual=TerminalCashSourceTotals(
            today=manual_tot["today"],
            last_7_days=manual_tot["last_7"],
            last_30_days=manual_tot["last_30"],
            last_mtd=manual_tot["mtd"],
        ),
    )

    # --- MRR/ARR: from Stripe subscriptions (active/trialing) or fallback to client estimated_mrr ---
    current_mrr = 0.0
    mrr_result = (
        db.query(func.coalesce(func.sum(StripeSubscription.mrr), 0))
        .filter(
            StripeSubscription.org_id == org_id,
            StripeSubscription.status.in_(["active", "trialing"]),
        )
        .scalar()
    )
    if mrr_result is not None:
        try:
            current_mrr = float(mrr_result)
        except (TypeError, ValueError):
            pass
    if current_mrr == 0.0:
        clients = db.query(Client).filter(Client.org_id == org_id).all()
        grouped = {}
        processed = set()
        for c in clients:
            if c.id in processed:
                continue
            key = normalize_email(c.email) if c.email else (f"stripe:{c.stripe_customer_id}" if c.stripe_customer_id else str(c.id))
            if key not in grouped:
                grouped[key] = []
            same = [x for x in clients if (x.id not in processed and (
                (normalize_email(x.email) == normalize_email(c.email) and c.email) or
                (x.stripe_customer_id == c.stripe_customer_id and c.stripe_customer_id and not c.email) or
                (x.id == c.id)
            ))]
            for x in same:
                grouped[key].append(x)
                processed.add(x.id)
        for group in grouped.values():
            max_mrr = max((float(c.estimated_mrr or 0) for c in group), default=0)
            current_mrr += max_mrr
    mrr = TerminalMRR(current_mrr=current_mrr, arr=current_mrr * 12)

    # --- Top contributors: revenue by client (then merge by email), 30d and 90d ---
    ninety_days_ago = today_start - timedelta(days=90)

    def _revenue_by_client(since: datetime):
        rev = {}
        stripe_q = (
            db.query(StripePayment.client_id, func.sum(StripePayment.amount_cents).label("total"))
            .filter(
                StripePayment.org_id == org_id,
                StripePayment.status == "succeeded",
                StripePayment.client_id.isnot(None),
                StripePayment.created_at >= since,
            )
            .group_by(StripePayment.client_id)
        )
        for row in stripe_q.all():
            cid = str(row.client_id)
            rev[cid] = rev.get(cid, 0) + (row.total or 0)
        manual_q = (
            db.query(ManualPayment.client_id, func.sum(ManualPayment.amount_cents).label("total"))
            .filter(
                ManualPayment.org_id == org_id,
                ManualPayment.payment_date >= since,
            )
            .group_by(ManualPayment.client_id)
        )
        for row in manual_q.all():
            cid = str(row.client_id)
            rev[cid] = rev.get(cid, 0) + (row.total or 0)
        return rev

    rev_30 = _revenue_by_client(thirty_days_ago)
    rev_90 = _revenue_by_client(ninety_days_ago)

    all_clients = db.query(Client).filter(Client.org_id == org_id).all()
    all_client_ids = [c.id for c in all_clients]

    # Postgres rejects IN () — skip when org has no clients yet.
    last_stripe = []
    last_manual = []
    if all_client_ids:
        last_stripe = (
            db.query(StripePayment.client_id, func.max(StripePayment.created_at).label("last_at"))
            .filter(
                StripePayment.org_id == org_id,
                StripePayment.client_id.in_(all_client_ids),
                StripePayment.status == "succeeded",
            )
            .group_by(StripePayment.client_id)
        ).all()
        last_manual = (
            db.query(ManualPayment.client_id, func.max(ManualPayment.payment_date).label("last_at"))
            .filter(
                ManualPayment.org_id == org_id,
                ManualPayment.client_id.in_(all_client_ids),
            )
            .group_by(ManualPayment.client_id)
        ).all()
    last_payment_by_client = {}
    for row in last_stripe:
        last_payment_by_client[str(row.client_id)] = row.last_at
    for row in last_manual:
        k = str(row.client_id)
        dt = row.last_at
        if dt and getattr(dt, "replace", None):
            dt = dt.replace(tzinfo=None) if dt.tzinfo else dt
        if k not in last_payment_by_client or (dt and (not last_payment_by_client[k] or dt > last_payment_by_client[k])):
            last_payment_by_client[k] = dt

    def _build_top(rev_by_client, limit=5):
        grouped = {}
        processed = set()
        for c in all_clients:
            if str(c.id) in processed:
                continue
            norm = normalize_email(c.email)
            if norm:
                key = f"email:{norm}"
                same = [x for x in all_clients if str(x.id) not in processed and normalize_email(x.email) == norm]
            elif c.stripe_customer_id:
                key = f"stripe:{c.stripe_customer_id}"
                same = [x for x in all_clients if str(x.id) not in processed and x.stripe_customer_id == c.stripe_customer_id]
            else:
                key = str(c.id)
                same = [c]
            for x in same:
                processed.add(str(x.id))
            if key not in grouped:
                grouped[key] = []
            grouped[key].extend(same)
        contributors = []
        for group in grouped.values():
            total_revenue_cents = sum(rev_by_client.get(str(c.id), 0) for c in group)
            if total_revenue_cents <= 0:
                continue
            primary = min(group, key=client_created_sort_key)
            names = set()
            for c in group:
                n = " ".join(filter(None, [c.first_name, c.last_name])).strip()
                if n:
                    names.add(n)
            display_name = " / ".join(sorted(names)) if names else (primary.email or "Unknown")
            last_dates = [last_payment_by_client.get(str(c.id)) for c in group if last_payment_by_client.get(str(c.id))]
            last_payment = max(last_dates, key=lambda d: d or datetime.min) if last_dates else None
            contributors.append({
                "client_id": str(primary.id),
                "display_name": display_name,
                "revenue": total_revenue_cents / 100.0,
                "last_payment_date": last_payment.isoformat() if last_payment else None,
                "merged_client_ids": [str(c.id) for c in group] if len(group) > 1 else None,
            })
        contributors.sort(key=lambda x: -x["revenue"])
        return [TerminalTopContributor(**c) for c in contributors[:limit]]

    top_30 = _build_top(rev_30)
    top_90 = _build_top(rev_90)

    return TerminalSummaryResponse(
        cash_collected=cash_collected,
        mrr=mrr,
        top_contributors_30d=top_30,
        top_contributors_90d=top_90,
        cash_by_source=cash_by_source,
    )

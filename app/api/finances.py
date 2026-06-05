"""
Combined Finances (Stripe + Whop + Manual) read APIs.
"""
from __future__ import annotations

import uuid
from collections import defaultdict
from datetime import datetime, timezone
from typing import Dict, List

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.api.deps import get_current_user
from app.models.user import User
from app.models.stripe_payment import StripePayment
from app.models.whop_payment import WhopPayment
from app.models.manual_payment import ManualPayment
from app.models.oauth_token import OAuthToken, OAuthProvider
from app.schemas.finances import (
    FinancesCombinedSummary,
    FinancesSourceSlice,
    FinancesRevenueTimelineResponse,
    FinancesTimelinePoint,
)
from app.api.stripe import check_stripe_connected
from app.services.finances_cash import finances_period_bounds

router = APIRouter()


def _org_id(user: User) -> uuid.UUID:
    return getattr(user, "selected_org_id", user.org_id)


def _whop_connected(db: Session, org_id: uuid.UUID) -> bool:
    return (
        db.query(OAuthToken)
        .filter(OAuthToken.org_id == org_id, OAuthToken.provider == OAuthProvider.WHOP)
        .first()
        is not None
    )


def _naive_utc(ts: datetime) -> datetime:
    if ts.tzinfo is not None:
        return ts.astimezone(timezone.utc).replace(tzinfo=None)
    return ts


def _stripe_succeeded_cents_since(
    db: Session, org_id: uuid.UUID, since: datetime, until: datetime
) -> int:
    """Dedupe by stripe_id (latest row wins), then sum amounts with created_at in [since, until]."""
    rows = (
        db.query(StripePayment)
        .filter(StripePayment.org_id == org_id, StripePayment.status == "succeeded")
        .order_by(StripePayment.created_at.desc())
        .all()
    )
    best: Dict[str, StripePayment] = {}
    for p in rows:
        sid = p.stripe_id or ""
        if not sid:
            continue
        if sid not in best:
            best[sid] = p
    total = 0
    for p in best.values():
        ts = p.created_at
        if not ts or ts < since or ts > until:
            continue
        total += p.amount_cents or 0
    return total


def _whop_paid_cents_since(db: Session, org_id: uuid.UUID, since: datetime, until: datetime) -> int:
    total = 0
    for p in db.query(WhopPayment).filter(WhopPayment.org_id == org_id).all():
        if (p.status or "").lower() != "paid":
            continue
        ts = p.created_at
        if not ts or ts < since or ts > until:
            continue
        total += p.amount_cents or 0
    return total


def _manual_cents_since(db: Session, org_id: uuid.UUID, since: datetime, until: datetime) -> int:
    total = 0
    for p in db.query(ManualPayment).filter(ManualPayment.org_id == org_id).all():
        ts = p.payment_date or p.created_at
        if not ts:
            continue
        ts = _naive_utc(ts)
        if ts < since or ts > until:
            continue
        total += p.amount_cents or 0
    return total


@router.get("/summary", response_model=FinancesCombinedSummary)
def finances_summary(
    range_days: int = Query(30, alias="range", ge=1, le=3660),
    scope: str | None = Query(None, description="Use 'mtd' for month-to-date primary window (matches Stripe dashboard)."),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Primary KPI window: rolling `range` days ending now, unless scope=mtd (calendar month-to-date)
    or scope=all (all recorded history). Combined cash = Stripe + Whop + manual payments.
    Field `last_30_days_revenue` holds the **primary** window total (not always 30d).
    """
    org_id = _org_id(current_user)
    now = datetime.utcnow()
    mtd_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    period_start, period_end = finances_period_bounds(scope, range_days, now)

    st_ok = check_stripe_connected(db, org_id)
    wh_ok = _whop_connected(db, org_id)

    smtd_only = _stripe_succeeded_cents_since(db, org_id, mtd_start, now) if st_ok else 0
    wmtd_only = _whop_paid_cents_since(db, org_id, mtd_start, now) if wh_ok else 0
    mmtd_only = _manual_cents_since(db, org_id, mtd_start, now)

    s_pri = _stripe_succeeded_cents_since(db, org_id, period_start, period_end) if st_ok else 0
    w_pri = _whop_paid_cents_since(db, org_id, period_start, period_end) if wh_ok else 0
    m_pri = _manual_cents_since(db, org_id, period_start, period_end)

    return FinancesCombinedSummary(
        stripe_connected=st_ok,
        whop_connected=wh_ok,
        combined=FinancesSourceSlice(
            last_30_days_revenue=(s_pri + w_pri + m_pri) / 100.0,
            last_mtd_revenue=(smtd_only + wmtd_only + mmtd_only) / 100.0,
        ),
        stripe=FinancesSourceSlice(
            last_30_days_revenue=s_pri / 100.0,
            last_mtd_revenue=smtd_only / 100.0,
        ),
        whop=FinancesSourceSlice(
            last_30_days_revenue=w_pri / 100.0,
            last_mtd_revenue=wmtd_only / 100.0,
        ),
        manual=FinancesSourceSlice(
            last_30_days_revenue=m_pri / 100.0,
            last_mtd_revenue=mmtd_only / 100.0,
        ),
    )


@router.get("/revenue-timeline", response_model=FinancesRevenueTimelineResponse)
def finances_revenue_timeline(
    range_days: int = Query(30, ge=1, le=3660, alias="range"),
    scope: str | None = Query(None, description="'mtd' = calendar month start; 'all' = entire history."),
    group_by: str = Query("day"),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    org_id = _org_id(current_user)
    since, _until = finances_period_bounds(scope, range_days)
    gb = group_by if group_by in ("day", "week") else "day"

    stripe_by_key: Dict[str, float] = defaultdict(float)
    if check_stripe_connected(db, org_id):
        seen: Dict[str, StripePayment] = {}
        for p in (
            db.query(StripePayment)
            .filter(
                StripePayment.org_id == org_id,
                StripePayment.status == "succeeded",
                StripePayment.created_at >= since,
            )
            .order_by(StripePayment.created_at.desc())
            .all()
        ):
            sid = p.stripe_id or ""
            if sid and sid not in seen:
                seen[sid] = p
        for p in seen.values():
            ts = p.created_at
            if not ts:
                continue
            if gb == "week":
                iso = ts.isocalendar()
                key = f"{iso[0]}-W{iso[1]:02d}"
            else:
                key = ts.strftime("%Y-%m-%d")
            stripe_by_key[key] += (p.amount_cents or 0) / 100.0

    whop_by_key: Dict[str, float] = defaultdict(float)
    if _whop_connected(db, org_id):
        for p in (
            db.query(WhopPayment)
            .filter(WhopPayment.org_id == org_id, WhopPayment.created_at >= since)
            .all()
        ):
            if (p.status or "").lower() != "paid":
                continue
            ts = p.created_at
            if not ts:
                continue
            if gb == "week":
                iso = ts.isocalendar()
                key = f"{iso[0]}-W{iso[1]:02d}"
            else:
                key = ts.strftime("%Y-%m-%d")
            whop_by_key[key] += (p.amount_cents or 0) / 100.0

    manual_by_key: Dict[str, float] = defaultdict(float)
    for p in db.query(ManualPayment).filter(ManualPayment.org_id == org_id).all():
        ts = p.payment_date or p.created_at
        if not ts:
            continue
        ts = _naive_utc(ts)
        if ts < since:
            continue
        if gb == "week":
            iso = ts.isocalendar()
            key = f"{iso[0]}-W{iso[1]:02d}"
        else:
            key = ts.strftime("%Y-%m-%d")
        manual_by_key[key] += (p.amount_cents or 0) / 100.0

    all_keys = sorted(
        set(stripe_by_key.keys()) | set(whop_by_key.keys()) | set(manual_by_key.keys())
    )
    timeline: List[FinancesTimelinePoint] = []
    for k in all_keys:
        sr = stripe_by_key.get(k, 0.0)
        wr = whop_by_key.get(k, 0.0)
        mr = manual_by_key.get(k, 0.0)
        timeline.append(
            FinancesTimelinePoint(
                date=k,
                stripe_revenue=sr,
                whop_revenue=wr,
                manual_revenue=mr,
                total_revenue=sr + wr + mr,
            )
        )
    return FinancesRevenueTimelineResponse(timeline=timeline, group_by=gb)

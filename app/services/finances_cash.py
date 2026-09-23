"""Combined cash window helpers (Stripe + Whop + manual)."""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Dict, Tuple
from uuid import UUID

from sqlalchemy import func
from sqlalchemy.orm import Session


def finances_period_bounds(
    scope: str | None,
    range_days: int,
    now: datetime | None = None,
) -> Tuple[datetime, datetime]:
    """
    Primary cash window [start, end] in naive UTC (matches Stripe dashboard MTD).
    - scope=mtd: calendar month start → now
    - scope=all: epoch → now (all recorded payments for the org)
    - else: rolling range_days ending now
    """
    end = now or datetime.utcnow()
    mtd_start = end.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if scope == "mtd":
        return mtd_start, end
    if scope == "all":
        return datetime(1970, 1, 1), end
    return end - timedelta(days=range_days), end


def _cents_map(rows) -> Dict[UUID, float]:
    out: Dict[UUID, float] = {}
    for org_id, cents in rows:
        out[org_id] = float(cents or 0) / 100.0
    return out


def _merge_usd(*maps: Dict[UUID, float]) -> Dict[UUID, float]:
    keys = set()
    for m in maps:
        keys |= set(m)
    return {k: sum(m.get(k, 0.0) for m in maps) for k in keys}


def _deduped_stripe_rows(db: Session):
    """Latest succeeded row per (org_id, stripe_id) — same rule as finances_summary."""
    from app.models.stripe_payment import StripePayment

    rn = func.row_number().over(
        partition_by=(StripePayment.org_id, StripePayment.stripe_id),
        order_by=(StripePayment.created_at.desc(), StripePayment.updated_at.desc()),
    )
    return (
        db.query(
            StripePayment.org_id.label("org_id"),
            StripePayment.amount_cents.label("amount_cents"),
            StripePayment.created_at.label("created_at"),
            rn.label("rn"),
        )
        .filter(
            StripePayment.status == "succeeded",
            StripePayment.stripe_id.isnot(None),
            StripePayment.stripe_id != "",
        )
        .subquery()
    )


def org_combined_cash_maps(
    db: Session, now: datetime
) -> Tuple[Dict[UUID, float], Dict[UUID, float], Dict[UUID, float]]:
    """
    Per-org combined cash (Stripe + Whop + manual) for last 30d, prior 30d, all-time.
    Matches Terminal / Finances combined: deduped Stripe succeeded + paid Whop + manual.
    Manual timestamps converted to naive UTC so tz-aware payment_date is not shifted.
    """
    from app.models.manual_payment import ManualPayment
    from app.models.whop_payment import WhopPayment
    from app.services.whop_sync import WHOP_PAID_STATUSES, stored_or_raw_amount_cents

    thirty = now - timedelta(days=30)
    sixty = now - timedelta(days=60)

    stripe_all: Dict[UUID, float] = {}
    stripe_30: Dict[UUID, float] = {}
    stripe_prev: Dict[UUID, float] = {}
    try:
        stripe_dedup = _deduped_stripe_rows(db)
        stripe_all = _cents_map(
            db.query(stripe_dedup.c.org_id, func.coalesce(func.sum(stripe_dedup.c.amount_cents), 0))
            .filter(stripe_dedup.c.rn == 1)
            .group_by(stripe_dedup.c.org_id)
            .all()
        )
        stripe_30 = _cents_map(
            db.query(stripe_dedup.c.org_id, func.coalesce(func.sum(stripe_dedup.c.amount_cents), 0))
            .filter(stripe_dedup.c.rn == 1, stripe_dedup.c.created_at >= thirty)
            .group_by(stripe_dedup.c.org_id)
            .all()
        )
        stripe_prev = _cents_map(
            db.query(stripe_dedup.c.org_id, func.coalesce(func.sum(stripe_dedup.c.amount_cents), 0))
            .filter(
                stripe_dedup.c.rn == 1,
                stripe_dedup.c.created_at >= sixty,
                stripe_dedup.c.created_at < thirty,
            )
            .group_by(stripe_dedup.c.org_id)
            .all()
        )
    except Exception:
        db.rollback()

    whop_all: Dict[UUID, float] = {}
    whop_30: Dict[UUID, float] = {}
    whop_prev: Dict[UUID, float] = {}
    try:
        for p in db.query(WhopPayment).all():
            if (p.status or "").lower() not in WHOP_PAID_STATUSES:
                continue
            ts = p.created_at
            if ts is None:
                continue
            usd = stored_or_raw_amount_cents(p) / 100.0
            oid = p.org_id
            whop_all[oid] = whop_all.get(oid, 0.0) + usd
            if ts >= thirty:
                whop_30[oid] = whop_30.get(oid, 0.0) + usd
            elif ts >= sixty:
                whop_prev[oid] = whop_prev.get(oid, 0.0) + usd
    except Exception:
        db.rollback()

    manual_all: Dict[UUID, float] = {}
    manual_30: Dict[UUID, float] = {}
    manual_prev: Dict[UUID, float] = {}
    try:
        pay_ts = func.timezone(
            "UTC", func.coalesce(ManualPayment.payment_date, ManualPayment.created_at)
        )
        manual_all = _cents_map(
            db.query(ManualPayment.org_id, func.coalesce(func.sum(ManualPayment.amount_cents), 0))
            .group_by(ManualPayment.org_id)
            .all()
        )
        manual_30 = _cents_map(
            db.query(ManualPayment.org_id, func.coalesce(func.sum(ManualPayment.amount_cents), 0))
            .filter(pay_ts >= thirty)
            .group_by(ManualPayment.org_id)
            .all()
        )
        manual_prev = _cents_map(
            db.query(ManualPayment.org_id, func.coalesce(func.sum(ManualPayment.amount_cents), 0))
            .filter(pay_ts >= sixty, pay_ts < thirty)
            .group_by(ManualPayment.org_id)
            .all()
        )
    except Exception:
        db.rollback()

    return (
        _merge_usd(stripe_30, whop_30, manual_30),
        _merge_usd(stripe_prev, whop_prev, manual_prev),
        _merge_usd(stripe_all, whop_all, manual_all),
    )

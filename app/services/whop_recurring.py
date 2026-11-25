"""Estimated recurring revenue (MRR) from synced Whop payments.

Whop sync stores payments only (no subscription objects), so MRR is inferred: payers
with a subscription billing reason or two or more paid payments are treated as recurring,
their billing cadence comes from the gaps between payments, and a payer counts as active
while their latest payment is no older than 1.5 cadences (plus a week of grace).
The result is an estimate and is labelled as such wherever it is surfaced.
"""
from __future__ import annotations

import statistics
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from sqlalchemy.orm import Session

from app.models.whop_payment import WhopPayment
from app.services.whop_sync import WHOP_PAID_STATUSES, _member_id, _payer_email

LOOKBACK_DAYS = 400
GRACE_DAYS = 7


def _payer_key(p: WhopPayment) -> Optional[str]:
    raw = p.raw if isinstance(p.raw, dict) else {}
    membership = raw.get("membership") or raw.get("membership_id")
    if isinstance(membership, dict):
        membership = membership.get("id")
    if isinstance(membership, str) and membership.strip():
        return f"membership:{membership.strip()}"
    member = _member_id(raw) if raw else None
    if member:
        return f"member:{member}"
    if p.client_id:
        return f"client:{p.client_id}"
    email = _payer_email(raw) if raw else None
    return f"email:{email.lower()}" if email else None


def _is_subscription_payment(p: WhopPayment) -> bool:
    raw = p.raw if isinstance(p.raw, dict) else {}
    return str(raw.get("billing_reason") or "").lower().startswith("subscription")


def _cadence(gap_days: Optional[float]) -> tuple[float, float, str]:
    """(cadence_days, monthly_factor, label) from a median payment gap; monthly if unknown."""
    if gap_days is None:
        return 30.0, 1.0, "monthly"
    if gap_days <= 10:
        return 7.0, 30.0 / 7.0, "weekly"
    if gap_days <= 45:
        return 30.0, 1.0, "monthly"
    if gap_days <= 120:
        return 91.0, 1.0 / 3.0, "quarterly"
    return 365.0, 1.0 / 12.0, "yearly"


def estimate_whop_mrr(db: Session, org_id: uuid.UUID, *, now: Optional[datetime] = None) -> Dict[str, Any]:
    now = now or datetime.utcnow()
    since = now - timedelta(days=LOOKBACK_DAYS)
    rows: List[WhopPayment] = [
        p
        for p in db.query(WhopPayment)
        .filter(WhopPayment.org_id == org_id, WhopPayment.created_at >= since)
        .order_by(WhopPayment.created_at.asc())
        .all()
        if (p.status or "").lower() in WHOP_PAID_STATUSES and (p.amount_cents or 0) > 0
    ]
    by_payer: Dict[str, List[WhopPayment]] = {}
    for p in rows:
        key = _payer_key(p)
        if key:
            by_payer.setdefault(key, []).append(p)

    active = 0
    lapsed = 0
    mrr_cents = 0.0
    for payments in by_payer.values():
        if len(payments) < 2 and not any(_is_subscription_payment(p) for p in payments):
            continue  # one-off purchase
        stamps = [p.created_at for p in payments if p.created_at]
        if not stamps:
            continue
        gaps = [(b - a).total_seconds() / 86400 for a, b in zip(stamps, stamps[1:]) if b > a]
        cadence_days, factor, _ = _cadence(statistics.median(gaps) if gaps else None)
        latest = payments[-1]
        if now - stamps[-1] <= timedelta(days=cadence_days * 1.5 + GRACE_DAYS):
            active += 1
            mrr_cents += (latest.amount_cents or 0) * factor
        else:
            lapsed += 1
    return {
        "estimated_mrr": round(mrr_cents / 100.0, 2),
        "estimated_arr": round(mrr_cents * 12 / 100.0, 2),
        "active_recurring_payers": active,
        "lapsed_recurring_payers": lapsed,
        "method": (
            "Estimated from paid Whop payments in the last 400 days: payers with a subscription "
            "billing reason or 2+ paid payments are recurring; cadence comes from payment gaps; "
            "active while the latest payment is within 1.5 cadences + 7 days. Not read from Whop "
            "subscription objects."
        ),
    }

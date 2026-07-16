"""Record and aggregate org-scoped LLM API usage."""
from __future__ import annotations

import logging
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from sqlalchemy import func
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

# Rough published list prices (USD per 1M tokens). Used for owner estimates only.
_MODEL_RATES_USD_PER_1M = {
    "gpt-4o-mini": (0.15, 0.60),
    "gpt-4o": (2.50, 10.00),
    "gpt-4.1-mini": (0.40, 1.60),
    "gpt-4.1": (2.00, 8.00),
    "gemini-2.0-flash": (0.10, 0.40),
    "gemini-1.5-flash": (0.075, 0.30),
    "gemini-1.5-pro": (1.25, 5.00),
}


def estimate_cost_usd(
    model: Optional[str],
    prompt_tokens: int,
    completion_tokens: int,
) -> Optional[float]:
    if prompt_tokens < 0 or completion_tokens < 0:
        return None
    key = (model or "").strip().lower()
    rates = None
    for name, pair in _MODEL_RATES_USD_PER_1M.items():
        if name in key:
            rates = pair
            break
    if rates is None:
        # Conservative default ≈ gpt-4o-mini
        rates = (0.15, 0.60)
    pin, pout = rates
    return round((prompt_tokens * pin + completion_tokens * pout) / 1_000_000.0, 6)


def record_llm_usage(
    *,
    org_id: Optional[uuid.UUID],
    provider: str,
    model: Optional[str],
    feature: str,
    prompt_tokens: int = 0,
    completion_tokens: int = 0,
    total_tokens: int = 0,
) -> None:
    """Best-effort persist; never raises into the LLM path."""
    if org_id is None:
        return
    pt = max(0, int(prompt_tokens or 0))
    ct = max(0, int(completion_tokens or 0))
    tt = max(0, int(total_tokens or 0)) or (pt + ct)
    if tt <= 0 and pt <= 0 and ct <= 0:
        return
    try:
        from app.db.session import SessionLocal
        from app.models.llm_usage_event import LlmUsageEvent

        cost = estimate_cost_usd(model, pt, ct)
        db = SessionLocal()
        try:
            db.add(
                LlmUsageEvent(
                    id=uuid.uuid4(),
                    org_id=org_id,
                    provider=(provider or "unknown")[:32],
                    model=(model or "")[:128] or None,
                    feature=(feature or "unknown")[:64],
                    prompt_tokens=pt,
                    completion_tokens=ct,
                    total_tokens=tt,
                    estimated_cost_usd=cost,
                    created_at=datetime.now(timezone.utc),
                )
            )
            db.commit()
        finally:
            db.close()
    except Exception:
        logger.exception("llm_usage record failed org=%s feature=%s", org_id, feature)


def _window_start(days: int) -> datetime:
    return datetime.now(timezone.utc) - timedelta(days=max(1, days))


def summarize_org_llm_usage(
    db: Session,
    org_id: uuid.UUID,
    *,
    days: int = 30,
) -> Dict[str, Any]:
    from app.models.llm_usage_event import LlmUsageEvent

    since = _window_start(days)
    q = db.query(LlmUsageEvent).filter(
        LlmUsageEvent.org_id == org_id,
        LlmUsageEvent.created_at >= since,
    )
    calls = q.count()
    totals = (
        db.query(
            func.coalesce(func.sum(LlmUsageEvent.prompt_tokens), 0),
            func.coalesce(func.sum(LlmUsageEvent.completion_tokens), 0),
            func.coalesce(func.sum(LlmUsageEvent.total_tokens), 0),
            func.coalesce(func.sum(LlmUsageEvent.estimated_cost_usd), 0.0),
        )
        .filter(
            LlmUsageEvent.org_id == org_id,
            LlmUsageEvent.created_at >= since,
        )
        .one()
    )
    by_feature_rows = (
        db.query(
            LlmUsageEvent.feature,
            func.count(LlmUsageEvent.id),
            func.coalesce(func.sum(LlmUsageEvent.total_tokens), 0),
            func.coalesce(func.sum(LlmUsageEvent.estimated_cost_usd), 0.0),
        )
        .filter(
            LlmUsageEvent.org_id == org_id,
            LlmUsageEvent.created_at >= since,
        )
        .group_by(LlmUsageEvent.feature)
        .order_by(func.sum(LlmUsageEvent.total_tokens).desc())
        .all()
    )
    by_feature = [
        {
            "feature": r[0],
            "calls": int(r[1]),
            "total_tokens": int(r[2]),
            "estimated_cost_usd": float(r[3] or 0),
        }
        for r in by_feature_rows
    ]
    return {
        "days": days,
        "calls": int(calls),
        "prompt_tokens": int(totals[0] or 0),
        "completion_tokens": int(totals[1] or 0),
        "total_tokens": int(totals[2] or 0),
        "estimated_cost_usd": float(totals[3] or 0),
        "by_feature": by_feature,
    }


def summarize_platform_llm_usage(db: Session, *, days: int = 30) -> Dict[str, Any]:
    from app.models.llm_usage_event import LlmUsageEvent
    from app.models.organization import Organization

    since = _window_start(days)
    totals = (
        db.query(
            func.count(LlmUsageEvent.id),
            func.coalesce(func.sum(LlmUsageEvent.prompt_tokens), 0),
            func.coalesce(func.sum(LlmUsageEvent.completion_tokens), 0),
            func.coalesce(func.sum(LlmUsageEvent.total_tokens), 0),
            func.coalesce(func.sum(LlmUsageEvent.estimated_cost_usd), 0.0),
        )
        .filter(LlmUsageEvent.created_at >= since)
        .one()
    )
    by_org_rows = (
        db.query(
            LlmUsageEvent.org_id,
            Organization.name,
            func.count(LlmUsageEvent.id),
            func.coalesce(func.sum(LlmUsageEvent.total_tokens), 0),
            func.coalesce(func.sum(LlmUsageEvent.estimated_cost_usd), 0.0),
        )
        .outerjoin(Organization, Organization.id == LlmUsageEvent.org_id)
        .filter(LlmUsageEvent.created_at >= since)
        .group_by(LlmUsageEvent.org_id, Organization.name)
        .order_by(func.sum(LlmUsageEvent.estimated_cost_usd).desc())
        .limit(25)
        .all()
    )
    by_org: List[Dict[str, Any]] = [
        {
            "org_id": str(r[0]),
            "organization_name": r[1] or str(r[0]),
            "calls": int(r[2]),
            "total_tokens": int(r[3]),
            "estimated_cost_usd": float(r[4] or 0),
        }
        for r in by_org_rows
    ]
    return {
        "days": days,
        "calls": int(totals[0] or 0),
        "prompt_tokens": int(totals[1] or 0),
        "completion_tokens": int(totals[2] or 0),
        "total_tokens": int(totals[3] or 0),
        "estimated_cost_usd": float(totals[4] or 0),
        "by_org": by_org,
    }


def llm_usage_timeseries(
    db: Session,
    *,
    org_id: Optional[uuid.UUID] = None,
    days: int = 30,
    scope: Optional[str] = None,
) -> Dict[str, Any]:
    """Daily estimated LLM API cost series for platform or a single org."""
    from app.models.llm_usage_event import LlmUsageEvent
    from app.models.organization import Organization
    from app.services.finances_cash import finances_period_bounds

    if scope is not None and scope not in ("mtd", "all"):
        raise ValueError("scope must be mtd or all")

    start, end = finances_period_bounds(scope, max(1, int(days or 30)))
    # llm_usage_events.created_at is timestamptz; finances bounds are naive UTC
    if start.tzinfo is None:
        start = start.replace(tzinfo=timezone.utc)
    if end.tzinfo is None:
        end = end.replace(tzinfo=timezone.utc)
    # Align day buckets to UTC midnight of the start bound
    start_day = start.replace(hour=0, minute=0, second=0, microsecond=0)

    filters = [
        LlmUsageEvent.created_at >= start,
        LlmUsageEvent.created_at <= end,
    ]
    organization_name: Optional[str] = None
    if org_id is not None:
        filters.append(LlmUsageEvent.org_id == org_id)
        org = db.query(Organization).filter(Organization.id == org_id).first()
        organization_name = org.name if org else str(org_id)

    day_expr = func.date_trunc("day", LlmUsageEvent.created_at)
    rows = (
        db.query(
            day_expr.label("day"),
            func.count(LlmUsageEvent.id),
            func.coalesce(func.sum(LlmUsageEvent.total_tokens), 0),
            func.coalesce(func.sum(LlmUsageEvent.estimated_cost_usd), 0.0),
        )
        .filter(*filters)
        .group_by(day_expr)
        .order_by(day_expr.asc())
        .all()
    )
    by_day: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        day_dt = r[0]
        if hasattr(day_dt, "date"):
            key = day_dt.date().isoformat()
        else:
            key = str(day_dt)[:10]
        by_day[key] = {
            "date": key,
            "calls": int(r[1] or 0),
            "total_tokens": int(r[2] or 0),
            "estimated_cost_usd": float(r[3] or 0),
        }

    # Fill continuous daily series for charting (cap "all" to first usage → now).
    if scope == "all":
        if by_day:
            fill_start = datetime.strptime(min(by_day.keys()), "%Y-%m-%d").replace(tzinfo=timezone.utc)
        else:
            fill_start = end.replace(hour=0, minute=0, second=0, microsecond=0)
    else:
        fill_start = start_day

    fill_end = end.replace(hour=0, minute=0, second=0, microsecond=0)
    points: List[Dict[str, Any]] = []
    cursor = fill_start
    # Safety cap (~3 years) so a sparse "all" window cannot explode memory
    max_days = 366 * 3
    filled = 0
    while cursor <= fill_end and filled < max_days:
        key = cursor.date().isoformat()
        points.append(
            by_day.get(
                key,
                {
                    "date": key,
                    "calls": 0,
                    "total_tokens": 0,
                    "estimated_cost_usd": 0.0,
                },
            )
        )
        cursor = cursor + timedelta(days=1)
        filled += 1

    totals = (
        db.query(
            func.count(LlmUsageEvent.id),
            func.coalesce(func.sum(LlmUsageEvent.total_tokens), 0),
            func.coalesce(func.sum(LlmUsageEvent.estimated_cost_usd), 0.0),
        )
        .filter(*filters)
        .one()
    )

    return {
        "org_id": str(org_id) if org_id else None,
        "organization_name": organization_name,
        "scope": scope,
        "days": days if scope not in ("mtd", "all") else None,
        "period_start": start.isoformat(),
        "period_end": end.isoformat(),
        "calls": int(totals[0] or 0),
        "total_tokens": int(totals[1] or 0),
        "estimated_cost_usd": float(totals[2] or 0),
        "points": points,
    }

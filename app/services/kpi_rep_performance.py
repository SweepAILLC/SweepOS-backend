"""Per-rep (setter/closer) KPI aggregation — feeds the By Rep performance dashboard.

Funnel metrics (outreach/bookings/show-up) come from org_kpi_daily_entries rows
that carry a rep_user_id (manual self-report or calendar-host attribution).
Close/cash credit comes from sales_activity_events (the append-only closer log),
which is independent of org_kpi_daily_entries and never touched by CSV import or
grid/calendar edits.
"""
from __future__ import annotations

import uuid
from datetime import date, datetime, timedelta, timezone
from typing import Dict, List, Optional

from sqlalchemy.orm import Session

from app.models.org_kpi_daily_entry import OrgKpiDailyEntry
from app.models.sales_activity_event import SalesActivityEvent
from app.models.user import User
from app.schemas.kpi import (
    KpiRepPerformanceMetrics,
    KpiRepPerformanceResponse,
    KpiRepPerformanceRow,
    safe_pct,
)
from app.services.org_members import user_display_name

_PERSONAL_BEST_LOOKBACK_MONTHS = 12
_FUNNEL_FIELDS = ("outreach_sent", "calls_booked", "calls_booked_activity", "calls_taken", "no_shows")


def _month_key(d: date) -> str:
    return d.strftime("%Y-%m")


def _empty_month_bucket() -> Dict[str, int]:
    return {
        "outreach_sent": 0,
        "calls_booked": 0,
        "calls_booked_activity": 0,
        "calls_taken": 0,
        "no_shows": 0,
        "closes": 0,
        "cash_collected_cents": 0,
    }


def _metrics_from_bucket(bucket: Dict[str, int]) -> KpiRepPerformanceMetrics:
    calls_booked = bucket.get("calls_booked", 0)
    calls_taken = bucket.get("calls_taken", 0)
    closes = bucket.get("closes", 0)
    return KpiRepPerformanceMetrics(
        outreach_sent=bucket.get("outreach_sent", 0),
        calls_booked=calls_booked,
        calls_booked_activity=bucket.get("calls_booked_activity", 0),
        calls_taken=calls_taken,
        no_shows=bucket.get("no_shows", 0),
        closes=closes,
        cash_collected_cents=bucket.get("cash_collected_cents", 0),
        show_up_pct=safe_pct(calls_taken, calls_booked),
        closing_rate_pct=safe_pct(closes, calls_taken),
    )


def build_rep_performance(
    db: Session,
    org_id: uuid.UUID,
    *,
    range_start: date,
    range_end: date,
) -> KpiRepPerformanceResponse:
    period_days = (range_end - range_start).days + 1
    previous_range_end = range_start - timedelta(days=1)
    previous_range_start = previous_range_end - timedelta(days=period_days - 1)

    lookback_start = range_end.replace(day=1)
    for _ in range(_PERSONAL_BEST_LOOKBACK_MONTHS - 1):
        lookback_start = (lookback_start - timedelta(days=1)).replace(day=1)
    query_start = min(previous_range_start, lookback_start)

    # entry_date-keyed per-rep-per-day funnel sums.
    entry_rows = (
        db.query(OrgKpiDailyEntry)
        .filter(
            OrgKpiDailyEntry.org_id == org_id,
            OrgKpiDailyEntry.rep_user_id.isnot(None),
            OrgKpiDailyEntry.entry_date >= query_start,
            OrgKpiDailyEntry.entry_date <= range_end,
        )
        .all()
    )
    # entry_date-keyed per-rep closer events.
    activity_rows = (
        db.query(SalesActivityEvent)
        .filter(
            SalesActivityEvent.org_id == org_id,
            SalesActivityEvent.rep_user_id.isnot(None),
            SalesActivityEvent.entry_date >= query_start,
            SalesActivityEvent.entry_date <= range_end,
        )
        .all()
    )

    rep_ids = {r.rep_user_id for r in entry_rows} | {r.rep_user_id for r in activity_rows}
    if not rep_ids:
        return KpiRepPerformanceResponse(
            org_id=org_id,
            range_start=range_start,
            range_end=range_end,
            previous_range_start=previous_range_start,
            previous_range_end=previous_range_end,
            generated_at=datetime.now(timezone.utc),
            reps=[],
        )

    users_by_id = {u.id: u for u in db.query(User).filter(User.id.in_(rep_ids)).all()}

    # rep_id -> period label ("current" | "previous" | "YYYY-MM") -> bucket
    by_rep: Dict[uuid.UUID, Dict[str, Dict[str, int]]] = {rid: {} for rid in rep_ids}

    def bucket_for(rep_id: uuid.UUID, label: str) -> Dict[str, int]:
        return by_rep[rep_id].setdefault(label, _empty_month_bucket())

    def labels_for_date(d: date) -> List[str]:
        labels = [_month_key(d)]
        if range_start <= d <= range_end:
            labels.append("current")
        elif previous_range_start <= d <= previous_range_end:
            labels.append("previous")
        return labels

    for e in entry_rows:
        for label in labels_for_date(e.entry_date):
            b = bucket_for(e.rep_user_id, label)
            for field in _FUNNEL_FIELDS:
                b[field] += int(getattr(e, field, None) or 0)

    for ev in activity_rows:
        for label in labels_for_date(ev.entry_date):
            b = bucket_for(ev.rep_user_id, label)
            if ev.is_closed:
                b["closes"] += 1
            b["cash_collected_cents"] += int(ev.cash_collected_cents or 0)

    rows: List[KpiRepPerformanceRow] = []
    for rep_id in rep_ids:
        periods = by_rep[rep_id]
        current = _metrics_from_bucket(periods.get("current", _empty_month_bucket()))
        previous = _metrics_from_bucket(periods.get("previous", _empty_month_bucket()))

        month_labels = [k for k in periods.keys() if k not in ("current", "previous")]
        best_bucket = _empty_month_bucket()
        best_month: Dict[str, Optional[str]] = {k: None for k in best_bucket}
        for key in (
            "outreach_sent",
            "calls_booked",
            "calls_booked_activity",
            "calls_taken",
            "no_shows",
            "closes",
            "cash_collected_cents",
        ):
            best_val = 0
            best_lbl: Optional[str] = None
            for lbl in month_labels:
                v = periods[lbl].get(key, 0)
                if v > best_val:
                    best_val = v
                    best_lbl = lbl
            best_bucket[key] = best_val
            best_month[key] = best_lbl
        personal_best = _metrics_from_bucket(best_bucket)

        user = users_by_id.get(rep_id)
        rows.append(
            KpiRepPerformanceRow(
                rep_user_id=rep_id,
                rep_name=user_display_name(user) if user else "Unknown",
                rep_email=(user.email if user else None),
                current=current,
                previous=previous,
                personal_best=personal_best,
                personal_best_month=best_month,
            )
        )

    rows.sort(key=lambda r: r.rep_name.lower())
    return KpiRepPerformanceResponse(
        org_id=org_id,
        range_start=range_start,
        range_end=range_end,
        previous_range_start=previous_range_start,
        previous_range_end=previous_range_end,
        generated_at=datetime.now(timezone.utc),
        reps=rows,
    )

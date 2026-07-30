"""
KPI Command Center helpers for Claude MCP.

Exposes snapshot cards/series, monthly rollups (totals), and multi-month trends
for the org bound to the connector — read-only, same math as /kpi/* APIs.
"""
from __future__ import annotations

import uuid
from datetime import date, timedelta
from typing import Any, Dict, List, Optional

from sqlalchemy.orm import Session

from app.models.org_kpi_benchmark import OrgKpiBenchmark
from app.models.org_kpi_daily_entry import OrgKpiDailyEntry
from app.schemas.kpi import DEFAULT_CONTENT_TYPE_TAGS, DEFAULT_KPI_THRESHOLDS
from app.services.kpi_bottleneck_service import detect_bottlenecks, utcnow
from app.services.kpi_compute import build_kpi_snapshot, build_monthly_rollups
from app.services.kpi_integration_sync import (
    has_calendar_source,
    has_payment_source,
    refresh_kpi_live_fields_for_range,
)

# Compact MoM series keys Claude should use for trend analysis.
_TREND_METRIC_KEYS = (
    "outreach_sent",
    "respondents",
    "followups_sent",
    "calls_booked",
    "calls_taken",
    "closes",
    "no_shows",
    "cash_collected",
    "revenue",
    "outreach_reply_pct",
    "convo_to_booking_pct",
    "show_up_pct",
    "closing_rate_pct",
    "avg_order_value",
    "days_with_data",
)


def _serialize(model: Any) -> Any:
    if model is None:
        return None
    if hasattr(model, "model_dump"):
        return model.model_dump(mode="json")
    if isinstance(model, list):
        return [_serialize(x) for x in model]
    return model


def _get_or_seed_benchmarks(db: Session, org_id: uuid.UUID) -> OrgKpiBenchmark:
    row = db.query(OrgKpiBenchmark).filter(OrgKpiBenchmark.org_id == org_id).first()
    if row:
        return row
    row = OrgKpiBenchmark(
        org_id=org_id,
        thresholds=dict(DEFAULT_KPI_THRESHOLDS),
        content_type_tags=list(DEFAULT_CONTENT_TYPE_TAGS),
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return row


def _parse_iso_date(value: Optional[str], *, field: str) -> Optional[date]:
    if not value:
        return None
    try:
        return date.fromisoformat(str(value).strip()[:10])
    except ValueError:
        raise ValueError(f"Invalid {field} date '{value}'. Use YYYY-MM-DD.") from None


def get_kpi_snapshot_for_mcp(
    db: Session,
    org_id: uuid.UUID,
    *,
    days: int = 30,
    start: Optional[str] = None,
    end: Optional[str] = None,
    include_flags: bool = True,
    include_series: bool = True,
    sync: bool = False,
) -> Dict[str, Any]:
    """Trailing-window KPI cards, daily series, current-month rollup, and bottleneck flags."""
    today = date.today()
    try:
        range_end = _parse_iso_date(end, field="end") or today
        range_start = _parse_iso_date(start, field="start") or (
            range_end - timedelta(days=max(1, min(int(days or 30), 365)) - 1)
        )
    except ValueError as e:
        return {"error": str(e)}

    if range_end < range_start:
        return {"error": "end must be on or after start"}

    if sync:
        try:
            refresh_kpi_live_fields_for_range(
                db, org_id, range_start, min(range_end, today)
            )
        except Exception:
            db.rollback()

    rows = (
        db.query(OrgKpiDailyEntry)
        .filter(
            OrgKpiDailyEntry.org_id == org_id,
            OrgKpiDailyEntry.entry_date >= range_start,
            OrgKpiDailyEntry.entry_date <= range_end,
        )
        .order_by(OrgKpiDailyEntry.entry_date.asc())
        .all()
    )
    bench = _get_or_seed_benchmarks(db, org_id)
    flags = []
    if include_flags:
        flags = detect_bottlenecks(
            db,
            org_id,
            thresholds_raw=bench.thresholds if isinstance(bench.thresholds, dict) else None,
        )

    snapshot = build_kpi_snapshot(
        rows,
        range_start=range_start,
        range_end=range_end,
        thresholds_raw=bench.thresholds if isinstance(bench.thresholds, dict) else None,
        flags=flags,
        include_series=include_series,
        calendar_available=has_calendar_source(db, org_id),
        payments_available=has_payment_source(db, org_id),
        generated_at=utcnow(),
    )
    payload = _serialize(snapshot)
    payload["org_id"] = str(org_id)
    payload["usage"] = (
        "KPI Command Center snapshot for this org. cards = window aggregates with optional "
        "tier (strong/okay/weak). current_month = calendar-month totals for the scoped window. "
        "series = daily points for charts. flags include threshold/stage bottlenecks and "
        "multi-month decline trends. Prefer get_kpi_monthly_rollups for longer MoM history and "
        "get_kpi_trends for a compact trend series."
    )
    if not rows:
        payload["hint"] = (
            "No KPI daily entries in this window. Enter KPIs in SweepOS Command Center "
            "(or the public kpi-entry form) and ensure calendar/payments are connected for live fields."
        )
    return payload


def get_kpi_monthly_rollups_for_mcp(
    db: Session,
    org_id: uuid.UUID,
    *,
    months: int = 12,
) -> Dict[str, Any]:
    """Calendar-month KPI totals (same as Command Center month footer / GET /kpi/rollups)."""
    n = max(1, min(int(months or 12), 36))
    start = date.today().replace(day=1) - timedelta(days=31 * n)
    rows = (
        db.query(OrgKpiDailyEntry)
        .filter(OrgKpiDailyEntry.org_id == org_id, OrgKpiDailyEntry.entry_date >= start)
        .order_by(OrgKpiDailyEntry.entry_date.asc())
        .all()
    )
    rollups = build_monthly_rollups(rows, months=n)
    return {
        "org_id": str(org_id),
        "months_requested": n,
        "count": len(rollups),
        "rollups": _serialize(rollups),
        "usage": (
            "Monthly KPI totals newest-first. Volume fields are sums; rate fields are "
            "ratio-of-sums (outreach_reply_pct, convo_to_booking_pct, show_up_pct, "
            "closing_rate_pct) plus day-average rate fields (avg_*). Use with get_kpi_trends "
            "to spot multi-month declines."
        ),
        "hint": (
            None
            if rollups
            else "No monthly KPI data yet. Add daily KPI entries in SweepOS Command Center."
        ),
        "generated_at": utcnow().isoformat(),
    }


def get_kpi_trends_for_mcp(
    db: Session,
    org_id: uuid.UUID,
    *,
    months: int = 6,
) -> Dict[str, Any]:
    """
    Compact month-over-month KPI trends + decline flags.

    Combines monthly rollup series with bottleneck trend flags (3+ months declining ≥10%).
    """
    n = max(3, min(int(months or 6), 24))
    start = date.today().replace(day=1) - timedelta(days=31 * n)
    rows = (
        db.query(OrgKpiDailyEntry)
        .filter(OrgKpiDailyEntry.org_id == org_id, OrgKpiDailyEntry.entry_date >= start)
        .order_by(OrgKpiDailyEntry.entry_date.asc())
        .all()
    )
    rollups = build_monthly_rollups(rows, months=n)
    # Chronological (oldest → newest) for trend reading
    chron = list(reversed(rollups))

    series: List[Dict[str, Any]] = []
    for r in chron:
        dumped = _serialize(r) or {}
        point: Dict[str, Any] = {
            "period_label": dumped.get("period_label"),
            "period_start": dumped.get("period_start"),
            "period_end": dumped.get("period_end"),
        }
        for key in _TREND_METRIC_KEYS:
            if key in dumped:
                point[key] = dumped[key]
        series.append(point)

    bench = _get_or_seed_benchmarks(db, org_id)
    all_flags = detect_bottlenecks(
        db,
        org_id,
        thresholds_raw=bench.thresholds if isinstance(bench.thresholds, dict) else None,
    )
    trend_flags = [
        _serialize(f) for f in all_flags if str(getattr(f, "id", "") or "").startswith("month-trend-")
    ]

    return {
        "org_id": str(org_id),
        "months_requested": n,
        "months_with_data": len(series),
        "series": series,
        "trend_flags": trend_flags,
        "bottleneck_flags": _serialize(all_flags),
        "usage": (
            "series is oldest→newest monthly KPIs for MoM comparison. trend_flags are "
            "multi-month declines (≥10% over 3 months) on reply/booking/show-up/close rates "
            "and outreach/follow-up daily averages. bottleneck_flags include threshold and "
            "stage comparisons as well. Pair with get_kpi_snapshot for the current window."
        ),
        "hint": (
            None
            if series
            else "No KPI history for trends. Enter at least a few months of daily KPIs first."
        ),
        "generated_at": utcnow().isoformat(),
    }


def get_kpi_flags_for_mcp(db: Session, org_id: uuid.UUID) -> Dict[str, Any]:
    """Bottleneck + trend flags from monthly KPI totals vs benchmarks."""
    bench = _get_or_seed_benchmarks(db, org_id)
    flags = detect_bottlenecks(
        db,
        org_id,
        thresholds_raw=bench.thresholds if isinstance(bench.thresholds, dict) else None,
    )
    return {
        "org_id": str(org_id),
        "flags": _serialize(flags),
        "count": len(flags),
        "generated_at": utcnow().isoformat(),
        "usage": (
            "Flags from monthly KPI totals: below-benchmark metrics, stage comparisons, "
            "and multi-month decline trends (ids like month-trend-*)."
        ),
    }

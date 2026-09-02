"""KPI computed rates, tier classification, and monthly rollups."""
from __future__ import annotations

from calendar import monthrange
from datetime import date
from typing import Any, Dict, Iterable, List, Optional, Sequence

from app.schemas.kpi import (
    DEFAULT_KPI_THRESHOLDS,
    KpiDailyEntryRead,
    KpiFlag,
    KpiMonthlyRollup,
    KpiSnapshotCard,
    KpiSnapshotResponse,
    KpiSnapshotSeriesPoint,
    MetricThreshold,
    TierName,
    compute_rates,
    safe_pct,
    safe_avg,
)

# Metric key on a daily entry → benchmark threshold key
METRIC_TO_THRESHOLD: Dict[str, str] = {
    "outreach_sent": "daily_dm_reachouts",
    "followups_sent": "daily_followups",
    "outreach_reply_pct": "dm_response_rate",
    "convo_to_booking_pct": "convo_to_booking_rate",
    "show_up_pct": "show_up_rate",
    "closing_rate_pct": "closing_rate",
}

FUNNEL_STAGES: List[Dict[str, Any]] = [
    {
        "id": "audience_growth",
        "label": "Audience Growth",
        "metrics": ["new_followers", "content_posted", "total_followers"],
        "primary": "new_followers",
    },
    {
        "id": "outreach_engagement",
        "label": "Outreach & Engagement",
        "metrics": ["outreach_sent", "respondents", "outreach_reply_pct"],
        "primary": "outreach_reply_pct",
        "volume": "outreach_sent",
    },
    {
        "id": "conversation_booking",
        "label": "Conversation → Booking",
        "metrics": [
            "inbound_icp_leads",
            "followups_sent",
            "new_conversations",
            "conversations_nurtured",
            "calls_pitched",
            "inbound_bookings",
            "outbound_bookings",
            "calls_booked",
            "convo_to_booking_pct",
        ],
        "primary": "convo_to_booking_pct",
        "volume": "calls_booked",
    },
    {
        "id": "sales_execution",
        "label": "Sales Execution",
        "metrics": [
            "calls_taken",
            "offers_made",
            "no_shows",
            "show_up_pct",
            "closes",
            "closing_rate_pct",
        ],
        "primary": "closing_rate_pct",
        "volume": "show_up_pct",
    },
    {
        "id": "revenue",
        "label": "Revenue",
        "metrics": ["cash_collected", "revenue"],
        "primary": None,
    },
]


def normalize_thresholds(raw: Optional[Dict[str, Any]]) -> Dict[str, MetricThreshold]:
    """Merge stored thresholds with defaults; return typed MetricThreshold map."""
    base = {k: MetricThreshold(**v) for k, v in DEFAULT_KPI_THRESHOLDS.items()}
    if not raw:
        return base
    for key, val in raw.items():
        if not isinstance(val, dict):
            continue
        try:
            if key in base:
                merged = base[key].model_dump()
                merged.update({k: v for k, v in val.items() if v is not None})
                base[key] = MetricThreshold(**merged)
            else:
                base[key] = MetricThreshold(**val)
        except Exception:
            continue
    return base


def thresholds_as_dict(thresholds: Dict[str, MetricThreshold]) -> Dict[str, Any]:
    return {k: v.model_dump() for k, v in thresholds.items()}


def classify_tier(value: Optional[float], threshold: MetricThreshold) -> Optional[TierName]:
    if value is None:
        return None
    try:
        v = float(value)
    except (TypeError, ValueError):
        return None
    if v >= threshold.strong_min:
        return "strong"
    # If okay_max is set and value exceeds it but is below strong, still treat as okay
    # when value >= okay_min (covers closed okay bands like show_up 80-90).
    if v >= threshold.okay_min:
        return "okay"
    return "weak"


def tier_for_metric(
    metric: str,
    value: Optional[float],
    thresholds: Dict[str, MetricThreshold],
) -> Optional[TierName]:
    key = METRIC_TO_THRESHOLD.get(metric)
    if not key or key not in thresholds:
        return None
    return classify_tier(value, thresholds[key])


def entry_to_dict(entry: Any) -> Dict[str, Any]:
    data = {
        "total_followers": entry.total_followers,
        "new_followers": entry.new_followers,
        "content_posted": entry.content_posted,
        "best_content_type": entry.best_content_type,
        "inboxes_checked": entry.inboxes_checked,
        "outreach_sent": entry.outreach_sent,
        "respondents": entry.respondents,
        "inbound_icp_leads": entry.inbound_icp_leads,
        "followups_sent": entry.followups_sent,
        "new_conversations": getattr(entry, "new_conversations", None),
        "conversations_nurtured": getattr(entry, "conversations_nurtured", None),
        "calls_pitched": entry.calls_pitched,
        "inbound_bookings": getattr(entry, "inbound_bookings", None),
        "outbound_bookings": getattr(entry, "outbound_bookings", None),
        "calls_booked": entry.calls_booked,
        "calls_booked_activity": getattr(entry, "calls_booked_activity", None),
        "calls_taken": entry.calls_taken,
        "offers_made": entry.offers_made,
        "no_shows": entry.no_shows,
        "closes": entry.closes,
        "cash_collected": float(entry.cash_collected) if entry.cash_collected is not None else None,
        "revenue": float(entry.revenue) if entry.revenue is not None else None,
        "setter_context": getattr(entry, "setter_context", None),
    }
    data.update(compute_rates(data))
    return data


def _mean(values: Sequence[float]) -> Optional[float]:
    if not values:
        return None
    return round(sum(values) / len(values), 2)


def _group_by_date(rows: Sequence[Any]) -> Dict[date, List[Any]]:
    """Group rows sharing an entry_date — orgs with per-rep entries can have >1 row/date."""
    by_date: Dict[date, List[Any]] = {}
    for e in rows:
        by_date.setdefault(e.entry_date, []).append(e)
    return by_date


def _per_date_rate(rows_for_date: List[Any], numer_key: str, denom_key: str) -> Optional[float]:
    """Sum numerator/denominator across all rows for one date, then compute one rate —
    correct whether a date has one org-aggregate row or several per-rep rows."""
    dicts = [entry_to_dict(e) for e in rows_for_date]
    numer = sum(float(d.get(numer_key) or 0) for d in dicts)
    denom = sum(float(d.get(denom_key) or 0) for d in dicts)
    return safe_pct(numer, denom)


def build_monthly_rollups(entries: Iterable[Any], months: int = 12) -> List[KpiMonthlyRollup]:
    """Group entries by calendar month; newest months first, capped at `months`."""
    by_month: Dict[str, List[Any]] = {}
    for e in entries:
        key = e.entry_date.strftime("%Y-%m")
        by_month.setdefault(key, []).append(e)

    keys = sorted(by_month.keys(), reverse=True)[: max(1, months)]
    rollups: List[KpiMonthlyRollup] = []

    for key in keys:
        rows = by_month[key]
        by_date = _group_by_date(rows)
        year, month = map(int, key.split("-"))
        period_start = date(year, month, 1)
        period_end = date(year, month, monthrange(year, month)[1])

        sums = {
            "new_followers": 0,
            "outreach_sent": 0,
            "respondents": 0,
            "inbound_icp_leads": 0,
            "followups_sent": 0,
            "new_conversations": 0,
            "conversations_nurtured": 0,
            "calls_pitched": 0,
            "inbound_bookings": 0,
            "outbound_bookings": 0,
            "calls_booked": 0,
            "calls_booked_activity": 0,
            "calls_taken": 0,
            "offers_made": 0,
            "no_shows": 0,
            "closes": 0,
            "cash_collected": 0.0,
            "revenue": 0.0,
            "content_posted_days": 0,
        }
        reply_rates: List[float] = []
        booking_rates: List[float] = []
        show_rates: List[float] = []
        close_rates: List[float] = []

        for e in rows:
            d = entry_to_dict(e)
            for k in (
                "new_followers",
                "outreach_sent",
                "respondents",
                "inbound_icp_leads",
                "followups_sent",
                "new_conversations",
                "conversations_nurtured",
                "calls_pitched",
                "inbound_bookings",
                "outbound_bookings",
                "calls_booked",
                "calls_booked_activity",
                "calls_taken",
                "offers_made",
                "no_shows",
                "closes",
            ):
                sums[k] += int(d.get(k) or 0)
            if d.get("cash_collected") is not None:
                sums["cash_collected"] += float(d["cash_collected"])
            if d.get("revenue") is not None:
                sums["revenue"] += float(d["revenue"])
            if d.get("content_posted"):
                sums["content_posted_days"] += 1

        # One rate per calendar date (summed across rows sharing that date first),
        # not one rate per row — otherwise a date with several per-rep rows would
        # count multiple times in the average instead of once.
        for rows_for_date in by_date.values():
            r = _per_date_rate(rows_for_date, "respondents", "outreach_sent")
            if r is not None:
                reply_rates.append(r)
            r = _per_date_rate(rows_for_date, "calls_booked", "respondents")
            if r is not None:
                booking_rates.append(r)
            r = _per_date_rate(rows_for_date, "calls_taken", "calls_booked")
            if r is not None:
                show_rates.append(r)
            r = _per_date_rate(rows_for_date, "closes", "calls_taken")
            if r is not None:
                close_rates.append(r)

        rollups.append(
            KpiMonthlyRollup(
                period_label=key,
                period_start=period_start,
                period_end=period_end,
                days_with_data=len(by_date),
                new_followers=sums["new_followers"],
                outreach_sent=sums["outreach_sent"],
                respondents=sums["respondents"],
                inbound_icp_leads=sums["inbound_icp_leads"],
                followups_sent=sums["followups_sent"],
                new_conversations=sums["new_conversations"],
                conversations_nurtured=sums["conversations_nurtured"],
                calls_pitched=sums["calls_pitched"],
                inbound_bookings=sums["inbound_bookings"],
                outbound_bookings=sums["outbound_bookings"],
                calls_booked=sums["calls_booked"],
                calls_booked_activity=sums["calls_booked_activity"],
                calls_taken=sums["calls_taken"],
                offers_made=sums["offers_made"],
                no_shows=sums["no_shows"],
                closes=sums["closes"],
                cash_collected=round(sums["cash_collected"], 2),
                revenue=round(sums["revenue"], 2),
                content_posted_days=sums["content_posted_days"],
                avg_outreach_reply_pct=_mean(reply_rates),
                avg_convo_to_booking_pct=_mean(booking_rates),
                avg_show_up_pct=_mean(show_rates),
                avg_closing_rate_pct=_mean(close_rates),
                outreach_reply_pct=safe_pct(sums["respondents"], sums["outreach_sent"]),
                convo_to_booking_pct=safe_pct(sums["calls_booked"], sums["respondents"]),
                show_up_pct=safe_pct(sums["calls_taken"], sums["calls_booked"]),
                closing_rate_pct=safe_pct(sums["closes"], sums["calls_taken"]),
                avg_order_value=safe_avg(sums["revenue"], sums["closes"]),
            )
        )
    return rollups


def metric_range_score(value: Optional[float], threshold: MetricThreshold) -> Optional[float]:
    """Map a metric onto a 0–2 continuum using okay/strong range bounds."""
    if value is None:
        return None
    try:
        v = float(value)
        okay = float(threshold.okay_min)
        strong = float(threshold.strong_min)
    except (TypeError, ValueError):
        return None
    if v >= strong:
        return 2.0
    if v >= okay:
        span = strong - okay or 1.0
        return 1.0 + (v - okay) / span
    if okay <= 0:
        return 0.0
    return max(0.0, min(1.0, v / okay))


def _tier_from_average_score(avg: float) -> TierName:
    if avg >= 1.5:
        return "strong"
    if avg >= 0.75:
        return "okay"
    return "weak"


def overall_day_tier(
    entry_dict: Dict[str, Any],
    thresholds: Dict[str, MetricThreshold],
) -> Optional[TierName]:
    """Average of primary metrics' range scores for calendar color-coding."""
    scores: List[float] = []
    for metric in (
        "outreach_sent",
        "followups_sent",
        "outreach_reply_pct",
        "convo_to_booking_pct",
        "show_up_pct",
        "closing_rate_pct",
    ):
        key = METRIC_TO_THRESHOLD.get(metric)
        if not key or key not in thresholds:
            continue
        score = metric_range_score(entry_dict.get(metric), thresholds[key])
        if score is None:
            continue
        scores.append(score)
    if not scores:
        return None
    return _tier_from_average_score(sum(scores) / len(scores))


SNAPSHOT_CARD_DEFS = (
    {"key": "total_conversations", "label": "Total Conversations", "kind": "int", "aggregation": "sum", "tier_metric": None},
    {"key": "calls_booked", "label": "Calls Booked", "kind": "int", "aggregation": "sum", "tier_metric": None},
    {"key": "calls_booked_activity", "label": "Calls Booked (Activity)", "kind": "int", "aggregation": "sum", "tier_metric": None},
    {"key": "calls_taken", "label": "Calls Taken", "kind": "int", "aggregation": "sum", "tier_metric": None},
    {"key": "closes", "label": "Closes", "kind": "int", "aggregation": "sum", "tier_metric": None},
    {"key": "convo_to_booking_pct", "label": "Convo→Book", "kind": "pct", "aggregation": "avg", "tier_metric": "convo_to_booking_pct"},
    {"key": "outreach_reply_pct", "label": "Reply %", "kind": "pct", "aggregation": "avg", "tier_metric": "outreach_reply_pct"},
    {"key": "show_up_pct", "label": "Show-up %", "kind": "pct", "aggregation": "avg", "tier_metric": "show_up_pct"},
    {"key": "closing_rate_pct", "label": "Close %", "kind": "pct", "aggregation": "avg", "tier_metric": "closing_rate_pct"},
    {"key": "cash_collected", "label": "Cash Collected", "kind": "currency", "aggregation": "sum", "tier_metric": None},
)


def build_kpi_snapshot(
    entries: Sequence[Any],
    *,
    range_start: date,
    range_end: date,
    thresholds_raw: Optional[Dict[str, Any]] = None,
    flags: Optional[List[KpiFlag]] = None,
    include_series: bool = True,
    calendar_available: bool = False,
    payments_available: bool = False,
    generated_at: Optional[Any] = None,
) -> KpiSnapshotResponse:
    """Compact insights payload for owner dashboard, terminal graphs, and cross-tab cards."""
    from datetime import datetime, timezone

    thresholds = normalize_thresholds(thresholds_raw)
    scoped = [
        e
        for e in entries
        if getattr(e, "entry_date", None) is not None
        and range_start <= e.entry_date <= range_end
    ]
    scoped.sort(key=lambda e: e.entry_date)
    days = (range_end - range_start).days + 1
    n = max(1, len(scoped))

    # Precompute rates onto dicts once
    dicts = [entry_to_dict(e) for e in scoped]

    def _total_conversations(d: Dict[str, Any]) -> float:
        return float(
            (d.get("new_conversations") or 0)
            + (d.get("followups_sent") or 0)
            + (d.get("outreach_sent") or 0)
            + (d.get("conversations_nurtured") or 0)
        )

    cards: List[KpiSnapshotCard] = []
    for defn in SNAPSHOT_CARD_DEFS:
        key = defn["key"]
        if key == "total_conversations":
            total = sum(_total_conversations(d) for d in dicts)
            any_val = bool(dicts)
            value = round(total, 2) if any_val else None
            tier_val = None
        elif defn["aggregation"] == "sum":
            total = 0.0
            any_val = False
            for d in dicts:
                v = d.get(key)
                if isinstance(v, (int, float)):
                    total += float(v)
                    any_val = True
            value = round(total, 2) if any_val else None
            # Tier volume metrics against daily average
            tier_val = (total / n) if any_val and defn.get("tier_metric") else None
        else:
            vals = [float(d[key]) for d in dicts if isinstance(d.get(key), (int, float))]
            value = round(sum(vals) / len(vals), 2) if vals else None
            tier_val = value
        tier = None
        if defn.get("tier_metric") and tier_val is not None:
            tier = tier_for_metric(defn["tier_metric"], tier_val, thresholds)
        cards.append(
            KpiSnapshotCard(
                key=key,
                label=defn["label"],
                value=value,
                kind=defn["kind"],
                aggregation=defn["aggregation"],
                tier=tier,
            )
        )

    rollups = build_monthly_rollups(scoped, months=1)
    current_month = rollups[0] if rollups else None

    series: List[KpiSnapshotSeriesPoint] = []
    if include_series:
        # One point per calendar date — sum across rows sharing a date first
        # (an org with per-rep entries can have several rows for one date).
        by_date = _group_by_date(scoped)
        for entry_date in sorted(by_date.keys()):
            rows_for_date = by_date[entry_date]
            day_dicts = [entry_to_dict(e) for e in rows_for_date]
            summed = {
                k: sum(float(dd.get(k) or 0) for dd in day_dicts)
                for k in (
                    "outreach_sent",
                    "followups_sent",
                    "new_conversations",
                    "conversations_nurtured",
                    "respondents",
                    "calls_booked",
                    "calls_taken",
                    "closes",
                    "cash_collected",
                    "revenue",
                )
            }
            rates = compute_rates(summed)
            total_convos = int(
                summed["new_conversations"]
                + summed["followups_sent"]
                + summed["outreach_sent"]
                + summed["conversations_nurtured"]
            )
            series.append(
                KpiSnapshotSeriesPoint(
                    date=entry_date,
                    outreach_sent=int(summed["outreach_sent"]),
                    total_conversations=total_convos,
                    calls_booked=int(summed["calls_booked"]),
                    calls_taken=int(summed["calls_taken"]),
                    closes=int(summed["closes"]),
                    cash_collected=summed["cash_collected"],
                    revenue=summed["revenue"],
                    show_up_pct=rates.get("show_up_pct"),
                    closing_rate_pct=rates.get("closing_rate_pct"),
                    convo_to_booking_pct=rates.get("convo_to_booking_pct"),
                    outreach_reply_pct=rates.get("outreach_reply_pct"),
                )
            )

    flag_list = list(flags or [])
    gen = generated_at or datetime.now(timezone.utc)
    return KpiSnapshotResponse(
        range_start=range_start,
        range_end=range_end,
        days=days,
        generated_at=gen,
        days_with_data=len({e.entry_date for e in scoped}),
        cards=cards,
        current_month=current_month,
        flags=flag_list[:8],
        flags_truncated=len(flag_list),
        series=series,
        calendar_available=calendar_available,
        payments_available=payments_available,
    )

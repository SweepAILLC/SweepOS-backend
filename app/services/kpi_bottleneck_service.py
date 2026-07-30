"""Bottleneck detection for KPI Command Center.

Suggestions are based on cumulative monthly metrics — the same totals shown
in the KPI grid month footer row (sums + ratio-of-sums rates).
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Sequence
from uuid import UUID

from sqlalchemy.orm import Session

from app.models.org_kpi_daily_entry import OrgKpiDailyEntry
from app.schemas.kpi import KpiFlag, KpiMonthlyRollup, MetricThreshold, TierName
from app.services.kpi_compute import (
    FUNNEL_STAGES,
    build_monthly_rollups,
    normalize_thresholds,
    tier_for_metric,
)

RELATED_FEATURE_BY_BOTTLENECK = {
    "messaging": "content_studio",
    "content_growth": "content_studio",
    "pre_call": "automations",
    "sales_execution": "call_library",
    "pitch_booking": None,
}

TIER_RANK = {"strong": 2, "okay": 1, "weak": 0}

# Daily-threshold metrics → compare month-average (sum / days_with_data)
# Rate metrics use ratio-of-sums from monthly rollups (same as grid footer).


def _load_entries(db: Session, org_id: UUID, lookback_days: int = 200) -> List[OrgKpiDailyEntry]:
    start = date.today() - timedelta(days=lookback_days)
    return (
        db.query(OrgKpiDailyEntry)
        .filter(OrgKpiDailyEntry.org_id == org_id, OrgKpiDailyEntry.entry_date >= start)
        .order_by(OrgKpiDailyEntry.entry_date.asc())
        .all()
    )


def _rollup_metric(rollup: KpiMonthlyRollup, metric: str) -> Optional[float]:
    """Value used for bottlenecking — matches grid month footer semantics.

    Rate metrics use ratio-of-sums. Volume metrics use per-day average so they
    remain comparable to daily benchmark floors.
    """
    days = max(int(rollup.days_with_data or 0), 1)
    if metric == "outreach_sent":
        return round(rollup.outreach_sent / days, 2)
    if metric == "followups_sent":
        return round(rollup.followups_sent / days, 2)
    if metric == "calls_booked":
        return round(rollup.calls_booked / days, 2)
    if metric == "calls_taken":
        return round(rollup.calls_taken / days, 2)
    if metric == "closes":
        return float(rollup.closes)
    if metric == "respondents":
        return round(rollup.respondents / days, 2)
    if metric == "outreach_reply_pct":
        return rollup.outreach_reply_pct
    if metric == "convo_to_booking_pct":
        return rollup.convo_to_booking_pct
    if metric == "show_up_pct":
        return rollup.show_up_pct
    if metric == "closing_rate_pct":
        return rollup.closing_rate_pct
    if metric == "no_shows":
        return float(rollup.no_shows)
    if metric == "cash_collected":
        return float(rollup.cash_collected) if rollup.cash_collected is not None else None
    if metric == "revenue":
        return float(rollup.revenue) if rollup.revenue is not None else None
    return None


def _related_for_metric(metric: str) -> Optional[str]:
    if metric in ("outreach_reply_pct",):
        return RELATED_FEATURE_BY_BOTTLENECK["messaging"]
    if metric in ("new_followers", "content_posted"):
        return RELATED_FEATURE_BY_BOTTLENECK["content_growth"]
    if metric in ("show_up_pct", "no_shows"):
        return RELATED_FEATURE_BY_BOTTLENECK["pre_call"]
    if metric in ("closing_rate_pct", "closes"):
        return RELATED_FEATURE_BY_BOTTLENECK["sales_execution"]
    if metric in ("convo_to_booking_pct", "calls_booked"):
        return RELATED_FEATURE_BY_BOTTLENECK["pitch_booking"]
    return None


def _flag_month_thresholds(
    rollup: KpiMonthlyRollup,
    thresholds: Dict[str, MetricThreshold],
) -> List[KpiFlag]:
    flags: List[KpiFlag] = []
    tracked = [
        ("outreach_sent", "Outreach & Engagement", "daily outreach (month avg)"),
        ("followups_sent", "Outreach & Engagement", "daily follow-ups (month avg)"),
        ("outreach_reply_pct", "Outreach & Engagement", "response rate"),
        ("convo_to_booking_pct", "Conversation → Booking", "convo-to-booking rate"),
        ("show_up_pct", "Sales Execution", "show-up rate"),
        ("closing_rate_pct", "Sales Execution", "closing rate"),
    ]
    for metric, stage, label in tracked:
        val = _rollup_metric(rollup, metric)
        tier = tier_for_metric(metric, val, thresholds)
        if tier != "weak" or val is None:
            continue
        th_key = {
            "outreach_sent": "daily_dm_reachouts",
            "followups_sent": "daily_followups",
            "outreach_reply_pct": "dm_response_rate",
            "convo_to_booking_pct": "convo_to_booking_rate",
            "show_up_pct": "show_up_rate",
            "closing_rate_pct": "closing_rate",
        }[metric]
        floor = thresholds[th_key].okay_min
        unit = "%" if thresholds[th_key].unit == "percent" else ""
        flags.append(
            KpiFlag(
                id=f"month-threshold-{metric}-{rollup.period_label}",
                metric=metric,
                stage=stage,
                tier="weak",
                message=(
                    f"Your {label} for {rollup.period_label} is weak "
                    f"({val}{unit} vs floor of {floor}{unit}) based on the month totals "
                    f"({rollup.days_with_data} days with data)."
                ),
                comparison=f"{rollup.period_label} cumulative",
                related_feature=_related_for_metric(metric),
                severity="critical",
                window_start=rollup.period_start,
                window_end=min(rollup.period_end, date.today()),
            )
        )
    return flags


def _flag_month_stage_comparisons(
    rollup: KpiMonthlyRollup,
    thresholds: Dict[str, MetricThreshold],
) -> List[KpiFlag]:
    flags: List[KpiFlag] = []
    rules = [
        {
            "upstream_metric": "outreach_sent",
            "downstream_metric": "outreach_reply_pct",
            "stage": "Outreach & Engagement",
            "cause": "messaging",
            "message_tpl": (
                "Your outreach volume for {period} is {up_tier} ({up_val:.0f}/day avg) but your "
                "reply rate is weak at {down_val:.0f}% (month totals). The bottleneck isn't effort — it's messaging."
            ),
            "related": "content_studio",
        },
        {
            "upstream_metric": "outreach_reply_pct",
            "downstream_metric": "convo_to_booking_pct",
            "stage": "Conversation → Booking",
            "cause": "pitch_booking",
            "message_tpl": (
                "Reply rate for {period} is {up_tier} ({up_val:.0f}%) but convo-to-booking is weak at "
                "{down_val:.0f}% on month totals. Conversations aren't converting — this is a pitch/booking-ask problem."
            ),
            "related": None,
        },
        {
            "upstream_metric": "calls_booked",
            "downstream_metric": "show_up_pct",
            "stage": "Sales Execution",
            "cause": "pre_call",
            "message_tpl": (
                "Show-up rate for {period} has fallen to {down_val:.0f}% on month totals, below your floor. "
                "Booking volume is unaffected — this is a pre-call experience problem, not a lead quality problem."
            ),
            "related": "automations",
        },
        {
            "upstream_metric": "show_up_pct",
            "downstream_metric": "closing_rate_pct",
            "stage": "Sales Execution",
            "cause": "sales_execution",
            "message_tpl": (
                "Show-up for {period} is {up_tier} ({up_val:.0f}%) but closing rate is weak at {down_val:.0f}% "
                "on month totals. This is a sales execution bottleneck — review call quality in Call Library."
            ),
            "related": "call_library",
        },
    ]

    for rule in rules:
        up_val = _rollup_metric(rollup, rule["upstream_metric"])
        down_val = _rollup_metric(rollup, rule["downstream_metric"])
        up_tier = tier_for_metric(rule["upstream_metric"], up_val, thresholds)
        down_tier = tier_for_metric(rule["downstream_metric"], down_val, thresholds)
        if up_tier is None or down_tier is None or up_val is None or down_val is None:
            continue
        if up_tier in ("strong", "okay") and down_tier == "weak":
            msg = rule["message_tpl"].format(
                period=rollup.period_label,
                up_tier=up_tier,
                up_val=up_val,
                down_tier=down_tier,
                down_val=down_val,
            )
            flags.append(
                KpiFlag(
                    id=f"month-stage-{rule['cause']}-{rollup.period_label}",
                    metric=rule["downstream_metric"],
                    stage=rule["stage"],
                    tier="weak",
                    message=msg,
                    comparison=(
                        f"{rule['upstream_metric']}={up_tier} vs "
                        f"{rule['downstream_metric']}={down_tier} ({rollup.period_label})"
                    ),
                    related_feature=rule["related"],
                    severity="critical",
                    window_start=rollup.period_start,
                    window_end=min(rollup.period_end, date.today()),
                )
            )

    if not flags:
        prev_tier: Optional[TierName] = None
        prev_label: Optional[str] = None
        for stage in FUNNEL_STAGES:
            primary = stage.get("primary")
            if not primary:
                continue
            val = _rollup_metric(rollup, primary)
            tier = tier_for_metric(primary, val, thresholds)
            if tier is None:
                continue
            if prev_tier is not None and TIER_RANK[tier] < TIER_RANK[prev_tier]:
                flags.append(
                    KpiFlag(
                        id=f"month-stage-drop-{stage['id']}-{rollup.period_label}",
                        metric=primary,
                        stage=stage["label"],
                        tier=tier,
                        message=(
                            f"Performance drops at {stage['label']} in {rollup.period_label}: "
                            f"{prev_label} was {prev_tier} but {primary} is {tier}"
                            + (f" ({val})" if val is not None else "")
                            + ". This is the first funnel stage where month totals fall off."
                        ),
                        comparison=f"{prev_label}={prev_tier} → {primary}={tier}",
                        related_feature=_related_for_metric(primary),
                        severity="watch",
                        window_start=rollup.period_start,
                        window_end=min(rollup.period_end, date.today()),
                    )
                )
                break
            prev_tier = tier
            prev_label = stage["label"]

    return flags


def _flag_month_trends(
    rollups: Sequence[KpiMonthlyRollup],
    thresholds: Dict[str, MetricThreshold],
) -> List[KpiFlag]:
    """Flag multi-month decline using cumulative month metrics (newest-first rollups)."""
    flags: List[KpiFlag] = []
    if len(rollups) < 3:
        return flags

    # Oldest → newest for trend reading
    chron = list(reversed(rollups[:4]))
    tracked = [
        ("outreach_reply_pct", "Outreach & Engagement", "reply rate"),
        ("convo_to_booking_pct", "Conversation → Booking", "convo-to-booking rate"),
        ("show_up_pct", "Sales Execution", "show-up rate"),
        ("closing_rate_pct", "Sales Execution", "closing rate"),
        ("outreach_sent", "Outreach & Engagement", "outreach (daily avg)"),
        ("followups_sent", "Outreach & Engagement", "follow-ups (daily avg)"),
    ]
    for metric, stage, label in tracked:
        vals: List[float] = []
        labels: List[str] = []
        for r in chron:
            v = _rollup_metric(r, metric)
            if v is None:
                continue
            vals.append(float(v))
            labels.append(r.period_label)
        if len(vals) < 3:
            continue
        last3 = vals[-3:]
        last3_labels = labels[-3:]
        if not (last3[0] > last3[1] > last3[2]):
            continue
        current_tier = tier_for_metric(metric, last3[2], thresholds)
        if current_tier is None:
            continue
        drop_pct = round(((last3[0] - last3[2]) / last3[0]) * 100, 1) if last3[0] else 0
        if drop_pct < 10:
            continue
        unit = "%" if "pct" in metric else ""
        newest = chron[-1]
        flags.append(
            KpiFlag(
                id=f"month-trend-{metric}",
                metric=metric,
                stage=stage,
                tier=current_tier if current_tier != "strong" else "okay",
                message=(
                    f"Your {label} has declined for 3 months straight "
                    f"({last3_labels[0]} {last3[0]}{unit} → {last3_labels[1]} {last3[1]}{unit} → "
                    f"{last3_labels[2]} {last3[2]}{unit}, down {drop_pct}%). "
                    + (
                        "Still above your weak floor — catch this before it becomes a bottleneck."
                        if current_tier != "weak"
                        else "Already in the weak tier on month totals — act now."
                    )
                ),
                comparison=f"3-month decline ({last3[0]} → {last3[2]})",
                related_feature=_related_for_metric(metric),
                severity="watch" if current_tier != "weak" else "critical",
                window_start=chron[-3].period_start if len(chron) >= 3 else newest.period_start,
                window_end=min(newest.period_end, date.today()),
            )
        )
    return flags


def detect_bottlenecks(
    db: Session,
    org_id: UUID,
    thresholds_raw: Optional[Dict[str, Any]] = None,
) -> List[KpiFlag]:
    thresholds = normalize_thresholds(thresholds_raw)
    entries = _load_entries(db, org_id)
    if not entries:
        return []

    # Same monthly totals as the KPI grid footer rows (newest first)
    rollups = build_monthly_rollups(entries, months=6)
    if not rollups:
        return []

    current = rollups[0]
    flags: List[KpiFlag] = []
    flags.extend(_flag_month_thresholds(current, thresholds))
    flags.extend(_flag_month_stage_comparisons(current, thresholds))
    flags.extend(_flag_month_trends(rollups, thresholds))

    seen = set()
    unique: List[KpiFlag] = []
    for f in flags:
        if f.id in seen:
            continue
        seen.add(f.id)
        unique.append(f)

    sev = {"critical": 0, "watch": 1, "info": 2}
    unique.sort(key=lambda f: (sev.get(f.severity, 9), f.stage, f.metric))
    return unique


def utcnow() -> datetime:
    return datetime.now(timezone.utc)

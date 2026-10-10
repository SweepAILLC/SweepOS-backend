"""Weekly scorecard grid: the Google Sheet's 23 funnel metrics, week by week, vs a benchmark.

Mirrors the sheet the funnel process was modeled on: rows are metrics, columns are
Mon-Sun weeks, and a Benchmark column holds the plain average of the weekly values
(average of weekly ratios, not recomputed from totals — matches the sheet).

Weeks in scope = every week whose Monday falls inside the selected range (a month
on the Funnels tab), so each week belongs to exactly one month and is never
double-counted across months. Weeks that haven't started are omitted; the current
week is returned as `in_progress` and excluded from the benchmark so its partial
counts don't drag the average down.

Three rows are typed in weekly (ad spend, ads deployed, angles deployed — stored on
funnel_ad_spend); the other twenty are computed from pipeline data via the same
collect_funnel_facts pass the dashboard uses.

Manual edits: any count row (EDITABLE_FIELDS) can be typed over per week, per view
(funnel + channel), stored in funnel_scorecard_overrides. An override replaces that
week's base count before the metrics compute, so rates, costs and ROAS follow it.
Rate/cost rows themselves are never edited directly.
"""
from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from sqlalchemy import func
from sqlalchemy.orm import Session

from app.models.client_checkin import ClientCheckIn
from app.models.event import Event
from app.models.funnel_ad_spend import FunnelAdSpend
from app.models.funnel_scorecard_override import FunnelScorecardOverride
from app.services.kpi_integration_sync import FunnelFacts, collect_funnel_facts, count_sales_calls


def _monday(d: date) -> date:
    return d - timedelta(days=d.weekday())


def scorecard_weeks(window_start: date, window_end: date, today: date) -> List[date]:
    """Mondays inside [window_start, window_end] for weeks that have started by today, oldest first."""
    weeks: List[date] = []
    cursor = _monday(window_start)
    if cursor < window_start:
        cursor += timedelta(days=7)
    while cursor <= window_end and cursor <= today:
        weeks.append(cursor)
        cursor += timedelta(days=7)
    return weeks


def week_in_progress(week_start: date, today: date) -> bool:
    return week_start + timedelta(days=6) >= today


@dataclass
class WeekBase:
    """Raw weekly counts every metric is derived from."""

    ads_deployed: Optional[int] = None
    angles_deployed: Optional[int] = None
    spend: float = 0.0
    has_spend_row: bool = False
    visitors: Optional[int] = None
    leads: int = 0
    booked_calls: int = 0
    calls_on_calendar: int = 0
    live_calls: int = 0
    deals_closed: int = 0
    cash: float = 0.0


def _div(n: Optional[float], d: Optional[float]) -> Optional[float]:
    if n is None or not d:
        return None
    return n / d


def _cost(w: WeekBase, denom: Optional[float]) -> Optional[float]:
    # No spend logged = no cost metric (never a misleading $0).
    return _div(w.spend, denom) if w.spend > 0 else None


@dataclass(frozen=True)
class MetricDef:
    key: str
    label: str
    group: str  # "ads" | "funnel" | "close" | "economics"
    fmt: str  # "int" | "usd" | "pct" | "ratio"
    better: str  # "up" | "down" | "neutral"
    compute: Callable[[WeekBase], Optional[float]]


# Order and wording follow the sheet. Ratios are fractions here; the UI formats "pct".
METRICS: List[MetricDef] = [
    MetricDef("new_ads", "New Ads Deployed", "ads", "int", "up", lambda w: w.ads_deployed),
    MetricDef("new_angles", "New Angles Deployed", "ads", "int", "up", lambda w: w.angles_deployed),
    MetricDef("ad_spend", "Ad Spend", "ads", "usd", "neutral", lambda w: w.spend if w.has_spend_row else None),
    MetricDef("visitors", "Visitors", "ads", "int", "up", lambda w: w.visitors),
    MetricDef("cost_per_lpv", "Cost per LPV", "ads", "usd", "down", lambda w: _cost(w, w.visitors)),
    MetricDef("leads", "Leads", "funnel", "int", "up", lambda w: w.leads),
    MetricDef("lp_conv_rate", "LP Conv Rate", "funnel", "pct", "up", lambda w: _div(w.leads, w.visitors)),
    MetricDef("booked_calls", "Booked Calls", "funnel", "int", "up", lambda w: w.booked_calls),
    MetricDef("lead_to_book_rate", "Lead to Book Rate", "funnel", "pct", "up", lambda w: _div(w.booked_calls, w.leads)),
    MetricDef("calls_on_calendar", "Calls on Calendar", "funnel", "int", "up", lambda w: w.calls_on_calendar),
    MetricDef("live_calls", "Live Calls", "funnel", "int", "up", lambda w: w.live_calls),
    MetricDef("show_rate", "Show Rate", "funnel", "pct", "up", lambda w: _div(w.live_calls, w.calls_on_calendar)),
    MetricDef("deals_closed", "Deals Closed", "close", "int", "up", lambda w: w.deals_closed),
    MetricDef("close_rate", "Close Rate", "close", "pct", "up", lambda w: _div(w.deals_closed, w.live_calls)),
    MetricDef("cash_collected", "Total Cash Collected", "close", "usd", "up", lambda w: w.cash),
    MetricDef("cash_per_close", "Cash Per Close", "close", "usd", "up", lambda w: _div(w.cash, w.deals_closed)),
    MetricDef("lead_to_close_rate", "Lead to Close Rate", "close", "pct", "up", lambda w: _div(w.deals_closed, w.leads)),
    MetricDef("cost_per_lead", "Cost per Lead", "economics", "usd", "down", lambda w: _cost(w, w.leads)),
    MetricDef("cost_per_booking", "Cost per Booking", "economics", "usd", "down", lambda w: _cost(w, w.booked_calls)),
    MetricDef("cac", "CAC", "economics", "usd", "down", lambda w: _cost(w, w.deals_closed)),
    MetricDef("roas", "ROAS", "economics", "ratio", "up", lambda w: _div(w.cash, w.spend) if w.spend > 0 else None),
    MetricDef("profit", "Profit", "economics", "usd", "up", lambda w: (w.cash - w.spend) if w.spend > 0 else None),
    MetricDef(
        "margin",
        "Margin",
        "economics",
        "pct",
        "up",
        lambda w: _div(w.cash - w.spend, w.cash) if w.spend > 0 else None,
    ),
]

# Ad-side rows have no meaning for organic (no landing page, no spend).
_PAID_ONLY_KEYS = {"new_ads", "new_angles", "ad_spend", "visitors", "cost_per_lpv", "lp_conv_rate"}


# Count rows a person can type over, mapped to the WeekBase field each replaces.
EDITABLE_FIELDS: Dict[str, str] = {
    "new_ads": "ads_deployed",
    "new_angles": "angles_deployed",
    "ad_spend": "spend",
    "visitors": "visitors",
    "leads": "leads",
    "booked_calls": "booked_calls",
    "calls_on_calendar": "calls_on_calendar",
    "live_calls": "live_calls",
    "deals_closed": "deals_closed",
    "cash_collected": "cash",
}
_MONEY_FIELDS = {"spend", "cash"}
OverrideMap = Dict[Tuple[date, str], float]


def view_channel(channel: Optional[str]) -> str:
    return channel or "all"


def _override_scope(q, org_id: uuid.UUID, funnel_id: Optional[uuid.UUID], channel: Optional[str]):
    return q.filter(
        FunnelScorecardOverride.org_id == org_id,
        FunnelScorecardOverride.funnel_id == funnel_id
        if funnel_id is not None
        else FunnelScorecardOverride.funnel_id.is_(None),
        FunnelScorecardOverride.channel == view_channel(channel),
    )


def load_overrides(
    db: Session,
    org_id: uuid.UUID,
    funnel_id: Optional[uuid.UUID],
    channel: Optional[str],
    first_week: date,
    last_week: date,
) -> OverrideMap:
    rows = _override_scope(db.query(FunnelScorecardOverride), org_id, funnel_id, channel).filter(
        FunnelScorecardOverride.week_start >= first_week,
        FunnelScorecardOverride.week_start <= last_week,
    )
    return {(r.week_start, r.metric_key): float(r.value) for r in rows.all() if r.metric_key in EDITABLE_FIELDS}


def _base_value(b: WeekBase, field_name: str) -> Optional[float]:
    if field_name == "spend":
        return b.spend if b.has_spend_row else None
    return getattr(b, field_name)


def apply_overrides(
    bases: Dict[date, WeekBase], overrides: OverrideMap, keys: Optional[Set[str]] = None
) -> Dict[Tuple[date, str], Optional[float]]:
    """Write overrides into the week bases in place. Returns the computed values they replaced."""
    replaced: Dict[Tuple[date, str], Optional[float]] = {}
    for (wk, key), value in overrides.items():
        b = bases.get(wk)
        if b is None or (keys is not None and key not in keys):
            continue
        field_name = EDITABLE_FIELDS[key]
        replaced[(wk, key)] = _base_value(b, field_name)
        setattr(b, field_name, float(value) if field_name in _MONEY_FIELDS else int(round(value)))
        if field_name == "spend":
            b.has_spend_row = True
    return replaced


def set_override(
    db: Session,
    org_id: uuid.UUID,
    funnel_id: Optional[uuid.UUID],
    channel: Optional[str],
    week_start: date,
    metric_key: str,
    value: Optional[float],
    user_id: Optional[uuid.UUID],
) -> None:
    """Upsert one cell; value None reverts it to the computed number. Commits."""
    if metric_key not in EDITABLE_FIELDS:
        raise ValueError(f"'{metric_key}' is calculated from other rows and can't be edited")
    week = _monday(week_start)
    row = (
        _override_scope(db.query(FunnelScorecardOverride), org_id, funnel_id, channel)
        .filter(FunnelScorecardOverride.week_start == week, FunnelScorecardOverride.metric_key == metric_key)
        .first()
    )
    if value is None:
        if row is not None:
            db.delete(row)
            db.commit()
        return
    if row is None:
        row = FunnelScorecardOverride(
            org_id=org_id,
            funnel_id=funnel_id,
            channel=view_channel(channel),
            week_start=week,
            metric_key=metric_key,
        )
        db.add(row)
    row.value = round(float(value), 2)
    row.updated_by_user_id = user_id
    row.updated_at = datetime.now(timezone.utc)
    db.commit()


def _mean(values: List[Optional[float]]) -> Optional[float]:
    """Average of weekly values, skipping weeks where the metric is undefined (not counted as 0)."""
    present = [v for v in values if v is not None]
    return sum(present) / len(present) if present else None


def _week_bases(
    db: Session,
    org_id: uuid.UUID,
    weeks: List[date],
    facts: FunnelFacts,
    funnel_ids: Set[uuid.UUID],
    funnel_id: Optional[uuid.UUID],
    *,
    include_ads: bool,
) -> Dict[date, WeekBase]:
    bases = {wk: WeekBase() for wk in weeks}
    if not weeks:
        return bases
    first, last_end = weeks[0], weeks[-1] + timedelta(days=6)
    start = datetime.combine(first, datetime.min.time(), tzinfo=timezone.utc)
    end = datetime.combine(last_end, datetime.max.time(), tzinfo=timezone.utc)

    def _b(d: date) -> Optional[WeekBase]:
        return bases.get(_monday(d))

    if include_ads:
        q = db.query(FunnelAdSpend).filter(
            FunnelAdSpend.org_id == org_id,
            FunnelAdSpend.week_start >= first,
            FunnelAdSpend.week_start <= weeks[-1],
        )
        if funnel_id is not None:
            q = q.filter(FunnelAdSpend.funnel_id == funnel_id)
        for row in q.all():
            b = bases.get(row.week_start)
            if not b:
                continue
            b.has_spend_row = True
            b.spend += (row.amount_cents or 0) / 100.0
            if row.ads_deployed is not None:
                b.ads_deployed = (b.ads_deployed or 0) + row.ads_deployed
            if row.angles_deployed is not None:
                b.angles_deployed = (b.angles_deployed or 0) + row.angles_deployed

        if funnel_ids:
            week_col = func.date_trunc("week", Event.occurred_at)
            for wk_start, n in (
                db.query(week_col, func.count(func.distinct(Event.visitor_id)))
                .filter(
                    Event.funnel_id.in_(list(funnel_ids)),
                    Event.visitor_id.isnot(None),
                    Event.occurred_at >= start.replace(tzinfo=None),
                    Event.occurred_at <= end.replace(tzinfo=None),
                )
                .group_by(week_col)
                .all()
            ):
                b = _b(wk_start.date() if isinstance(wk_start, datetime) else wk_start)
                if b:
                    b.visitors = int(n)
        for b in bases.values():
            if b.visitors is None:
                b.visitors = 0

    for created in facts.opt_ins.values():
        b = _b(created.date())
        if b:
            b.leads += 1
    for d, _o, respondents in facts.outreach_rows:
        b = _b(d)
        if b:
            b.leads += respondents

    # Call counts come from the same count_sales_calls Sales KPIs uses, so the two
    # tabs agree. Query lower bound only: a booking made inside the window can be
    # for a call after it (booking always precedes the call).
    scope = facts.scope_ids
    scoped_calls = [
        c
        for c in db.query(ClientCheckIn)
        .filter(
            ClientCheckIn.org_id == org_id,
            ClientCheckIn.is_sales_call.is_(True),
            ClientCheckIn.start_time >= start,
        )
        .all()
        if c.client_id and (scope is None or c.client_id in scope)
    ]
    for wk, b in bases.items():
        counts = count_sales_calls(
            scoped_calls,
            datetime.combine(wk, datetime.min.time(), tzinfo=timezone.utc),
            datetime.combine(wk + timedelta(days=6), datetime.max.time(), tzinfo=timezone.utc),
        )
        b.booked_calls = counts["calls_booked_activity"]
        b.calls_on_calendar = counts["calls_booked"]
        b.live_calls = counts["calls_taken"]

    for d in facts.closed.values():
        b = _b(d)
        if b:
            b.deals_closed += 1
    for _cid, ts, cents in facts.cash:
        b = _b(ts.date())
        if b:
            b.cash += cents / 100.0
    return bases


def compute_scorecard(
    db: Session,
    org_id: uuid.UUID,
    window_start: date,
    window_end: date,
    channel: Optional[str],
    funnel_id: Optional[uuid.UUID],
    funnel_ids: Set[uuid.UUID],
    *,
    today: Optional[date] = None,
    compare: Optional[Tuple[date, date]] = None,
) -> Dict[str, Any]:
    """
    Every sheet metric for every week in range (the grid), plus its benchmark:
    the average of the weekly values across the *complete* weeks shown — or, when
    the shared date-range filter has compare on, across the weeks of the compare range.
    """
    today = today or datetime.now(timezone.utc).date()
    weeks = scorecard_weeks(window_start, window_end, today)
    if not weeks:
        return {"weeks": [], "benchmark_weeks": 0, "benchmark_source": "range", "metrics": []}
    complete = [not week_in_progress(wk, today) for wk in weeks]
    show_ads = channel != "organic"

    def _weekly_by_metric(
        wks: List[date],
    ) -> Tuple[Dict[str, List[Optional[float]]], Dict[Tuple[date, str], Optional[float]]]:
        span_start, span_end = wks[0], wks[-1] + timedelta(days=6)
        facts = collect_funnel_facts(db, org_id, span_start, span_end, channel, funnel_id)
        bases = _week_bases(db, org_id, wks, facts, funnel_ids, funnel_id, include_ads=show_ads)
        overrides = load_overrides(db, org_id, funnel_id, channel, wks[0], wks[-1])
        replaced = apply_overrides(bases, overrides)
        # Economics rows follow the dashboard's money rule: with channel "all", spend is
        # only divided against paid leads/bookings/closes/cash, never organic ones.
        econ_bases = bases
        if channel is None and funnel_id is None:
            paid_facts = collect_funnel_facts(db, org_id, span_start, span_end, "paid", None)
            econ_bases = _week_bases(db, org_id, wks, paid_facts, funnel_ids, None, include_ads=True)
            # Spend is shared; edited lead/close/cash counts in this view include organic,
            # so the paid-only economics bases keep their computed counts.
            apply_overrides(econ_bases, overrides, keys={"ad_spend"})
        values = {
            m.key: [m.compute((econ_bases if m.group == "economics" else bases)[wk]) for wk in wks] for m in METRICS
        }
        return values, replaced

    current, replaced = _weekly_by_metric(weeks)
    compare_weeks = (
        [wk for wk in scorecard_weeks(compare[0], compare[1], today) if not week_in_progress(wk, today)]
        if compare is not None
        else []
    )
    baseline = _weekly_by_metric(compare_weeks)[0] if compare_weeks else None

    def _r(v: Optional[float]) -> Optional[float]:
        return None if v is None else round(v, 4)

    out: List[Dict[str, Any]] = []
    for m in METRICS:
        if not show_ads and (m.group == "economics" or m.key in _PAID_ONLY_KEYS):
            continue
        weekly = current[m.key]
        if baseline is not None:
            benchmark = _mean(baseline[m.key])
        else:
            benchmark = _mean([v for v, done in zip(weekly, complete) if done])
        out.append(
            {
                "key": m.key,
                "label": m.label,
                "group": m.group,
                "format": m.fmt,
                "better": m.better,
                "values": [_r(v) for v in weekly],
                "benchmark": _r(benchmark),
                "editable": m.key in EDITABLE_FIELDS,
                # Hand-edited cells, and the computed value each one replaced.
                "overridden": [(wk, m.key) in replaced for wk in weeks],
                "original": [_r(replaced.get((wk, m.key))) for wk in weeks],
            }
        )
    return {
        "weeks": [{"week_start": wk, "in_progress": not done} for wk, done in zip(weeks, complete)],
        "benchmark_weeks": len(compare_weeks) if baseline is not None else sum(complete),
        # "compare" = benchmark is the compare range's average week; else this range's.
        "benchmark_source": "compare" if baseline is not None else "range",
        "metrics": out,
    }

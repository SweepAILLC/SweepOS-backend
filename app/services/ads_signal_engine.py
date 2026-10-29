"""Deterministic Meta ads diagnostics. No LLM.

Thresholds follow cost-up/volume-down fatigue, spend-tier messaging
ceiling, pull-back-then-check, foundational hold, and ~50 results before
scaling, plus 2026 creative-fatigue practice: two or more leading
signals against the ad's own baseline (frequency, CTR, CPM). CPR confirms
severity; it does not lead. Meta publishes no budget-step percent — 20/30%
is the practitioner step, gated on ~50 results in 7 days.
"""
from __future__ import annotations

import logging
import uuid
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

from sqlalchemy.orm import Session

from app.core.config import settings
from app.models.meta_ad import MetaAd
from app.models.meta_ad_account import MetaAdAccount
from app.models.meta_ad_alert import MetaAdAlert
from app.models.meta_ad_insight_daily import MetaAdInsightDaily
from app.models.meta_ad_set import MetaAdSet
from app.models.meta_campaign import MetaCampaign
from app.services import meta_ads_client as mac

logger = logging.getLogger(__name__)

THRESHOLDS: Dict[str, float] = {
    "freq_warn": 2.5,
    "freq_critical": 3.5,
    "freq_scale_max": 2.0,
    "ctr_drop": 0.15,
    "cpm_rise": 0.15,
    "cpr_confirm": 1.25,
    "cpr_critical_multiple": 2.0,
    "crossover_cpr_rise": 1.10,
    "ceiling_cpr_gap": 0.30,
    "spend_drop": 0.15,
    "cpr_recovery": 0.10,
    "scale_min_results": 50,
    "scale_cpr_stability": 0.10,
    "scale_step": 0.20,
    "scale_step_high_volume": 0.30,
    "scale_high_volume_results": 80,
    "budget_quiet_hours": 72,
    "concentration": 0.80,
    "bench_min_active": 25,
    "bench_stale_days": 14,
    "min_days_fatigue": 10,
}

TRIGGER_TYPES = {"creative_fatigue", "fatigue_crossover", "messaging_ceiling"}
JUDGED_TYPES = {"ceiling_confirmed", "fatigue_confirmed"}
REEVALUATED = {
    "creative_fatigue",
    "fatigue_crossover",
    "messaging_ceiling",
    "scale_ready",
    "learning_limited",
    "spend_concentration",
    "creative_bench_low",
}
_INACTIVE = {"PAUSED", "ARCHIVED", "DELETED", "CAMPAIGN_PAUSED", "ADSET_PAUSED"}
_SEV_RANK = {"critical": 0, "warn": 1, "info": 2}
_QUIET_NOTIFY = {"bot not configured", "no channel mapped for this event type"}

Key = Tuple[str, str, str]


@dataclass
class DayPoint:
    day: date
    spend_cents: int = 0
    results: int = 0
    cost_per_result_cents: Optional[int] = None
    impressions: int = 0
    clicks: int = 0
    cpm_cents: Optional[int] = None
    ctr: Optional[float] = None
    reach: int = 0
    frequency: Optional[float] = None


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _mean(vals: Iterable[Optional[float]]) -> Optional[float]:
    clean = [float(v) for v in vals if v is not None]
    if not clean:
        return None
    return sum(clean) / len(clean)


def _freq(p: DayPoint) -> Optional[float]:
    if p.frequency is not None:
        return float(p.frequency)
    if p.reach and p.impressions:
        return p.impressions / p.reach
    return None


def _weighted_freq(pts: Sequence[DayPoint]) -> Optional[float]:
    num = 0.0
    den = 0.0
    for p in pts:
        f = _freq(p)
        if f is None:
            continue
        weight = float(p.impressions or 1)
        num += f * weight
        den += weight
    if den <= 0:
        return None
    return num / den


def _agg_cpr(pts: Sequence[DayPoint]) -> Optional[float]:
    spend = sum(p.spend_cents or 0 for p in pts)
    results = sum(p.results or 0 for p in pts)
    if spend > 0 and results > 0:
        return spend / results
    return None


def _agg_cpm(pts: Sequence[DayPoint]) -> Optional[float]:
    spend = sum(p.spend_cents or 0 for p in pts)
    imps = sum(p.impressions or 0 for p in pts)
    if spend > 0 and imps > 0:
        return spend / imps * 1000
    vals = [p.cpm_cents for p in pts if p.cpm_cents]
    if not vals:
        return None
    return sum(vals) / len(vals)


def _delivering(status: Optional[str]) -> bool:
    if not status:
        return True
    return status.upper() not in _INACTIVE


def _signal(
    alert_type: str,
    severity: str,
    playbook: str,
    recommended_action: str,
    metrics: Dict[str, Any],
) -> Dict[str, Any]:
    return {
        "alert_type": alert_type,
        "severity": severity,
        "playbook": playbook,
        "recommended_action": recommended_action,
        "metrics": metrics,
    }


def detect_creative_fatigue(points: Sequence[DayPoint]) -> Optional[Dict[str, Any]]:
    """Ad-level. Needs 2+ leading signals vs the ad's own early baseline."""
    active = [
        p
        for p in sorted(points, key=lambda p: p.day)
        if (p.impressions or 0) > 0 or (p.spend_cents or 0) > 0
    ]
    if len(active) < int(THRESHOLDS["min_days_fatigue"]):
        return None
    if len(active) >= 14:
        baseline, recent = active[:7], active[-7:]
        prior_cpm, recent_cpm_src = active[-14:-7], active[-7:]
    else:
        mid = len(active) // 2
        baseline, recent = active[:mid], active[mid:]
        prior_cpm, recent_cpm_src = baseline, recent
    if not baseline or not recent:
        return None

    leading: List[str] = []
    freq = _weighted_freq(recent)
    if freq is not None and freq >= THRESHOLDS["freq_warn"]:
        leading.append("frequency")

    base_ctr = _mean(p.ctr for p in baseline)
    rec_ctr = _mean(p.ctr for p in recent)
    ctr_drop = None
    if base_ctr and rec_ctr is not None and base_ctr > 0:
        ctr_drop = (base_ctr - rec_ctr) / base_ctr
        if ctr_drop >= THRESHOLDS["ctr_drop"]:
            leading.append("ctr")

    base_cpm = _agg_cpm(prior_cpm)
    rec_cpm = _agg_cpm(recent_cpm_src)
    cpm_rise = None
    if base_cpm and rec_cpm and base_cpm > 0:
        cpm_rise = (rec_cpm - base_cpm) / base_cpm
        if cpm_rise >= THRESHOLDS["cpm_rise"]:
            leading.append("cpm")

    if len(leading) < 2:
        return None

    base_cpr = _agg_cpr(baseline)
    rec_cpr = _agg_cpr(recent)
    cpr_ratio = (rec_cpr / base_cpr) if base_cpr and rec_cpr else None
    severity = "warn"
    if (freq is not None and freq >= THRESHOLDS["freq_critical"]) or (
        cpr_ratio is not None and cpr_ratio >= THRESHOLDS["cpr_critical_multiple"]
    ):
        severity = "critical"

    bits: List[str] = []
    if "frequency" in leading and freq is not None:
        bits.append(f"frequency {freq:.1f}")
    if "ctr" in leading and ctr_drop is not None:
        bits.append(f"CTR down {round(ctr_drop * 100)}%")
    if "cpm" in leading and cpm_rise is not None:
        bits.append(f"CPM up {round(cpm_rise * 100)}%")
    if cpr_ratio is not None and cpr_ratio >= THRESHOLDS["cpr_confirm"]:
        bits.append(f"CPR {cpr_ratio:.1f}x baseline")
    evidence = ", ".join(bits)
    return _signal(
        "creative_fatigue",
        severity,
        "fatigue",
        (
            f"Creative fatigue signals agree ({evidence}). Pull spend back and check cost per result "
            "tomorrow. If it does not recover, duplicate the ad set and replace the ads taking the spend. "
            "Keep the ads that got no reach — they are the fatigue bench."
        ),
        {
            "evidence": evidence,
            "frequency": round(freq, 2) if freq is not None else None,
            "ctr_drop_pct": round(ctr_drop * 100) if ctr_drop is not None else None,
            "cpm_rise_pct": round(cpm_rise * 100) if cpm_rise is not None else None,
            "cpr_ratio": round(cpr_ratio, 2) if cpr_ratio is not None else None,
            "leading": leading,
        },
    )


def detect_fatigue_crossover(points: Sequence[DayPoint]) -> Optional[Dict[str, Any]]:
    """3-day CPR up while result volume falls."""
    pts = sorted(points, key=lambda p: p.day)
    if len(pts) < 6:
        return None
    prev, recent = pts[-6:-3], pts[-3:]
    prev_cpr, rec_cpr = _agg_cpr(prev), _agg_cpr(recent)
    prev_res = sum(p.results or 0 for p in prev)
    rec_res = sum(p.results or 0 for p in recent)
    if not prev_cpr or not rec_cpr or prev_res <= 0:
        return None
    if rec_cpr < prev_cpr * THRESHOLDS["crossover_cpr_rise"] or rec_res >= prev_res:
        return None
    return _signal(
        "fatigue_crossover",
        "warn",
        "fatigue",
        (
            "Cost per result is rising while results are falling — the fatigue lines crossed. "
            "Pull spend back and check tomorrow before refreshing creative. Recovery means a ceiling; "
            "no recovery means fatigue."
        ),
        {
            "evidence": (
                f"CPR {round(prev_cpr)} → {round(rec_cpr)} cents, results {prev_res} → {rec_res}"
            ),
            "prev_cpr_cents": round(prev_cpr),
            "recent_cpr_cents": round(rec_cpr),
            "prev_results": prev_res,
            "recent_results": rec_res,
        },
    )


def detect_messaging_ceiling(points: Sequence[DayPoint]) -> Optional[Dict[str, Any]]:
    """High-spend days cost materially more per result than the efficient tier."""
    pts = [
        p
        for p in sorted(points, key=lambda p: p.day)
        if (p.spend_cents or 0) > 0 and (p.results or 0) > 0
    ]
    if len(pts) < 9:
        return None
    ordered = sorted(pts, key=lambda p: p.spend_cents)
    n = len(ordered)
    a, b = n // 3, (2 * n) // 3
    buckets = {"low": ordered[:a], "mid": ordered[a:b], "high": ordered[b:]}
    scored = []
    for name, bucket in buckets.items():
        cpr = _agg_cpr(bucket)
        results = sum(p.results or 0 for p in bucket)
        if cpr is None or results < 3 or not bucket:
            continue
        scored.append((name, cpr, bucket))
    high = next((s for s in scored if s[0] == "high"), None)
    others = [s for s in scored if s[0] != "high"]
    if high is None or not others:
        return None
    efficient = min(others, key=lambda s: s[1])
    if high[1] < efficient[1] * (1 + THRESHOLDS["ceiling_cpr_gap"]):
        return None
    avg_daily = sum(p.spend_cents for p in efficient[2]) / len(efficient[2])
    dollars = avg_daily / 100
    gap = (high[1] / efficient[1]) - 1
    return _signal(
        "messaging_ceiling",
        "warn",
        "scaling",
        (
            f"Higher spend days cost {round(gap * 100)}% more per result. Pull daily spend back to about "
            f"${dollars:.0f} (the efficient tier), tag the campaign foundational, and open a new messaging "
            "pocket for more scale. Do not broaden the niche to force this message further."
        ),
        {
            "evidence": (
                f"efficient CPR {round(efficient[1])} cents vs high-spend CPR {round(high[1])} cents"
            ),
            "efficient_daily_spend_cents": round(avg_daily),
            "efficient_cpr_cents": round(efficient[1]),
            "high_cpr_cents": round(high[1]),
            "gap_pct": round(gap * 100),
        },
    )


def evaluate_pullback(
    points: Sequence[DayPoint],
    *,
    trigger_open: bool,
    existing: Optional[Dict[str, Any]],
    today: date,
    already_judged: bool,
) -> Optional[Dict[str, Any]]:
    """Watch a spend drop after fatigue/ceiling, then call ceiling vs fatigue."""
    pts = sorted(points, key=lambda p: p.day)
    if len(pts) < 6:
        return {"action": "keep"} if existing else None
    prior, recent = pts[-6:-3], pts[-3:]
    prior_spend = sum(p.spend_cents or 0 for p in prior)
    recent_spend = sum(p.spend_cents or 0 for p in recent)
    dropped = prior_spend > 0 and recent_spend <= prior_spend * (1 - THRESHOLDS["spend_drop"])

    if not existing:
        if already_judged or not trigger_open or not dropped:
            return None
        base = _agg_cpr(prior)
        if base is None:
            return None
        return {
            "action": "open",
            "signal": _signal(
                "pullback_watch",
                "warn",
                "fatigue",
                (
                    "Spend stepped down after a fatigue or ceiling flag. Check cost per result on the next "
                    "sync: recovery means a messaging ceiling (tag foundational); no recovery means creative "
                    "fatigue (refresh the ads taking the spend)."
                ),
                {
                    "evidence": f"spend {prior_spend} → {recent_spend} cents over 3-day windows",
                    "baseline_cpr_cents": round(base),
                    "pre_spend_cents": prior_spend,
                    "recent_spend_cents": recent_spend,
                    "opened_on": today.isoformat(),
                    "verdict": "watching",
                },
            ),
        }

    opened_raw = existing.get("opened_on")
    try:
        opened_on = date.fromisoformat(str(opened_raw))
    except (TypeError, ValueError):
        return {"action": "keep"}
    if today <= opened_on:
        return {"action": "keep"}
    if not dropped and not trigger_open:
        return {"action": "resolve"}
    baseline = existing.get("baseline_cpr_cents")
    rec_cpr = _agg_cpr(recent)
    if baseline is None or rec_cpr is None:
        return {"action": "keep"}
    metrics = dict(existing)
    metrics["recent_cpr_cents"] = round(rec_cpr)
    if rec_cpr <= float(baseline) * (1 + THRESHOLDS["cpr_recovery"]):
        metrics["verdict"] = "ceiling_confirmed"
        return {
            "action": "resolve_and_open",
            "signal": _signal(
                "ceiling_confirmed",
                "warn",
                "scaling",
                (
                    "Cost per result recovered after the spend pullback. This is a messaging ceiling, not "
                    "fatigue. Tag the campaign foundational and hold spend near the efficient level. Find "
                    "more scale with a new messaging pocket."
                ),
                metrics,
            ),
        }
    metrics["verdict"] = "fatigue_confirmed"
    return {
        "action": "resolve_and_open",
        "signal": _signal(
            "fatigue_confirmed",
            "critical",
            "fatigue",
            (
                "Cost per result did not recover after pulling spend back. This is creative fatigue. "
                "Duplicate the ad set, swap fresh executions of the same buying reason for the 1–3 ads "
                "taking the spend, and keep the ads that got no reach."
            ),
            metrics,
        ),
    }


def detect_scale_ready(
    points: Sequence[DayPoint],
    *,
    learning_stage: Optional[str],
    target_cpr_cents: Optional[int],
    budget_quiet: bool,
    active: bool,
) -> Optional[Dict[str, Any]]:
    if not active or not budget_quiet:
        return None
    if (learning_stage or "").upper() in ("LEARNING", "LEARNING_LIMITED"):
        return None
    pts = sorted(points, key=lambda p: p.day)
    if len(pts) < 14:
        return None
    prior, recent = pts[-14:-7], pts[-7:]
    results_7 = sum(p.results or 0 for p in recent)
    if results_7 < THRESHOLDS["scale_min_results"]:
        return None
    prev_cpr, rec_cpr = _agg_cpr(prior), _agg_cpr(recent)
    if not prev_cpr or not rec_cpr:
        return None
    if abs(rec_cpr - prev_cpr) / prev_cpr > THRESHOLDS["scale_cpr_stability"]:
        return None
    if target_cpr_cents is not None and rec_cpr > target_cpr_cents:
        return None
    freq = _weighted_freq(recent)
    if freq is not None and freq >= THRESHOLDS["freq_scale_max"]:
        return None
    high = results_7 >= THRESHOLDS["scale_high_volume_results"]
    step = THRESHOLDS["scale_step_high_volume"] if high else THRESHOLDS["scale_step"]
    pct = int(round(step * 100))
    target_note = ""
    if target_cpr_cents is None:
        target_note = " No target CPR is set — confirm unit economics before the step."
    return _signal(
        "scale_ready",
        "warn",
        "scaling",
        (
            f"Out of learning, cost per result is stable, frequency is healthy, and the last budget "
            f"change was at least 72 hours ago. Raise the ad set budget about {pct}% and wait 48–72 hours "
            f"before the next step. Meta publishes no fixed percent; this is the practitioner step."
            f"{target_note}"
        ),
        {
            "evidence": f"{results_7} results in 7 days, CPR {round(rec_cpr)} cents",
            "results_7": results_7,
            "cpr_cents": round(rec_cpr),
            "frequency": round(freq, 2) if freq is not None else None,
            "step_pct": pct,
        },
    )


def detect_learning_limited(
    points: Sequence[DayPoint],
    *,
    active: bool,
    today: date,
) -> Optional[Dict[str, Any]]:
    if not active:
        return None
    pts = sorted(points, key=lambda p: p.day)
    if not pts:
        return None
    if (today - pts[0].day).days < 7:
        return None
    start = today - timedelta(days=6)
    results_7 = sum(p.results or 0 for p in pts if start <= p.day <= today)
    if results_7 >= THRESHOLDS["scale_min_results"]:
        return None
    return _signal(
        "learning_limited",
        "warn",
        "scaling",
        (
            f"Only {results_7} results in the last 7 days after a week of delivery. Meta usually needs "
            "about 50 optimization events in 7 days to exit learning. Consolidate ad sets or raise budget "
            "toward that volume — do not scale a learning-limited ad set."
        ),
        {"evidence": f"{results_7} results in 7 days", "results_7": results_7},
    )


def detect_spend_concentration(
    ads: Sequence[Tuple[str, str, int]],
) -> Optional[Dict[str, Any]]:
    """1–2 ads taking >=80% of spend inside an ad set that has 3+ ads with spend."""
    rows = [(ad_id, name, int(spend)) for ad_id, name, spend in ads if spend > 0]
    total = sum(s for _, _, s in rows)
    if total <= 0 or len(rows) < 3:
        return None
    ranked = sorted(rows, key=lambda r: r[2], reverse=True)
    top1 = ranked[0][2] / total
    top2 = (ranked[0][2] + ranked[1][2]) / total
    if top1 < THRESHOLDS["concentration"] and top2 < THRESHOLDS["concentration"]:
        return None
    shown = ranked[:2]
    names = ", ".join(name or ad_id for ad_id, name, _s in shown)
    return _signal(
        "spend_concentration",
        "info",
        "fatigue",
        (
            f"{names} are taking the spend. If you run the fatigue protocol, swap these first and keep "
            "the ads that got no reach."
        ),
        {
            "evidence": f"top ads {round(max(top1, top2) * 100)}% of spend",
            "ads": [
                {"id": ad_id, "name": name, "spend_cents": spend, "share": round(spend / total, 3)}
                for ad_id, name, spend in shown
            ],
        },
    )


def detect_creative_bench(
    *,
    active_count: int,
    newest_created: Optional[date],
    today: date,
    active: bool,
) -> Optional[Dict[str, Any]]:
    if not active:
        return None
    reasons: List[str] = []
    if active_count < THRESHOLDS["bench_min_active"]:
        reasons.append(f"{active_count} active ads, target is 25–30")
    stale = newest_created is None or (today - newest_created).days >= THRESHOLDS["bench_stale_days"]
    if stale:
        reasons.append("no new ad in the last 14 days")
    if not reasons:
        return None
    return _signal(
        "creative_bench_low",
        "info",
        "surplus",
        (
            "Creative bench is thin (" + "; ".join(reasons) + "). A cold ad set should keep 25–30 "
            "distinct ads and a surplus on the side so fatigue does not cost 24–48 hours. "
            "Andromeda has shortened practical creative life to about 14–21 days."
        ),
        {
            "evidence": "; ".join(reasons),
            "active_count": active_count,
            "bench_target": int(THRESHOLDS["bench_min_active"]),
        },
    )


def campaign_pill(sweep_status: Optional[str], alert_types: Set[str]) -> str:
    if alert_types & {"creative_fatigue", "fatigue_crossover", "fatigue_confirmed"}:
        return "fatigued"
    if alert_types & {"messaging_ceiling", "ceiling_confirmed", "pullback_watch"}:
        return "pull_back"
    if "learning_limited" in alert_types:
        return "learning"
    if "scale_ready" in alert_types:
        return "scale"
    if sweep_status == "foundational":
        return "foundational"
    return "hold"


def _with_context(signal: Dict[str, Any], **ctx: Any) -> Dict[str, Any]:
    out = dict(signal)
    metrics = dict(out.get("metrics") or {})
    for key, value in ctx.items():
        if value is not None:
            metrics[key] = value
    if out.get("playbook"):
        metrics["playbook"] = out["playbook"]
    out["metrics"] = metrics
    return out


def _budget_quiet(updated_at: Optional[datetime], now: datetime) -> bool:
    if updated_at is None:
        return True
    if updated_at.tzinfo is None:
        updated_at = updated_at.replace(tzinfo=timezone.utc)
    return (now - updated_at).total_seconds() >= THRESHOLDS["budget_quiet_hours"] * 3600


def _rollup_ads_by_day(ads: Sequence[MetaAd], grouped: Dict[Tuple[str, str], List[DayPoint]]) -> List[DayPoint]:
    buckets: Dict[date, DayPoint] = {}
    for ad in ads:
        for p in grouped.get(("ad", ad.meta_ad_id), []):
            acc = buckets.get(p.day)
            if acc is None:
                buckets[p.day] = DayPoint(
                    day=p.day,
                    spend_cents=p.spend_cents,
                    results=p.results,
                    impressions=p.impressions,
                    clicks=p.clicks,
                    reach=p.reach,
                )
            else:
                acc.spend_cents += p.spend_cents
                acc.results += p.results
                acc.impressions += p.impressions
                acc.clicks += p.clicks
                acc.reach += p.reach
    out: List[DayPoint] = []
    for p in buckets.values():
        if p.impressions and p.clicks:
            p.ctr = p.clicks / p.impressions * 100
        if p.reach and p.impressions:
            p.frequency = p.impressions / p.reach
        if p.impressions and p.spend_cents:
            p.cpm_cents = int(round(p.spend_cents / p.impressions * 1000))
        out.append(p)
    out.sort(key=lambda p: p.day)
    return out


def _adset_points(
    adset: MetaAdSet,
    ads: Sequence[MetaAd],
    grouped: Dict[Tuple[str, str], List[DayPoint]],
) -> List[DayPoint]:
    direct = grouped.get(("adset", adset.meta_adset_id)) or []
    if direct:
        return direct
    return _rollup_ads_by_day(ads, grouped)


def _point_from_row(row: MetaAdInsightDaily) -> DayPoint:
    return DayPoint(
        day=row.date,
        spend_cents=int(row.spend_cents or 0),
        results=int(row.results or 0),
        cost_per_result_cents=int(row.cost_per_result_cents) if row.cost_per_result_cents is not None else None,
        impressions=int(row.impressions or 0),
        clicks=int(row.clicks or 0),
        cpm_cents=int(row.cpm_cents) if row.cpm_cents is not None else None,
        ctr=float(row.ctr) if row.ctr is not None else None,
        reach=int(row.reach or 0),
        frequency=float(row.frequency) if row.frequency is not None else None,
    )


def _as_date(value: Any) -> Optional[date]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return None


def _entity_name(fired: Dict[Key, Dict[str, Any]], level: str, entity: str, watch: Optional[MetaAdAlert]) -> Optional[str]:
    for kind in list(TRIGGER_TYPES) + ["pullback_watch"]:
        sig = fired.get((level, entity, kind))
        if sig and (sig.get("metrics") or {}).get("entity_name"):
            return sig["metrics"]["entity_name"]
    if watch is not None and watch.metrics_snapshot:
        return watch.metrics_snapshot.get("entity_name")
    return None


def _resolve(row: MetaAdAlert, now: datetime) -> None:
    row.status = "resolved"
    row.last_evaluated_at = now


def _upsert(
    db: Session,
    org_id: uuid.UUID,
    level: str,
    entity_id: str,
    signal: Dict[str, Any],
    now: datetime,
    existing: Optional[MetaAdAlert],
) -> MetaAdAlert:
    metrics = dict(signal.get("metrics") or {})
    if signal.get("playbook"):
        metrics["playbook"] = signal["playbook"]
    if existing is None:
        row = MetaAdAlert(
            org_id=org_id,
            level=level,
            entity_id=entity_id,
            alert_type=signal["alert_type"],
            severity=signal["severity"],
            metrics_snapshot=metrics,
            recommended_action=signal["recommended_action"],
            status="open",
            first_detected_at=now,
            last_evaluated_at=now,
        )
        db.add(row)
        db.flush()
        return row
    existing.severity = signal["severity"]
    existing.metrics_snapshot = metrics
    existing.recommended_action = signal["recommended_action"]
    existing.last_evaluated_at = now
    return existing


def _notify(db: Session, org_id: uuid.UUID, rows: Sequence[MetaAdAlert]) -> None:
    from app.services.discord_notify import send_discord_event

    actionable = [
        r for r in rows if r.severity in ("warn", "critical") and r.status == "open" and r.notified_at is None
    ]
    if not actionable:
        return
    front = (getattr(settings, "FRONTEND_URL", None) or "http://localhost:3003").rstrip("/")
    url = f"{front}/?tab=content_studio&sub=ads"
    if len(actionable) == 1:
        row = actionable[0]
        snap = row.metrics_snapshot or {}
        name = snap.get("entity_name") or row.entity_id
        title = f"Meta Ads: {row.alert_type.replace('_', ' ')} — {name}"
        description = row.recommended_action
        fields = [("Severity", row.severity), ("Evidence", str(snap.get("evidence") or "—")[:180])]
    else:
        title = f"Meta Ads: {len(actionable)} alerts"
        lines = []
        for row in actionable[:8]:
            snap = row.metrics_snapshot or {}
            name = snap.get("entity_name") or row.entity_id
            lines.append(f"• {name}: {row.alert_type.replace('_', ' ')} — {snap.get('evidence') or row.severity}")
        extra = len(actionable) - 8
        if extra > 0:
            lines.append(f"• +{extra} more in the Ads tab")
        description = "\n".join(lines)[:3900]
        fields = [("Open", str(len(actionable)))]
    sent, reason = send_discord_event(
        db, org_id, "ads_alert", title=title, description=description, fields=fields, url=url,
    )
    if sent or reason in _QUIET_NOTIFY:
        stamp = _utcnow()
        for row in actionable:
            row.notified_at = stamp


def evaluate_org_alerts(db: Session, org_id: uuid.UUID, *, now: Optional[datetime] = None) -> None:
    """Recompute alerts from Postgres insights. Caller commits."""
    now = now or _utcnow()
    today = now.date()
    since = today - timedelta(days=27)

    insights = (
        db.query(MetaAdInsightDaily)
        .filter(MetaAdInsightDaily.org_id == org_id, MetaAdInsightDaily.date >= since)
        .all()
    )
    campaigns = db.query(MetaCampaign).filter(MetaCampaign.org_id == org_id).all()
    adsets = db.query(MetaAdSet).filter(MetaAdSet.org_id == org_id).all()
    ads = db.query(MetaAd).filter(MetaAd.org_id == org_id).all()
    account = _account_for_org(db, org_id)
    target_cpr = int(account.target_cpr_cents) if account and account.target_cpr_cents is not None else None

    grouped: Dict[Tuple[str, str], List[DayPoint]] = defaultdict(list)
    for row in insights:
        grouped[(row.level, row.entity_id)].append(_point_from_row(row))
    for key in grouped:
        grouped[key].sort(key=lambda p: p.day)

    campaigns_by_id = {c.id: c for c in campaigns}
    adsets_by_id = {a.id: a for a in adsets}
    ads_by_adset: Dict[Any, List[MetaAd]] = defaultdict(list)
    for ad in ads:
        ads_by_adset[ad.ad_set_id].append(ad)

    existing_rows = (
        db.query(MetaAdAlert)
        .filter(MetaAdAlert.org_id == org_id, MetaAdAlert.status != "resolved")
        .all()
    )
    existing_map: Dict[Key, MetaAdAlert] = {
        (row.level, row.entity_id, row.alert_type): row for row in existing_rows
    }

    fired: Dict[Key, Dict[str, Any]] = {}
    points_for: Dict[Tuple[str, str], List[DayPoint]] = {}

    for ad in ads:
        if not _delivering(ad.effective_status):
            continue
        pts = grouped.get(("ad", ad.meta_ad_id)) or []
        points_for[("ad", ad.meta_ad_id)] = pts
        sig = detect_creative_fatigue(pts)
        if not sig:
            continue
        adset = adsets_by_id.get(ad.ad_set_id)
        camp = campaigns_by_id.get(adset.campaign_id) if adset else None
        fired[("ad", ad.meta_ad_id, sig["alert_type"])] = _with_context(
            sig,
            entity_name=ad.name,
            adset_id=adset.meta_adset_id if adset else None,
            campaign_id=camp.meta_campaign_id if camp else None,
        )

    for adset in adsets:
        child_ads = ads_by_adset.get(adset.id, [])
        pts = _adset_points(adset, child_ads, grouped)
        points_for[("adset", adset.meta_adset_id)] = pts
        camp = campaigns_by_id.get(adset.campaign_id)
        active = _delivering(adset.effective_status)
        ctx = {"entity_name": adset.name, "campaign_id": camp.meta_campaign_id if camp else None}
        detectors = []
        if active:
            detectors.append(detect_fatigue_crossover(pts))
        detectors.append(
            detect_scale_ready(
                pts,
                learning_stage=adset.learning_stage,
                target_cpr_cents=target_cpr,
                budget_quiet=_budget_quiet(adset.budget_updated_at, now),
                active=active,
            )
        )
        detectors.append(detect_learning_limited(pts, active=active, today=today))
        for detector in detectors:
            if detector:
                fired[("adset", adset.meta_adset_id, detector["alert_type"])] = _with_context(detector, **ctx)
        if not active:
            continue
        cutoff = today - timedelta(days=13)
        spends = []
        newest = None
        active_count = 0
        for ad in child_ads:
            spend = sum(p.spend_cents for p in grouped.get(("ad", ad.meta_ad_id), []) if p.day >= cutoff)
            spends.append((ad.meta_ad_id, ad.name or ad.meta_ad_id, spend))
            if _delivering(ad.effective_status):
                active_count += 1
                created = _as_date(ad.created_time)
                if created and (newest is None or created > newest):
                    newest = created
        conc = detect_spend_concentration(spends)
        if conc:
            fired[("adset", adset.meta_adset_id, conc["alert_type"])] = _with_context(conc, **ctx)
        bench = detect_creative_bench(
            active_count=active_count, newest_created=newest, today=today, active=True,
        )
        if bench:
            fired[("adset", adset.meta_adset_id, bench["alert_type"])] = _with_context(bench, **ctx)

    for camp in campaigns:
        if not _delivering(camp.effective_status or camp.status):
            continue
        pts = grouped.get(("campaign", camp.meta_campaign_id)) or []
        points_for[("campaign", camp.meta_campaign_id)] = pts
        for detector in (detect_fatigue_crossover(pts), detect_messaging_ceiling(pts)):
            if detector:
                fired[("campaign", camp.meta_campaign_id, detector["alert_type"])] = _with_context(
                    detector, entity_name=camp.name,
                )

    watch_entities = {(level, entity) for (level, entity, kind) in fired if kind in TRIGGER_TYPES}
    for level, entity, kind in existing_map:
        if kind == "pullback_watch":
            watch_entities.add((level, entity))

    for level, entity in watch_entities:
        trigger_open = any((level, entity, kind) in fired for kind in TRIGGER_TYPES)
        judged = any((level, entity, kind) in existing_map for kind in JUDGED_TYPES)
        watch = existing_map.get((level, entity, "pullback_watch"))
        decision = evaluate_pullback(
            points_for.get((level, entity), []),
            trigger_open=trigger_open,
            existing=(watch.metrics_snapshot if watch else None),
            today=today,
            already_judged=judged,
        )
        if not decision:
            continue
        action = decision.get("action")
        if action == "resolve" and watch is not None:
            _resolve(watch, now)
        elif action == "open":
            fired[(level, entity, "pullback_watch")] = _with_context(
                decision["signal"], entity_name=_entity_name(fired, level, entity, watch),
            )
        elif action == "resolve_and_open":
            if watch is not None:
                _resolve(watch, now)
                db.flush()
            sig = decision["signal"]
            fired[(level, entity, sig["alert_type"])] = _with_context(
                sig, entity_name=_entity_name(fired, level, entity, watch),
            )

    for key, signal in fired.items():
        level, entity, _kind = key
        _upsert(db, org_id, level, entity, signal, now, existing_map.get(key))

    for row in existing_rows:
        if row.alert_type not in REEVALUATED:
            continue
        if (row.level, row.entity_id, row.alert_type) not in fired and row.status != "resolved":
            _resolve(row, now)

    db.flush()
    pending = (
        db.query(MetaAdAlert)
        .filter(
            MetaAdAlert.org_id == org_id,
            MetaAdAlert.status == "open",
            MetaAdAlert.notified_at.is_(None),
            MetaAdAlert.severity.in_(("warn", "critical")),
        )
        .all()
    )
    try:
        _notify(db, org_id, pending)
    except Exception:
        logger.exception("ads alert discord notify failed org=%s", org_id)


def _account_for_org(db: Session, org_id: uuid.UUID) -> Optional[MetaAdAccount]:
    token = mac.get_meta_ads_token(db, org_id)
    q = db.query(MetaAdAccount).filter(MetaAdAccount.org_id == org_id)
    if token is not None and token.account_id:
        match = q.filter(MetaAdAccount.meta_ad_account_id == token.account_id).first()
        if match is not None:
            return match
    return q.order_by(MetaAdAccount.updated_at.desc()).first()


def _rollup(pts: Sequence[DayPoint]) -> Dict[str, Any]:
    spend = sum(p.spend_cents or 0 for p in pts)
    results = sum(p.results or 0 for p in pts)
    imps = sum(p.impressions or 0 for p in pts)
    clicks = sum(p.clicks or 0 for p in pts)
    cpr = round(spend / results) if results else None
    ctr = (clicks / imps * 100) if imps else _mean(p.ctr for p in pts)
    cpm = round(spend / imps * 1000) if imps and spend else None
    freq = _weighted_freq(pts)
    return {
        "spend_cents": spend,
        "results": results,
        "cpr_cents": cpr,
        "ctr": round(ctr, 2) if ctr is not None else None,
        "cpm_cents": cpm,
        "frequency": round(freq, 2) if freq is not None else None,
    }


def _delta_pct(new: Optional[float], old: Optional[float]) -> Optional[float]:
    if new is None or old is None or old == 0:
        return None
    return round((float(new) - float(old)) / float(old) * 100, 1)


def _in_span(day: date, start: Optional[date], end: date) -> bool:
    if start is not None and day < start:
        return False
    return day <= end


def _series(pts: Sequence[DayPoint], since: Optional[date], until: Optional[date] = None) -> List[Dict[str, Any]]:
    out = []
    for p in sorted(pts, key=lambda item: item.day):
        if since is not None and p.day < since:
            continue
        if until is not None and p.day > until:
            continue
        cpr = round(p.spend_cents / p.results) if p.results else p.cost_per_result_cents
        freq = _freq(p)
        out.append(
            {
                "date": p.day.isoformat(),
                "spend_cents": p.spend_cents,
                "results": p.results,
                "cpr_cents": cpr,
                "ctr": p.ctr,
                "cpm_cents": p.cpm_cents,
                "frequency": round(freq, 2) if freq is not None else None,
            }
        )
    return out


def _alert_out(row: MetaAdAlert) -> Dict[str, Any]:
    snap = row.metrics_snapshot or {}
    return {
        "id": str(row.id),
        "level": row.level,
        "entity_id": row.entity_id,
        "alert_type": row.alert_type,
        "severity": row.severity,
        "status": row.status,
        "recommended_action": row.recommended_action,
        "playbook": snap.get("playbook"),
        "entity_name": snap.get("entity_name"),
        "metrics": snap,
        "first_detected_at": row.first_detected_at.isoformat() if row.first_detected_at else None,
    }


def _load_alerts(db: Session, org_id: uuid.UUID) -> List[MetaAdAlert]:
    rows = (
        db.query(MetaAdAlert)
        .filter(MetaAdAlert.org_id == org_id, MetaAdAlert.status.in_(("open", "acknowledged")))
        .all()
    )
    rows.sort(
        key=lambda a: (
            0 if a.status == "open" else 1,
            _SEV_RANK.get(a.severity, 9),
            -(a.first_detected_at.timestamp() if a.first_detected_at else 0),
        )
    )
    return rows


def _alert_campaign_id(alert: MetaAdAlert) -> Optional[str]:
    snap = alert.metrics_snapshot or {}
    return alert.entity_id if alert.level == "campaign" else snap.get("campaign_id")


def build_ads_dashboard(
    db: Session,
    org_id: uuid.UUID,
    days: int = 28,
    *,
    start: Optional[date] = None,
    end: Optional[date] = None,
    compare_start: Optional[date] = None,
    compare_end: Optional[date] = None,
    funnel_id: Optional[uuid.UUID] = None,
) -> Dict[str, Any]:
    """`end` set means the page calendar owns the window. Otherwise trailing `days`."""
    today = _utcnow().date()
    if end is None:
        days = max(7, min(int(days or 28), 90))
        series_start: Optional[date] = today - timedelta(days=days - 1)
        series_end = today
        recent_start, recent_end = today - timedelta(days=6), today
        prior_start, prior_end = today - timedelta(days=13), recent_start - timedelta(days=1)
    else:
        series_start, series_end = start, end
        recent_start, recent_end = start, end
        prior_start, prior_end = compare_start, compare_end
        days = (series_end - series_start).days + 1 if series_start is not None else 0
    bounds = [d for d in (series_start, prior_start) if d is not None]
    fetch_since = min(bounds) if bounds else None
    fetch_until = max(d for d in (series_end, prior_end) if d is not None)

    token = mac.get_meta_ads_token(db, org_id)
    account = _account_for_org(db, org_id)
    campaigns_q = db.query(MetaCampaign).filter(MetaCampaign.org_id == org_id)
    if account is not None:
        # Org-wide alone isn't enough — an org that ever reconnected to a
        # different Meta ad account (different business/portfolio) has old
        # campaigns from the prior account still sitting in this table with
        # no cleanup. Scope to the currently-connected account only.
        campaigns_q = campaigns_q.filter(MetaCampaign.ad_account_id == account.id)
    if funnel_id is not None:
        campaigns_q = campaigns_q.filter(MetaCampaign.funnel_id == funnel_id)
    campaigns = campaigns_q.all()
    # Funnel-scoped callers (the Funnels tab) only want this funnel's linked
    # campaigns reflected in KPIs/alerts too, not the whole org's.
    scoped_meta_campaign_ids = {c.meta_campaign_id for c in campaigns} if funnel_id is not None else None

    insight_q = db.query(MetaAdInsightDaily).filter(
        MetaAdInsightDaily.org_id == org_id,
        MetaAdInsightDaily.level == "campaign",
        MetaAdInsightDaily.date <= fetch_until,
    )
    if fetch_since is not None:
        insight_q = insight_q.filter(MetaAdInsightDaily.date >= fetch_since)
    if scoped_meta_campaign_ids is not None:
        insight_q = insight_q.filter(MetaAdInsightDaily.entity_id.in_(scoped_meta_campaign_ids))
    insights = insight_q.all()
    grouped: Dict[str, List[DayPoint]] = defaultdict(list)
    for row in insights:
        grouped[row.entity_id].append(_point_from_row(row))

    all_pts = [p for pts in grouped.values() for p in pts]
    recent = _rollup([p for p in all_pts if _in_span(p.day, recent_start, recent_end)])
    prior = (
        _rollup([p for p in all_pts if _in_span(p.day, prior_start, prior_end)])
        if prior_start is not None and prior_end is not None
        else _rollup([])
    )
    alerts = _load_alerts(db, org_id)
    if scoped_meta_campaign_ids is not None:
        alerts = [a for a in alerts if _alert_campaign_id(a) in scoped_meta_campaign_ids]

    types_by_campaign: Dict[str, Set[str]] = defaultdict(set)
    ceiling_by_campaign: Dict[str, Optional[int]] = {}
    for alert in alerts:
        snap = alert.metrics_snapshot or {}
        camp_id = _alert_campaign_id(alert)
        if camp_id:
            types_by_campaign[str(camp_id)].add(alert.alert_type)
        if alert.alert_type == "messaging_ceiling" and alert.level == "campaign":
            ceiling_by_campaign[alert.entity_id] = snap.get("efficient_daily_spend_cents")

    campaign_out = []
    for camp in campaigns:
        pts = grouped.get(camp.meta_campaign_id, [])
        window = [p for p in pts if _in_span(p.day, series_start, series_end)]
        totals = _rollup(window)
        campaign_out.append(
            {
                "id": str(camp.id),
                "meta_campaign_id": camp.meta_campaign_id,
                "name": camp.name,
                "objective": camp.objective,
                "status": camp.effective_status or camp.status,
                "sweep_status": camp.sweep_status,
                "daily_budget_cents": camp.daily_budget_cents,
                "pill": campaign_pill(camp.sweep_status, types_by_campaign.get(camp.meta_campaign_id, set())),
                "spend_cents": totals["spend_cents"],
                "results": totals["results"],
                "cpr_cents": totals["cpr_cents"],
                "series": _series(pts, series_start, series_end),
                "ceiling_daily_spend_cents": ceiling_by_campaign.get(camp.meta_campaign_id),
                "funnel_id": str(camp.funnel_id) if camp.funnel_id else None,
            }
        )
    campaign_out.sort(key=lambda c: c["spend_cents"] or 0, reverse=True)

    return {
        "connected": token is not None,
        "app_configured": mac.meta_ads_app_configured(),
        "last_sync_at": token.last_sync_at.isoformat() if token and token.last_sync_at else None,
        "currency": account.currency if account else None,
        "settings": {
            "target_cpr_cents": account.target_cpr_cents if account else None,
            "min_roas": account.min_roas if account else None,
        },
        "kpis": {
            "recent": recent,
            "prior": prior,
            "delta": {
                "spend_pct": _delta_pct(recent["spend_cents"], prior["spend_cents"]),
                "results_pct": _delta_pct(recent["results"], prior["results"]),
                "cpr_pct": _delta_pct(recent["cpr_cents"], prior["cpr_cents"]),
                "ctr_pct": _delta_pct(recent["ctr"], prior["ctr"]),
                "cpm_pct": _delta_pct(recent["cpm_cents"], prior["cpm_cents"]),
                "frequency_pct": _delta_pct(recent["frequency"], prior["frequency"]),
            },
        },
        "campaigns": campaign_out,
        "alerts": [_alert_out(a) for a in alerts],
        "days": days,
        "start": series_start.isoformat() if series_start else None,
        "end": series_end.isoformat(),
    }


def build_campaign_breakdown(
    db: Session,
    org_id: uuid.UUID,
    campaign_uuid: uuid.UUID,
    days: int = 28,
    *,
    start: Optional[date] = None,
    end: Optional[date] = None,
) -> Optional[Dict[str, Any]]:
    today = _utcnow().date()
    if end is None:
        days = max(7, min(int(days or 28), 90))
        series_start: Optional[date] = today - timedelta(days=days - 1)
        series_end = today
    else:
        series_start, series_end = start, end
        days = (series_end - series_start).days + 1 if series_start is not None else 0
    camp = (
        db.query(MetaCampaign)
        .filter(MetaCampaign.org_id == org_id, MetaCampaign.id == campaign_uuid)
        .first()
    )
    if camp is None:
        return None
    learn_since = today - timedelta(days=6)
    query_until = max(series_end, today)
    query_since = None if series_start is None else min(series_start, learn_since)
    adsets = (
        db.query(MetaAdSet)
        .filter(MetaAdSet.org_id == org_id, MetaAdSet.campaign_id == camp.id)
        .all()
    )
    adset_ids = [a.id for a in adsets]
    ads: List[MetaAd] = []
    if adset_ids:
        ads = db.query(MetaAd).filter(MetaAd.org_id == org_id, MetaAd.ad_set_id.in_(adset_ids)).all()
    insight_q = db.query(MetaAdInsightDaily).filter(
        MetaAdInsightDaily.org_id == org_id,
        MetaAdInsightDaily.date <= query_until,
    )
    if query_since is not None:
        insight_q = insight_q.filter(MetaAdInsightDaily.date >= query_since)
    insights = insight_q.all()
    grouped: Dict[Tuple[str, str], List[DayPoint]] = defaultdict(list)
    for row in insights:
        grouped[(row.level, row.entity_id)].append(_point_from_row(row))

    alerts = _load_alerts(db, org_id)
    fatigued_ads = {
        a.entity_id
        for a in alerts
        if a.level == "ad" and a.alert_type == "creative_fatigue" and a.status == "open"
    }
    ads_by_adset: Dict[Any, List[MetaAd]] = defaultdict(list)
    for ad in ads:
        ads_by_adset[ad.ad_set_id].append(ad)

    adset_out = []
    for adset in adsets:
        child = ads_by_adset.get(adset.id, [])
        pts = _adset_points(adset, child, grouped)
        results_7 = sum(p.results for p in pts if learn_since <= p.day <= today)
        prepared = []
        spend_total = 0
        for ad in child:
            ad_pts = [p for p in grouped.get(("ad", ad.meta_ad_id), []) if _in_span(p.day, series_start, series_end)]
            spend = sum(p.spend_cents for p in ad_pts)
            spend_total += spend
            prepared.append((ad, ad_pts, spend))
        ad_rows = []
        for ad, ad_pts, spend in prepared:
            totals = _rollup(ad_pts)
            ad_rows.append(
                {
                    "id": str(ad.id),
                    "meta_ad_id": ad.meta_ad_id,
                    "name": ad.name,
                    "thumbnail_url": ad.creative_thumbnail_url,
                    "effective_status": ad.effective_status,
                    "created_time": ad.created_time.isoformat() if ad.created_time else None,
                    "spend_cents": spend,
                    "spend_share": round(spend / spend_total, 3) if spend_total else 0,
                    "results": totals["results"],
                    "cpr_cents": totals["cpr_cents"],
                    "frequency": totals["frequency"],
                    "ctr": totals["ctr"],
                    "fatigued": ad.meta_ad_id in fatigued_ads,
                }
            )
        ad_rows.sort(key=lambda r: r["spend_cents"], reverse=True)
        active_count = sum(1 for ad in child if _delivering(ad.effective_status))
        adset_out.append(
            {
                "id": str(adset.id),
                "meta_adset_id": adset.meta_adset_id,
                "name": adset.name,
                "optimization_goal": adset.optimization_goal,
                "effective_status": adset.effective_status,
                "learning_stage": adset.learning_stage,
                "daily_budget_cents": adset.daily_budget_cents,
                "results_7": results_7,
                "active_ads": active_count,
                "bench_target": int(THRESHOLDS["bench_min_active"]),
                "series": _series(pts, series_start, series_end),
                "ads": ad_rows,
            }
        )

    camp_pts = grouped.get(("campaign", camp.meta_campaign_id), [])
    ceiling = None
    for alert in alerts:
        if (
            alert.level == "campaign"
            and alert.entity_id == camp.meta_campaign_id
            and alert.alert_type == "messaging_ceiling"
        ):
            ceiling = (alert.metrics_snapshot or {}).get("efficient_daily_spend_cents")
    return {
        "campaign": {
            "id": str(camp.id),
            "meta_campaign_id": camp.meta_campaign_id,
            "name": camp.name,
            "sweep_status": camp.sweep_status,
            "status": camp.effective_status or camp.status,
            "series": _series(camp_pts, series_start, series_end),
            "ceiling_daily_spend_cents": ceiling,
        },
        "ad_sets": adset_out,
        "days": days,
        "start": series_start.isoformat() if series_start else None,
        "end": series_end.isoformat(),
    }


def build_funnel_creative_grid(
    db: Session,
    org_id: uuid.UUID,
    funnel_id: uuid.UUID,
    days: int = 28,
    *,
    start: Optional[date] = None,
    end: Optional[date] = None,
    campaign_id: Optional[uuid.UUID] = None,
) -> Dict[str, Any]:
    """Per-creative (ad) rows across every campaign linked to one funnel —
    the funnel scorecard's replacement for the old metric-rows x week-columns
    sheet, for a funnel that has linked campaigns. See
    docs/features/META_ADS_PERFORMANCE_PRD.md, "Creative grid".

    Each row is one ad. Columns are the ad-native metrics (spend/results/cpr/ctr)
    this data already supports — each with a current-range total plus a daily
    series for the frontend's inline sparkline (same shape AdsTab's CprSpark
    already consumes). The 20 funnel-economics metrics (leads, close rate, cash
    collected, CAC, ROAS, etc.) have no attribution path down to an individual
    ad yet — that needs the Content Engine PRD's UTM-to-ad attribution, not
    built — so they are deliberately not computed here at all; the frontend
    renders their column headers with a static placeholder, not a query that
    always returns null.
    """
    today = _utcnow().date()
    if end is None:
        days = max(7, min(int(days or 28), 90))
        series_start: Optional[date] = today - timedelta(days=days - 1)
        series_end = today
    else:
        series_start, series_end = start, end
        days = (series_end - series_start).days + 1 if series_start is not None else 0

    # Prior period: same length, immediately preceding — drives the up/down
    # trend arrow on Spend/Results, same idea as build_ads_dashboard's
    # recent-vs-prior delta, generalized to whatever range the page's shared
    # date picker currently has selected (not a fixed 7 days).
    prior_start: Optional[date] = None
    prior_end: Optional[date] = None
    if series_start is not None:
        prior_end = series_start - timedelta(days=1)
        prior_start = prior_end - (series_end - series_start)

    campaigns_q = db.query(MetaCampaign).filter(MetaCampaign.org_id == org_id, MetaCampaign.funnel_id == funnel_id)
    if campaign_id is not None:
        campaigns_q = campaigns_q.filter(MetaCampaign.id == campaign_id)
    campaigns = campaigns_q.all()
    account = _account_for_org(db, org_id)
    out_base = {
        "ads": [],
        "currency": account.currency if account else None,
        "days": days,
        "start": series_start.isoformat() if series_start else None,
        "end": series_end.isoformat(),
    }
    if not campaigns:
        return out_base

    campaign_by_id = {c.id: c for c in campaigns}
    adsets = (
        db.query(MetaAdSet)
        .filter(MetaAdSet.org_id == org_id, MetaAdSet.campaign_id.in_(list(campaign_by_id.keys())))
        .all()
    )
    adset_by_id = {a.id: a for a in adsets}
    adset_ids = list(adset_by_id.keys())
    ads: List[MetaAd] = []
    if adset_ids:
        ads = db.query(MetaAd).filter(MetaAd.org_id == org_id, MetaAd.ad_set_id.in_(adset_ids)).all()
    if not ads:
        return out_base

    insight_q = db.query(MetaAdInsightDaily).filter(
        MetaAdInsightDaily.org_id == org_id,
        MetaAdInsightDaily.level == "ad",
        MetaAdInsightDaily.entity_id.in_([a.meta_ad_id for a in ads]),
        MetaAdInsightDaily.date <= series_end,
    )
    if prior_start is not None:
        insight_q = insight_q.filter(MetaAdInsightDaily.date >= prior_start)
    elif series_start is not None:
        insight_q = insight_q.filter(MetaAdInsightDaily.date >= series_start)
    grouped: Dict[str, List[DayPoint]] = defaultdict(list)
    for row in insight_q.all():
        grouped[row.entity_id].append(_point_from_row(row))

    alerts = _load_alerts(db, org_id)
    fatigued_ads = {
        a.entity_id for a in alerts if a.level == "ad" and a.alert_type == "creative_fatigue" and a.status == "open"
    }

    ad_rows = []
    for ad in ads:
        pts = [p for p in grouped.get(ad.meta_ad_id, []) if _in_span(p.day, series_start, series_end)]
        totals = _rollup(pts)
        prior_totals = None
        if prior_start is not None and prior_end is not None:
            prior_pts = [p for p in grouped.get(ad.meta_ad_id, []) if _in_span(p.day, prior_start, prior_end)]
            prior_totals = _rollup(prior_pts)
        adset = adset_by_id.get(ad.ad_set_id)
        camp = campaign_by_id.get(adset.campaign_id) if adset else None
        ad_rows.append(
            {
                "id": str(ad.id),
                "meta_ad_id": ad.meta_ad_id,
                "name": ad.name,
                "thumbnail_url": ad.creative_thumbnail_url,
                "effective_status": ad.effective_status,
                "ad_set_id": str(adset.id) if adset else None,
                "ad_set_name": adset.name if adset else None,
                "campaign_name": camp.name if camp else None,
                "spend_cents": totals["spend_cents"],
                "results": totals["results"],
                "cpr_cents": totals["cpr_cents"],
                "ctr": totals["ctr"],
                "prior_spend_cents": prior_totals["spend_cents"] if prior_totals else None,
                "prior_results": prior_totals["results"] if prior_totals else None,
                "fatigued": ad.meta_ad_id in fatigued_ads,
                "series": _series(pts, series_start, series_end),
            }
        )
    ad_rows.sort(key=lambda r: r["spend_cents"] or 0, reverse=True)
    out_base["ads"] = ad_rows
    return out_base

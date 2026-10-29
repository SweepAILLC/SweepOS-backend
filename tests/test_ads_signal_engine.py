"""Pure rules for the Ads tab. No database."""
from datetime import date, timedelta

from app.services.ads_signal_engine import (
    DayPoint,
    campaign_pill,
    detect_creative_bench,
    detect_creative_fatigue,
    detect_fatigue_crossover,
    detect_learning_limited,
    detect_messaging_ceiling,
    detect_scale_ready,
    detect_spend_concentration,
    evaluate_pullback,
)


def _days(start: date, rows):
    return [DayPoint(day=start + timedelta(days=i), **row) for i, row in enumerate(rows)]


def _flat(start, n, **fields):
    return _days(start, [dict(fields) for _ in range(n)])


START = date(2026, 10, 1)


def test_creative_fatigue_needs_two_leading_signals():
    base = dict(spend_cents=1000, results=10, impressions=2000, ctr=2.0, frequency=1.2)
    hot = dict(spend_cents=2000, results=5, impressions=2000, ctr=1.0, frequency=3.6)
    sig = detect_creative_fatigue(_flat(START, 7, **base) + _flat(START + timedelta(days=7), 7, **hot))
    assert sig is not None
    assert sig["alert_type"] == "creative_fatigue"
    assert sig["severity"] == "critical"
    assert set(sig["metrics"]["leading"]) >= {"frequency", "ctr", "cpm"}


def test_creative_fatigue_ignores_a_single_signal_and_cpr_alone():
    only_freq = dict(spend_cents=1000, results=10, impressions=2000, ctr=2.0, frequency=3.6)
    base = dict(spend_cents=1000, results=10, impressions=2000, ctr=2.0, frequency=1.2)
    assert detect_creative_fatigue(_flat(START, 7, **base) + _flat(START + timedelta(days=7), 7, **only_freq)) is None

    cpr_only = dict(spend_cents=4000, results=2, impressions=2000, ctr=2.0, frequency=1.2)
    assert detect_creative_fatigue(_flat(START, 7, **base) + _flat(START + timedelta(days=7), 7, **cpr_only)) is None


def test_creative_fatigue_two_signals_without_cpr_spike_is_warn():
    base = dict(spend_cents=1000, results=10, impressions=2000, ctr=2.0, frequency=1.2)
    recent = dict(spend_cents=1000, results=10, impressions=2000, ctr=1.0, frequency=2.6)
    sig = detect_creative_fatigue(_flat(START, 7, **base) + _flat(START + timedelta(days=7), 7, **recent))
    assert sig is not None
    assert sig["severity"] == "warn"


def test_fatigue_crossover_when_cost_rises_and_volume_falls():
    prev = dict(spend_cents=3000, results=30, impressions=1000)
    recent = dict(spend_cents=4500, results=15, impressions=1000)
    sig = detect_fatigue_crossover(_flat(START, 3, **prev) + _flat(START + timedelta(days=3), 3, **recent))
    assert sig is not None
    assert sig["alert_type"] == "fatigue_crossover"
    assert detect_fatigue_crossover(_flat(START, 6, **prev)) is None


def test_messaging_ceiling_flags_expensive_high_spend_tier():
    low = dict(spend_cents=1000, results=20)
    mid = dict(spend_cents=2000, results=20)
    high = dict(spend_cents=8000, results=10)
    pts = _flat(START, 3, **low) + _flat(START + timedelta(days=3), 3, **mid) + _flat(START + timedelta(days=6), 3, **high)
    sig = detect_messaging_ceiling(pts)
    assert sig is not None
    assert sig["metrics"]["efficient_daily_spend_cents"] == 1000

    cheap_high = dict(spend_cents=8000, results=80)
    inverse = _flat(START, 3, spend_cents=1000, results=2) + _flat(START + timedelta(days=3), 3, **mid) + _flat(
        START + timedelta(days=6), 3, **cheap_high
    )
    assert detect_messaging_ceiling(inverse) is None


def test_pullback_opens_then_calls_ceiling_or_fatigue():
    prior = dict(spend_cents=3000, results=30)
    dropped = dict(spend_cents=1000, results=10)
    pts = _flat(START, 3, **prior) + _flat(START + timedelta(days=3), 3, **dropped)
    today = START + timedelta(days=5)
    opened = evaluate_pullback(pts, trigger_open=True, existing=None, today=today, already_judged=False)
    assert opened["action"] == "open"
    assert opened["signal"]["alert_type"] == "pullback_watch"

    existing = opened["signal"]["metrics"]
    same_day = evaluate_pullback(pts, trigger_open=True, existing=existing, today=today, already_judged=False)
    assert same_day["action"] == "keep"

    recovered = evaluate_pullback(
        pts, trigger_open=True, existing=existing, today=today + timedelta(days=1), already_judged=False
    )
    assert recovered["action"] == "resolve_and_open"
    assert recovered["signal"]["alert_type"] == "ceiling_confirmed"

    worse = _flat(START, 3, **prior) + _flat(START + timedelta(days=3), 3, spend_cents=1000, results=2)
    fatigued = evaluate_pullback(
        worse, trigger_open=True, existing=existing, today=today + timedelta(days=1), already_judged=False
    )
    assert fatigued["signal"]["alert_type"] == "fatigue_confirmed"

    bounced = evaluate_pullback(
        _flat(START, 6, **prior),
        trigger_open=False,
        existing=existing,
        today=today + timedelta(days=1),
        already_judged=False,
    )
    assert bounced["action"] == "resolve"


def test_scale_ready_gates_and_step_size():
    stable = dict(spend_cents=5000, results=8, impressions=1000, frequency=1.2)
    pts = _flat(START, 14, **stable)
    sig = detect_scale_ready(pts, learning_stage=None, target_cpr_cents=None, budget_quiet=True, active=True)
    assert sig is not None
    assert sig["metrics"]["step_pct"] == 20

    busy = dict(spend_cents=5000, results=12, impressions=1000, frequency=1.2)
    high = detect_scale_ready(
        _flat(START, 14, **busy), learning_stage="SUCCESS", target_cpr_cents=1000, budget_quiet=True, active=True
    )
    assert high["metrics"]["step_pct"] == 30

    assert detect_scale_ready(pts, learning_stage="LEARNING", target_cpr_cents=None, budget_quiet=True, active=True) is None
    assert detect_scale_ready(pts, learning_stage=None, target_cpr_cents=None, budget_quiet=False, active=True) is None
    loud = dict(spend_cents=5000, results=8, impressions=1000, frequency=2.5)
    assert detect_scale_ready(
        _flat(START, 14, **loud), learning_stage=None, target_cpr_cents=None, budget_quiet=True, active=True
    ) is None
    assert detect_scale_ready(pts, learning_stage=None, target_cpr_cents=100, budget_quiet=True, active=True) is None


def test_learning_limited_after_a_week_under_50_results():
    today = START + timedelta(days=9)
    pts = _flat(START, 10, spend_cents=1000, results=1)
    sig = detect_learning_limited(pts, active=True, today=today)
    assert sig is not None
    assert sig["metrics"]["results_7"] == 7
    young = _flat(START, 3, spend_cents=1000, results=1)
    assert detect_learning_limited(young, active=True, today=START + timedelta(days=2)) is None
    assert detect_learning_limited(pts, active=False, today=today) is None


def test_spend_concentration_and_bench():
    sig = detect_spend_concentration([("a", "A", 800), ("b", "B", 100), ("c", "C", 100)])
    assert sig is not None
    assert sig["severity"] == "info"
    assert detect_spend_concentration([("a", "A", 100), ("b", "B", 100), ("c", "C", 100)]) is None

    thin = detect_creative_bench(active_count=4, newest_created=START, today=START, active=True)
    assert thin is not None
    assert detect_creative_bench(active_count=30, newest_created=START, today=START, active=True) is None
    stale = detect_creative_bench(active_count=30, newest_created=START, today=START + timedelta(days=20), active=True)
    assert stale is not None


def test_campaign_pill_priority():
    assert campaign_pill("foundational", {"creative_fatigue", "scale_ready"}) == "fatigued"
    assert campaign_pill(None, {"messaging_ceiling"}) == "pull_back"
    assert campaign_pill(None, {"learning_limited"}) == "learning"
    assert campaign_pill(None, {"scale_ready"}) == "scale"
    assert campaign_pill("foundational", set()) == "foundational"
    assert campaign_pill(None, set()) == "hold"

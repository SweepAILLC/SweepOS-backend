"""Kill rule, scale-test verdict, and low pixel volume. Pure rules, no database."""
from datetime import date, timedelta

from app.services.ads_signal_engine import (
    DayPoint,
    campaign_pill,
    detect_learning_limited,
    detect_no_result_kill,
    detect_scale_test,
)

START = date(2026, 10, 1)


def _flat(start, n, **fields):
    return [DayPoint(day=start + timedelta(days=i), **fields) for i in range(n)]


def test_kill_when_spend_passes_three_targets_with_no_results():
    pts = _flat(START, 5, spend_cents=15_000, results=0)  # $750, target $200
    sig = detect_no_result_kill(pts, target_cpr_cents=20_000, active=True)
    assert sig["alert_type"] == "no_result_kill"
    assert sig["severity"] == "critical"


def test_no_kill_below_limit_or_with_results_or_without_target():
    assert detect_no_result_kill(_flat(START, 2, spend_cents=15_000), target_cpr_cents=20_000, active=True) is None
    with_result = _flat(START, 5, spend_cents=15_000) + [DayPoint(day=START + timedelta(days=6), results=1)]
    assert detect_no_result_kill(with_result, target_cpr_cents=20_000, active=True) is None
    assert detect_no_result_kill(_flat(START, 9, spend_cents=15_000), target_cpr_cents=None, active=True) is None


def test_scale_test_fails_when_cost_rises_with_spend():
    prior = _flat(START, 7, spend_cents=10_000, results=5)  # $20 / result
    recent = _flat(START + timedelta(days=7), 3, spend_cents=20_000, results=5)  # spend 2x, $40 / result
    sig = detect_scale_test(prior + recent, active=True)
    assert sig["alert_type"] == "scale_test_failed"
    assert sig["metrics"]["spend_rise_pct"] == 100.0


def test_scale_test_passes_when_cost_holds():
    prior = _flat(START, 7, spend_cents=10_000, results=5)
    recent = _flat(START + timedelta(days=7), 3, spend_cents=20_000, results=9)  # ~$22 / result
    sig = detect_scale_test(prior + recent, active=True)
    assert sig["alert_type"] == "scale_test_passed"
    assert sig["severity"] == "info"


def test_no_scale_test_without_a_push():
    pts = _flat(START, 10, spend_cents=10_000, results=5)
    assert detect_scale_test(pts, active=True) is None


def test_failed_scale_test_pills_pull_back():
    assert campaign_pill(None, {"scale_test_failed"}) == "pull_back"


def test_learning_limited_suggests_earlier_event_under_one_a_day():
    today = START + timedelta(days=13)
    sig = detect_learning_limited(_flat(START, 14, spend_cents=5_000, results=0), active=True, today=today)
    assert sig["metrics"]["below_one_per_day"] is True
    assert "earlier" in sig["recommended_action"]
    busy = detect_learning_limited(_flat(START, 14, spend_cents=5_000, results=3), active=True, today=today)
    assert busy["metrics"]["below_one_per_day"] is False

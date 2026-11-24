"""Ads tab targets derived from the Scale Planner, with a manual override switch."""
from types import SimpleNamespace

from app.services import scale_planner as sp
from app.services.ads_signal_engine import campaign_pill, detect_roas_floor
from app.services.ads_targets import resolve_targets


def _account(follow=True, cpr=None, roas=None):
    return SimpleNamespace(targets_follow_planner=follow, target_cpr_cents=cpr, min_roas=roas)


SUGGESTED = sp.suggested_ad_targets(
    {"cash_per_booking": 900.0, "lead_to_booking_rate": 0.25, "booked_calls": 40},
    breakeven_roas=2.14,
    marginal_breakeven_roas=1.43,
)


def test_suggested_targets_math():
    assert SUGGESTED["min_roas"] == 2.14
    assert SUGGESTED["target_cost_per_booking"] == round(900 / 2.14, 2)  # 420.56
    assert SUGGESTED["max_cost_per_booking"] == round(900 / 1.43, 2)
    assert SUGGESTED["target_cost_per_lead"] == round(900 / 2.14 * 0.25, 2)


def test_suggested_targets_without_bookings_are_none():
    s = sp.suggested_ad_targets({"cash_per_booking": None}, 2.0, 1.4)
    assert s["target_cost_per_booking"] is None and s["target_cost_per_lead"] is None
    assert s["min_roas"] == 2.0


def test_trailing_summary_reports_bookings():
    t = sp.trailing_summary(
        {
            "ad_spend": [1000.0] * 4,
            "cash_collected": [3000.0] * 4,
            "booked_calls": [5.0] * 4,
            "leads": [20.0] * 4,
            "cost_per_booking": [200.0] * 4,
        }
    )
    assert t["booked_calls"] == 20 and t["leads"] == 80
    assert t["cash_per_booking"] == 600.0
    assert t["lead_to_booking_rate"] == 0.25


def test_follow_planner_by_default_and_without_account():
    r = resolve_targets(None, SUGGESTED, None)
    assert r["follow_planner"] is True
    assert r["target_cpr_cents"] == 42056 and r["target_cpr_source"] == "planner"
    assert r["min_roas"] == 2.14 and r["min_roas_source"] == "planner"


def test_switch_off_uses_manual_values():
    r = resolve_targets(_account(follow=False, cpr=5000, roas=3.0), SUGGESTED, None)
    assert (r["target_cpr_cents"], r["target_cpr_source"]) == (5000, "manual")
    assert (r["min_roas"], r["min_roas_source"]) == (3.0, "manual")


def test_following_without_planner_numbers_falls_back():
    r = resolve_targets(_account(follow=True, cpr=None, roas=2.5), None, 15000)
    assert (r["target_cpr_cents"], r["target_cpr_source"]) == (15000, "business_numbers")
    assert (r["min_roas"], r["min_roas_source"]) == (2.5, "manual")
    assert resolve_targets(_account(follow=True), None, None)["target_cpr_cents"] is None


def _floor(roas, weeks=4, active=True):
    return detect_roas_floor(
        roas=roas, weeks_with_spend=weeks, min_roas=2.14, marginal_breakeven_roas=1.43,
        funnel_name="VSL", active=active,
    )


def test_roas_floor_levels():
    assert _floor(1.2)["alert_type"] == "roas_below_breakeven"
    assert _floor(1.2)["severity"] == "critical"
    assert _floor(1.8)["alert_type"] == "roas_below_floor"
    assert _floor(2.5) is None


def test_roas_floor_needs_data_and_delivery():
    assert _floor(1.0, weeks=1) is None
    assert _floor(1.0, active=False) is None
    assert _floor(None) is None


def test_below_breakeven_pills_pull_back():
    assert campaign_pill(None, {"roas_below_breakeven"}) == "pull_back"
    assert campaign_pill(None, {"roas_below_floor"}) == "hold"

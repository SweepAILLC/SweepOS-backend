"""Weekly sheet scorecard: 23 metrics, last full week vs average-of-weeks benchmark."""
import json
import uuid
from datetime import date, datetime, timezone
from types import SimpleNamespace
from unittest.mock import patch

from app.services import funnel_scorecard as sc
from app.services.kpi_integration_sync import count_sales_calls, sales_call_booked_at


def test_month_weeks_are_mondays_inside_month_up_to_today():
    # September 2026, today Thu Sep 24: Mondays Sep 7, 14, 21 (Aug 31 belongs to August).
    weeks = sc.scorecard_weeks(date(2026, 9, 1), date(2026, 9, 30), date(2026, 9, 24))
    assert weeks == [date(2026, 9, 7), date(2026, 9, 14), date(2026, 9, 21)]
    assert sc.week_in_progress(date(2026, 9, 21), date(2026, 9, 24)) is True
    assert sc.week_in_progress(date(2026, 9, 14), date(2026, 9, 24)) is False
    # August owns the week of Mon Aug 31, so it is never double-counted.
    assert sc.scorecard_weeks(date(2026, 8, 1), date(2026, 8, 31), date(2026, 9, 24))[-1] == date(2026, 8, 31)


def test_booking_date_prefers_provider_timestamp_then_row_created_then_call_time():
    call = datetime(2026, 9, 10, 18, tzinfo=timezone.utc)
    calcom = SimpleNamespace(
        raw_event_data=json.dumps({"createdAt": "2026-09-03T16:04:33.513Z"}),
        start_time=call,
        created_at=datetime(2026, 9, 20, tzinfo=timezone.utc),
    )
    assert sales_call_booked_at(calcom) == datetime(2026, 9, 3, 16, 4, 33, 513000, tzinfo=timezone.utc)
    ghl = SimpleNamespace(raw_event_data=json.dumps({"dateAdded": "2026-09-01T00:00:00Z"}), start_time=call, created_at=None)
    assert sales_call_booked_at(ghl).date() == date(2026, 9, 1)
    # Backfilled row (synced after the call) must not count as the booking date.
    backfill = SimpleNamespace(raw_event_data=None, start_time=call, created_at=datetime(2026, 9, 20, tzinfo=timezone.utc))
    assert sales_call_booked_at(backfill) == call
    live_sync = SimpleNamespace(raw_event_data="{}", start_time=call, created_at=datetime(2026, 9, 5, tzinfo=timezone.utc))
    assert sales_call_booked_at(live_sync) == datetime(2026, 9, 5, tzinfo=timezone.utc)


def _metric(key):
    return next(m for m in sc.METRICS if m.key == key)


def test_sheet_formulas():
    w = sc.WeekBase(
        spend=1000.0, has_spend_row=True, visitors=500, leads=50, booked_calls=10,
        calls_on_calendar=8, live_calls=6, deals_closed=2, cash=6000.0,
    )
    got = {m.key: m.compute(w) for m in sc.METRICS}
    assert got["cost_per_lpv"] == 2.0
    assert got["lp_conv_rate"] == 0.1
    assert got["lead_to_book_rate"] == 0.2
    assert got["show_rate"] == 0.75  # live / on calendar
    assert got["close_rate"] == 2 / 6
    assert got["cash_per_close"] == 3000.0
    assert got["lead_to_close_rate"] == 0.04
    assert (got["cost_per_lead"], got["cost_per_booking"], got["cac"]) == (20.0, 100.0, 500.0)
    assert got["roas"] == 6.0
    assert got["profit"] == 5000.0
    assert got["margin"] == 5000.0 / 6000.0
    assert len(sc.METRICS) == 23


def test_cost_metrics_undefined_without_spend():
    w = sc.WeekBase(leads=10, deals_closed=1, cash=500.0)
    for key in ("cost_per_lead", "cac", "roas", "profit", "margin", "ad_spend"):
        assert _metric(key).compute(w) is None


def test_benchmark_is_average_of_weekly_values_skipping_undefined():
    assert sc._mean([0.5, None, 0.25]) == 0.375
    assert sc._mean([None, None]) is None


def test_compute_scorecard_grid_and_benchmark_over_complete_weeks():
    weeks = [date(2026, 9, 7), date(2026, 9, 14), date(2026, 9, 21)]
    bases = {
        weeks[0]: sc.WeekBase(leads=10, calls_on_calendar=4, live_calls=2),
        weeks[1]: sc.WeekBase(leads=30, calls_on_calendar=4, live_calls=0),
        weeks[2]: sc.WeekBase(leads=1, calls_on_calendar=1, live_calls=1),  # in progress
    }
    with patch.object(sc, "collect_funnel_facts", return_value=SimpleNamespace()), patch.object(
        sc, "_week_bases", return_value=bases
    ):
        out = sc.compute_scorecard(
            None, uuid.uuid4(), date(2026, 9, 1), date(2026, 9, 30), "organic", None, set(), today=date(2026, 9, 24)
        )
    rows = {m["key"]: m for m in out["metrics"]}
    assert [w["in_progress"] for w in out["weeks"]] == [False, False, True]
    assert out["benchmark_weeks"] == 2
    assert rows["leads"]["values"] == [10, 30, 1]
    assert rows["leads"]["benchmark"] == 20  # in-progress week excluded
    # Average of weekly ratios (0.5, 0.0), not recomputed from totals (2/8).
    assert rows["show_rate"]["benchmark"] == 0.25
    # Organic hides ad-side and economics rows.
    assert "ad_spend" not in rows and "cac" not in rows and "visitors" not in rows


def test_no_complete_weeks_returns_empty():
    # A month that hasn't started yet has no weeks.
    out = sc.compute_scorecard(None, uuid.uuid4(), date(2026, 10, 1), date(2026, 10, 31), None, None, set(), today=date(2026, 9, 24))
    assert out == {"weeks": [], "benchmark_weeks": 0, "benchmark_source": "range", "metrics": []}


def test_compare_range_sets_the_benchmark():
    weeks = [date(2026, 9, 7), date(2026, 9, 14)]
    prior = [date(2026, 8, 10), date(2026, 8, 17)]
    by_weeks = {
        tuple(weeks): {weeks[0]: sc.WeekBase(leads=10), weeks[1]: sc.WeekBase(leads=30)},
        tuple(prior): {prior[0]: sc.WeekBase(leads=2), prior[1]: sc.WeekBase(leads=4)},
    }
    with patch.object(sc, "collect_funnel_facts", return_value=SimpleNamespace()), patch.object(
        sc, "_week_bases", side_effect=lambda _db, _org, wks, *a, **k: by_weeks[tuple(wks)]
    ):
        out = sc.compute_scorecard(
            None, uuid.uuid4(), date(2026, 9, 7), date(2026, 9, 20), "organic", None, set(),
            today=date(2026, 9, 27), compare=(date(2026, 8, 10), date(2026, 8, 23)),
        )
    leads = next(m for m in out["metrics"] if m["key"] == "leads")
    assert leads["values"] == [10, 30]
    assert leads["benchmark"] == 3  # compare weeks' average, not this range's (20)
    assert (out["benchmark_source"], out["benchmark_weeks"]) == ("compare", 2)


def _call(event_id, *, start, booked=None, cancelled=False, completed=False, no_show=False, provider="calcom"):
    return SimpleNamespace(
        id=uuid.uuid4(),
        event_id=event_id,
        provider=provider,
        is_sales_call=True,
        start_time=start,
        created_at=datetime(2026, 9, 30, tzinfo=timezone.utc),  # backfill: synced after the call
        raw_event_data=json.dumps({"createdAt": booked.isoformat()}) if booked else None,
        cancelled=cancelled,
        completed=completed,
        no_show=no_show,
    )


def test_shared_counter_is_the_one_definition_for_both_tabs():
    wk_start = datetime(2026, 9, 7, tzinfo=timezone.utc)
    wk_end = datetime(2026, 9, 13, 23, 59, 59, tzinfo=timezone.utc)
    booked_in_week = datetime(2026, 9, 8, tzinfo=timezone.utc)
    calls = [
        # booked this week for next week: Booked Calls yes, Calls on Calendar no
        _call("a", start=datetime(2026, 9, 16, tzinfo=timezone.utc), booked=booked_in_week),
        # held this week and attended, booked the week before
        _call("b", start=datetime(2026, 9, 9, tzinfo=timezone.utc), booked=datetime(2026, 9, 1, tzinfo=timezone.utc), completed=True),
        # duplicate row of the same calendar event counts once
        _call("b", start=datetime(2026, 9, 9, tzinfo=timezone.utc), booked=datetime(2026, 9, 1, tzinfo=timezone.utc), completed=True),
        # no-show this week
        _call("c", start=datetime(2026, 9, 10, tzinfo=timezone.utc), booked=booked_in_week, no_show=True),
        # cancelled: excluded from booked and on-calendar
        _call("d", start=datetime(2026, 9, 11, tzinfo=timezone.utc), booked=booked_in_week, cancelled=True),
        # no provider timestamp + row synced after the call: booked date falls back to the call time
        _call("e", start=datetime(2026, 9, 12, tzinfo=timezone.utc)),
    ]
    counts = count_sales_calls(calls, wk_start, wk_end)
    assert counts == {
        "calls_booked_activity": 3,  # a, c, e
        "calls_booked": 3,  # b, c, e
        "calls_taken": 1,  # b (deduped)
        "no_shows": 1,  # c
    }

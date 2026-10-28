"""Funnels dashboard + weekly ad spend (PRD phase 8)."""
import uuid
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from app.services import funnel_dashboard as fd
from app.services.kpi_integration_sync import FunnelFacts

WS, WE = date(2026, 9, 1), date(2026, 9, 21)  # Tue Sep 1 .. Mon Sep 21


def _facts(*, opt_ins=None, booked=None, showed=None, closed=None, cash=None, outreach=None, organic=False):
    return FunnelFacts(
        window_start=WS,
        window_end=WE,
        opt_ins=opt_ins or {},
        booked=booked or {},
        showed=showed or {},
        closed=closed or {},
        cash=cash or [],
        outreach_rows=outreach or [],
        include_organic_outreach=organic,
    )


def _at(d: date) -> datetime:
    return datetime(d.year, d.month, d.day, 12, tzinfo=timezone.utc)


def test_week_start_is_monday():
    assert fd.week_start_for(date(2026, 9, 24)) == date(2026, 9, 21)  # Thu -> Mon
    assert fd.week_start_for(date(2026, 9, 21)) == date(2026, 9, 21)
    assert fd.week_start_for(date(2026, 9, 27)) == date(2026, 9, 21)  # Sun -> Mon


def test_ratio_zero_denominator_is_none():
    assert fd._ratio(100, 0) is None
    assert fd._ratio(100, 4) == 25.0


def test_upsert_zero_clears_existing_row():
    row = SimpleNamespace(amount_cents=5000)
    db = MagicMock()
    db.query.return_value.filter.return_value.first.return_value = row
    assert fd.upsert_ad_spend(db, uuid.uuid4(), uuid.uuid4(), date(2026, 9, 24), 0, None) is None
    db.delete.assert_called_once_with(row)


def test_upsert_creates_row_on_monday():
    db = MagicMock()
    db.query.return_value.filter.return_value.first.return_value = None
    org, funnel = uuid.uuid4(), uuid.uuid4()
    row = fd.upsert_ad_spend(db, org, funnel, date(2026, 9, 24), 12_345, None)
    assert row.week_start == date(2026, 9, 21)
    assert row.amount_cents == 12_345
    assert row.funnel_id == funnel
    db.add.assert_called_once()


def test_weekly_from_scorecard_maps_override_applied_values_by_week():
    """The graph's weekly buckets are a pure mapping over whatever weekly_metric_values
    returns — i.e. the same override-applied numbers the scorecard grid renders, not a
    second independent pass over raw facts."""
    weeks = [date(2026, 9, 7), date(2026, 9, 14), date(2026, 9, 21)]
    values = {
        "ad_spend": [None, 1000.0, None],
        "cash_collected": [0.0, 3000.0, 0.0],
        "leads": [0, 1, 0],
        "deals_closed": [0, 1, 0],
    }
    out = {w["week_start"]: w for w in fd._weekly_from_scorecard(weeks, values)}
    wk = out[date(2026, 9, 14)]
    assert (wk["opt_ins"], wk["closed"], wk["cash_usd"], wk["spend_usd"], wk["cac_usd"]) == (1, 1, 3000.0, 1000.0, 1000.0)
    # No ad_spend logged that week (None, not 0) → no CAC, not a misleading $0.
    assert out[date(2026, 9, 7)]["cac_usd"] is None
    assert out[date(2026, 9, 7)]["spend_usd"] == 0.0


def test_utm_sources_group_and_rank_no_utm_last():
    fb, ig, none = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    facts = _facts(
        opt_ins={fb: _at(WS), ig: _at(WS), none: _at(WS)},
        closed={fb: WS},
        cash=[(fb, _at(WS), 200_000), (none, _at(WS), 900_000)],
    )
    db = MagicMock()
    db.query.return_value.filter.return_value = [
        (fb, {"prospect": {"utm": {"source": "facebook"}}}),
        (ig, {"prospect": {"utm": {"source": "instagram"}}}),
        (none, {"prospect": {}}),
    ]
    rows = fd._utm_sources(db, uuid.uuid4(), facts)
    assert [r["source"] for r in rows] == ["facebook", "instagram", fd.NO_UTM_SOURCE]
    assert rows[0] == {"source": "facebook", "opt_ins": 1, "booked": 0, "closed": 1, "cash_usd": 2000.0}


def _run_dashboard(channel, funnel_id=None, *, facts, paid_facts=None, spend_rows=()):
    db = MagicMock()
    db.query.return_value.filter.return_value.all.return_value = [(funnel_id or uuid.uuid4(),)]
    calls = []

    def fake_collect(_db, _org, _ws, _we, ch, fid):
        calls.append((ch, fid))
        return paid_facts if (ch == "paid" and paid_facts is not None and fid is None and channel is None) else facts

    def fake_weekly_values(_db, _org, weeks, ch, fid, _scope, *, include_ads):
        # Same dispatch as fake_collect: the "all channel" dashboard view's money row
        # re-fetches the paid-only subset (channel "paid") itself, mirroring
        # compute_scorecard's own econ_bases branch instead of the removed money_facts path.
        f = paid_facts if (ch == "paid" and paid_facts is not None and fid is None and channel is None) else facts
        n = len(weeks)

        def _col(total):
            return ([total] + [0] * (n - 1)) if n else []

        leads = len(f.opt_ins) + sum(r for _d, _o, r in f.outreach_rows)
        spend = sum(r.amount_cents or 0 for r in spend_rows) / 100.0
        return {
            "leads": _col(leads),
            "booked_calls": _col(0),
            "live_calls": _col(0),
            "deals_closed": _col(len(f.closed)),
            "visitors": _col(400) if include_ads else [None] * n,
            "ad_spend": _col(spend) if spend else [None] * n,
            "cash_collected": _col(sum(c for _cid, _ts, c in f.cash) / 100.0),
        }, {}

    with patch.object(fd, "collect_funnel_facts", side_effect=fake_collect), patch.object(
        fd, "_tracking_status", return_value={"status": "live", "last_event_at": None, "errors_24h": 0}
    ), patch.object(fd, "weekly_metric_values", side_effect=fake_weekly_values), patch.object(
        fd, "_utm_sources", return_value=[]
    ), patch.object(
        fd, "compute_scorecard", return_value={"weeks": [], "benchmark_weeks": 0, "metrics": []}
    ):
        out = fd.compute_funnel_dashboard(db, uuid.uuid4(), WS, WE, channel, funnel_id)
    return out, calls


def test_money_row_paid_metrics():
    a, b = uuid.uuid4(), uuid.uuid4()
    facts = _facts(
        opt_ins={a: _at(WS), b: _at(WS)},
        closed={a: WS},
        cash=[(a, _at(WS), 400_000)],
    )
    spend = [SimpleNamespace(week_start=date(2026, 8, 31), amount_cents=100_000)]
    out, calls = _run_dashboard("paid", facts=facts, spend_rows=spend)
    m = out["money"]
    assert (m["spend_usd"], m["cpl_usd"], m["cac_usd"], m["roas"], m["profit_usd"]) == (1000.0, 500.0, 1000.0, 4.0, 3000.0)
    assert calls == [("paid", None)]  # paid scope reuses the one facts pass
    assert out["visitors"] == 400


def test_money_row_hidden_for_organic_and_no_visitors():
    out, _ = _run_dashboard("organic", facts=_facts(organic=True))
    assert out["money"] is None
    assert out["visitors"] is None


def test_all_channel_money_uses_paid_subset():
    organic_close, paid_close = uuid.uuid4(), uuid.uuid4()
    all_facts = _facts(
        closed={organic_close: WS, paid_close: WS},
        cash=[(organic_close, _at(WS), 100_000), (paid_close, _at(WS), 300_000)],
    )
    paid_facts = _facts(closed={paid_close: WS}, cash=[(paid_close, _at(WS), 300_000)])
    spend = [SimpleNamespace(week_start=date(2026, 8, 31), amount_cents=150_000)]
    out, calls = _run_dashboard(None, facts=all_facts, paid_facts=paid_facts, spend_rows=spend)
    assert out["summary"]["closed"] == 2  # stage row covers both channels
    assert out["money"]["cac_usd"] == 1500.0  # CAC only over the paid close
    assert out["money"]["roas"] == 2.0
    # The paid-only subset is now re-fetched via weekly_metric_values (asserted above
    # through cac/roas), not a second collect_funnel_facts call like the removed
    # money_facts path — only the top-level "all channel" facts pass shows up here.
    assert calls == [(None, None)]
    # The chart pairs spend against cash the same way the money row does: paid cash
    # (3000), not the all-channel cash_collected (4000) that includes organic.
    assert out["weekly"][0]["cash_usd"] == 3000.0


def test_no_spend_leaves_ratios_empty():
    a = uuid.uuid4()
    out, _ = _run_dashboard("paid", facts=_facts(opt_ins={a: _at(WS)}, closed={a: WS}))
    m = out["money"]
    assert m["has_spend"] is False
    assert (m["cpl_usd"], m["cac_usd"], m["roas"], m["profit_usd"]) == (None, None, None, None)



def test_summarize_vsl_watch_avg_and_play_rate():
    rows = [
        ("s1", "v1", "vsl_play", {"watched_seconds": 10}),
        ("s1", "v1", "vsl_watch_heartbeat", {"watched_seconds": 45}),
        ("s2", "v2", "vsl_play", {"watched_seconds": 5}),
        ("s2", "v2", "vsl_watch_heartbeat", {"watched_seconds": 15}),
    ]
    out = fd.summarize_vsl_watch(rows, visitors=10)
    assert out["sessions"] == 2
    assert out["plays"] == 2
    assert out["avg_seconds"] == 30.0
    assert out["play_rate"] == 0.2


def test_summarize_vsl_watch_caps_play_rate_at_one():
    rows = [("s1", "v1", "video_play", {"watched_seconds": 12})]
    out = fd.summarize_vsl_watch(rows, visitors=1)
    assert out["play_rate"] == 1.0


def test_summarize_vsl_watch_empty_is_none():
    assert fd.summarize_vsl_watch([], visitors=10) is None

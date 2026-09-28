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


def test_weekly_buckets_spend_cash_opt_ins_and_closes():
    a, b = uuid.uuid4(), uuid.uuid4()
    facts = _facts(
        opt_ins={a: _at(date(2026, 9, 2)), b: _at(date(2026, 9, 15))},
        closed={a: date(2026, 9, 16)},
        cash=[(a, _at(date(2026, 9, 16)), 300_000)],
    )
    spend = [
        SimpleNamespace(week_start=date(2026, 8, 31), amount_cents=50_000),
        SimpleNamespace(week_start=date(2026, 9, 14), amount_cents=100_000),
    ]
    weeks = {w["week_start"]: w for w in fd._weekly(WS, WE, facts, facts, spend)}
    assert list(weeks) == [date(2026, 8, 31), date(2026, 9, 7), date(2026, 9, 14), date(2026, 9, 21)]
    assert weeks[date(2026, 8, 31)]["opt_ins"] == 1 and weeks[date(2026, 8, 31)]["spend_usd"] == 500.0
    wk = weeks[date(2026, 9, 14)]
    assert (wk["opt_ins"], wk["closed"], wk["cash_usd"], wk["spend_usd"], wk["cac_usd"]) == (1, 1, 3000.0, 1000.0, 1000.0)
    assert weeks[date(2026, 9, 7)]["cac_usd"] is None


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

    with patch.object(fd, "collect_funnel_facts", side_effect=fake_collect), patch.object(
        fd, "list_ad_spend", return_value=list(spend_rows)
    ), patch.object(fd, "_tracking_status", return_value={"status": "live", "last_event_at": None, "errors_24h": 0}), patch.object(
        fd, "_visitors", return_value=400
    ), patch.object(fd, "_utm_sources", return_value=[]), patch.object(
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
    assert calls == [(None, None), ("paid", None)]


def test_no_spend_leaves_ratios_empty():
    a = uuid.uuid4()
    out, _ = _run_dashboard("paid", facts=_facts(opt_ins={a: _at(WS)}, closed={a: WS}))
    m = out["money"]
    assert m["has_spend"] is False
    assert (m["cpl_usd"], m["cac_usd"], m["roas"], m["profit_usd"]) == (None, None, None, None)

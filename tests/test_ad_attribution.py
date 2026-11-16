"""Per-ad attribution: UTM -> ad resolution, per-ad funnel counts, derived metrics."""
import uuid
from datetime import date, datetime, timezone
from types import SimpleNamespace

from app.services import ad_attribution as aa
from app.services.kpi_integration_sync import FunnelFacts

NOW = datetime(2026, 1, 5, tzinfo=timezone.utc)


def _ad(meta_id, name):
    return SimpleNamespace(meta_ad_id=meta_id, name=name)


class _Q:
    def __init__(self, rows):
        self.rows = rows

    def filter(self, *a, **k):
        return self

    def __iter__(self):
        return iter(self.rows)


class _DB:
    def __init__(self, rows):
        self.rows = rows

    def query(self, *a, **k):
        return _Q(self.rows)


def _facts(**kw):
    base = dict(
        window_start=date(2026, 1, 1), window_end=date(2026, 1, 31),
        opt_ins={}, booked={}, showed={}, closed={}, cash=[], outreach_rows=[],
        include_organic_outreach=False, scope_ids=None,
    )
    base.update(kw)
    return FunnelFacts(**base)


def test_resolver_prefers_id_then_unique_name():
    r = aa.AdResolver([_ad("111", "Hook A"), _ad("222", "Hook B"), _ad("333", "Dup"), _ad("444", "dup ")])
    assert r.resolve({"content": "111"}) == "111"
    assert r.resolve({"term": "222"}) == "222"
    assert r.resolve({"content": "  hook   b "}) == "222"
    assert r.resolve({"content": "Dup"}) is None  # ambiguous name never guesses
    assert r.resolve({"content": "unknown"}) is None
    assert r.resolve({}) is None and r.resolve(None) is None


def test_attribute_ads_splits_counts_and_reports_unattributed(monkeypatch):
    a, b, c, d = (uuid.uuid4() for _ in range(4))
    monkeypatch.setattr(
        aa,
        "collect_funnel_facts",
        lambda *args, **kw: _facts(
            opt_ins={a: NOW, b: NOW, c: NOW},
            booked={a: NOW},
            showed={a: NOW},
            closed={a: date(2026, 1, 9)},
            cash=[(a, NOW, 5000_00), (a, NOW, 1000_00), (d, NOW, 700_00), (None, NOW, 10_00)],
        ),
    )
    db = _DB(
        [
            (a, {"prospect": {"utm": {"content": "111"}}}),
            (b, {"prospect": {"utm": {"content": "Hook B"}}}),
            (c, {"prospect": {"utm": {"content": "nope"}}}),
            (d, None),
        ]
    )
    out = aa.attribute_ads(db, uuid.uuid4(), [_ad("111", "Hook A"), _ad("222", "Hook B")], date(2026, 1, 1), date(2026, 1, 31))
    assert out["by_ad"]["111"] == {"leads": 1, "booked_calls": 1, "live_calls": 1, "deals_closed": 1, "cash_cents": 6000_00}
    assert out["by_ad"]["222"]["leads"] == 1 and out["by_ad"]["222"]["cash_cents"] == 0
    assert out["coverage"] == {
        "attributed_leads": 2,
        "total_paid_leads": 3,
        "attributed_cash_cents": 6000_00,
        "unattributed_cash_cents": 710_00,
    }


def test_ad_metrics_derived_values_and_zero_guards():
    m = aa.ad_metrics({"leads": 10, "booked_calls": 5, "live_calls": 4, "deals_closed": 2, "cash_cents": 8000_00}, 2000_00)
    assert m["lead_to_book_pct"] == 50.0 and m["show_rate_pct"] == 80.0
    assert m["cost_per_lead_cents"] == 200_00 and m["cost_per_booking_cents"] == 400_00 and m["cac_cents"] == 1000_00
    assert m["cash_per_close_cents"] == 4000_00 and m["roas"] == 4.0 and m["profit_cents"] == 6000_00
    z = aa.ad_metrics(None, 0)
    assert z["leads"] == 0 and z["cash_cents"] == 0
    assert z["roas"] is None and z["profit_cents"] is None and z["cost_per_lead_cents"] is None
    assert z["lead_to_book_pct"] is None and z["cash_per_close_cents"] is None
    # no spend: costs undefined, but cash still shown
    assert aa.ad_metrics({"leads": 3, "booked_calls": 0, "live_calls": 0, "deals_closed": 1, "cash_cents": 500_00}, 0)["cac_cents"] is None


def test_attach_attribution_failure_leaves_rows_usable(monkeypatch):
    def boom(*a, **k):
        raise RuntimeError("db down")

    monkeypatch.setattr(aa, "attribute_ads", boom)
    rows = [{"meta_ad_id": "111", "spend_cents": 100}]
    assert aa.attach_attribution(None, uuid.uuid4(), [], rows, None, date(2026, 1, 31)) is None
    assert rows[0]["attribution"] is None


def test_attach_attribution_open_ended_start(monkeypatch):
    seen = {}

    def fake(db, org, ads, start, end, funnel_id=None):
        seen["start"] = start
        return {"by_ad": {"111": {"leads": 2, "booked_calls": 0, "live_calls": 0, "deals_closed": 0, "cash_cents": 0}}, "coverage": {"x": 1}}

    monkeypatch.setattr(aa, "attribute_ads", fake)
    rows = [{"meta_ad_id": "111", "spend_cents": 1000}, {"meta_ad_id": "999", "spend_cents": 0}]
    assert aa.attach_attribution(None, uuid.uuid4(), [], rows, None, date(2026, 1, 31)) == {"x": 1}
    assert rows[0]["attribution"]["leads"] == 2 and rows[1]["attribution"]["leads"] == 0

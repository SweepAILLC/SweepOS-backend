"""dialed_at update parsing, sale_closed_at stamping, and first-time-only AOV events."""
import uuid
from datetime import datetime, timezone
from types import SimpleNamespace

from app.api import finances
from app.schemas.client import ClientUpdate
from app.services.sale_close_stamp import stamp_sale_closed


def test_dialed_at_accepts_iso_z_and_stores_naive_utc():
    upd = ClientUpdate(dialed_at="2026-11-09T12:30:00.000Z")
    assert upd.dialed_at == datetime(2026, 11, 9, 12, 30)
    assert upd.dialed_at.tzinfo is None


def test_dialed_at_explicit_null_is_set_and_clears():
    upd = ClientUpdate(dialed_at=None)
    assert "dialed_at" in upd.model_dump(exclude_unset=True)
    assert upd.dialed_at is None


def test_dialed_at_omitted_is_not_in_update():
    assert "dialed_at" not in ClientUpdate(instagram="x").model_dump(exclude_unset=True)


def test_stamp_sale_closed_sets_once_and_clears():
    row = SimpleNamespace(sale_closed=None, sale_closed_at=None)
    stamp_sale_closed(row, True)
    first = row.sale_closed_at
    assert row.sale_closed is True and first is not None
    stamp_sale_closed(row, True)
    assert row.sale_closed_at == first
    stamp_sale_closed(row, False)
    assert row.sale_closed is False and row.sale_closed_at is None
    stamp_sale_closed(row, None)
    assert row.sale_closed_at is None


def _patch_sources(monkeypatch, stripe=(), whop=(), manual=()):
    monkeypatch.setattr(finances, "_stripe_succeeded_all", lambda db, o: list(stripe))
    monkeypatch.setattr(finances, "_whop_paid_all", lambda db, o: list(whop))
    monkeypatch.setattr(finances, "_manual_all", lambda db, o: list(manual))


def _stripe(client_id, day, cents):
    return SimpleNamespace(client_id=client_id, created_at=datetime(2026, 1, day, tzinfo=timezone.utc), amount_cents=cents)


def test_first_time_events_keep_earliest_per_client_across_sources(monkeypatch):
    c1, c2 = uuid.uuid4(), uuid.uuid4()
    _patch_sources(
        monkeypatch,
        stripe=[_stripe(c1, 10, 500_00), _stripe(c1, 2, 100_00), _stripe(c2, 5, 200_00)],
        whop=[SimpleNamespace(client_id=c2, created_at=datetime(2026, 1, 1), amount_cents=50_00)],
        manual=[SimpleNamespace(client_id=c1, payment_date=datetime(2026, 1, 20), created_at=None, amount_cents=900_00)],
    )
    events = finances._first_time_payment_events(None, uuid.uuid4())
    assert sorted(a for _, a in events) == [50_00, 100_00]


def test_first_time_events_count_each_unmatched_payment(monkeypatch):
    _patch_sources(monkeypatch, stripe=[_stripe(None, 3, 10_00), _stripe(None, 4, 20_00)])
    assert len(finances._first_time_payment_events(None, uuid.uuid4())) == 2


def test_first_time_value_window_excludes_renewals(monkeypatch):
    c1 = uuid.uuid4()
    _patch_sources(monkeypatch, stripe=[_stripe(c1, 2, 100_00), _stripe(c1, 20, 100_00)])
    cents, n = finances._first_time_order_value_since(
        None, uuid.uuid4(), datetime(2026, 1, 15), datetime(2026, 1, 31)
    )
    assert (cents, n) == (0, 0)
    cents, n = finances._first_time_order_value_since(
        None, uuid.uuid4(), datetime(2026, 1, 1), datetime(2026, 1, 31)
    )
    assert (cents, n) == (100_00, 1)

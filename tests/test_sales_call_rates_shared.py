"""compute_sales_call_rates_for_window: the shared close-rate/show-up-rate
engine that replaces admin.py's two org-scoped helpers and
calendar_trend_summary.py's inline math (phase 5 of the sales-pipeline
attribution PRD)."""
import uuid
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

from app.services.kpi_integration_sync import compute_sales_call_rates_for_window


def _query_mock(
    *, checkins, all_checkins=None, stripe_rows=(), whop_rows=(), manual_rows=(), activity_rows=()
):
    """First db.query(ClientCheckIn) call is the windowed query, second is the
    unfiltered all-org query feeding conversion_dates_by_client — mirrors the
    two real, differently-filtered calls compute_sales_call_rates_for_window
    makes against the same model."""
    checkin_calls = iter([checkins, all_checkins if all_checkins is not None else checkins])

    def query_side_effect(*entities):
        q = MagicMock()
        first = entities[0]
        owner = getattr(first, "class_", first)
        name = getattr(owner, "__name__", "")
        if name == "ClientCheckIn":
            q.filter.return_value.all.return_value = next(checkin_calls, checkins)
        elif name == "StripePayment":
            q.filter.return_value.all.return_value = stripe_rows
        elif name == "WhopPayment":
            q.filter.return_value.all.return_value = whop_rows
        elif name == "ManualPayment":
            q.filter.return_value.all.return_value = manual_rows
        elif name == "SalesActivityEvent":
            q.filter.return_value.all.return_value = activity_rows
        else:
            q.filter.return_value.all.return_value = []
        return q

    db = MagicMock()
    db.query.side_effect = query_side_effect
    return db


def _checkin(client_id, *, start_time, completed, no_show, is_sales_call=True, sale_closed=False):
    return SimpleNamespace(
        client_id=client_id,
        start_time=start_time,
        completed=completed,
        no_show=no_show,
        is_sales_call=is_sales_call,
        sale_closed=sale_closed,
    )


class TestComputeSalesCallRatesForWindow:
    def test_empty_window_returns_none_rates(self):
        db = _query_mock(checkins=[])
        org_id = uuid.uuid4()
        window_start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        window_end = datetime(2026, 2, 1, tzinfo=timezone.utc)
        result = compute_sales_call_rates_for_window(db, org_id, window_start, window_end)
        assert result["sales_calls_booked"] == 0
        assert result["show_up_rate_pct"] is None
        assert result["close_rate_pct"] is None

    def test_show_up_rate_excludes_no_shows_from_taken_not_booked(self):
        org_id = uuid.uuid4()
        window_start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        window_end = datetime(2026, 2, 1, tzinfo=timezone.utc)
        c1, c2 = uuid.uuid4(), uuid.uuid4()
        windowed = [
            _checkin(c1, start_time=window_start + timedelta(days=1), completed=True, no_show=False),
            _checkin(c2, start_time=window_start + timedelta(days=2), completed=True, no_show=True),
        ]
        db = _query_mock(checkins=windowed)
        result = compute_sales_call_rates_for_window(db, org_id, window_start, window_end)
        assert result["sales_calls_booked"] == 2
        assert result["sales_calls_taken"] == 1
        assert result["show_up_rate_pct"] == 50.0

    def test_close_rate_denominator_is_calls_taken_not_booked(self):
        org_id = uuid.uuid4()
        window_start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        window_end = datetime(2026, 2, 1, tzinfo=timezone.utc)
        c1, c2 = uuid.uuid4(), uuid.uuid4()
        windowed = [
            _checkin(c1, start_time=window_start + timedelta(days=1), completed=True, no_show=False),
            _checkin(c2, start_time=window_start + timedelta(days=2), completed=True, no_show=True),
        ]
        stripe_rows = [
            SimpleNamespace(client_id=c1, created_at=window_start + timedelta(days=3))
        ]
        db = _query_mock(checkins=windowed, stripe_rows=stripe_rows)
        result = compute_sales_call_rates_for_window(db, org_id, window_start, window_end)
        # taken_total=1 (c1 only), closed_count=1 (c1 paid in window) -> 100%, not 50%
        assert result["sales_calls_taken"] == 1
        assert result["closed_count"] == 1
        assert result["close_rate_pct"] == 100.0

    def test_conversion_resolved_from_unfiltered_all_org_checkins(self):
        """conversion_dates_by_client must see ALL org checkins (sale_closed
        signal), not just the ones in the requested window — same precedent
        as refresh_kpi_live_fields_for_range."""
        org_id = uuid.uuid4()
        window_start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        window_end = datetime(2026, 2, 1, tzinfo=timezone.utc)
        c1 = uuid.uuid4()
        taken = _checkin(c1, start_time=window_start + timedelta(days=1), completed=True, no_show=False)
        taken.sale_closed = True
        db = _query_mock(checkins=[taken])
        result = compute_sales_call_rates_for_window(db, org_id, window_start, window_end)
        assert result["closed_count"] == 1
        assert result["close_rate_pct"] == 100.0

    def test_now_utc_clips_booked_taken_but_not_conversions(self):
        """A payment made 'today' still counts toward closed_count even
        though today's not-yet-happened calls are excluded from booked/taken."""
        org_id = uuid.uuid4()
        window_start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        window_end = datetime(2026, 2, 1, tzinfo=timezone.utc)
        now_utc = datetime(2026, 1, 15, 10, 0, tzinfo=timezone.utc)
        c1 = uuid.uuid4()
        future_call = _checkin(c1, start_time=now_utc + timedelta(days=1), completed=False, no_show=False)
        stripe_rows = [SimpleNamespace(client_id=c1, created_at=now_utc)]
        db = _query_mock(checkins=[], all_checkins=[future_call], stripe_rows=stripe_rows)
        result = compute_sales_call_rates_for_window(
            db, org_id, window_start, window_end, now_utc=now_utc
        )
        assert result["sales_calls_booked"] == 0
        assert result["closed_count"] == 1

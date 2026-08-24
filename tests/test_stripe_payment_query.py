"""Failed-payment date window stays on latest_attempt_at."""
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from app.api.stripe import _filter_failed_payments_by_window, _payments_window_from_params


def test_failed_window_keeps_in_range_only():
    now = datetime.now(timezone.utc)
    in_row = SimpleNamespace(latest_attempt_at=int(now.timestamp()) - 60, created_at=0)
    old = SimpleNamespace(latest_attempt_at=int((now - timedelta(days=40)).timestamp()), created_at=0)
    out = _filter_failed_payments_by_window([in_row, old], None, 30)
    assert out == [in_row]


def test_all_scope_skips_window_filter():
    row = SimpleNamespace(latest_attempt_at=1, created_at=1)
    assert _filter_failed_payments_by_window([row], None, None) == [row]


def test_payments_window_none_without_range():
    assert _payments_window_from_params(None, None) == (None, None)

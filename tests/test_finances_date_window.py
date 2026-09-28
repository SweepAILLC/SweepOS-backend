"""Finances summary follows the shared date-range filter (start/end/compare_*)."""
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from app.api import finances


def _call(**params):
    windows = []

    def record(_db, _org, since, until):
        windows.append((since, until))
        return 0

    db = MagicMock()
    db.query.return_value.filter.return_value.scalar.return_value = "UTC"  # org timezone
    user = SimpleNamespace(org_id="org", selected_org_id="org")
    with patch.object(finances, "check_stripe_connected", return_value=False), patch.object(
        finances, "_whop_connected", return_value=False
    ), patch.object(finances, "_manual_cents_since", side_effect=record), patch.object(
        finances, "_manual_order_count_since", return_value=0
    ):
        out = finances.finances_summary(range_days=30, scope=None, db=db, current_user=user, **params)
    return out, windows


def test_explicit_range_and_previous_period():
    out, windows = _call(start="2026-09-01", end="2026-09-10", compare_start=None, compare_end=None)
    # calls: mtd, primary, prior
    assert windows[1] == (datetime(2026, 9, 1), datetime(2026, 9, 11))
    assert windows[2] == (datetime(2026, 8, 22), datetime(2026, 9, 1))  # same 10 days before
    assert out.prior_period_revenue == 0


def test_custom_compare_and_all_time():
    _out, windows = _call(start="2026-09-01", end="2026-09-10", compare_start="2025-09-01", compare_end="2025-09-10")
    assert windows[2] == (datetime(2025, 9, 1), datetime(2025, 9, 11))
    out, windows = _call(start=None, end="2026-09-10", compare_start=None, compare_end=None)
    assert len(windows) == 2 and out.prior_period_revenue is None  # all history: no prior

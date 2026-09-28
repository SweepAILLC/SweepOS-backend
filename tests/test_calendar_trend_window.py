"""Calendar trend summary follows an explicit date-range window."""
from datetime import datetime, timezone
from unittest.mock import patch

from app.services import calendar_trend_summary as cts


def _run(window, now):
    seen = {}

    def count(_db, _org, start, end, *, upcoming, now_utc):
        seen["upcoming" if upcoming else "past"] = (start, end)
        return 0

    def rates(_db, _org, start, end, now_utc):
        seen["rates"] = (start, end)
        return {"close_rate_pct": None, "sales_calls_booked": 0, "closed_count": 0, "show_up_rate_pct": None, "sales_calls_taken": 0}

    with patch.object(cts, "_count_meetings_in_window", side_effect=count), patch(
        "app.services.kpi_integration_sync.compute_sales_call_rates_for_window", side_effect=rates
    ):
        cts.compute_calendar_trend_summary(None, "org", window=window, now=now)
    return seen


def test_past_range_has_no_upcoming_part():
    now = datetime(2026, 9, 27, 12, tzinfo=timezone.utc)
    seen = _run((datetime(2026, 8, 1), datetime(2026, 9, 1)), now)
    utc = timezone.utc
    assert seen["past"] == (datetime(2026, 8, 1, tzinfo=utc), datetime(2026, 9, 1, tzinfo=utc))
    assert seen["rates"] == seen["past"]
    assert seen["upcoming"][0] == seen["upcoming"][1] == now  # empty


def test_range_spanning_now_splits_at_now():
    now = datetime(2026, 9, 27, 12, tzinfo=timezone.utc)
    seen = _run((datetime(2026, 9, 1), datetime(2026, 10, 1)), now)
    assert seen["past"][1] == now and seen["upcoming"] == (now, datetime(2026, 10, 1, tzinfo=timezone.utc))

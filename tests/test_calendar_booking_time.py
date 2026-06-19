"""Tests for calendar time helpers and Cal.com fetch configuration."""
from datetime import datetime, timezone, timedelta

from app.services.calendar_booking_time import format_calendly_api_time, format_cal_api_time
from app.services.calcom_bookings_client import _status_date_filters, CALCOM_BOOKING_STATUSES


def test_format_calendly_api_time_uses_microseconds():
    dt = datetime(2024, 2, 7, 23, 30, 0, tzinfo=timezone.utc)
    assert format_calendly_api_time(dt) == "2024-02-07T23:30:00.000000Z"


def test_format_cal_api_time_uses_milliseconds():
    dt = datetime(2024, 2, 7, 23, 30, 0, 500000, tzinfo=timezone.utc)
    assert format_cal_api_time(dt) == "2024-02-07T23:30:00.500Z"


def test_upcoming_status_uses_future_window():
    now = datetime(2025, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
    filters = _status_date_filters("upcoming", now=now, lookback_days=365, lookahead_days=365)
    assert filters["before_end"] == now + timedelta(days=365)
    assert filters["after_start"] == now - timedelta(hours=2)


def test_calcom_status_list_includes_recurring():
    assert "recurring" in CALCOM_BOOKING_STATUSES

"""Accuracy tests for calendar upcoming/past classification across API → DB → UI."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from app.services.calendar_booking_time import (
    booking_boundary_from_iso,
    check_in_is_past,
    check_in_is_upcoming,
    classify_booking_window,
)


def _row(start: str, end: str | None = None):
    return SimpleNamespace(
        start_time=datetime.fromisoformat(start.replace("Z", "+00:00")),
        end_time=datetime.fromisoformat(end.replace("Z", "+00:00")) if end else None,
        cancelled=False,
        no_show=False,
    )


def test_in_progress_meeting_is_upcoming():
    """Meeting started 30m ago, ends in 30m — still upcoming."""
    now = datetime(2025, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
    ci = _row("2025-06-01T11:30:00Z", "2025-06-01T12:30:00Z")
    assert check_in_is_upcoming(ci, now) is True
    assert check_in_is_past(ci, now) is False
    assert classify_booking_window("2025-06-01T11:30:00Z", "2025-06-01T12:30:00Z", now=now) == "upcoming"


def test_ended_meeting_is_past():
    now = datetime(2025, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
    ci = _row("2025-06-01T10:00:00Z", "2025-06-01T11:00:00Z")
    assert check_in_is_past(ci, now) is True
    assert check_in_is_upcoming(ci, now) is False


def test_no_end_time_uses_start_boundary():
    now = datetime(2025, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
    ci = _row("2025-06-01T13:00:00Z", None)
    assert check_in_is_upcoming(ci, now) is True
    assert booking_boundary_from_iso("2025-06-01T13:00:00Z", None) == datetime(
        2025, 6, 1, 13, 0, 0, tzinfo=timezone.utc
    )


def test_boundary_exactly_now_is_upcoming():
    now = datetime(2025, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
    assert classify_booking_window("2025-06-01T11:00:00Z", "2025-06-01T12:00:00Z", now=now) == "upcoming"


def test_future_start_only_classified_upcoming():
    now = datetime(2025, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
    future = (now + timedelta(days=3)).strftime("%Y-%m-%dT%H:%M:%SZ")
    assert classify_booking_window(future, None, now=now) == "upcoming"


def test_past_start_only_classified_past():
    now = datetime(2025, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
    past = (now - timedelta(days=3)).strftime("%Y-%m-%dT%H:%M:%SZ")
    assert classify_booking_window(past, None, now=now) == "past"

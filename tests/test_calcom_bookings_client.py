"""Tests for Cal.com booking fetch helpers."""
from app.services.calcom_bookings_client import extract_calcom_attendees, _booking_uid


def test_booking_uid_prefers_uid():
    assert _booking_uid({"uid": "abc-123", "id": 99}) == "abc-123"


def test_extract_attendees_from_attendees_list():
    booking = {
        "attendees": [{"email": "guest@example.com", "name": "Guest User"}],
    }
    rows = extract_calcom_attendees(booking)
    assert len(rows) == 1
    assert rows[0]["email"] == "guest@example.com"


def test_extract_attendees_from_guests_and_responses():
    booking = {
        "guests": ["other@example.com"],
        "bookingFieldsResponses": {"email": "form@example.com", "name": "Form User"},
    }
    rows = extract_calcom_attendees(booking)
    emails = {r["email"] for r in rows}
    assert "other@example.com" in emails
    assert "form@example.com" in emails


def test_extract_attendees_dedupes():
    booking = {
        "attendees": [{"email": "dup@example.com"}],
        "guests": ["dup@example.com"],
    }
    rows = extract_calcom_attendees(booking)
    assert len(rows) == 1

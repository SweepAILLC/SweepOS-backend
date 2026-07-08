"""Tests for deterministic Fathom-derived Call Library labels."""
from datetime import datetime, timezone

from app.services.fathom_call_labels import (
    derive_call_library_title,
    fathom_meeting_title_from_payload,
    primary_external_attendee_label,
)


def test_fathom_meeting_title_prefers_meeting_title():
    assert fathom_meeting_title_from_payload(
        {"meeting_title": "QBR 2025", "title": "Other"}
    ) == "QBR 2025"


def test_fathom_meeting_title_falls_back_to_title():
    assert fathom_meeting_title_from_payload({"title": "Sales call"}) == "Sales call"


def test_derive_call_library_title_uses_meeting_title():
    title = derive_call_library_title(
        meeting_title="Pro Client Consultation between Sweep and Shai",
        attendees_json=[
            {"email": "shai@example.com", "name": "Shai", "is_team_member": False},
        ],
        meeting_at=datetime(2026, 6, 19, tzinfo=timezone.utc),
        recording_id=123,
    )
    assert title == "Pro Client Consultation between Sweep and Shai"


def test_derive_call_library_title_uses_external_attendees():
    title = derive_call_library_title(
        meeting_title=None,
        attendees_json=[
            {"email": "rep@sweep.com", "name": "Rep", "is_team_member": True},
            {"email": "shai@example.com", "name": "Shai", "is_team_member": False},
            {"email": "alex@client.com", "name": "Alex", "is_team_member": False},
        ],
        meeting_at=None,
        recording_id=456,
    )
    assert title == "Shai · Alex"


def test_primary_external_attendee_label_skips_team():
    label = primary_external_attendee_label(
        [
            {"email": "rep@sweep.com", "name": "Rep", "is_team_member": True},
            {"email": "shai@example.com", "name": "Shai", "is_team_member": False},
        ]
    )
    assert label == "Shai"


def test_derive_call_library_title_truncates_long_meeting_title():
    long_title = "A" * 600
    title = derive_call_library_title(
        meeting_title=long_title,
        attendees_json=None,
        meeting_at=None,
        recording_id=1,
    )
    assert len(title) == 500


def test_external_attendees_skips_rows_without_email():
    from app.services.fathom_call_labels import external_attendees_from_json

    rows = external_attendees_from_json(
        [
            {"name": "No Email", "is_team_member": False},
            {"email": "ok@client.com", "name": "Ok", "is_team_member": False},
        ]
    )
    assert len(rows) == 1
    assert rows[0]["email"] == "ok@client.com"

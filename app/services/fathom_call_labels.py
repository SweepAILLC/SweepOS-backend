"""Deterministic Call Library labels from Fathom meeting payloads (not CRM client links)."""
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional


def fathom_meeting_title_from_payload(meeting: Dict[str, Any]) -> Optional[str]:
    """Calendar / Fathom meeting title, in API field priority order."""
    for key in ("meeting_title", "title"):
        v = meeting.get(key)
        if isinstance(v, str) and v.strip():
            return v.strip()[:500]
    return None


def external_attendees_from_json(
    attendees_json: Optional[List[Dict[str, Any]]],
) -> List[Dict[str, Any]]:
    """External attendees in stored order (calendar invitees first, then transcript)."""
    out: List[Dict[str, Any]] = []
    for row in attendees_json or []:
        if not isinstance(row, dict):
            continue
        if row.get("is_team_member"):
            continue
        email = str(row.get("email") or "").strip()
        if not email:
            continue
        out.append(row)
    return out


def attendee_display_label(attendee: Dict[str, Any]) -> str:
    name = str(attendee.get("name") or "").strip()
    email = str(attendee.get("email") or "").strip()
    return name or email


def primary_external_attendee_label(
    attendees_json: Optional[List[Dict[str, Any]]],
) -> Optional[str]:
    """First external attendee name/email from Fathom-derived attendees_json."""
    externals = external_attendees_from_json(attendees_json)
    if not externals:
        return None
    label = attendee_display_label(externals[0])
    return label[:200] if label else None


def derive_call_library_title(
    *,
    meeting_title: Optional[str],
    attendees_json: Optional[List[Dict[str, Any]]],
    meeting_at: Optional[datetime],
    recording_id: Optional[int],
) -> str:
    """
    Stable display title for a Call Library row.

    Priority:
    1. Fathom calendar/meeting title (meeting_title / title)
    2. External invitee names/emails from Fathom attendees_json (calendar order)
    3. Meeting date or recording id fallback
    """
    mt = (meeting_title or "").strip()
    if mt:
        return mt[:500]

    externals = external_attendees_from_json(attendees_json)
    if externals:
        labels = [attendee_display_label(a) for a in externals]
        labels = [x for x in labels if x]
        if labels:
            return " · ".join(labels)[:500]

    if meeting_at:
        return f"Call on {meeting_at.strftime('%B %d, %Y')}"
    if recording_id is not None:
        return f"Call #{recording_id}"
    return "Call"

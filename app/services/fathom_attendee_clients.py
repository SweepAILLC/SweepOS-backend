"""Resolve Fathom meeting attendees → Client rows; pick primary prospect for the recording."""
from __future__ import annotations

import re
import uuid
from typing import Any, Dict, List, Optional, Set, Tuple

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.models.client import Client, LifecycleState, find_client_by_email
from app.models.user import User
from app.models.user_organization import UserOrganization


def _norm_email(e: Optional[str]) -> Optional[str]:
    if not e or not isinstance(e, str):
        return None
    s = re.sub(r"\s+", "", e.lower().strip())
    return s or None


def _client_display_name(c: Client) -> str:
    parts = [p for p in (c.first_name, c.last_name) if p and str(p).strip()]
    if parts:
        return " ".join(parts)
    if c.email:
        return c.email.split("@")[0].replace(".", " ").title()
    return "Client"


def get_org_internal_emails(db: Session, org_id: uuid.UUID) -> Set[str]:
    """Emails that belong to org members (sales / team) — not prospects."""
    # Only load `email` — never full User rows. Loading `role` into the session can cause a flush
    # of stray lowercase role strings against PostgreSQL `userrole` when Client rows are inserted
    # in the same transaction (sync path).
    out: Set[str] = set()
    for (em,) in db.query(User.email).filter(User.org_id == org_id).all():
        ne = _norm_email(em)
        if ne:
            out.add(ne)
    for (em,) in (
        db.query(User.email)
        .join(UserOrganization, UserOrganization.user_id == User.id)
        .filter(UserOrganization.org_id == org_id)
        .all()
    ):
        ne = _norm_email(em)
        if ne:
            out.add(ne)
    return out


def extract_attendee_payloads(meeting: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Ordered list of {email, name, source} from calendar invitees and transcript speakers."""
    seen: Set[str] = set()
    out: List[Dict[str, Any]] = []

    for inv in meeting.get("calendar_invitees") or []:
        if not isinstance(inv, dict):
            continue
        raw = inv.get("email")
        ne = _norm_email(str(raw) if raw else None)
        if not ne or ne in seen:
            continue
        seen.add(ne)
        name = str(inv.get("name") or inv.get("display_name") or "").strip() or None
        out.append({"email": ne, "name": name, "source": "calendar_invitee"})

    for block in meeting.get("transcript") or []:
        if not isinstance(block, dict):
            continue
        sp = block.get("speaker") or {}
        if not isinstance(sp, dict):
            continue
        raw = sp.get("matched_calendar_invitee_email") or sp.get("email")
        ne = _norm_email(str(raw) if raw else None)
        if not ne or ne in seen:
            continue
        seen.add(ne)
        name = str(sp.get("display_name") or "").strip() or None
        out.append({"email": ne, "name": name, "source": "transcript"})

    return out


def infer_recording_url(meeting: Dict[str, Any], recording_id: int) -> str:
    for key in ("recording_url", "url", "share_url", "playback_url", "fathom_url"):
        v = meeting.get(key)
        if isinstance(v, str) and v.strip().startswith("http"):
            return v.strip()[:2000]
    return f"https://app.usefathom.com/recording/{recording_id}"


def ensure_client_for_email(
    db: Session,
    org_id: uuid.UUID,
    email: str,
    display_name: Optional[str],
) -> Client:
    """Find or create a client card for this email (cold lead)."""
    existing = find_client_by_email(db, org_id, email)
    if existing:
        return existing
    first = last = None
    if display_name:
        parts = display_name.strip().split(None, 1)
        first = parts[0][:80] if parts else None
        last = parts[1][:80] if len(parts) > 1 else None
    try:
        with db.begin_nested():
            c = Client(
                id=uuid.uuid4(),
                org_id=org_id,
                email=_norm_email(email),
                first_name=first,
                last_name=last,
                lifecycle_state=LifecycleState.COLD_LEAD,
            )
            db.add(c)
            db.flush()
        return c
    except IntegrityError:
        existing = find_client_by_email(db, org_id, email)
        if existing:
            return existing
        raise


def resolve_clients_for_meeting(
    db: Session,
    org_id: uuid.UUID,
    meeting: Dict[str, Any],
    *,
    create_missing_clients: bool = False,
) -> Tuple[Optional[uuid.UUID], List[Dict[str, Any]], List[str], str]:
    """
    Ensure Client rows for external attendees; pick primary prospect client_id.

    Returns (primary_client_id, attendees_json_for_storage, related_client_id_strs, recording_url).
    """
    rid = meeting.get("recording_id")
    try:
        rid_int = int(rid) if rid is not None else 0
    except (TypeError, ValueError):
        rid_int = 0

    internal = get_org_internal_emails(db, org_id)
    payloads = extract_attendee_payloads(meeting)
    recording_url = infer_recording_url(meeting, rid_int) if rid_int else ""

    stored_attendees: List[Dict[str, Any]] = []
    external_client_ids_ordered: List[uuid.UUID] = []
    seen_clients: Set[uuid.UUID] = set()

    for p in payloads:
        email = p["email"]
        is_internal = email in internal
        stored_attendees.append(
            {
                "email": email,
                "name": p.get("name"),
                "source": p.get("source"),
                "is_team_member": is_internal,
            }
        )
        if is_internal:
            continue

        client = find_client_by_email(db, org_id, email)
        if client is None and create_missing_clients:
            client = ensure_client_for_email(db, org_id, email, p.get("name"))
        if client is not None and client.id not in seen_clients:
            seen_clients.add(client.id)
            external_client_ids_ordered.append(client.id)

    primary_id = external_client_ids_ordered[0] if external_client_ids_ordered else None
    rel_strs = [str(x) for x in external_client_ids_ordered[1:]]

    return primary_id, stored_attendees, rel_strs, recording_url


def client_display_name(client: Client) -> str:
    return _client_display_name(client)

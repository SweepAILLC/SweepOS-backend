"""
Brevo helpers for MCP: list verified senders + send email to a client.

Claude must collect sender_email + sender_name from list_brevo_senders before sending.
"""
from __future__ import annotations

import logging
import re
import uuid
from typing import Any, Dict, List, Optional

import httpx
from sqlalchemy.orm import Session

from app.models.client import Client
from app.services.brevo_client import (
    BrevoNotConnectedError,
    BrevoSendError,
    get_brevo_auth_headers,
    send_email_for_org,
)

logger = logging.getLogger(__name__)

_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


def list_brevo_senders_for_mcp(
    db: Session,
    org_id: uuid.UUID,
    *,
    user_id: Optional[uuid.UUID] = None,
    active_only: bool = True,
) -> Dict[str, Any]:
    """
    Return Brevo sender options Claude should present to the user before sending.
    """
    try:
        headers = get_brevo_auth_headers(db, org_id, user_id=user_id)
    except BrevoNotConnectedError as e:
        return {
            "connected": False,
            "senders": [],
            "count": 0,
            "error": str(e),
            "instruction": "Brevo is not connected for this org. Connect Brevo in SweepOS → Integrations.",
        }

    try:
        response = httpx.get(
            "https://api.brevo.com/v3/senders",
            headers=headers,
            timeout=30.0,
        )
    except httpx.RequestError as e:
        return {
            "connected": True,
            "senders": [],
            "count": 0,
            "error": f"Network error contacting Brevo: {e}",
        }

    if response.status_code != 200:
        try:
            body = response.json()
            msg = body.get("message") or body.get("error") or response.text
        except Exception:
            msg = response.text
        return {
            "connected": True,
            "senders": [],
            "count": 0,
            "error": f"Brevo HTTP {response.status_code}: {msg}",
        }

    raw = response.json().get("senders") or []
    senders: List[Dict[str, Any]] = []
    for s in raw:
        if not isinstance(s, dict):
            continue
        active = bool(s.get("active", False))
        if active_only and not active:
            continue
        email = str(s.get("email") or "").strip()
        if not email:
            continue
        senders.append(
            {
                "id": s.get("id"),
                "name": str(s.get("name") or "").strip(),
                "email": email,
                "active": active,
            }
        )

    return {
        "connected": True,
        "senders": senders,
        "count": len(senders),
        "active_only": active_only,
        "instruction": (
            "Ask the user which sender_email and sender_name to use before calling "
            "send_client_email. Prefer an active sender from this list. "
            "sender_name may default to the listed name when the user only picks an email."
        ),
    }


def _client_recipient_email(client: Client) -> Optional[str]:
    candidates: List[str] = []
    if client.email:
        candidates.append(str(client.email).strip().lower())
    if isinstance(client.emails, list):
        candidates.extend([str(e).strip().lower() for e in client.emails if e])
    for raw in candidates:
        if raw and _EMAIL_RE.match(raw):
            return raw
    return None


def send_client_email_for_mcp(
    db: Session,
    org_id: uuid.UUID,
    *,
    user_id: Optional[uuid.UUID] = None,
    client_id: str,
    sender_email: str,
    sender_name: str,
    subject: str,
    html_content: Optional[str] = None,
    text_content: Optional[str] = None,
    reply_to_email: Optional[str] = None,
    reply_to_name: Optional[str] = None,
    confirm_send: bool = False,
) -> Dict[str, Any]:
    """
    Send a transactional email to one client via the org's Brevo connection.

    Requires explicit sender_email + sender_name (from list_brevo_senders) and confirm_send=true.
    """
    if not confirm_send:
        return {
            "success": False,
            "error": "confirm_send_required",
            "message": (
                "Set confirm_send=true only after the user has approved the recipient, "
                "subject, body, sender_email, and sender_name."
            ),
        }

    sender_email_n = (sender_email or "").strip().lower()
    sender_name_n = (sender_name or "").strip()
    subject_n = (subject or "").strip()
    if not sender_email_n or not _EMAIL_RE.match(sender_email_n):
        return {"success": False, "error": "invalid_sender_email"}
    if not sender_name_n:
        return {
            "success": False,
            "error": "sender_name_required",
            "message": "Ask the user for a sender display name (or use the name from list_brevo_senders).",
        }
    if not subject_n:
        return {"success": False, "error": "subject_required"}
    if not (html_content or text_content):
        return {
            "success": False,
            "error": "content_required",
            "message": "Provide html_content and/or text_content.",
        }

    try:
        client_uuid = uuid.UUID(str(client_id))
    except ValueError:
        return {"success": False, "error": "invalid_client_id"}

    client = (
        db.query(Client)
        .filter(Client.id == client_uuid, Client.org_id == org_id)
        .first()
    )
    if not client:
        return {"success": False, "error": "client_not_found"}

    to_email = _client_recipient_email(client)
    if not to_email:
        return {
            "success": False,
            "error": "client_missing_email",
            "client_id": str(client.id),
            "client_name": client.name,
        }

    # Prefer active verified senders; still allow if Brevo accepts custom (will fail upstream if not)
    senders_payload = list_brevo_senders_for_mcp(
        db, org_id, user_id=user_id, active_only=False
    )
    known = {
        str(s.get("email") or "").strip().lower(): s
        for s in (senders_payload.get("senders") or [])
        if isinstance(s, dict)
    }
    matched = known.get(sender_email_n)
    if matched and matched.get("active") is False:
        return {
            "success": False,
            "error": "sender_inactive",
            "message": f"{sender_email_n} is not an active Brevo sender. Pick an active sender.",
            "senders": senders_payload.get("senders") or [],
        }
    if senders_payload.get("connected") and known and sender_email_n not in known:
        return {
            "success": False,
            "error": "sender_not_in_brevo_list",
            "message": (
                "sender_email must be chosen from list_brevo_senders. "
                "Ask the user to pick one of the returned options."
            ),
            "senders": [s for s in (senders_payload.get("senders") or []) if s.get("active")],
        }

    reply_to = None
    if reply_to_email and _EMAIL_RE.match(reply_to_email.strip()):
        reply_to = {
            "email": reply_to_email.strip().lower(),
            "name": (reply_to_name or sender_name_n).strip(),
        }

    try:
        result = send_email_for_org(
            db,
            org_id,
            user_id=user_id,
            sender={"email": sender_email_n, "name": sender_name_n},
            to=[{"email": to_email, "name": (client.name or "").strip() or to_email}],
            subject=subject_n,
            html_content=html_content,
            text_content=text_content,
            reply_to=reply_to,
            tags=["sweepos-mcp", "claude"],
        )
    except BrevoNotConnectedError as e:
        return {"success": False, "error": "brevo_not_connected", "message": str(e)}
    except BrevoSendError as e:
        return {
            "success": False,
            "error": "brevo_send_failed",
            "message": str(e),
            "status_code": e.status_code,
            "retryable": e.retryable,
        }

    return {
        "success": True,
        "message_id": (result or {}).get("messageId") or (result or {}).get("message_id"),
        "client_id": str(client.id),
        "client_name": client.name,
        "to_email": to_email,
        "sender_email": sender_email_n,
        "sender_name": sender_name_n,
        "subject": subject_n,
        "brevo": result or {},
    }

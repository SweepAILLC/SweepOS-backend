"""GHL (GoHighLevel) private-integration REST client.

Auth model differs from every other provider in this file's family: GHL issues a
static Private Integration token (Bearer, sub-account/location scoped) rather than
an OAuth authorization-code flow, so there is no refresh_token dance here - just a
long-lived encrypted token in `oauth_tokens.access_token` and the location id in
`oauth_tokens.account_id`.

API version pinned per GHL's versioning header requirement; bump deliberately.
"""
from __future__ import annotations

import logging
import uuid
from typing import Any, Dict, Iterator, List, Optional

import httpx
from sqlalchemy.orm import Session

from app.core.encryption import decrypt_token, encrypt_token
from app.models.oauth_token import OAuthProvider, OAuthToken

LOG = logging.getLogger(__name__)

GHL_API_BASE = "https://services.leadconnectorhq.com"
GHL_API_VERSION = "2021-07-28"
GHL_CONTACTS_PAGE_SIZE = 100
_REQUEST_TIMEOUT = httpx.Timeout(15.0, connect=5.0)


class GhlNotConnectedError(Exception):
    pass


class GhlApiError(Exception):
    """Wraps an upstream GHL error so callers can decide retry/reconnect semantics."""

    def __init__(self, message: str, *, status_code: Optional[int] = None):
        super().__init__(message)
        self.status_code = status_code


class GhlConfigError(Exception):
    pass


def get_ghl_token(db: Session, org_id: uuid.UUID) -> Optional[OAuthToken]:
    return (
        db.query(OAuthToken)
        .filter(OAuthToken.provider == OAuthProvider.GHL, OAuthToken.org_id == org_id)
        .first()
    )


def upsert_ghl_credentials(
    db: Session,
    org_id: uuid.UUID,
    *,
    api_key: str,
    location_id: str,
) -> OAuthToken:
    """Store (or replace) the org's GHL private integration token. Caller is responsible
    for verifying the key against GHL (see verify_ghl_connection) before calling this,
    so a bad key never silently persists as "connected"."""
    key = (api_key or "").strip()
    loc = (location_id or "").strip()
    if not key:
        raise GhlConfigError("GHL private integration token is required")
    if not loc:
        raise GhlConfigError("GHL location ID is required")

    enc = encrypt_token(key)
    row = get_ghl_token(db, org_id)
    if row is None:
        row = OAuthToken(
            org_id=org_id,
            provider=OAuthProvider.GHL,
            account_id=loc,
            access_token=enc,
        )
        db.add(row)
    else:
        row.access_token = enc
        row.account_id = loc
    db.commit()
    db.refresh(row)
    return row


def delete_ghl_credentials(db: Session, org_id: uuid.UUID) -> bool:
    row = get_ghl_token(db, org_id)
    if row is None:
        return False
    db.delete(row)
    db.commit()
    return True


def set_ghl_webhook_secret(db: Session, org_id: uuid.UUID, secret: str) -> Optional[OAuthToken]:
    """Store a per-org shared secret the org pastes into their GHL Workflow's
    webhook custom header, so the webhook endpoint can verify inbound requests.
    Unlike Calcom/Calendly, GHL's Workflow webhook action can't HMAC-sign the
    body — it can only send a static custom header — so this is a shared-secret
    equality check, not an HMAC verification."""
    row = get_ghl_token(db, org_id)
    if row is None:
        return None
    row.webhook_secret = encrypt_token(secret)
    db.commit()
    db.refresh(row)
    return row


def resolve_ghl_webhook_secret(db: Session, org_id: uuid.UUID) -> Optional[str]:
    from app.core.config import settings

    row = get_ghl_token(db, org_id)
    if row and row.webhook_secret:
        try:
            return decrypt_token(row.webhook_secret)
        except Exception:
            LOG.warning("ghl webhook secret decrypt failed org=%s", org_id)
    return getattr(settings, "GHL_WEBHOOK_SECRET", None) or None


def get_ghl_connection(
    db: Session,
    org_id: uuid.UUID,
    user_id: Optional[uuid.UUID] = None,
) -> tuple[Dict[str, str], str]:
    """
    Resolve (headers, location_id) for the given org's GHL connection.
    Raises GhlNotConnectedError if not connected.
    """
    token_row = (
        db.query(OAuthToken)
        .filter(
            OAuthToken.provider == OAuthProvider.GHL,
            OAuthToken.org_id == org_id,
        )
        .first()
    )
    if not token_row:
        raise GhlNotConnectedError("GHL not connected for org")

    if not token_row.account_id:
        raise GhlNotConnectedError("GHL connection missing locationId; reconnect required")

    audit_ctx: Optional[Dict[str, Any]] = None
    if user_id is not None:
        audit_ctx = {
            "db": db,
            "org_id": org_id,
            "user_id": user_id,
            "resource_type": "ghl_token",
            "resource_id": str(token_row.id),
        }
    access_token = decrypt_token(token_row.access_token, audit_context=audit_ctx)
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Version": GHL_API_VERSION,
        "Accept": "application/json",
    }
    return headers, token_row.account_id


def _raise_for_status(resp: httpx.Response, *, action: str) -> None:
    if resp.status_code == 401:
        raise GhlApiError(f"GHL {action} failed: token expired or revoked", status_code=401)
    if resp.status_code >= 400:
        raise GhlApiError(
            f"GHL {action} failed: {resp.status_code} {resp.text[:500]}",
            status_code=resp.status_code,
        )


def verify_ghl_connection(headers: Dict[str, str], location_id: str) -> bool:
    """Lightweight call to confirm the token + locationId pair is valid."""
    with httpx.Client(timeout=_REQUEST_TIMEOUT) as client:
        resp = client.get(
            f"{GHL_API_BASE}/locations/{location_id}",
            headers=headers,
        )
    _raise_for_status(resp, action="connection check")
    return True


def list_ghl_calendars(headers: Dict[str, str], location_id: str) -> List[Dict[str, Any]]:
    with httpx.Client(timeout=_REQUEST_TIMEOUT) as client:
        resp = client.get(
            f"{GHL_API_BASE}/calendars/",
            headers=headers,
            params={"locationId": location_id},
        )
    _raise_for_status(resp, action="list calendars")
    data = resp.json() if resp.content else {}
    calendars = data.get("calendars") if isinstance(data, dict) else None
    return calendars if isinstance(calendars, list) else []


def iter_ghl_contacts(
    headers: Dict[str, str],
    location_id: str,
    *,
    page_size: int = GHL_CONTACTS_PAGE_SIZE,
) -> Iterator[Dict[str, Any]]:
    """Yield every contact for a location, paginating via GHL's startAfterId cursor."""
    start_after_id: Optional[str] = None
    start_after: Optional[int] = None
    with httpx.Client(timeout=_REQUEST_TIMEOUT) as client:
        while True:
            params: Dict[str, Any] = {"locationId": location_id, "limit": page_size}
            if start_after_id:
                params["startAfterId"] = start_after_id
            if start_after:
                params["startAfter"] = start_after

            resp = client.get(f"{GHL_API_BASE}/contacts/", headers=headers, params=params)
            _raise_for_status(resp, action="list contacts")
            data = resp.json() if resp.content else {}
            contacts = data.get("contacts") if isinstance(data, dict) else None
            if not isinstance(contacts, list) or not contacts:
                return
            for contact in contacts:
                yield contact
            if len(contacts) < page_size:
                return
            meta = data.get("meta") if isinstance(data, dict) else {}
            start_after_id = meta.get("startAfterId") if isinstance(meta, dict) else None
            start_after = meta.get("startAfter") if isinstance(meta, dict) else None
            if not start_after_id and not start_after:
                return


def normalize_ghl_contact(raw: Dict[str, Any]) -> Dict[str, Optional[str]]:
    """Map a GHL contact payload to the field names Client upsert logic expects."""
    return {
        "ghl_contact_id": str(raw.get("id") or "").strip() or None,
        "email": (raw.get("email") or "").strip() or None,
        "phone": (raw.get("phone") or "").strip() or None,
        "first_name": (raw.get("firstName") or "").strip() or None,
        "last_name": (raw.get("lastName") or "").strip() or None,
    }


_GHL_CANCELLED_STATUSES = frozenset({"cancelled", "canceled"})


def normalize_ghl_appointment_event(payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Normalize a GHL AppointmentCreate/Update/Delete webhook payload (or a GHL
    Workflow "Webhook" action forwarding the same appointment fields) into the
    shape app.api.calendar_webhooks._upsert_check_in expects.

    GHL's exact webhook envelope depends on whether it's a native marketplace-app
    webhook or an org-configured Workflow action; both nest an "appointment" object
    with the same field names, so this accepts either the nested shape or a flat
    payload with those same keys at the top level. Returns None when the payload
    has neither a recognizable event id nor calendar id (not an appointment event
    this integration can process).
    """
    event_type = str(payload.get("type") or payload.get("event") or "").strip()
    appointment = payload.get("appointment") if isinstance(payload.get("appointment"), dict) else payload
    if not isinstance(appointment, dict):
        return None

    event_id = str(appointment.get("id") or appointment.get("appointmentId") or "").strip()
    calendar_id = str(appointment.get("calendarId") or "").strip()
    if not event_id or not calendar_id:
        return None

    contact = payload.get("contact") if isinstance(payload.get("contact"), dict) else {}
    attendee_email = (contact.get("email") or appointment.get("email") or "").strip() or None
    first = contact.get("firstName") or appointment.get("firstName")
    last = contact.get("lastName") or appointment.get("lastName")
    attendee_name = " ".join(p for p in (first, last) if p) or None

    raw_status = str(appointment.get("appointmentStatus") or appointment.get("status") or "").strip().lower()
    cancelled = raw_status in _GHL_CANCELLED_STATUSES or event_type.lower().endswith("delete")

    return {
        "event_type": event_type,
        "event_id": event_id,
        "calendar_id": calendar_id,
        "title": (appointment.get("title") or "").strip() or None,
        "start_time": appointment.get("startTime"),
        "end_time": appointment.get("endTime"),
        "attendee_email": attendee_email,
        "attendee_name": attendee_name,
        "cancelled": cancelled,
    }

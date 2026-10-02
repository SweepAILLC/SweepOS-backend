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
from datetime import date, datetime, timezone
from typing import Any, Dict, Iterator, List, Optional
from urllib.parse import parse_qs, urlsplit

import httpx
from sqlalchemy.orm import Session

from app.core.encryption import decrypt_token, encrypt_token
from app.models.oauth_token import OAuthProvider, OAuthToken

LOG = logging.getLogger(__name__)

GHL_API_BASE = "https://services.leadconnectorhq.com"
GHL_API_VERSION = "2021-07-28"
GHL_CONTACTS_PAGE_SIZE = 100
GHL_FUNNELS_PAGE_SIZE = 50
GHL_SUBMISSIONS_PAGE_SIZE = 100  # API max
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

    contact_id = str(contact.get("id") or appointment.get("contactId") or payload.get("contact_id") or "").strip() or None

    return {
        "event_type": event_type,
        "event_id": event_id,
        "calendar_id": calendar_id,
        "contact_id": contact_id,
        "title": (appointment.get("title") or "").strip() or None,
        "start_time": appointment.get("startTime"),
        "end_time": appointment.get("endTime"),
        "attendee_email": attendee_email,
        "attendee_name": attendee_name,
        "cancelled": cancelled,
    }


# --- Funnels + form/survey submissions (GHL-paired Sweep funnels) -------------------


def normalize_funnel_path(raw: Any) -> Optional[str]:
    """Path key shared by GHL step urls, submission page urls and the visitor snippet:
    lowercase, leading slash, no trailing slash, no query. Full URLs are reduced to
    their path, so custom domains and the GHL preview domain compare equal."""
    text = str(raw or "").strip()
    if not text:
        return None
    path = urlsplit(text).path if "://" in text else text.split("?", 1)[0].split("#", 1)[0]
    path = "/" + path.strip().strip("/").lower()
    return path


def normalize_ghl_funnel(raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """GHL funnel list item -> {ghl_funnel_id, name, path, steps[{id, name, path, sequence, type}]}.
    Steps are sorted by GHL's sequence; steps without a url are dropped."""
    funnel_id = str(raw.get("_id") or raw.get("id") or "").strip()
    if not funnel_id:
        return None
    steps: List[Dict[str, Any]] = []
    for step in raw.get("steps") or []:
        if not isinstance(step, dict):
            continue
        path = normalize_funnel_path(step.get("url"))
        if not path:
            continue
        try:
            sequence = int(step.get("sequence"))
        except (TypeError, ValueError):
            sequence = len(steps) + 1
        steps.append(
            {
                "id": str(step.get("id") or "") or None,
                "name": (str(step.get("name") or "").strip() or None),
                "path": path,
                "sequence": sequence,
                "type": step.get("type"),
            }
        )
    steps.sort(key=lambda s: s["sequence"])
    return {
        "ghl_funnel_id": funnel_id,
        "name": (str(raw.get("name") or "").strip() or "Untitled GHL funnel"),
        "path": normalize_funnel_path(raw.get("url")),
        "steps": steps,
    }


def list_ghl_funnels(headers: Dict[str, str], location_id: str) -> List[Dict[str, Any]]:
    """Every funnel in the location, normalized (see normalize_ghl_funnel)."""
    out: List[Dict[str, Any]] = []
    offset = 0
    with httpx.Client(timeout=_REQUEST_TIMEOUT) as client:
        while True:
            resp = client.get(
                f"{GHL_API_BASE}/funnels/funnel/list",
                headers=headers,
                params={"locationId": location_id, "limit": GHL_FUNNELS_PAGE_SIZE, "offset": offset},
            )
            _raise_for_status(resp, action="list funnels")
            data = resp.json() if resp.content else {}
            items = data.get("funnels") if isinstance(data, dict) else None
            if isinstance(items, dict):  # docs show a single object; accept both shapes
                items = [items]
            if not isinstance(items, list) or not items:
                break
            for item in items:
                if isinstance(item, dict):
                    norm = normalize_ghl_funnel(item)
                    if norm:
                        out.append(norm)
            total = data.get("count") if isinstance(data, dict) else None
            offset += len(items)
            if len(items) < GHL_FUNNELS_PAGE_SIZE or (isinstance(total, int) and offset >= total):
                break
    return out


def _iter_submissions(
    headers: Dict[str, str],
    location_id: str,
    *,
    kind: str,
    start: date,
    end: date,
) -> Iterator[Dict[str, Any]]:
    """Page through /forms/submissions or /surveys/submissions for [start, end] (whole days)."""
    page = 1
    with httpx.Client(timeout=_REQUEST_TIMEOUT) as client:
        while True:
            resp = client.get(
                f"{GHL_API_BASE}/{kind}/submissions",
                headers=headers,
                params={
                    "locationId": location_id,
                    "page": page,
                    "limit": GHL_SUBMISSIONS_PAGE_SIZE,
                    "startAt": start.isoformat(),
                    "endAt": end.isoformat(),
                },
            )
            _raise_for_status(resp, action=f"list {kind} submissions")
            data = resp.json() if resp.content else {}
            rows = data.get("submissions") if isinstance(data, dict) else None
            if not isinstance(rows, list) or not rows:
                return
            for row in rows:
                if isinstance(row, dict):
                    yield row
            meta = data.get("meta") if isinstance(data.get("meta"), dict) else {}
            next_page = meta.get("nextPage")
            if not next_page or len(rows) < GHL_SUBMISSIONS_PAGE_SIZE:
                return
            page = int(next_page)


def iter_ghl_form_submissions(headers: Dict[str, str], location_id: str, start: date, end: date) -> Iterator[Dict[str, Any]]:
    return _iter_submissions(headers, location_id, kind="forms", start=start, end=end)


def iter_ghl_survey_submissions(headers: Dict[str, str], location_id: str, start: date, end: date) -> Iterator[Dict[str, Any]]:
    return _iter_submissions(headers, location_id, kind="surveys", start=start, end=end)


# Keys in a submission's `others` that are GHL plumbing or identity, not answers.
_SUBMISSION_META_KEYS = frozenset(
    {"eventData", "fieldsOriSequance", "fieldsOriSequence", "full_name", "first_name", "last_name", "email", "phone"}
)


def _parse_iso(value: Any) -> Optional[datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def normalize_ghl_submission(raw: Dict[str, Any], kind: str) -> Optional[Dict[str, Any]]:
    """Form/survey submission -> the fields lead intake needs. None when it has no id.

    `utm_raw` holds utm_* params from the page URL, falling back to GHL's own
    source/medium; callers run it through app.api.funnels.normalize_utm.
    """
    submission_id = str(raw.get("id") or "").strip()
    if not submission_id:
        return None
    others = raw.get("others") if isinstance(raw.get("others"), dict) else {}
    event = others.get("eventData") if isinstance(others.get("eventData"), dict) else {}
    page = event.get("page") if isinstance(event.get("page"), dict) else {}
    page_url = str(page.get("url") or event.get("url") or "").strip() or None

    utm_raw: Dict[str, str] = {}
    if page_url:
        query = parse_qs(urlsplit(page_url).query)
        for key in ("source", "medium", "campaign", "term", "content"):
            val = (query.get(f"utm_{key}") or [None])[0]
            if val:
                utm_raw[key] = val
    if not utm_raw:
        for key in ("source", "medium"):
            val = str(event.get(key) or "").strip()
            if val:
                utm_raw[key] = val

    def _s(value: Any) -> Optional[str]:
        if value is None:
            return None
        return str(value).strip() or None

    answers = {
        k: v
        for k, v in others.items()
        if k not in _SUBMISSION_META_KEYS and not (k.startswith("__") and k.endswith("__"))
    }
    return {
        "submission_id": submission_id,
        "kind": kind,
        "contact_id": _s(raw.get("contactId")),
        "form_id": _s(raw.get("formId") or raw.get("surveyId")),
        "created_at": _parse_iso(raw.get("createdAt")),
        "email": _s(raw.get("email") or others.get("email")),
        "phone": _s(others.get("phone") or raw.get("phone")),
        "name": _s(raw.get("name") or others.get("full_name")),
        "first_name": _s(others.get("first_name")),
        "last_name": _s(others.get("last_name")),
        "page_url": page_url,
        "page_path": normalize_funnel_path(page_url),
        "referrer": _s(event.get("referrer")),
        "ad_source": _s(event.get("adSource")),
        "utm_raw": utm_raw,
        "answers": answers,
    }

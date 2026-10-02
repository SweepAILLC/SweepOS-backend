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

    def __init__(self, message: str, *, status_code: Optional[int] = None, scope_missing: bool = False):
        super().__init__(message)
        self.status_code = status_code
        # 401 "The token is not authorized for this scope": the token is valid but the
        # Private Integration lacks this scope (vs. "Invalid Private Integration token").
        self.scope_missing = scope_missing


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


def _is_scope_error(resp: httpx.Response) -> bool:
    return resp.status_code == 401 and "not authorized for this scope" in resp.text.lower()


def _raise_for_status(resp: httpx.Response, *, action: str) -> None:
    if _is_scope_error(resp):
        raise GhlApiError(
            f"GHL {action} failed: the private integration token is missing a required scope",
            status_code=401,
            scope_missing=True,
        )
    if resp.status_code == 401:
        raise GhlApiError(f"GHL {action} failed: token expired or revoked", status_code=401)
    if resp.status_code >= 400:
        raise GhlApiError(
            f"GHL {action} failed: {resp.status_code} {resp.text[:500]}",
            status_code=resp.status_code,
        )


# Read scopes Sweep uses, each with a cheap probe call. A Private Integration token
# can be valid with any subset of them.
GHL_SCOPE_PROBES: Dict[str, tuple] = {
    "locations.readonly": ("/locations/{loc}", {}),
    "contacts.readonly": ("/contacts/", {"limit": 1}),
    "calendars.readonly": ("/calendars/", {}),
    "funnels/funnel.readonly": ("/funnels/funnel/list", {"limit": 1}),
    "forms.readonly": ("/forms/", {"limit": 1}),
    "surveys.readonly": ("/surveys/", {"limit": 1}),
}


def probe_ghl_scopes(headers: Dict[str, str], location_id: str) -> Dict[str, bool]:
    """{scope: granted} for GHL_SCOPE_PROBES. Raises GhlApiError for an invalid token
    or location (anything other than success or a missing-scope 401)."""
    granted: Dict[str, bool] = {}
    with httpx.Client(timeout=_REQUEST_TIMEOUT) as client:
        for scope, (path, extra) in GHL_SCOPE_PROBES.items():
            params = {} if "{loc}" in path else {"locationId": location_id, **extra}
            resp = client.get(f"{GHL_API_BASE}{path.format(loc=location_id)}", headers=headers, params=params)
            if _is_scope_error(resp):
                granted[scope] = False
                continue
            _raise_for_status(resp, action=f"scope check ({scope})")
            granted[scope] = True
    return granted


def verify_ghl_connection(headers: Dict[str, str], location_id: str) -> Dict[str, bool]:
    """Confirm the token + locationId pair is valid and report which scopes it has.

    A token missing locations.readonly is still valid; it fails only when no probe
    succeeds (every scope missing) or GHL rejects the token/location outright."""
    granted = probe_ghl_scopes(headers, location_id)
    if not any(granted.values()):
        raise GhlApiError("GHL connection check failed: the token has none of the scopes Sweep uses", status_code=401)
    return granted


def list_ghl_forms_and_surveys(headers: Dict[str, str], location_id: str) -> List[Dict[str, Any]]:
    """[{id, name, kind: "form"|"survey"}] for the extra-forms picker (first page of each:
    GHL caps forms at 100 and surveys at 50 per page)."""
    out: List[Dict[str, Any]] = []
    with httpx.Client(timeout=_REQUEST_TIMEOUT) as client:
        for kind, path, key, limit in (("form", "/forms/", "forms", 100), ("survey", "/surveys/", "surveys", 50)):
            resp = client.get(
                f"{GHL_API_BASE}{path}",
                headers=headers,
                params={"locationId": location_id, "limit": limit},
            )
            _raise_for_status(resp, action=f"list {key}")
            data = resp.json() if resp.content else {}
            rows = data.get(key) if isinstance(data, dict) else None
            for row in rows if isinstance(rows, list) else []:
                if isinstance(row, dict) and row.get("id"):
                    out.append({"id": str(row["id"]), "name": str(row.get("name") or "Untitled"), "kind": kind})
    return out


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
# Real payloads (GHL-0) also carry the submitter's IP, a signature hash and session
# ids; none of that is an answer and the IP must not be stored.
_SUBMISSION_META_KEYS = frozenset(
    {
        "eventData", "fieldsOriSequance", "fieldsOriSequence",
        "full_name", "first_name", "last_name", "email", "phone",
        "formId", "location_id", "submissionId", "sessionId", "sessionFingerprint",
        "signatureHash", "ip", "Timezone", "terms_and_conditions",
    }
)


def _is_ghl_widget_url(url: Optional[str]) -> bool:
    """GHL serves embedded forms/surveys from its own widget host (e.g.
    link.apisystem.tech/widget/form/<id>); that URL is the iframe, not the funnel page."""
    return bool(url) and "/widget/" in urlsplit(url).path


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
    page_url = str(page.get("url") or event.get("documentURL") or event.get("url") or "").strip() or None
    # An embedded form reports its widget iframe as the page; the funnel page that
    # embeds it is the iframe's referrer.
    referrer = str(event.get("referrer") or "").strip() or None
    if _is_ghl_widget_url(page_url) and referrer and not _is_ghl_widget_url(referrer):
        host_url = referrer
    elif _is_ghl_widget_url(page_url):
        host_url = None
    else:
        host_url = page_url

    utm_raw: Dict[str, str] = {}
    url_params = event.get("url_params") if isinstance(event.get("url_params"), dict) else {}
    for key in ("source", "medium", "campaign", "term", "content"):
        val = str(url_params.get(f"utm_{key}") or "").strip()
        if val:
            utm_raw[key] = val
    for url in (host_url, page_url):
        if not url or utm_raw:
            continue
        query = parse_qs(urlsplit(url).query)
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
        # The funnel page the person was on (None when only GHL's widget URL is known);
        # routing matches page_path against the paired funnel's step paths.
        "page_url": host_url,
        "page_path": normalize_funnel_path(host_url),
        "widget_url": page_url if _is_ghl_widget_url(page_url) else None,
        "referrer": _s(event.get("referrer")),
        "ad_source": _s(event.get("adSource")),
        "utm_raw": utm_raw,
        "answers": answers,
    }


def _first(*values: Any) -> Optional[str]:
    for v in values:
        if v is None:
            continue
        text = str(v).strip()
        if text:
            return text
    return None


SWEEP_WEBHOOK_KEYS = frozenset({"sweep_event", "sweep_funnel_id"})


def webhook_custom_data(body: Dict[str, Any]) -> Dict[str, Any]:
    """Workflow Webhook action custom data (key/value pairs the client adds in GHL)."""
    for key in ("customData", "custom_data"):
        value = body.get(key)
        if isinstance(value, dict):
            return value
    return {}


def webhook_sweep_field(body: Dict[str, Any], key: str) -> Optional[str]:
    """`sweep_event` / `sweep_funnel_id` from custom data, or the top level as a fallback."""
    return _first(webhook_custom_data(body).get(key), body.get(key))


def normalize_ghl_workflow_opt_in(body: Dict[str, Any], *, received_at: datetime) -> Dict[str, Any]:
    """A GHL Workflow (Form/Survey Submitted -> Webhook) payload in the same shape as
    normalize_ghl_submission, so the webhook and the reconcile pull share one processor.

    Workflow payloads carry the contact's standard fields at the top level (and/or a
    nested `contact`), the client's custom data, and the contact's attribution. Field
    names vary by trigger, so several spellings are accepted; GHL-0 pins the real one.
    """
    contact = body.get("contact") if isinstance(body.get("contact"), dict) else {}
    custom = webhook_custom_data(body)
    attribution: Dict[str, Any] = {}
    for src in (contact, body):
        for key in ("attributionSource", "attribution_source", "lastAttributionSource"):
            if isinstance(src.get(key), dict):
                attribution = src[key]
                break
        if attribution:
            break

    page_url = _first(
        custom.get("page_url"),
        body.get("page_url"),
        (body.get("page") or {}).get("url") if isinstance(body.get("page"), dict) else None,
        attribution.get("url"),
        attribution.get("pageUrl"),
    )
    utm_raw: Dict[str, str] = {}
    if page_url:
        query = parse_qs(urlsplit(page_url).query)
        for key in ("source", "medium", "campaign", "term", "content"):
            val = (query.get(f"utm_{key}") or [None])[0]
            if val:
                utm_raw[key] = val
    if not utm_raw:
        spelled = {
            "source": ("utmSource", "utm_source"),
            "medium": ("utmMedium", "utm_medium"),
            "campaign": ("utmCampaign", "utm_campaign", "campaign"),
            "term": ("utmTerm", "utm_term"),
            "content": ("utmContent", "utm_content"),
        }
        for key, names in spelled.items():
            val = _first(*(attribution.get(n) for n in names))
            if val:
                utm_raw[key] = val

    answers = {k: v for k, v in custom.items() if k not in SWEEP_WEBHOOK_KEYS and k != "page_url"}
    return {
        "submission_id": _first(custom.get("submission_id"), body.get("submission_id"), body.get("submissionId")),
        "kind": "forms",
        "contact_id": _first(body.get("contact_id"), body.get("contactId"), contact.get("id")),
        "form_id": _first(custom.get("form_id"), body.get("form_id"), body.get("formId")),
        "created_at": received_at,
        "email": _first(body.get("email"), contact.get("email")),
        "phone": _first(body.get("phone"), contact.get("phone")),
        "name": _first(body.get("full_name"), body.get("name"), contact.get("name")),
        "first_name": _first(body.get("first_name"), body.get("firstName"), contact.get("firstName")),
        "last_name": _first(body.get("last_name"), body.get("lastName"), contact.get("lastName")),
        "page_url": page_url,
        "page_path": normalize_funnel_path(page_url),
        "referrer": _first(attribution.get("referrer")),
        "ad_source": _first(attribution.get("adSource"), attribution.get("sessionSource")),
        "utm_raw": utm_raw,
        "answers": answers,
    }

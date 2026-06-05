"""Calendly + Cal.com booking webhooks → upsert ClientCheckIn → fire pre-sale automation.

Real-time complement to the pull-based ``checkin_sync`` job. Configure these in your
Calendly / Cal.com dashboards as:

- Calendly: ``{BACKEND_PUBLIC_URL}/webhooks/calendly/{org_id}`` — subscribe to
  ``invitee.created`` (and optionally ``invitee.canceled``). Set the Calendly signing
  secret via ``CALENDLY_WEBHOOK_SECRET`` and we will verify ``Calendly-Webhook-Signature``.
- Cal.com: ``{BACKEND_PUBLIC_URL}/webhooks/calcom/{org_id}`` — trigger ``BOOKING_CREATED``
  (and optionally ``BOOKING_CANCELLED``). Set the per-org webhook secret via
  ``CALCOM_WEBHOOK_SECRET`` and we will verify the ``X-Cal-Signature-256`` HMAC-SHA256.

Trust model mirrors :mod:`app.api.fathom_webhooks`: org_id in the URL plus an optional
shared HMAC secret. When the secret env var isn't set we accept the webhook (matching
Fathom's behavior) and log a warning, so local-dev / first-time setup still works while
production deployments can lock things down by setting the secret.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

from fastapi import APIRouter, Depends, HTTPException, Request, status
from sqlalchemy import and_
from sqlalchemy.orm import Session

from app.core.config import settings
from app.db.session import get_db
from app.models.client_checkin import ClientCheckIn
from app.services.automation_engine import on_booking_created_pre_sale
from app.services.checkin_sync import (
    ensure_client_for_booking_attendee,
    normalize_email,
)
from app.services.terminal_metrics_service import invalidate_terminal_monthly_trends_cache

LOG = logging.getLogger(__name__)
router = APIRouter()

_MAX_WEBHOOK_BODY_BYTES = 1 * 1024 * 1024


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_org(org_id: str) -> uuid.UUID:
    try:
        return uuid.UUID(org_id)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid org_id") from exc


def _verify_hmac_sha256(secret: Optional[str], header_value: str, raw_body: bytes) -> None:
    """If a secret is configured, verify the header matches HMAC-SHA256 of the raw body."""
    if not secret:
        # Match Fathom webhook posture: warn-but-accept when unset so local dev / first-time
        # connections work; production should set the env var to lock this down.
        return
    expected = hmac.new(secret.encode(), raw_body, hashlib.sha256).hexdigest()
    candidates = [
        header_value.strip(),
        header_value.strip().split("=", 1)[-1].strip() if "=" in header_value else "",
    ]
    if not any(hmac.compare_digest(c, expected) for c in candidates if c):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid webhook signature")


def _parse_iso_datetime(value: Any) -> Optional[datetime]:
    if not isinstance(value, str) or not value:
        return None
    try:
        # Accept "...Z" suffix (Calendly + Cal.com both emit it).
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None


async def _read_body_async(request: Request) -> bytes:
    raw_body = await request.body()
    if len(raw_body) > _MAX_WEBHOOK_BODY_BYTES:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail="Payload too large",
        )
    return raw_body


def _upsert_check_in(
    db: Session,
    *,
    org_id: uuid.UUID,
    client_id: uuid.UUID,
    provider: str,
    event_id: str,
    event_uri: Optional[str],
    title: Optional[str],
    start_time: datetime,
    end_time: Optional[datetime],
    location: Optional[str],
    meeting_url: Optional[str],
    attendee_email: str,
    attendee_name: Optional[str],
    event_type_id: Optional[str],
    event_type_label: Optional[str],
    cancelled: bool,
    raw_payload: Dict[str, Any],
) -> Tuple[ClientCheckIn, bool]:
    """Insert or refresh a ClientCheckIn row. Returns (row, is_new)."""
    existing = (
        db.query(ClientCheckIn)
        .filter(
            and_(
                ClientCheckIn.org_id == org_id,
                ClientCheckIn.event_id == event_id,
                ClientCheckIn.provider == provider,
            )
        )
        .first()
    )
    if existing:
        existing.title = title or existing.title
        existing.start_time = start_time
        existing.end_time = end_time
        existing.location = location or existing.location
        existing.meeting_url = meeting_url or existing.meeting_url
        if event_type_id and not existing.event_type_id:
            existing.event_type_id = event_type_id
        if event_type_label and not existing.event_type_label:
            existing.event_type_label = event_type_label
        if cancelled:
            existing.cancelled = True
        existing.completed = (start_time < datetime.now(timezone.utc))
        existing.updated_at = datetime.now(timezone.utc)
        existing.raw_event_data = json.dumps(raw_payload)
        return existing, False

    row = ClientCheckIn(
        org_id=org_id,
        client_id=client_id,
        event_id=event_id,
        event_uri=event_uri,
        provider=provider,
        title=title,
        start_time=start_time,
        end_time=end_time,
        location=location,
        meeting_url=meeting_url,
        attendee_email=attendee_email,
        attendee_name=attendee_name,
        event_type_id=event_type_id,
        event_type_label=event_type_label,
        completed=start_time < datetime.now(timezone.utc),
        cancelled=cancelled,
        no_show=False,
        is_sales_call=False,
        sale_closed=None,
        raw_event_data=json.dumps(raw_payload),
    )
    db.add(row)
    return row, True


# ---------------------------------------------------------------------------
# Calendly
# ---------------------------------------------------------------------------

def _extract_calendly_invitee(payload: Dict[str, Any]) -> Tuple[str, Dict[str, Any], Dict[str, Any]]:
    """Return (event_kind, invitee_dict, scheduled_event_dict) from a Calendly v2 payload.

    Calendly's webhook envelope is ``{"event": "<kind>", "payload": {...invitee...}}``
    where the invitee dict has a nested ``scheduled_event`` block. We accept both the
    raw envelope and the legacy "invitee at the top level" shape so we don't break if
    Calendly tweaks their payload again.
    """
    kind = str(payload.get("event") or payload.get("kind") or "").lower()
    invitee = payload.get("payload") if isinstance(payload.get("payload"), dict) else payload
    scheduled = invitee.get("scheduled_event") if isinstance(invitee, dict) else None
    if not isinstance(scheduled, dict):
        scheduled = {}
    return kind, invitee or {}, scheduled


@router.post("/calendly/{org_id}")
async def calendly_webhook(
    org_id: str,
    request: Request,
    db: Session = Depends(get_db),
):
    """Public Calendly webhook (set this URL on a v2 webhook subscription).

    Fires the pre-sale post-booking automation when a brand-new invitee is created
    AND the client has no recorded sale. ``invitee.canceled`` flips the corresponding
    check-in's cancelled flag so the timeline stays accurate.
    """
    org_uuid = _parse_org(org_id)
    raw_body = await _read_body_async(request)

    secret = getattr(settings, "CALENDLY_WEBHOOK_SECRET", None) or None
    sig_header = (
        request.headers.get("calendly-webhook-signature")
        or request.headers.get("x-calendly-signature")
        or ""
    )
    _verify_hmac_sha256(secret, sig_header, raw_body)

    try:
        body = json.loads(raw_body)
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid JSON") from exc
    if not isinstance(body, dict):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Expected JSON object")

    kind, invitee, scheduled = _extract_calendly_invitee(body)
    if kind not in ("invitee.created", "invitee.canceled"):
        # Acknowledge so Calendly doesn't keep retrying, but skip work.
        return {"ok": True, "skipped": True, "reason": f"unsupported_event:{kind or 'unknown'}"}

    invitee_email = str(invitee.get("email") or "").strip()
    if not invitee_email:
        return {"ok": True, "skipped": True, "reason": "no_invitee_email"}
    invitee_name = str(invitee.get("name") or "").strip() or None

    event_uri = str(scheduled.get("uri") or "")
    event_uuid = event_uri.rsplit("/", 1)[-1] if event_uri else (str(invitee.get("uri") or "").rsplit("/", 1)[-1])
    if not event_uuid:
        return {"ok": True, "skipped": True, "reason": "no_event_id"}

    title = scheduled.get("name") or invitee.get("name")
    start_time = _parse_iso_datetime(scheduled.get("start_time")) or datetime.now(timezone.utc)
    end_time = _parse_iso_datetime(scheduled.get("end_time"))
    location_raw = scheduled.get("location") or {}
    location = (
        location_raw.get("location") if isinstance(location_raw, dict) else (str(location_raw) if location_raw else None)
    )
    meeting_url = location_raw.get("join_url") if isinstance(location_raw, dict) else None

    event_type_uri = scheduled.get("event_type")
    if isinstance(event_type_uri, dict):
        event_type_uri = event_type_uri.get("uri") or ""
    event_type_uri = str(event_type_uri or "") or None
    event_type_label = (scheduled.get("name") or title or None)

    cancelled = kind == "invitee.canceled" or str(invitee.get("status") or "").lower() in ("canceled", "cancelled")

    try:
        client = ensure_client_for_booking_attendee(db, org_uuid, invitee_email, invitee_name)
    except Exception as e:  # pragma: no cover - defensive
        LOG.exception("calendly webhook: failed to resolve client for %s: %s", invitee_email, e)
        client = None
    if not client:
        return {"ok": True, "skipped": True, "reason": "no_matching_client"}

    _, is_new = _upsert_check_in(
        db,
        org_id=org_uuid,
        client_id=client.id,
        provider="calendly",
        event_id=event_uuid,
        event_uri=event_uri or None,
        title=title,
        start_time=start_time,
        end_time=end_time,
        location=location,
        meeting_url=meeting_url,
        attendee_email=invitee_email,
        attendee_name=invitee_name,
        event_type_id=event_type_uri,
        event_type_label=event_type_label,
        cancelled=cancelled,
        raw_payload=body,
    )
    db.commit()
    invalidate_terminal_monthly_trends_cache(org_uuid)

    fired_jobs: list[str] = []
    if is_new and not cancelled:
        try:
            ids = on_booking_created_pre_sale(
                db,
                org_id=org_uuid,
                client_id=client.id,
                provider="calendly",
                external_booking_id=event_uuid,
                event_type_id=event_type_uri,
                event_type_label=event_type_label,
                attendee_email=invitee_email,
                start_time=start_time,
            )
            db.commit()
            fired_jobs = [str(x) for x in ids]
        except Exception as e:
            db.rollback()
            LOG.exception("calendly webhook: pre-sale automation failed: %s", e)

    LOG.info(
        "calendly webhook org=%s event=%s invitee=%s new=%s fired_jobs=%s",
        org_id, kind, normalize_email(invitee_email), is_new, len(fired_jobs),
    )
    return {"ok": True, "is_new": is_new, "fired_jobs": fired_jobs}


# ---------------------------------------------------------------------------
# Cal.com
# ---------------------------------------------------------------------------

@router.post("/calcom/{org_id}")
async def calcom_webhook(
    org_id: str,
    request: Request,
    db: Session = Depends(get_db),
):
    """Public Cal.com webhook (set this URL on a BOOKING_CREATED / BOOKING_CANCELLED hook).

    Fires the pre-sale post-booking automation on BOOKING_CREATED.
    """
    org_uuid = _parse_org(org_id)
    raw_body = await _read_body_async(request)

    secret = getattr(settings, "CALCOM_WEBHOOK_SECRET", None) or None
    sig_header = (
        request.headers.get("x-cal-signature-256")
        or request.headers.get("x-cal-signature")
        or ""
    )
    _verify_hmac_sha256(secret, sig_header, raw_body)

    try:
        body = json.loads(raw_body)
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid JSON") from exc
    if not isinstance(body, dict):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Expected JSON object")

    trigger = str(body.get("triggerEvent") or body.get("type") or "").upper()
    if trigger not in ("BOOKING_CREATED", "BOOKING_CANCELLED", "BOOKING_RESCHEDULED"):
        return {"ok": True, "skipped": True, "reason": f"unsupported_event:{trigger or 'unknown'}"}

    payload = body.get("payload") if isinstance(body.get("payload"), dict) else body
    event_id = str(payload.get("uid") or payload.get("id") or "")
    if not event_id:
        return {"ok": True, "skipped": True, "reason": "no_event_id"}

    title = payload.get("title") or payload.get("eventTitle")
    start_time = _parse_iso_datetime(payload.get("startTime")) or datetime.now(timezone.utc)
    end_time = _parse_iso_datetime(payload.get("endTime"))
    location = payload.get("location")
    meeting_url = payload.get("meetingUrl")
    if not meeting_url:
        meta = payload.get("metadata")
        if isinstance(meta, dict):
            meeting_url = meta.get("videoCallUrl")

    attendees = payload.get("attendees") or []
    primary = next((a for a in attendees if isinstance(a, dict) and a.get("email")), None)
    attendee_email = (primary or {}).get("email")
    if not attendee_email:
        responses = payload.get("responses")
        if isinstance(responses, dict):
            attendee_email = responses.get("email")
    if not attendee_email:
        return {"ok": True, "skipped": True, "reason": "no_attendee_email"}
    attendee_email = str(attendee_email).strip()
    attendee_name = (primary or {}).get("name") if primary else None

    event_type_id = (
        str(payload.get("eventTypeId"))
        if payload.get("eventTypeId") is not None
        else (str((payload.get("eventType") or {}).get("id")) if (payload.get("eventType") or {}).get("id") is not None else None)
    )
    event_type_label = (payload.get("eventType") or {}).get("title") or title

    cancelled = trigger == "BOOKING_CANCELLED"

    try:
        client = ensure_client_for_booking_attendee(db, org_uuid, attendee_email, attendee_name)
    except Exception as e:  # pragma: no cover - defensive
        LOG.exception("calcom webhook: failed to resolve client for %s: %s", attendee_email, e)
        client = None
    if not client:
        return {"ok": True, "skipped": True, "reason": "no_matching_client"}

    _, is_new = _upsert_check_in(
        db,
        org_id=org_uuid,
        client_id=client.id,
        provider="calcom",
        event_id=event_id,
        event_uri=None,
        title=title,
        start_time=start_time,
        end_time=end_time,
        location=location if isinstance(location, str) else None,
        meeting_url=meeting_url if isinstance(meeting_url, str) else None,
        attendee_email=attendee_email,
        attendee_name=attendee_name,
        event_type_id=event_type_id,
        event_type_label=event_type_label,
        cancelled=cancelled,
        raw_payload=body,
    )
    db.commit()
    invalidate_terminal_monthly_trends_cache(org_uuid)

    fired_jobs: list[str] = []
    if is_new and not cancelled:
        try:
            ids = on_booking_created_pre_sale(
                db,
                org_id=org_uuid,
                client_id=client.id,
                provider="calcom",
                external_booking_id=event_id,
                event_type_id=event_type_id,
                event_type_label=event_type_label,
                attendee_email=attendee_email,
                start_time=start_time,
            )
            db.commit()
            fired_jobs = [str(x) for x in ids]
        except Exception as e:
            db.rollback()
            LOG.exception("calcom webhook: pre-sale automation failed: %s", e)

    LOG.info(
        "calcom webhook org=%s trigger=%s attendee=%s new=%s fired_jobs=%s",
        org_id, trigger, normalize_email(attendee_email), is_new, len(fired_jobs),
    )
    return {"ok": True, "is_new": is_new, "fired_jobs": fired_jobs}

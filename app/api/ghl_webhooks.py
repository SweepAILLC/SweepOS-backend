"""GHL appointment webhook -> upsert ClientCheckIn -> fire pre-sale automation.

Configure this URL as the target of a GHL Workflow's "Webhook" action, triggered
on an Appointment Created/Updated/Cancelled event: {BACKEND_PUBLIC_URL}/webhooks/ghl/{org_id}

GHL's Private Integration model has no native push-webhook subscription (that's a
Marketplace OAuth app capability) and a Workflow's Webhook action can't HMAC-sign
the body — only send a static custom header — so trust here is: org_id in the URL
plus an optional shared-secret header (see app.services.ghl_client.resolve_ghl_webhook_secret).
Unset secret warns-and-accepts, matching the Fathom/Calendly/Cal.com posture.

Only calendars the org has explicitly enabled (ghl_calendar_sync_settings) are
processed; unselected calendars are acknowledged and dropped without touching
`clients` or `client_check_ins`, per the integration's PRD.

Opt-ins (real-time leads for a GHL-paired funnel): a Form/Survey Submitted Workflow
posts to the same URL with custom data `sweep_event=opt_in` and `sweep_funnel_id`.
The funnel must belong to the org in the URL, and outside local dev an org secret
is required. Leads go through the same processor as the reconcile pull
(app.services.ghl_lead_sync), so webhook + pull never double count.
"""
from __future__ import annotations

import hmac
import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.concurrency import run_in_threadpool
from sqlalchemy.orm import Session
from sqlalchemy.orm.attributes import flag_modified

from app.api.calendar_webhooks import (
    _parse_iso_datetime,
    _parse_org,
    _read_body_async,
    _run_pipeline_after_webhook,
    _upsert_check_in,
)
from app.db.session import get_db
from app.models.ghl_calendar_sync_setting import GhlCalendarSyncSetting
from app.services.automation_engine import on_booking_created_pre_sale
from app.services.checkin_sync import (
    ensure_client_for_booking_attendee,
    get_or_create_calendar_placeholder_client,
    normalize_email,
)
from app.core.config import settings
from app.models.funnel import Funnel
from app.services.ghl_client import (
    normalize_ghl_appointment_event,
    normalize_ghl_workflow_opt_in,
    webhook_sweep_field,
)
from app.services.ghl_funnels import GHL_SOURCE
from app.services.terminal_metrics_service import invalidate_terminal_monthly_trends_cache

LOG = logging.getLogger(__name__)
router = APIRouter()

OPT_IN_EVENT = "opt_in"
# Original receipt time, kept in the inbox payload so a retry dates the lead correctly.
RECEIVED_AT_KEY = "_sweep_received_at"


def _verify_ghl_shared_secret(secret: str | None, header_value: str) -> None:
    """Static equality check (constant-time), not HMAC — see module docstring for why."""
    if not secret:
        LOG.warning("ghl webhook: no shared secret configured; accepting")
        return
    if not header_value or not hmac.compare_digest(header_value.strip(), secret.strip()):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid webhook secret")


@router.post("/ghl/{org_id}")
async def ghl_webhook(
    org_id: str,
    request: Request,
    db: Session = Depends(get_db),
):
    org_uuid = _parse_org(org_id)
    raw_body = await _read_body_async(request)

    from app.services.ghl_client import resolve_ghl_webhook_secret

    secret = resolve_ghl_webhook_secret(db, org_uuid)
    header_value = (
        request.headers.get("x-ghl-webhook-secret")
        or request.headers.get("x-webhook-secret")
        or ""
    )
    _verify_ghl_shared_secret(secret, header_value)

    try:
        body = json.loads(raw_body)
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid JSON") from exc
    if not isinstance(body, dict):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Expected JSON object")

    sweep_event = webhook_sweep_field(body, "sweep_event")
    if sweep_event is not None:
        if sweep_event != OPT_IN_EVENT:
            return {"ok": True, "skipped": True, "reason": "unsupported_sweep_event"}
        _require_secret_for_opt_in(secret)
        # The lead upsert is several queries; keep it off the event loop.
        return await run_in_threadpool(_ingest_ghl_opt_in, db, org_uuid, body)

    return _ingest_and_process_ghl(db, org_uuid, body)


def _require_secret_for_opt_in(secret: Optional[str]) -> None:
    """Opt-ins create leads and notify the coach, so outside local dev an org must
    have a webhook secret before Sweep accepts them (appointments keep warn-and-accept)."""
    if secret:
        return
    if (settings.ENVIRONMENT or "development").strip().lower() == "development":
        LOG.warning("ghl opt-in webhook: no shared secret configured; accepting (development)")
        return
    raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="Generate a GoHighLevel webhook secret in Sweep before sending opt-ins.",
    )


def _load_paired_funnel(db: Session, org_uuid: uuid.UUID, raw_funnel_id: Optional[str]) -> Optional[Funnel]:
    try:
        funnel_id = uuid.UUID(str(raw_funnel_id))
    except (TypeError, ValueError):
        return None
    # Org comes only from the URL; a funnel id from another org never matches.
    return (
        db.query(Funnel)
        .filter(Funnel.id == funnel_id, Funnel.org_id == org_uuid, Funnel.source == GHL_SOURCE)
        .first()
    )


def _ingest_ghl_opt_in(db: Session, org_uuid: uuid.UUID, body: Dict[str, Any]) -> Dict[str, Any]:
    from app.services.inbound_webhook_inbox import mark_inbound_done, mark_inbound_retry, record_inbound_event

    funnel = _load_paired_funnel(db, org_uuid, webhook_sweep_field(body, "sweep_funnel_id"))
    if funnel is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="GHL-paired funnel not found")

    received_at = datetime.now(timezone.utc)
    norm = normalize_ghl_workflow_opt_in(body, received_at=received_at)
    person = norm["submission_id"] or norm["contact_id"] or (norm["email"] or "").lower() or uuid.uuid4().hex
    row, _ = record_inbound_event(
        db,
        org_id=org_uuid,
        provider="ghl",
        event_id=f"optin:{funnel.id}:{person}",
        event_type=OPT_IN_EVENT,
        payload={**body, RECEIVED_AT_KEY: received_at.isoformat()},
    )
    if row.status == "done":
        return {"ok": True, "deduped": True}
    try:
        _process_ghl_opt_in(db, org_uuid, row.payload)
        mark_inbound_done(db, row)
    except HTTPException:
        raise
    except Exception as exc:
        mark_inbound_retry(db, row, str(exc))
        raise
    LOG.info("ghl opt-in webhook org=%s funnel=%s", org_uuid, funnel.id)
    return {"ok": True}


def _process_ghl_opt_in(db: Session, org_uuid: uuid.UUID, body: Dict[str, Any]) -> None:
    """Shared by the live webhook and inbox retries: tag the lead through the same
    processor as the reconcile pull, then record webhook health for the funnel."""
    from app.services.ghl_lead_sync import process_submission_payload

    funnel = _load_paired_funnel(db, org_uuid, webhook_sweep_field(body, "sweep_funnel_id"))
    if funnel is None:  # unpaired since the event was recorded
        return
    received_at = _parse_iso_datetime(body.get(RECEIVED_AT_KEY)) or datetime.now(timezone.utc)
    norm = normalize_ghl_workflow_opt_in(body, received_at=received_at)
    norm["created_at"] = received_at.isoformat()
    process_submission_payload(db, org_uuid, {"funnel_id": str(funnel.id), "submission": norm, "notify": True})

    cfg = dict(funnel.ghl_config or {})
    cfg["webhook"] = {**(cfg.get("webhook") or {}), "last_received_at": received_at.isoformat()}
    funnel.ghl_config = cfg
    flag_modified(funnel, "ghl_config")
    db.commit()


def _ingest_and_process_ghl(db: Session, org_uuid: uuid.UUID, body: Dict[str, Any]) -> Dict[str, Any]:
    from app.services.inbound_webhook_inbox import (
        mark_inbound_done,
        mark_inbound_retry,
        record_inbound_event,
    )

    event = normalize_ghl_appointment_event(body)
    if event is None:
        # Not a recognizable appointment event (e.g. a contact event forwarded to the
        # same URL by mistake) — acknowledge so GHL doesn't retry, but do no work.
        return {"ok": True, "skipped": True, "reason": "unrecognized_payload"}

    # Unselected calendars are ignored entirely, before touching the dedup inbox
    # or any client/check-in row — per the integration's calendar picker scope.
    setting = (
        db.query(GhlCalendarSyncSetting)
        .filter(
            GhlCalendarSyncSetting.org_id == org_uuid,
            GhlCalendarSyncSetting.calendar_id == event["calendar_id"],
            GhlCalendarSyncSetting.enabled.is_(True),
        )
        .first()
    )
    if setting is None:
        return {"ok": True, "skipped": True, "reason": "calendar_not_enabled"}

    row, _ = record_inbound_event(
        db,
        org_id=org_uuid,
        provider="ghl",
        event_id=event["event_id"],
        event_type=event["event_type"],
        payload=body,
    )
    if row.status == "done":
        return {"ok": True, "is_new": False, "fired_jobs": [], "deduped": True}
    try:
        result = _process_ghl_appointment_event(db, org_uuid, event, setting, body)
        mark_inbound_done(db, row)
        return result
    except HTTPException:
        raise
    except Exception as exc:
        mark_inbound_retry(db, row, str(exc))
        raise


def process_ghl_webhook_payload(db: Session, org_uuid: uuid.UUID, body: Dict[str, Any]) -> None:
    """Inbox retry processor for provider "ghl" (inbound_webhook_inbox.flush_due_inbound_webhooks).
    Same work as a live delivery, minus re-recording the inbox row."""
    if webhook_sweep_field(body, "sweep_event") == OPT_IN_EVENT:
        _process_ghl_opt_in(db, org_uuid, body)
        return
    event = normalize_ghl_appointment_event(body)
    if event is None:
        return
    setting = (
        db.query(GhlCalendarSyncSetting)
        .filter(
            GhlCalendarSyncSetting.org_id == org_uuid,
            GhlCalendarSyncSetting.calendar_id == event["calendar_id"],
            GhlCalendarSyncSetting.enabled.is_(True),
        )
        .first()
    )
    if setting is None:
        return
    _process_ghl_appointment_event(db, org_uuid, event, setting, body)


def _stamp_ghl_contact_id(client: Any, contact_id: Optional[str]) -> None:
    """Remember the GHL contact id so funnel lead intake can match this person even
    when the opt-in carries a different email. Never overwrites an existing id."""
    if not contact_id:
        return
    meta = client.meta if isinstance(client.meta, dict) else {}
    if meta.get("ghl_contact_id"):
        return
    client.meta = {**meta, "ghl_contact_id": contact_id}
    flag_modified(client, "meta")


def _process_ghl_appointment_event(
    db: Session,
    org_uuid: uuid.UUID,
    event: Dict[str, Any],
    setting: GhlCalendarSyncSetting,
    raw_payload: Dict[str, Any],
) -> Dict[str, Any]:
    attendee_email = event["attendee_email"]
    use_placeholder = not attendee_email

    start_time = _parse_iso_datetime(event["start_time"]) or datetime.now(timezone.utc)
    end_time = _parse_iso_datetime(event["end_time"])

    try:
        if use_placeholder:
            client = get_or_create_calendar_placeholder_client(db, org_uuid)
            attendee_email = client.email
            attendee_name = event.get("title") or "Calendar event"
        else:
            attendee_name = event["attendee_name"]
            client = ensure_client_for_booking_attendee(db, org_uuid, attendee_email, attendee_name)
    except Exception as e:  # pragma: no cover - defensive
        LOG.exception("ghl webhook: failed to resolve client for %s: %s", attendee_email, e)
        client = None
    if not client:
        return {"ok": True, "skipped": True, "reason": "no_matching_client"}
    if not use_placeholder:
        _stamp_ghl_contact_id(client, event.get("contact_id"))

    _, is_new = _upsert_check_in(
        db,
        org_id=org_uuid,
        client_id=client.id,
        provider="ghl",
        event_id=event["event_id"],
        event_uri=None,
        title=event.get("title"),
        start_time=start_time,
        end_time=end_time,
        location=None,
        meeting_url=None,
        attendee_email=attendee_email,
        attendee_name=attendee_name,
        event_type_id=event["calendar_id"],
        event_type_label=setting.calendar_name,
        cancelled=event["cancelled"],
        raw_payload=raw_payload,
    )
    db.commit()
    invalidate_terminal_monthly_trends_cache(org_uuid)
    try:
        from app.services.kpi_integration_sync import sync_kpi_for_datetime

        sync_kpi_for_datetime(db, org_uuid, start_time, commit=True)
    except Exception:
        LOG.exception("ghl webhook: KPI live sync failed")
    _run_pipeline_after_webhook(db, org_uuid, client.id)

    fired_jobs: list[str] = []
    if is_new and not event["cancelled"] and not use_placeholder:
        try:
            ids = on_booking_created_pre_sale(
                db,
                org_id=org_uuid,
                client_id=client.id,
                provider="ghl",
                external_booking_id=event["event_id"],
                event_type_id=event["calendar_id"],
                event_type_label=setting.calendar_name,
                attendee_email=attendee_email,
                start_time=start_time,
            )
            db.commit()
            fired_jobs = [str(x) for x in ids]
        except Exception as e:
            db.rollback()
            LOG.exception("ghl webhook: pre-sale automation failed: %s", e)

    LOG.info(
        "ghl webhook org=%s event=%s attendee=%s new=%s fired_jobs=%s",
        org_uuid, event["event_type"], normalize_email(attendee_email), is_new, len(fired_jobs),
    )
    return {"ok": True, "is_new": is_new, "fired_jobs": fired_jobs}

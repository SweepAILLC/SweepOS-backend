"""
Fathom webhook: new meeting content (no user session). Org is encoded in URL path.

Primary webhook URL (created via Integrations → Fathom save):
  POST {BACKEND_PUBLIC_URL}/integrations/fathom/webhook/{org_id}

Legacy alias (same handler logic):
  POST {BACKEND_PUBLIC_URL}/webhooks/fathom/{org_id}
"""
from __future__ import annotations

import json
import logging
import uuid

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Request, status
from sqlalchemy.orm import Session

from app.api.integrations import _ensure_org_fathom_webhook_columns, _verify_fathom_webhook_signature
from app.core.config import settings
from app.db.session import get_db
from app.models.organization import Organization
from app.services.fathom_client import resolve_fathom_api_key
from app.services.fathom_ingest import ingest_meeting_payload, queue_fathom_webhook_record_followups

logger = logging.getLogger(__name__)
router = APIRouter()

_MAX_WEBHOOK_BODY_BYTES = 2 * 1024 * 1024


def _extract_meeting_payload(body: dict) -> dict | None:
    if not isinstance(body, dict):
        return None
    for k in ("meeting", "recording", "data", "item"):
        v = body.get(k)
        if isinstance(v, dict) and (
            "recording_id" in v or "calendar_invitees" in v or "transcript" in v
        ):
            return v
    return body


@router.post("/fathom/{org_id}")
async def fathom_new_meeting_webhook(
    org_id: str,
    request: Request,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    try:
        org_uuid = uuid.UUID(org_id)
    except ValueError:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid org_id")

    if not resolve_fathom_api_key(db, org_uuid):
        return {"ok": True, "skipped": True, "reason": "no_fathom_api_key_for_org"}

    raw_body = await request.body()
    if len(raw_body) > _MAX_WEBHOOK_BODY_BYTES:
        raise HTTPException(status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE, detail="Payload too large")

    _ensure_org_fathom_webhook_columns(db)
    org = db.query(Organization).filter(Organization.id == org_uuid).first()
    secret = (
        (getattr(org, "fathom_webhook_secret", None) if org else None)
        or getattr(settings, "FATHOM_WEBHOOK_SECRET", None)
        or ""
    ).strip()
    if secret:
        headers = {k.lower(): v for k, v in request.headers.items()}
        if not _verify_fathom_webhook_signature(secret, headers, raw_body):
            logger.warning("Fathom webhook signature mismatch for org %s", org_id)
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid webhook signature")

    try:
        body = json.loads(raw_body)
    except Exception:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid JSON")

    meeting = _extract_meeting_payload(body)
    if not isinstance(meeting, dict):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Missing meeting payload")

    try:
        status_str, _cid, fathom_row_id = ingest_meeting_payload(db, org_uuid, meeting)
    except Exception:
        logger.exception("Fathom ingest failed for org %s", org_id)
        return {"ok": False, "status": "ingest_error"}

    if status_str in ("ok", "ok_unlinked") and fathom_row_id:
        queue_fathom_webhook_record_followups(background_tasks, org_uuid, fathom_row_id)
        logger.info(
            "Fathom webhook scheduled enrichment + follow-ups org=%s record=%s (status=%s)",
            org_id,
            fathom_row_id,
            status_str,
        )
    return {"ok": True, "status": status_str}

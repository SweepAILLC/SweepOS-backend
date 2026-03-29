"""
Fathom webhook: new meeting content (no user session). Org is encoded in URL path.
"""
from __future__ import annotations

import hashlib
import hmac
import logging
import uuid

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Request, status
from sqlalchemy.orm import Session

from app.core.config import settings
from app.db.session import get_db
from app.services.fathom_client import resolve_fathom_api_key
from app.services.fathom_ingest import ingest_meeting_payload
from app.services.call_insight_service import run_call_insight_background

logger = logging.getLogger(__name__)
router = APIRouter()

_MAX_WEBHOOK_BODY_BYTES = 2 * 1024 * 1024


@router.post("/fathom/{org_id}")
async def fathom_new_meeting_webhook(
    org_id: str,
    request: Request,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """
    Public endpoint for Fathom 'new meeting content ready'. Configure webhook URL as:
    {BACKEND_PUBLIC_URL}/webhooks/fathom/{org_id}
    """
    try:
        org_uuid = uuid.UUID(org_id)
    except ValueError:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid org_id")

    if not resolve_fathom_api_key(db, org_uuid):
        return {"ok": True, "skipped": True, "reason": "no_fathom_api_key_for_org"}

    raw_body = await request.body()
    if len(raw_body) > _MAX_WEBHOOK_BODY_BYTES:
        raise HTTPException(status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE, detail="Payload too large")

    webhook_secret = settings.FATHOM_WEBHOOK_SECRET
    if webhook_secret:
        sig_header = request.headers.get("x-fathom-signature") or request.headers.get("x-webhook-signature") or ""
        expected = hmac.new(webhook_secret.encode(), raw_body, hashlib.sha256).hexdigest()
        if not hmac.compare_digest(sig_header, expected):
            logger.warning("Fathom webhook signature mismatch for org %s", org_id)
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid webhook signature")

    try:
        import json
        body = json.loads(raw_body)
    except Exception:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid JSON")

    try:
        status_str, _cid, fathom_row_id = ingest_meeting_payload(db, org_uuid, body)
    except Exception:
        logger.exception("Fathom ingest failed for org %s", org_id)
        return {"ok": False, "status": "ingest_error"}

    if status_str == "ok" and fathom_row_id:
        background_tasks.add_task(
            run_call_insight_background, str(org_uuid), str(fathom_row_id)
        )
    return {"ok": True, "status": status_str}

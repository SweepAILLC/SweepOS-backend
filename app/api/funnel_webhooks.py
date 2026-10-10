"""Custom funnel webhooks: public intake + admin management.

Public:  POST /webhooks/funnels/{token}   (GET/HEAD answer URL-verification probes)
Admin:   GET|POST|DELETE /funnels/{funnel_id}/webhook
         PUT  /funnels/{funnel_id}/webhook/field-map
         POST /funnels/{funnel_id}/webhook/deliveries/{delivery_id}/retry

Design + reliability notes live in app.services.funnel_webhooks.
"""
from __future__ import annotations

import logging
import uuid
from typing import Any, Dict, Optional

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Request, status
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.api.deps import get_current_user
from app.core.config import settings
from app.db.session import get_db
from app.models.funnel import Funnel
from app.models.user import User
from app.services import funnel_webhooks as fw

LOG = logging.getLogger(__name__)

router = APIRouter()  # mounted at /webhooks
admin_router = APIRouter()  # mounted at /funnels


# ---------------------------------------------------------------------------
# Public intake
# ---------------------------------------------------------------------------

async def _read_capped_body(request: Request) -> bytes:
    cap = int(getattr(settings, "FUNNEL_WEBHOOK_MAX_BODY_BYTES", 262144) or 262144)
    declared = request.headers.get("content-length")
    if declared and declared.isdigit() and int(declared) > cap:
        raise HTTPException(status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE, detail="Payload too large")
    chunks = []
    size = 0
    async for chunk in request.stream():
        size += len(chunk)
        if size > cap:
            raise HTTPException(status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE, detail="Payload too large")
        chunks.append(chunk)
    return b"".join(chunks)


async def _resolve_or_404(db: Session, token: str, request: Request) -> fw.WebhookTarget:
    target = await run_in_threadpool(fw.resolve_target, db, token)
    if target is None:
        from app.core.request_ip import get_client_ip

        if not await run_in_threadpool(fw.try_acquire_bad_token, get_client_ip(request)):
            raise HTTPException(status_code=429, detail="Too many requests", headers={"Retry-After": "60"})
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Unknown webhook URL")
    return target


@router.api_route("/funnels/{token}", methods=["GET", "HEAD"])
async def funnel_webhook_probe(token: str, request: Request, db: Session = Depends(get_db)):
    """Some senders verify a URL with GET before saving it."""
    await _resolve_or_404(db, token, request)
    return {"ok": True}


@router.post("/funnels/{token}", status_code=status.HTTP_202_ACCEPTED)
async def funnel_webhook(
    token: str,
    request: Request,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    target = await _resolve_or_404(db, token, request)
    if not await run_in_threadpool(fw.try_acquire_rate, target):
        LOG.warning("funnel webhook rate limited funnel=%s org=%s", target.funnel_id, target.org_id)
        return JSONResponse(
            status_code=429,
            content={"detail": "Rate limit exceeded for this funnel; retry later"},
            headers={"Retry-After": "60"},
        )

    raw = await _read_capped_body(request)
    try:
        payload = fw.parse_body(request.headers.get("content-type"), raw)
    except fw.WebhookPayloadError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    lead = fw.normalize_payload(payload, target.field_map)
    if not lead.has_identity:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="No email, phone, name or instagram found in the payload; set a field map in Sweep.",
        )

    key = fw.dedupe_key(request.headers, payload)
    row_id, duplicate = await run_in_threadpool(fw.enqueue_delivery, db, target, lead, payload, key)
    if row_id is not None and fw.inline_enabled():
        background_tasks.add_task(fw.process_delivery_now, row_id)
    return {
        "ok": True,
        "id": str(row_id) if row_id else None,
        "duplicate": duplicate,
        "detected": lead.detected(),
    }


# ---------------------------------------------------------------------------
# Admin (org-scoped, integration managers only)
# ---------------------------------------------------------------------------

class FieldMapIn(BaseModel):
    field_map: Dict[str, str] = Field(default_factory=dict)


def _manager_funnel(db: Session, user: User, funnel_id: uuid.UUID) -> Funnel:
    from app.services.org_user_context import user_can_manage_org_integrations

    if not user_can_manage_org_integrations(user, db):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Admin or owner access required.")
    org_id = getattr(user, "selected_org_id", user.org_id)
    funnel = db.query(Funnel).filter(Funnel.id == funnel_id, Funnel.org_id == org_id).first()
    if funnel is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Funnel not found")
    return funnel


def _state(db: Session, funnel: Funnel, request: Request, token: Optional[str] = None) -> Dict[str, Any]:
    cfg = funnel.webhook_config or {}
    enabled = bool(funnel.webhook_token_hash)
    token = token or fw.current_token(funnel)
    return {
        "enabled": enabled,
        "url": fw.webhook_url(token, str(request.base_url)) if token else None,
        "token_prefix": cfg.get("token_prefix") if enabled else None,
        "created_at": cfg.get("created_at") if enabled else None,
        "field_map": cfg.get("field_map") or {},
        "map_targets": list(fw.MAP_TARGETS),
        "rate_limit_per_minute": int(getattr(settings, "FUNNEL_WEBHOOK_RATE_LIMIT_PER_MINUTE", 600) or 0),
        **fw.delivery_summary(db, funnel.org_id, funnel.id),
    }


@admin_router.get("/{funnel_id}/webhook")
def get_funnel_webhook(
    funnel_id: uuid.UUID,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    funnel = _manager_funnel(db, current_user, funnel_id)
    return _state(db, funnel, request)


@admin_router.post("/{funnel_id}/webhook")
def enable_or_rotate_funnel_webhook(
    funnel_id: uuid.UUID,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Create the webhook URL, or rotate it (the previous URL stops working)."""
    funnel = _manager_funnel(db, current_user, funnel_id)
    token = fw.enable_or_rotate(db, funnel)
    LOG.info("funnel webhook issued funnel=%s org=%s by=%s", funnel.id, funnel.org_id, current_user.id)
    return _state(db, funnel, request, token)


@admin_router.delete("/{funnel_id}/webhook")
def disable_funnel_webhook(
    funnel_id: uuid.UUID,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    funnel = _manager_funnel(db, current_user, funnel_id)
    fw.disable(db, funnel)
    LOG.info("funnel webhook disabled funnel=%s org=%s by=%s", funnel.id, funnel.org_id, current_user.id)
    return _state(db, funnel, request)


@admin_router.put("/{funnel_id}/webhook/field-map")
def put_funnel_webhook_field_map(
    funnel_id: uuid.UUID,
    body: FieldMapIn,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    funnel = _manager_funnel(db, current_user, funnel_id)
    try:
        fw.set_field_map(db, funnel, body.field_map)
    except fw.WebhookPayloadError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    return _state(db, funnel, request)


@admin_router.post("/{funnel_id}/webhook/deliveries/{delivery_id}/retry")
def retry_funnel_webhook_delivery(
    funnel_id: uuid.UUID,
    delivery_id: uuid.UUID,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    funnel = _manager_funnel(db, current_user, funnel_id)
    if not fw.retry_delivery(db, funnel.org_id, funnel.id, delivery_id):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No failed delivery with that id")
    background_tasks.add_task(fw.process_delivery_now, delivery_id)
    return {"ok": True}

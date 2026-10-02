"""GHL (GoHighLevel) integration — private integration token connect + contact sync.

Mirrors the Composio/Instagram private-credential pattern (app.api.instagram,
app.services.composio_client) rather than the OAuth-redirect providers, since GHL's
Private Integration is a static Bearer token pasted by the org, not an authorization
code flow. Credential writes (connect/disconnect) require admin/owner; read + sync
trigger are open to any org member, matching the Fathom sync endpoints.
"""
from __future__ import annotations

import logging
import uuid
from typing import Optional

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Response, status
from pydantic import BaseModel, Field
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.api.deps import get_current_user, get_db, require_admin_or_owner
from app.long_jobs import schedule_background_work
from app.models.ghl_calendar_sync_setting import GhlCalendarSyncSetting
from app.models.user import User
from app.services import ghl_client as gc
from app.services.ghl_sync_service import run_ghl_contact_sync_background

logger = logging.getLogger(__name__)
router = APIRouter()


class GhlConnectIn(BaseModel):
    api_key: str = Field(..., min_length=8, description="GHL Private Integration token")
    location_id: str = Field(..., min_length=3, description="GHL sub-account (location) ID")


class GhlStatusResponse(BaseModel):
    connected: bool
    location_id: Optional[str] = None
    last_sync_at: Optional[str] = None
    needs_reconnect: bool = False
    message: Optional[str] = None
    webhook_secret_set: bool = False


class GhlWebhookSecretResponse(BaseModel):
    secret: str
    header: str = "x-ghl-webhook-secret"


class GhlSyncResponse(BaseModel):
    started: bool
    message: Optional[str] = None


class GhlCalendarSyncSettingIn(BaseModel):
    calendar_id: str = Field(..., min_length=1)
    calendar_name: Optional[str] = None
    enabled: bool = True
    is_sales_call: bool = False


def _org_id(user: User) -> uuid.UUID:
    raw = getattr(user, "selected_org_id", None) or user.org_id
    return raw if isinstance(raw, uuid.UUID) else uuid.UUID(str(raw))


def _upstream_status(exc: gc.GhlApiError) -> int:
    # Never surface a raw 401 from GHL as our own session's 401 — the frontend
    # treats a 401 as "your Sweep session expired", not "reconnect GHL".
    if exc.status_code == 401:
        return status.HTTP_400_BAD_REQUEST
    return status.HTTP_502_BAD_GATEWAY


@router.post("/connect", response_model=GhlStatusResponse)
def connect_ghl(
    body: GhlConnectIn,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin_or_owner),
):
    org_id = _org_id(current_user)
    api_key = body.api_key.strip()
    location_id = body.location_id.strip()

    # Verify against GHL before persisting — a bad key must never silently save as "connected".
    try:
        gc.verify_ghl_connection(
            {
                "Authorization": f"Bearer {api_key}",
                "Version": gc.GHL_API_VERSION,
                "Accept": "application/json",
            },
            location_id,
        )
    except gc.GhlApiError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Could not verify GHL credentials. Check the private integration token and location ID.",
        ) from e

    try:
        row = gc.upsert_ghl_credentials(db, org_id, api_key=api_key, location_id=location_id)
    except gc.GhlConfigError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e)) from e

    return GhlStatusResponse(connected=True, location_id=row.account_id)


@router.delete("/disconnect")
def disconnect_ghl(
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin_or_owner),
):
    org_id = _org_id(current_user)
    deleted = gc.delete_ghl_credentials(db, org_id)
    return {"ok": True, "deleted": deleted}


@router.get("/status", response_model=GhlStatusResponse)
def get_ghl_status(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    org_id = _org_id(current_user)
    token_row = gc.get_ghl_token(db, org_id)
    if token_row is None:
        return GhlStatusResponse(connected=False)

    return GhlStatusResponse(
        connected=True,
        location_id=token_row.account_id,
        last_sync_at=token_row.last_sync_at.isoformat() if token_row.last_sync_at else None,
        webhook_secret_set=bool(token_row.webhook_secret),
    )


@router.post("/webhook-secret", response_model=GhlWebhookSecretResponse)
def rotate_webhook_secret(
    response: Response,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin_or_owner),
):
    """Generate (or rotate) the org's GHL webhook secret. Shown once: the client pastes
    it as the `x-ghl-webhook-secret` header on every GHL Workflow Webhook action.
    Rotating invalidates the old value immediately."""
    import secrets

    org_id = _org_id(current_user)
    secret = secrets.token_urlsafe(32)
    if gc.set_ghl_webhook_secret(db, org_id, secret) is None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Connect GoHighLevel first.")
    logger.info("ghl webhook secret rotated org=%s by user=%s", org_id, current_user.id)
    response.headers["Cache-Control"] = "no-store"
    return GhlWebhookSecretResponse(secret=secret)


@router.get("/calendars")
def list_calendars(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    org_id = _org_id(current_user)
    try:
        headers, location_id = gc.get_ghl_connection(db, org_id, user_id=current_user.id)
    except gc.GhlNotConnectedError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e)) from e

    try:
        calendars = gc.list_ghl_calendars(headers, location_id)
    except gc.GhlApiError as e:
        raise HTTPException(status_code=_upstream_status(e), detail=str(e)) from e

    settings_by_id = {
        row.calendar_id: row
        for row in db.query(GhlCalendarSyncSetting).filter(GhlCalendarSyncSetting.org_id == org_id).all()
    }
    return {
        "calendars": [
            {
                "id": c.get("id"),
                "name": c.get("name"),
                "enabled": settings_by_id[c["id"]].enabled if c.get("id") in settings_by_id else False,
                "is_sales_call": settings_by_id[c["id"]].is_sales_call if c.get("id") in settings_by_id else False,
            }
            for c in calendars
            if isinstance(c, dict) and c.get("id")
        ]
    }


@router.get("/forms")
def list_forms(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """GHL forms + surveys in the connected location, for a funnel's extra-forms picker."""
    org_id = _org_id(current_user)
    try:
        headers, location_id = gc.get_ghl_connection(db, org_id, user_id=current_user.id)
    except gc.GhlNotConnectedError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e)) from e
    try:
        return {"forms": gc.list_ghl_forms_and_surveys(headers, location_id)}
    except gc.GhlApiError as e:
        raise HTTPException(status_code=_upstream_status(e), detail=str(e)) from e


@router.get("/funnels")
def list_funnels(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """GHL funnels in the connected location, for the funnel-create picker. Each item
    says which Sweep funnel (if any) in this org is already paired to it."""
    from app.services.ghl_funnels import paired_funnels_by_ghl_id

    org_id = _org_id(current_user)
    try:
        headers, location_id = gc.get_ghl_connection(db, org_id, user_id=current_user.id)
    except gc.GhlNotConnectedError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e)) from e

    try:
        funnels = gc.list_ghl_funnels(headers, location_id)
    except gc.GhlApiError as e:
        raise HTTPException(status_code=_upstream_status(e), detail=str(e)) from e

    paired = paired_funnels_by_ghl_id(db, org_id)
    return {
        "funnels": [
            {
                "id": f["ghl_funnel_id"],
                "name": f["name"],
                "path": f["path"],
                "steps": [{"name": s["name"], "path": s["path"]} for s in f["steps"]],
                "paired_funnel_id": str(paired[f["ghl_funnel_id"]].id) if f["ghl_funnel_id"] in paired else None,
                "paired_funnel_name": paired[f["ghl_funnel_id"]].name if f["ghl_funnel_id"] in paired else None,
            }
            for f in funnels
        ]
    }


@router.put("/calendars/sync-settings")
def upsert_calendar_sync_setting(
    body: GhlCalendarSyncSettingIn,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin_or_owner),
):
    org_id = _org_id(current_user)
    calendar_id = body.calendar_id.strip()
    row = (
        db.query(GhlCalendarSyncSetting)
        .filter(GhlCalendarSyncSetting.org_id == org_id, GhlCalendarSyncSetting.calendar_id == calendar_id)
        .first()
    )
    if row is None:
        row = GhlCalendarSyncSetting(org_id=org_id, calendar_id=calendar_id)
        db.add(row)
        row.calendar_name = body.calendar_name
        row.enabled = body.enabled
        row.is_sales_call = body.is_sales_call
        try:
            db.commit()
        except IntegrityError:
            # Two admins toggled the same calendar at once; the other insert won the
            # race under our unique constraint — fall through and update its row instead.
            db.rollback()
            row = (
                db.query(GhlCalendarSyncSetting)
                .filter(GhlCalendarSyncSetting.org_id == org_id, GhlCalendarSyncSetting.calendar_id == calendar_id)
                .first()
            )
            row.calendar_name = body.calendar_name
            row.enabled = body.enabled
            row.is_sales_call = body.is_sales_call
            db.commit()
    else:
        row.calendar_name = body.calendar_name
        row.enabled = body.enabled
        row.is_sales_call = body.is_sales_call
        db.commit()
    return {
        "calendar_id": row.calendar_id,
        "enabled": row.enabled,
        "is_sales_call": row.is_sales_call,
    }


@router.delete("/calendars/sync-settings/{calendar_id}")
def delete_calendar_sync_setting(
    calendar_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin_or_owner),
):
    org_id = _org_id(current_user)
    row = (
        db.query(GhlCalendarSyncSetting)
        .filter(GhlCalendarSyncSetting.org_id == org_id, GhlCalendarSyncSetting.calendar_id == calendar_id)
        .first()
    )
    if row is None:
        return {"ok": True, "deleted": False}
    db.delete(row)
    db.commit()
    return {"ok": True, "deleted": True}


@router.post("/contacts/sync", response_model=GhlSyncResponse)
def sync_contacts(
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    org_id = _org_id(current_user)
    if gc.get_ghl_token(db, org_id) is None:
        return GhlSyncResponse(started=False, message="GHL is not connected for this organization.")

    schedule_background_work(run_ghl_contact_sync_background, background_tasks, str(org_id))
    return GhlSyncResponse(
        started=True,
        message="GHL contact sync started. This can take a few minutes for large contact lists.",
    )

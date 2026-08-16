"""Instagram Performance Intel API — per-org Composio credentials + OAuth + analytics."""
from __future__ import annotations

import logging
import uuid
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.responses import RedirectResponse
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.api.deps import (
    check_tab_access,
    get_current_user,
    get_db,
    require_admin_or_owner,
)
from app.core.config import settings
from app.core.rate_limit import check_sliding_window
from app.long_jobs import schedule_background_work
from app.models.oauth_token import OAuthProvider, OAuthToken
from app.models.user import User
from app.services import composio_client as cc
from app.services.composio_client import (
    ComposioConfigError,
    ComposioNotConnectedError,
    ComposioToolError,
)
from app.services.instagram_performance import build_instagram_performance
from app.services.instagram_sync_service import purge_instagram_cache, sync_instagram_for_org

logger = logging.getLogger(__name__)
router = APIRouter()


class ConnectResponse(BaseModel):
    redirect_url: str
    connection_request_id: Optional[str] = None


class ComposioCredentialsIn(BaseModel):
    api_key: str = Field(..., min_length=8)
    auth_config_id: str = Field(..., min_length=3, description="Composio Instagram auth config id (ac_…)")


class StatusResponse(BaseModel):
    connected: bool
    configured: bool = False
    composio_configured: bool = False
    auth_config_id: Optional[str] = None
    username: Optional[str] = None
    ig_user_id: Optional[str] = None
    followers_count: Optional[int] = None
    capabilities: Dict[str, Any] = Field(default_factory=dict)
    last_sync_at: Optional[str] = None
    message: Optional[str] = None


class SyncResponse(BaseModel):
    ok: bool
    queued: bool = True
    result: Dict[str, Any] = Field(default_factory=dict)
    message: Optional[str] = None
    cooldown_seconds: Optional[int] = None
    last_sync_at: Optional[str] = None


def _org_id(user: User) -> uuid.UUID:
    raw = getattr(user, "selected_org_id", None) or user.org_id
    return raw if isinstance(raw, uuid.UUID) else uuid.UUID(str(raw))


def _require_content_studio(db: Session, user: User) -> None:
    if not check_tab_access("content_studio", user, db):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Marketing Intel is not enabled for your organization.",
        )


def _callback_url() -> str:
    base = (settings.BACKEND_PUBLIC_URL or settings.MCP_ISSUER_URL or "http://localhost:8000").rstrip(
        "/"
    )
    return f"{base}/instagram/callback"


def _frontend_integrations_url(*, connected: bool = False, error: Optional[str] = None) -> str:
    front = (settings.FRONTEND_URL or "http://localhost:3002").rstrip("/")
    qs = "tab=integrations&instagram="
    qs += "connected" if connected else "error"
    if error:
        qs += f"&reason={error[:120]}"
    return f"{front}/?{qs}"


@router.put("/composio-credentials")
def put_composio_credentials(
    body: ComposioCredentialsIn,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin_or_owner),
):
    _require_content_studio(db, current_user)
    org_id = _org_id(current_user)
    try:
        cc.upsert_composio_credentials(
            db,
            org_id,
            api_key=body.api_key,
            auth_config_id=body.auth_config_id,
        )
    except ComposioConfigError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    return {
        "ok": True,
        "composio_configured": True,
        "auth_config_id": body.auth_config_id.strip(),
    }


@router.delete("/composio-credentials")
def delete_composio_credentials(
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin_or_owner),
):
    _require_content_studio(db, current_user)
    org_id = _org_id(current_user)
    deleted = cc.delete_composio_credentials(db, org_id)
    return {"ok": True, "composio_configured": False, "deleted": deleted}


@router.post("/connect", response_model=ConnectResponse)
def connect_instagram(
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin_or_owner),
):
    _require_content_studio(db, current_user)
    org_id = _org_id(current_user)
    if not cc.composio_configured(db, org_id):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Add your Composio API key and Instagram auth config ID in Integrations first.",
        )
    # If Composio already has an ACTIVE Instagram account for this org, bind it
    # and skip a fresh OAuth round-trip (avoids "multiple connected accounts" dead-ends).
    try:
        existing = cc.list_connected_account_ids(db, org_id)
    except Exception:
        existing = []
        logger.info("instagram connect: could not list existing accounts org=%s", org_id)
    if existing:
        ca_id = existing[0]
        cc.upsert_instagram_connection(db, org_id, connected_account_id=ca_id)
        try:
            info = cc.execute(db, org_id, "INSTAGRAM_GET_USER_INFO", {"ig_user_id": "me"})
            if isinstance(info, dict):
                data = info.get("data") if isinstance(info.get("data"), dict) else info
                if isinstance(data, list) and data:
                    data = data[0]
                if isinstance(data, dict):
                    ig_user_id = str(data.get("id") or "") or None
                    username = str(data.get("username") or "") or None
                    if ig_user_id:
                        cc.upsert_instagram_connection(
                            db,
                            org_id,
                            connected_account_id=ca_id,
                            ig_user_id=ig_user_id,
                            scope=f"instagram:{username}" if username else "composio_instagram",
                        )
        except Exception:
            logger.exception("instagram connect: reuse existing account user info failed org=%s", org_id)
        schedule_background_work(_sync_org_outside_session, None, org_id, True)
        return ConnectResponse(redirect_url=_frontend_integrations_url(connected=True))

    try:
        result = cc.start_instagram_link(db, org_id, callback_url=_callback_url())
    except ComposioConfigError as e:
        msg = str(e)
        logger.warning("instagram connect config error org=%s: %s", org_id, msg)
        # Missing package / server setup vs missing org credentials
        code = (
            status.HTTP_503_SERVICE_UNAVAILABLE
            if "not installed" in msg.lower()
            else status.HTTP_400_BAD_REQUEST
        )
        raise HTTPException(status_code=code, detail=msg) from e
    except ComposioToolError as e:
        logger.warning("instagram connect tool error org=%s: %s", org_id, e)
        raise HTTPException(status_code=502, detail=str(e)) from e
    except Exception as e:
        logger.exception("instagram connect unexpected error org=%s", org_id)
        raise HTTPException(
            status_code=502,
            detail=f"Failed to start Instagram connect: {e}",
        ) from e
    return ConnectResponse(
        redirect_url=result["redirect_url"],
        connection_request_id=result.get("connection_request_id"),
    )


@router.get("/callback")
def instagram_oauth_callback(
    db: Session = Depends(get_db),
    status_q: Optional[str] = Query(None, alias="status"),
    connected_account_id: Optional[str] = Query(None),
    user_id: Optional[str] = Query(None),
):
    """
    Composio redirects here after Instagram OAuth.

    We resolve the org from user_id (SweepOS org_id), persist the connected
    account id, and kick off the first sync in the background.
    """
    ca_id = connected_account_id
    if not ca_id:
        pass

    org_id: Optional[uuid.UUID] = None
    if user_id:
        try:
            org_id = uuid.UUID(str(user_id))
        except ValueError:
            org_id = None

    if org_id is None:
        return RedirectResponse(
            url=_frontend_integrations_url(connected=False, error="missing_org"),
            status_code=303,
        )

    try:
        if not cc.composio_configured(db, org_id):
            return RedirectResponse(
                url=_frontend_integrations_url(connected=False, error="no_composio_creds"),
                status_code=303,
            )
        if not ca_id:
            ids = cc.list_connected_account_ids(db, org_id)
            ca_id = ids[0] if ids else None
        if not ca_id:
            return RedirectResponse(
                url=_frontend_integrations_url(connected=False, error="no_account"),
                status_code=303,
            )

        # Persist connection first so execute() can run
        cc.upsert_instagram_connection(db, org_id, connected_account_id=ca_id)

        ig_user_id = None
        username = None
        try:
            info = cc.execute(db, org_id, "INSTAGRAM_GET_USER_INFO", {"ig_user_id": "me"})
            if isinstance(info, dict):
                data = info.get("data") if isinstance(info.get("data"), dict) else info
                if isinstance(data, list) and data:
                    data = data[0]
                if isinstance(data, dict):
                    ig_user_id = str(data.get("id") or "") or None
                    username = str(data.get("username") or "") or None
            if ig_user_id:
                cc.upsert_instagram_connection(
                    db,
                    org_id,
                    connected_account_id=ca_id,
                    ig_user_id=ig_user_id,
                    scope=f"instagram:{username}" if username else "composio_instagram",
                )
        except Exception:
            logger.exception("instagram callback: user info failed org=%s", org_id)

        schedule_background_work(_sync_org_outside_session, None, org_id, True)
    except Exception:
        logger.exception("instagram callback failed")
        return RedirectResponse(
            url=_frontend_integrations_url(connected=False, error="callback_failed"),
            status_code=303,
        )

    return RedirectResponse(
        url=_frontend_integrations_url(connected=True),
        status_code=303,
    )


def _sync_org_outside_session(org_id: uuid.UUID, full: bool = False) -> None:
    from app.db.session import SessionLocal

    db = SessionLocal()
    try:
        sync_instagram_for_org(db, org_id, full=full)
    except Exception:
        logger.exception("background instagram sync failed org=%s", org_id)
        try:
            db.rollback()
        except Exception:
            pass
    finally:
        db.close()


@router.get("/status", response_model=StatusResponse)
def instagram_status(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    _require_content_studio(db, current_user)
    org_id = _org_id(current_user)
    creds = cc.get_composio_credentials(db, org_id)
    configured = creds is not None
    auth_config_id = creds["auth_config_id"] if creds else None
    token = cc.get_instagram_token(db, org_id)
    if token is None:
        return StatusResponse(
            connected=False,
            configured=configured,
            composio_configured=configured,
            auth_config_id=auth_config_id,
            message=(
                None
                if configured
                else "Add your Composio API key and Instagram auth config ID in Integrations, then connect Instagram."
            ),
        )

    followers = None
    try:
        from app.models.instagram_account_snapshot import InstagramAccountSnapshot

        snap = (
            db.query(InstagramAccountSnapshot)
            .filter(InstagramAccountSnapshot.org_id == org_id)
            .order_by(InstagramAccountSnapshot.snapshot_date.desc())
            .first()
        )
        if snap:
            followers = snap.followers_count
    except Exception:
        pass

    username = None
    if token.scope and token.scope.startswith("instagram:"):
        username = token.scope.split(":", 1)[1] or None

    # Infer capabilities from recent media
    from app.models.instagram_media import InstagramMedia

    recent = (
        db.query(InstagramMedia)
        .filter(InstagramMedia.org_id == org_id)
        .order_by(InstagramMedia.posted_at.desc())
        .limit(30)
        .all()
    )
    ok = sum(1 for m in recent if m.insights_status in ("ok", "partial"))
    from app.services.instagram_sync_service import resolve_capabilities

    caps = resolve_capabilities(
        followers_count=followers,
        insights_ok_count=ok,
        insights_attempted=len(recent),
    )

    return StatusResponse(
        connected=True,
        configured=configured,
        composio_configured=configured,
        auth_config_id=auth_config_id,
        username=username,
        ig_user_id=token.account_id,
        followers_count=followers,
        capabilities=caps,
        last_sync_at=token.last_sync_at.isoformat() if token.last_sync_at else None,
        message=(
            None
            if configured
            else "Composio credentials missing — sync will fail until you re-save them in Integrations."
        ),
    )


@router.post("/sync", response_model=SyncResponse)
def sync_instagram(
    full: bool = Query(False),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Enqueue Instagram sync in the background (never blocks the HTTP request).

    Rate-limited: per-org cooldown + sliding window (defaults: 15 min / 3 per hour).
    """
    from datetime import datetime

    _require_content_studio(db, current_user)
    org_id = _org_id(current_user)
    cooldown = int(getattr(settings, "INSTAGRAM_MANUAL_SYNC_COOLDOWN_SEC", 900) or 900)
    cooldown = max(60, min(cooldown, 86400))
    max_per_hour = int(getattr(settings, "INSTAGRAM_MANUAL_SYNC_MAX_PER_HOUR", 3) or 3)
    max_per_hour = max(1, min(max_per_hour, 12))

    token = cc.get_instagram_token(db, org_id)
    if token is None:
        raise HTTPException(status_code=400, detail="Instagram is not connected for this org")
    if not cc.composio_configured(db, org_id):
        raise HTTPException(
            status_code=400,
            detail="Composio credentials missing — save them in Integrations first.",
        )

    last_sync = token.last_sync_at
    if last_sync is not None and cooldown > 0:
        # Normalize naive UTC timestamps from DB
        if getattr(last_sync, "tzinfo", None) is not None:
            last_naive = last_sync.replace(tzinfo=None)
        else:
            last_naive = last_sync
        elapsed = (datetime.utcnow() - last_naive).total_seconds()
        remaining = int(cooldown - elapsed)
        if remaining > 0:
            mins = max(1, (remaining + 59) // 60)
            raise HTTPException(
                status_code=429,
                detail={
                    "message": f"Sync is rate-limited. Try again in about {mins} minute{'s' if mins != 1 else ''}.",
                    "cooldown_seconds": remaining,
                    "last_sync_at": last_naive.isoformat(),
                },
            )

    check_sliding_window(
        f"ig_sync_{org_id}",
        max_requests=max_per_hour,
        window_seconds=3600,
        db=db,
        audit_user=current_user,
        endpoint_name="instagram_sync",
    )

    budget = int(getattr(settings, "INSTAGRAM_SYNC_BUDGET_SEC", 90) or 90)
    schedule_background_work(
        _sync_org_outside_session,
        None,
        org_id,
        full,
        prefer_rq=True,
        job_timeout=max(180, budget + 120),
    )
    return SyncResponse(
        ok=True,
        queued=True,
        result={"org_id": str(org_id), "full": full},
        message="Instagram sync queued. Metrics will refresh in the background shortly.",
        cooldown_seconds=cooldown,
        last_sync_at=token.last_sync_at.isoformat() if token.last_sync_at else None,
    )


@router.get("/performance")
def get_performance(
    days: int = Query(90, ge=7, le=365),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    _require_content_studio(db, current_user)
    org_id = _org_id(current_user)
    payload = build_instagram_performance(db, org_id, days=days)
    # Attach username from token scope when available
    token = cc.get_instagram_token(db, org_id)
    if token and token.scope and token.scope.startswith("instagram:"):
        payload["username"] = token.scope.split(":", 1)[1] or None
    return payload


@router.delete("/disconnect")
def disconnect_instagram(
    purge: bool = Query(True, description="Also delete cached media/snapshots"),
    clear_composio: bool = Query(
        False, description="Also remove stored Composio API key / auth config"
    ),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin_or_owner),
):
    _require_content_studio(db, current_user)
    org_id = _org_id(current_user)
    token = (
        db.query(OAuthToken)
        .filter(
            OAuthToken.org_id == org_id,
            OAuthToken.provider == OAuthProvider.INSTAGRAM,
        )
        .first()
    )
    if token:
        db.delete(token)
        db.commit()
    if purge:
        try:
            purge_instagram_cache(db, org_id)
        except Exception:
            logger.exception("instagram disconnect purge failed org=%s", org_id)
            db.rollback()
    composio_cleared = False
    if clear_composio:
        composio_cleared = cc.delete_composio_credentials(db, org_id)
    return {
        "ok": True,
        "connected": False,
        "composio_configured": cc.composio_configured(db, org_id),
        "composio_cleared": composio_cleared,
    }

"""
Whop integration: Company API key + company_id, payment sync, read APIs.
"""
from __future__ import annotations

import logging
import uuid
from datetime import datetime, timedelta, timezone
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.api.deps import get_current_user, require_admin_or_owner
from app.models.user import User
from app.models.oauth_token import OAuthToken, OAuthProvider
from app.models.whop_payment import WhopPayment
from app.core.encryption import encrypt_token
from app.schemas.whop import (
    WhopConnectRequest,
    WhopConnectionStatus,
    WhopPaymentOut,
    WhopSummaryOut,
    WhopRevenueTimelinePoint,
    WhopRevenueTimelineResponse,
)
from app.services import whop_client
from app.services.whop_sync import _payer_email, sync_whop_incremental

logger = logging.getLogger(__name__)

router = APIRouter()


def _org_id(user: User) -> uuid.UUID:
    return getattr(user, "selected_org_id", user.org_id)


def check_whop_connected(db: Session, org_id: uuid.UUID) -> bool:
    t = (
        db.query(OAuthToken)
        .filter(OAuthToken.org_id == org_id, OAuthToken.provider == OAuthProvider.WHOP)
        .first()
    )
    return bool(t and t.access_token)


@router.get("/status", response_model=WhopConnectionStatus)
def whop_status(db: Session = Depends(get_db), current_user: User = Depends(get_current_user)):
    org_id = _org_id(current_user)
    t = (
        db.query(OAuthToken)
        .filter(OAuthToken.org_id == org_id, OAuthToken.provider == OAuthProvider.WHOP)
        .first()
    )
    if not t:
        return WhopConnectionStatus(connected=False, webhook_active=False, message="Whop is not connected.")
    return WhopConnectionStatus(
        connected=True,
        company_id=t.account_id,
        webhook_active=bool(t.webhook_endpoint_id and t.webhook_secret),
        message="Whop is connected.",
    )


@router.post("/connect", status_code=status.HTTP_200_OK)
def whop_connect(
    body: WhopConnectRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin_or_owner),
):
    org_id = _org_id(current_user)

    # Same 3-attempts/15-minute guard as the Stripe/Brevo direct-API-key-connect
    # endpoints — this one was missing it, letting an authenticated admin hammer
    # Whop's credential-validation API (and this endpoint) without limit.
    from app.core.rate_limit import _rate_limit_store, _rate_limit_lock, _cleanup_old_entries

    _cleanup_old_entries()
    identifier = f"whop_connect_{current_user.id}_{org_id}"
    now = datetime.utcnow()
    window_start = now - timedelta(seconds=900)
    with _rate_limit_lock:
        if identifier not in _rate_limit_store:
            _rate_limit_store[identifier] = []
        recent_requests = [ts for ts, _ in _rate_limit_store[identifier] if ts > window_start]
        if len(recent_requests) >= 3:
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail="Rate limit exceeded: 3 Whop connection attempts per 15 minutes. Please try again later.",
            )
        _rate_limit_store[identifier].append((now, 1))

    try:
        whop_client.validate_credentials(body.api_key.strip(), body.company_id.strip())
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Could not validate Whop credentials: {e}")

    try:
        enc = encrypt_token(body.api_key.strip())
    except ValueError as e:
        logger.warning("Whop connect: encryption failed: %s", e)
        raise HTTPException(
            status_code=503,
            detail="Server encryption is not configured (set ENCRYPTION_KEY). Cannot store API keys.",
        )

    existing = (
        db.query(OAuthToken)
        .filter(OAuthToken.org_id == org_id, OAuthToken.provider == OAuthProvider.WHOP)
        .first()
    )
    if existing:
        existing.access_token = enc
        existing.account_id = body.company_id.strip()
        existing.refresh_token = None
        existing.expires_at = None
        existing.scope = "company_api_key"
    else:
        db.add(
            OAuthToken(
                org_id=org_id,
                provider=OAuthProvider.WHOP,
                account_id=body.company_id.strip(),
                access_token=enc,
                refresh_token=None,
                expires_at=None,
                scope="company_api_key",
            )
        )
    try:
        db.commit()
    except SQLAlchemyError as e:
        db.rollback()
        logger.exception("Whop connect: database error for org_id=%s", org_id)
        hint = (
            "Could not save Whop credentials. Run database migrations so the OAuth enum includes "
            "'whop' and table whop_payments exists: `cd backend && alembic upgrade head`. "
        )
        raise HTTPException(status_code=503, detail=f"{hint}({e.__class__.__name__})") from e

    from app.services.whop_webhook_onboard import ensure_whop_webhook_for_org

    webhook_result = ensure_whop_webhook_for_org(org_id, db=db, force=True)

    import threading

    def bg():
        from app.db.session import SessionLocal

        bg_db = SessionLocal()
        try:
            sync_whop_incremental(bg_db, org_id=org_id, force_full=True)
        finally:
            bg_db.close()

    threading.Thread(target=bg, daemon=True).start()
    message = "Whop connected. Initial sync started in the background."
    if webhook_result.get("webhook_active"):
        message = "Whop connected. Webhook registered; new payments ingest automatically. Initial sync started."
    elif webhook_result.get("message"):
        message = f"Whop connected. {webhook_result.get('message')} Initial sync started."
    elif webhook_result.get("error"):
        message = f"Whop connected. Webhook not registered ({webhook_result.get('error')}). Use Sync payments as fallback. Initial sync started."
    return {
        "success": True,
        "message": message,
        "webhook_active": bool(webhook_result.get("webhook_active")),
        "webhook": webhook_result,
    }


@router.post("/disconnect", status_code=status.HTTP_200_OK)
def whop_disconnect(db: Session = Depends(get_db), current_user: User = Depends(require_admin_or_owner)):
    org_id = _org_id(current_user)
    from app.services.whop_webhook_onboard import delete_whop_webhook_for_org

    delete_whop_webhook_for_org(org_id, db=db)
    db.query(OAuthToken).filter(
        OAuthToken.org_id == org_id,
        OAuthToken.provider == OAuthProvider.WHOP,
    ).delete()
    db.commit()
    return {"success": True}


@router.post("/repair-webhook", status_code=status.HTTP_200_OK)
def whop_repair_webhook(
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin_or_owner),
):
    org_id = _org_id(current_user)
    if not check_whop_connected(db, org_id):
        raise HTTPException(status_code=404, detail="Whop not connected.")
    from app.services.whop_webhook_onboard import ensure_whop_webhook_for_org

    result = ensure_whop_webhook_for_org(org_id, db=db, force=True)
    if not result.get("success") and result.get("error"):
        raise HTTPException(status_code=400, detail=result["error"])
    return result


@router.post("/sync", status_code=status.HTTP_200_OK)
def whop_sync(
    force_full: bool = Query(False),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    org_id = _org_id(current_user)
    if not check_whop_connected(db, org_id):
        raise HTTPException(status_code=404, detail="Whop not connected.")
    from app.services.whop_webhook_onboard import ensure_whop_webhook_for_org

    token = (
        db.query(OAuthToken)
        .filter(OAuthToken.org_id == org_id, OAuthToken.provider == OAuthProvider.WHOP)
        .first()
    )
    if token and not (token.webhook_endpoint_id and token.webhook_secret):
        ensure_whop_webhook_for_org(org_id, db=db, force=False)
    out = sync_whop_incremental(db, org_id=org_id, force_full=force_full)
    if out.get("error"):
        raise HTTPException(status_code=400, detail=out["error"])
    return out


def _whop_succeeded(p: WhopPayment) -> bool:
    from app.services.whop_sync import WHOP_PAID_STATUSES

    return (p.status or "").lower() in WHOP_PAID_STATUSES


@router.get("/summary", response_model=WhopSummaryOut)
def whop_summary(db: Session = Depends(get_db), current_user: User = Depends(get_current_user)):
    org_id = _org_id(current_user)
    if not check_whop_connected(db, org_id):
        raise HTTPException(status_code=404, detail="Whop not connected.")
    now = datetime.utcnow()
    start_30 = now - timedelta(days=30)
    start_mtd = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    cnt = db.query(WhopPayment).filter(WhopPayment.org_id == org_id).count()

    def sum_paid_since(since: datetime) -> int:
        s = 0
        for p in db.query(WhopPayment).filter(WhopPayment.org_id == org_id, WhopPayment.created_at >= since).all():
            if _whop_succeeded(p):
                s += p.amount_cents or 0
        return s

    return WhopSummaryOut(
        payment_count=cnt,
        succeeded_cents_30d=sum_paid_since(start_30),
        succeeded_cents_mtd=sum_paid_since(start_mtd),
    )


@router.get("/payments", response_model=List[WhopPaymentOut])
def whop_payments(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    org_id = _org_id(current_user)
    if not check_whop_connected(db, org_id):
        raise HTTPException(status_code=404, detail="Whop not connected.")
    q = (
        db.query(WhopPayment)
        .filter(WhopPayment.org_id == org_id)
        .order_by(WhopPayment.created_at.desc())
        .offset((page - 1) * page_size)
        .limit(page_size)
    )
    out: List[WhopPaymentOut] = []
    for p in q.all():
        raw = p.raw if isinstance(p.raw, dict) else {}
        email = _payer_email(raw) if raw else None
        ts = int(p.created_at.timestamp()) if p.created_at else 0
        out.append(
            WhopPaymentOut(
                id=str(p.id),
                whop_id=p.whop_id,
                amount_cents=p.amount_cents or 0,
                currency=p.currency or "usd",
                status=p.status,
                client_id=str(p.client_id) if p.client_id else None,
                payer_email=email,
                created_at=ts,
            )
        )
    return out


@router.get("/revenue-timeline", response_model=WhopRevenueTimelineResponse)
def whop_revenue_timeline(
    range_days: int = Query(30, ge=1, le=365),
    group_by: str = Query("day"),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    org_id = _org_id(current_user)
    if not check_whop_connected(db, org_id):
        raise HTTPException(status_code=404, detail="Whop not connected.")
    since = datetime.utcnow() - timedelta(days=range_days)
    gb = group_by if group_by in ("day", "week") else "day"
    rows = (
        db.query(WhopPayment)
        .filter(WhopPayment.org_id == org_id, WhopPayment.created_at >= since)
        .order_by(WhopPayment.created_at.asc())
        .all()
    )
    buckets: dict[str, int] = {}
    for p in rows:
        if not _whop_succeeded(p):
            continue
        dt = p.created_at or datetime.utcnow()
        if gb == "week":
            iso = dt.isocalendar()
            key = f"{iso[0]}-W{iso[1]:02d}"
            label = key
        else:
            key = dt.strftime("%Y-%m-%d")
            label = key
        buckets[key] = buckets.get(key, 0) + (p.amount_cents or 0)
    timeline = [
        WhopRevenueTimelinePoint(t=0, label=k, amount_cents=v)
        for k, v in sorted(buckets.items(), key=lambda x: x[0])
    ]
    return WhopRevenueTimelineResponse(timeline=timeline, group_by=gb)

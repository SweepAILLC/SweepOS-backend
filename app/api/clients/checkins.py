"""Clients API — checkins routes."""
from __future__ import annotations

import logging
import re
import uuid
from datetime import datetime, timedelta, timezone
from threading import Lock as ThreadingLock
from typing import List, Optional, Tuple
from uuid import UUID

import httpx
from fastapi import APIRouter, BackgroundTasks, Body, Depends, HTTPException, Query, Request, status
from fastapi.security import HTTPAuthorizationCredentials
from sqlalchemy import and_, desc, func, or_
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, defer
from sqlalchemy.orm.attributes import flag_modified
from starlette.concurrency import run_in_threadpool

from app.api.deps import get_current_user, security
from app.api.clients.helpers import (
    LOG,
    WHOP_PAID_STATUSES,
    effective_org_id,
    merge_client_meta_from_duplicates,
    normalize_email,
    client_created_sort_key,
    load_whop_payments,
    org_checkin_sync_lock,
    refresh_call_insights_after_checkin_sync,
    scope_org_id,
    sync_check_ins_in_worker,
    user_pipeline_priorities,
    brevo_merged_stats_for_client,
    fetch_brevo_email_stats,
    merge_brevo_stats,
)
from app.core.config import settings
from app.core.rate_limit import check_sliding_window
from app.db.session import get_db, SessionLocal
from app.long_jobs import schedule_background_work
from app.models.calendar_booking_sales import CalendarBookingSales
from app.models.client import Client, LifecycleState
from app.models.client_checkin import ClientCheckIn
from app.models.manual_payment import ManualPayment
from app.models.organization import Organization
from app.models.stripe_payment import StripePayment
from app.models.stripe_subscription import StripeSubscription
from app.models.stripe_treasury_transaction import StripeTreasuryTransaction, TreasuryTransactionStatus
from app.models.user import User
from app.models.whop_payment import WhopPayment
from app.utils.stripe_helpers import extract_email_from_payment_raw
from app.utils.stripe_ids import normalize_stripe_id_for_dedup

router = APIRouter()


from app.services.client_automation import process_pipeline_lifecycle_for_client


# Check-in endpoints - MUST be before /{client_id}/check-ins to avoid route conflicts
@router.post("/check-ins/sync")
async def sync_check_ins(
    background_tasks: BackgroundTasks,
    apply_pipeline_rules: bool = Query(
        True,
        description="When false, sync calendar rows only — skip automated pipeline stage moves.",
    ),
    force_lifecycle: bool = Query(
        False,
        description="When true with pipeline rules, bypass 14-day manual column-move protection.",
    ),
    credentials: HTTPAuthorizationCredentials = Depends(security),
):
    """
    Sync calendar events (Cal.com/Calendly) with clients by matching attendee emails.
    Creates or updates check-in records for matching clients.

    Runs the heavy sync in a worker thread with dedicated DB session(s) so we do not hold two
    pool connections (get_current_user + route get_db) for the entire Cal.com/Calendly pull.
    """
    token = credentials.credentials
    print("[CHECKIN SYNC API] ===== ENDPOINT CALLED ===== (async + worker pool)")

    try:
        results = await run_in_threadpool(
            sync_check_ins_in_worker,
            token,
            apply_pipeline_lifecycle_rules=apply_pipeline_rules,
            force_lifecycle=force_lifecycle,
        )
    except ImportError as e:
        import traceback

        error_trace = traceback.format_exc()
        print(f"[CHECKIN SYNC API] ❌ Import error: {str(e)}")
        print(f"[CHECKIN SYNC API] Traceback:\n{error_trace}")
        traceback.print_exc()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to import checkin_sync service: {str(e)}",
        )
    except HTTPException:
        raise
    except Exception as e:
        import traceback

        error_trace = traceback.format_exc()
        print(f"[CHECKIN SYNC API] ❌ Error: {str(e)}")
        print(f"[CHECKIN SYNC API] Traceback:\n{error_trace}")
        traceback.print_exc()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error syncing check-ins: {str(e)}",
        )

    affected_client_ids = results.get("affected_client_ids", [])
    org_id_str = results.get("sync_org_id")
    if affected_client_ids and org_id_str:
        cids = [str(x) for x in affected_client_ids]
        print(f"[CHECKIN SYNC API] Queuing {len(cids)} call-insight refreshes (batched, deferred)...")
        schedule_background_work(
            refresh_call_insights_after_checkin_sync,
            background_tasks,
            org_id_str,
            cids,
        )

    print("[CHECKIN SYNC API] ✅ Sync completed successfully")
    return {
        "success": True,
        "message": f"Synced {results['total']} check-ins",
        "calcom": results["calcom"],
        "calendly": results["calendly"],
        "total": results["total"],
    }


@router.get("/{client_id}/check-ins")
def get_client_check_ins(
    client_id: str,
    limit: int = Query(50, ge=1, le=100),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Get check-in history for a client, ordered by start_time (most recent first).
    """
    try:
        client_uuid = UUID(client_id)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid client ID format"
        )
    
    org_id = effective_org_id(current_user)
    # Verify client exists and belongs to user's org
    client = db.query(Client).filter(
        Client.id == client_uuid,
        Client.org_id == org_id
    ).first()
    
    if not client:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Client not found"
        )
    
    # Get check-ins for this client (defer optional columns so this works when migration 029 not applied)
    check_ins = db.query(ClientCheckIn).options(
        defer(ClientCheckIn.is_sales_call),
        defer(ClientCheckIn.sale_closed),
    ).filter(
        ClientCheckIn.client_id == client_uuid,
        ClientCheckIn.org_id == org_id
    ).order_by(desc(ClientCheckIn.start_time)).limit(limit).all()

    def _get_sales_flags(c):
        try:
            return getattr(c, "is_sales_call", False), getattr(c, "sale_closed", None)
        except Exception:
            return False, None

    return [
        {
            "id": str(checkin.id),
            "event_id": checkin.event_id,
            "event_uri": checkin.event_uri,
            "provider": checkin.provider,
            "title": checkin.title,
            "start_time": checkin.start_time.isoformat() if checkin.start_time else None,
            "end_time": checkin.end_time.isoformat() if checkin.end_time else None,
            "location": checkin.location,
            "meeting_url": checkin.meeting_url,
            "attendee_email": checkin.attendee_email,
            "attendee_name": checkin.attendee_name,
            "completed": checkin.completed,
            "cancelled": checkin.cancelled,
            "no_show": getattr(checkin, "no_show", False),
            "is_sales_call": is_sc,
            "sale_closed": sale_cl,
            "created_at": checkin.created_at.isoformat() if checkin.created_at else None,
        }
        for checkin in check_ins
        for is_sc, sale_cl in [_get_sales_flags(checkin)]
    ]


@router.patch("/check-ins/{check_in_id}")
def update_check_in(
    check_in_id: str,
    completed: Optional[bool] = Body(None),
    cancelled: Optional[bool] = Body(None),
    no_show: Optional[bool] = Body(None),
    is_sales_call: Optional[bool] = Body(None),
    sale_closed: Optional[bool] = Body(None),
    start_time: Optional[str] = Body(None),
    end_time: Optional[str] = Body(None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Update a check-in (mark as completed/cancelled/no-show/sales call/sale closed).
    Accepts JSON body with optional 'completed', 'cancelled', 'no_show', 'is_sales_call', 'sale_closed', 'start_time', 'end_time'.
    """
    try:
        check_in_uuid = UUID(check_in_id)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid check-in ID format"
        )
    
    org_id = effective_org_id(current_user)
    # Get check-in and verify it belongs to user's org (defer optional columns for pre-029 DBs)
    check_in = db.query(ClientCheckIn).options(
        defer(ClientCheckIn.is_sales_call),
        defer(ClientCheckIn.sale_closed),
    ).filter(
        and_(
            ClientCheckIn.id == check_in_uuid,
            ClientCheckIn.org_id == org_id
        )
    ).first()

    if not check_in:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Check-in not found"
        )

    # Update fields
    if completed is not None:
        check_in.completed = completed
    if cancelled is not None:
        check_in.cancelled = cancelled
    if no_show is not None:
        check_in.no_show = no_show
    if is_sales_call is not None:
        check_in.is_sales_call = is_sales_call
    if sale_closed is not None:
        check_in.sale_closed = sale_closed

    # Mirror sales flags to CalendarBookingSales so they survive provider
    # re-syncs (the sync reads from that table for new rows).
    if (is_sales_call is not None or sale_closed is not None) and getattr(check_in, "provider", None) in ("calcom", "calendly"):
        _event_id = check_in.event_id or ""
        if _event_id:
            sales_row = db.query(CalendarBookingSales).filter(
                CalendarBookingSales.org_id == org_id,
                CalendarBookingSales.provider == check_in.provider,
                CalendarBookingSales.event_id == _event_id,
            ).first()
            if sales_row:
                if is_sales_call is not None:
                    sales_row.is_sales_call = is_sales_call
                if sale_closed is not None:
                    sales_row.sale_closed = sale_closed
            else:
                sales_row = CalendarBookingSales(
                    org_id=org_id,
                    provider=check_in.provider,
                    event_id=_event_id,
                    is_sales_call=is_sales_call if is_sales_call is not None else False,
                    sale_closed=sale_closed,
                )
                db.add(sales_row)

    # Allow rescheduling only for manually created check-ins.
    if start_time is not None or end_time is not None:
        if getattr(check_in, "provider", None) != "manual":
            raise HTTPException(status_code=400, detail="Only manual check-ins can be rescheduled")

        def _parse_iso_dt(s: str) -> datetime:
            # Handle Cal.com / client inputs with trailing 'Z'
            return datetime.fromisoformat(s.replace("Z", "+00:00"))

        if start_time is not None:
            check_in.start_time = _parse_iso_dt(start_time)
        if end_time is not None:
            check_in.end_time = _parse_iso_dt(end_time)
    
    check_in.updated_at = datetime.now(timezone.utc)
    
    try:
        db.commit()
        db.refresh(check_in)

        if check_in.client_id and (is_sales_call is not None or sale_closed is not None):
            client = db.query(Client).filter(
                Client.id == check_in.client_id,
                Client.org_id == org_id,
            ).first()
            if client and process_pipeline_lifecycle_for_client(db, client):
                db.commit()
        
        return {
            "id": str(check_in.id),
            "event_id": check_in.event_id,
            "event_uri": check_in.event_uri,
            "provider": check_in.provider,
            "title": check_in.title,
            "start_time": check_in.start_time.isoformat() if check_in.start_time else None,
            "end_time": check_in.end_time.isoformat() if check_in.end_time else None,
            "location": check_in.location,
            "meeting_url": check_in.meeting_url,
            "attendee_email": check_in.attendee_email,
            "attendee_name": check_in.attendee_name,
            "completed": check_in.completed,
            "cancelled": check_in.cancelled,
            "no_show": getattr(check_in, "no_show", False),
            "is_sales_call": getattr(check_in, "is_sales_call", False),
            "sale_closed": getattr(check_in, "sale_closed", None),
            "created_at": check_in.created_at.isoformat() if check_in.created_at else None,
        }
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to update check-in: {str(e)}"
        )


@router.get("/check-ins/{check_in_id}")
def get_check_in(
    check_in_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Fetch a single check-in (used by calendar modal for manual events)."""
    try:
        check_in_uuid = UUID(check_in_id)
    except ValueError:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid check-in ID format")

    org_id = effective_org_id(current_user)
    check_in = db.query(ClientCheckIn).options(
        defer(ClientCheckIn.is_sales_call),
        defer(ClientCheckIn.sale_closed),
    ).filter(
        and_(
            ClientCheckIn.id == check_in_uuid,
            ClientCheckIn.org_id == org_id,
        )
    ).first()
    if not check_in:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Check-in not found")

    calcom_uid = None
    if check_in.provider == "calcom" and getattr(check_in, "raw_event_data", None):
        try:
            import json as _json

            raw = _json.loads(check_in.raw_event_data)
            if isinstance(raw, dict):
                u = raw.get("uid")
                if not u and isinstance(raw.get("data"), dict):
                    u = raw["data"].get("uid")
                calcom_uid = str(u).strip() if u else None
        except Exception:
            calcom_uid = None

    return {
        "id": str(check_in.id),
        "event_id": check_in.event_id,
        "event_uri": check_in.event_uri,
        "provider": check_in.provider,
        "title": check_in.title,
        "start_time": check_in.start_time.isoformat() if check_in.start_time else None,
        "end_time": check_in.end_time.isoformat() if check_in.end_time else None,
        "location": check_in.location,
        "meeting_url": check_in.meeting_url,
        "attendee_email": check_in.attendee_email,
        "attendee_name": check_in.attendee_name,
        "completed": check_in.completed,
        "cancelled": check_in.cancelled,
        "no_show": getattr(check_in, "no_show", False),
        "is_sales_call": getattr(check_in, "is_sales_call", False),
        "sale_closed": getattr(check_in, "sale_closed", None),
        "created_at": check_in.created_at.isoformat() if check_in.created_at else None,
        "calcom_uid": calcom_uid,
    }


@router.delete("/check-ins/{check_in_id}")
def delete_check_in(
    check_in_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Delete a check-in.
    """
    try:
        check_in_uuid = UUID(check_in_id)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid check-in ID format"
        )
    
    org_id = effective_org_id(current_user)
    # Get check-in and verify it belongs to user's org (defer optional columns for pre-029 DBs)
    check_in = db.query(ClientCheckIn).options(
        defer(ClientCheckIn.is_sales_call),
        defer(ClientCheckIn.sale_closed),
    ).filter(
        and_(
            ClientCheckIn.id == check_in_uuid,
            ClientCheckIn.org_id == org_id
        )
    ).first()

    if not check_in:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Check-in not found"
        )
    
    try:
        db.delete(check_in)
        db.commit()
        return {"success": True, "message": "Check-in deleted successfully"}
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to delete check-in: {str(e)}"
        )


@router.post("/{client_id}/check-ins")
def create_manual_check_in(
    client_id: str,
    title: str = Body(...),
    start_time: str = Body(...),
    end_time: Optional[str] = Body(None),
    completed: Optional[bool] = Body(False),
    cancelled: Optional[bool] = Body(False),
    no_show: Optional[bool] = Body(False),
    is_sales_call: Optional[bool] = Body(False),
    sale_closed: Optional[bool] = Body(None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Create a manual check-in for a client (for days without calendar bookings).
    Optional status: completed, cancelled, no_show, is_sales_call (default False); sale_closed (default None).
    """
    try:
        client_uuid = UUID(client_id)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid client ID format"
        )
    
    org_id = effective_org_id(current_user)
    # Verify client exists and belongs to user's org
    client = db.query(Client).filter(
        Client.id == client_uuid,
        Client.org_id == org_id
    ).first()
    
    if not client:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Client not found"
        )
    
    # Parse datetime strings
    try:
        start_datetime = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
        end_datetime = datetime.fromisoformat(end_time.replace('Z', '+00:00')) if end_time else None
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid datetime format: {str(e)}"
        )
    
    # Create manual check-in
    manual_check_in = ClientCheckIn(
        org_id=org_id,
        client_id=client_uuid,
        event_id=f"manual_{uuid.uuid4()}",  # Generate unique ID for manual check-ins
        provider="manual",
        title=title,
        start_time=start_datetime,
        end_time=end_datetime,
        attendee_email=client.email or "",
        attendee_name=f"{client.first_name} {client.last_name}".strip(),
        completed=completed or False,
        cancelled=cancelled or False,
        no_show=no_show or False,
        is_sales_call=is_sales_call or False,
        sale_closed=sale_closed,
    )
    
    try:
        db.add(manual_check_in)
        db.commit()
        db.refresh(manual_check_in)

        if is_sales_call and process_pipeline_lifecycle_for_client(db, client):
            db.commit()
        
        return {
            "id": str(manual_check_in.id),
            "event_id": manual_check_in.event_id,
            "event_uri": manual_check_in.event_uri,
            "provider": manual_check_in.provider,
            "title": manual_check_in.title,
            "start_time": manual_check_in.start_time.isoformat() if manual_check_in.start_time else None,
            "end_time": manual_check_in.end_time.isoformat() if manual_check_in.end_time else None,
            "location": manual_check_in.location,
            "meeting_url": manual_check_in.meeting_url,
            "attendee_email": manual_check_in.attendee_email,
            "attendee_name": manual_check_in.attendee_name,
            "completed": manual_check_in.completed,
            "cancelled": manual_check_in.cancelled,
            "no_show": getattr(manual_check_in, "no_show", False),
            "is_sales_call": getattr(manual_check_in, "is_sales_call", False),
            "sale_closed": getattr(manual_check_in, "sale_closed", None),
            "created_at": manual_check_in.created_at.isoformat() if manual_check_in.created_at else None,
        }
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create check-in: {str(e)}"
        )


@router.get("/{client_id}/check-ins/next")
def get_next_check_in(
    client_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Get the next upcoming check-in for a client.
    Returns None if no upcoming check-ins.
    """
    try:
        client_uuid = UUID(client_id)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid client ID format"
        )
    
    org_id = effective_org_id(current_user)
    # Verify client exists and belongs to user's org
    client = db.query(Client).filter(
        Client.id == client_uuid,
        Client.org_id == org_id
    ).first()
    
    if not client:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Client not found"
        )
    
    # Get next upcoming check-in (defer optional columns for pre-029 DBs)
    now = datetime.now(timezone.utc)
    next_checkin = db.query(ClientCheckIn).options(
        defer(ClientCheckIn.is_sales_call),
        defer(ClientCheckIn.sale_closed),
    ).filter(
        ClientCheckIn.client_id == client_uuid,
        ClientCheckIn.org_id == org_id,
        ClientCheckIn.completed == False,
        ClientCheckIn.cancelled == False,
        ClientCheckIn.no_show == False,
        ClientCheckIn.start_time > now
    ).order_by(ClientCheckIn.start_time).first()
    
    if not next_checkin:
        return None
    
    return {
        "id": str(next_checkin.id),
        "event_id": next_checkin.event_id,
        "event_uri": next_checkin.event_uri,
        "provider": next_checkin.provider,
        "title": next_checkin.title,
        "start_time": next_checkin.start_time.isoformat() if next_checkin.start_time else None,
        "end_time": next_checkin.end_time.isoformat() if next_checkin.end_time else None,
        "location": next_checkin.location,
        "meeting_url": next_checkin.meeting_url,
        "attendee_email": next_checkin.attendee_email,
        "attendee_name": next_checkin.attendee_name,
    }


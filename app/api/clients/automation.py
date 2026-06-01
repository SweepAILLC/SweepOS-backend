"""Clients API — automation routes."""
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


from app.services.client_automation import process_client_automation


@router.post("/automation/process", status_code=status.HTTP_200_OK)
def process_client_automation_endpoint(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Manually trigger client automation processing.
    Updates progress and lifecycle states for all clients with programs.
    This can also be called via a scheduled task/cron job.
    """
    try:
        org_id = effective_org_id(current_user)
        result = process_client_automation(db, org_id=org_id)
        return {
            "success": True,
            "message": "Client automation processed successfully",
            **result
        }
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error processing automation: {str(e)}"
        )


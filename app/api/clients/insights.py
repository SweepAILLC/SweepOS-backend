"""Clients API — insights routes."""
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


from pydantic import ValidationError
from app.schemas.client import (
    ClientHealthFactor,
    ClientHealthScoreResponse,
    ClientAIRecommendationsResponse,
    AIRecommendationActionOut,
    AIRecommendationActionPatch,
    AIRecommendationEmailDraftResponse,
)
from app.schemas.call_insights import (
    ClientCallInsightsResponse,
    ClientInsightSummaryOut,
    CallInsightPerCallOut,
    CallInsightsRollupOut,
    OfferSuggestionOut,
    RefreshCallInsightsResponse,
)
from app.services.health_score_cache_service import resolve_health_score, batch_read_cached_health_scores
from app.services.client_ai_recommendations_service import get_recommendation_state_dict, set_action_completed
from app.services.ai_recommendation_email_draft import build_recommendation_email_draft
from app.services.call_insight_service import (
    get_client_insights_response,
    get_call_insight_tags_batch,
    refresh_latest_call_insight,
)
from app.schemas.client import Client as ClientSchema


@router.get("/health-scores")
def get_clients_health_scores(
    request: Request,
    client_ids: str = Query(..., description="Comma-separated client IDs"),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Batch health scores for board tags. Returns { client_id: { score, grade, source } }.
    Reads persisted cache first (fast). For org clients with no row yet, computes and persists a
    logic-only score (no Brevo fetch, no LLM) so leads without Fathom/AI still show a tag.
    Drawer GET /health-score?use_ai=true may later upgrade cache to AI when configured.
    """
    org_id = getattr(current_user, "selected_org_id", current_user.org_id)
    check_sliding_window(
        f"health_scores_batch:{current_user.id}:{org_id}",
        max_requests=200,
        window_seconds=getattr(settings, "HEALTH_SCORE_RATE_LIMIT_WINDOW_SEC", 300),
        db=db,
        audit_user=current_user,
        audit_request=request,
        endpoint_name="get_clients_health_scores",
    )
    ids_str = [x.strip() for x in client_ids.split(",") if x.strip()]
    if not ids_str:
        return {}
    uuids = []
    for i in ids_str:
        try:
            uuids.append(UUID(i))
        except ValueError:
            continue
    if not uuids:
        return {}
    cached = batch_read_cached_health_scores(db, uuids, org_id)
    out: dict = {}
    for cid, data in cached.items():
        out[str(cid)] = {
            "score": data["score"],
            "grade": data["grade"],
            "source": data.get("source"),
        }

    allowed_ids = {
        row[0]
        for row in db.query(Client.id).filter(Client.id.in_(uuids), Client.org_id == org_id).all()
    }
    missing = [cid for cid in uuids if cid in allowed_ids and cid not in cached]
    # Cap backfill so very large boards stay bounded (subsequent polls fill more if needed)
    _MAX_LOGIC_BACKFILL = 100
    for cid in missing[:_MAX_LOGIC_BACKFILL]:
        try:
            filled = resolve_health_score(
                db,
                cid,
                org_id,
                brevo_email_stats=None,
                use_ai=False,
                persist_cache=True,
                record_outcome_snapshot=False,
            )
            if filled and filled.get("score") is not None:
                out[str(cid)] = {
                    "score": filled["score"],
                    "grade": filled["grade"],
                    "source": filled.get("source"),
                }
        except Exception:
            continue

    return out


@router.get("/call-insight-tags")
def get_call_insight_tags(
    request: Request,
    client_ids: str = Query(..., description="Comma-separated client IDs"),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Batch opportunity tags + short headline for board chips (same pattern as health-scores)."""
    org_id = getattr(current_user, "selected_org_id", current_user.org_id)
    check_sliding_window(
        f"call_insight_tags_batch:{current_user.id}:{org_id}",
        max_requests=200,
        window_seconds=getattr(settings, "HEALTH_SCORE_RATE_LIMIT_WINDOW_SEC", 300),
        db=db,
        audit_user=current_user,
        audit_request=request,
        endpoint_name="get_call_insight_tags",
    )
    ids_str = [x.strip() for x in client_ids.split(",") if x.strip()]
    if not ids_str:
        return {}
    uuids = []
    for i in ids_str:
        try:
            uuids.append(UUID(i))
        except ValueError:
            continue
    if not uuids:
        return {}
    clients_ok = db.query(Client.id).filter(Client.id.in_(uuids), Client.org_id == org_id).all()
    allowed = {row[0] for row in clients_ok}
    filtered = [i for i in uuids if i in allowed]
    return get_call_insight_tags_batch(db, org_id, filtered)


@router.get("/{client_id}/call-insights", response_model=ClientCallInsightsResponse)
def get_client_call_insights(
    client_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    try:
        client_uuid = UUID(client_id)
    except ValueError:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid client ID format")
    org_id = getattr(current_user, "selected_org_id", current_user.org_id)
    client = db.query(Client).filter(Client.id == client_uuid, Client.org_id == org_id).first()
    if not client:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Client not found")
    data = get_client_insights_response(db, org_id, client_uuid)
    summary = data.get("summary")
    summary_out = None
    if summary:
        summary_out = ClientInsightSummaryOut(
            headline=summary.get("headline"),
            tags=summary.get("tags") or [],
            last_call_at=summary.get("last_call_at"),
            last_insight_at=summary.get("last_insight_at"),
        )
    insights_out = [CallInsightPerCallOut(**x) for x in data.get("insights") or []]
    rollup_raw = data.get("rollup")
    rollup_out = CallInsightsRollupOut(**rollup_raw) if isinstance(rollup_raw, dict) else None
    offer_raw = data.get("offer_suggestion")
    offer_out = OfferSuggestionOut(**offer_raw) if isinstance(offer_raw, dict) else None
    return ClientCallInsightsResponse(
        client_id=data.get("client_id", str(client_uuid)),
        summary=summary_out,
        insights=insights_out,
        rollup=rollup_out,
        offer_suggestion=offer_out,
    )


@router.post("/{client_id}/call-insights/refresh", response_model=RefreshCallInsightsResponse)
def post_client_call_insights_refresh(
    client_id: str,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    try:
        client_uuid = UUID(client_id)
    except ValueError:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid client ID format")
    org_id = getattr(current_user, "selected_org_id", current_user.org_id)
    client = db.query(Client).filter(Client.id == client_uuid, Client.org_id == org_id).first()
    if not client:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Client not found")
    check_sliding_window(
        f"call_insights_refresh_{org_id}_{current_user.id}_{client_uuid}",
        max_requests=8,
        window_seconds=3600,
        endpoint_name="post_client_call_insights_refresh",
        db=db,
        audit_user=current_user,
        audit_request=request,
    )
    refresh_status, detail = refresh_latest_call_insight(db, org_id, client_uuid)
    return RefreshCallInsightsResponse(status=refresh_status, detail=detail)


@router.get("/{client_id}/ai-recommendations", response_model=ClientAIRecommendationsResponse)
def get_client_ai_recommendations(
    client_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Lifecycle-based recommended actions (modular checklist). User can mark items complete via PATCH.
    Populated from defaults until call-insights AI is wired; completions persist per client.
    """
    try:
        client_uuid = UUID(client_id)
    except ValueError:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid client ID format")
    org_id = getattr(current_user, "selected_org_id", current_user.org_id)
    client = db.query(Client).filter(Client.id == client_uuid, Client.org_id == org_id).first()
    if not client:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Client not found")
    pp = user_pipeline_priorities(current_user)
    data = get_recommendation_state_dict(db, client, pipeline_priorities=pp)
    actions = [AIRecommendationActionOut.model_validate(a) for a in data.get("actions", []) if isinstance(a, dict)]
    return ClientAIRecommendationsResponse(
        client_id=data["client_id"],
        headline=data.get("headline"),
        actions=actions,
        updated_at=data.get("updated_at"),
    )


@router.patch(
    "/{client_id}/ai-recommendations/actions/{action_id}",
    response_model=ClientAIRecommendationsResponse,
)
def patch_client_ai_recommendation_action(
    client_id: str,
    action_id: str,
    body: AIRecommendationActionPatch,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    try:
        client_uuid = UUID(client_id)
    except ValueError:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid client ID format")
    org_id = getattr(current_user, "selected_org_id", current_user.org_id)
    client = db.query(Client).filter(Client.id == client_uuid, Client.org_id == org_id).first()
    if not client:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Client not found")
    updated = set_action_completed(
        db, client, action_id, body.completed, user_id=current_user.id
    )
    if updated is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Action not found")
    pp = user_pipeline_priorities(current_user)
    data = get_recommendation_state_dict(db, client, pipeline_priorities=pp)
    actions = [AIRecommendationActionOut.model_validate(a) for a in data.get("actions", []) if isinstance(a, dict)]
    return ClientAIRecommendationsResponse(
        client_id=data["client_id"],
        headline=data.get("headline"),
        actions=actions,
        updated_at=data.get("updated_at"),
    )


@router.post(
    "/{client_id}/ai-recommendations/actions/{action_id}/email-draft",
    response_model=AIRecommendationEmailDraftResponse,
)
def post_ai_recommendation_email_draft(
    client_id: str,
    action_id: str,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Generate a conversion-focused email draft using rich client context (health, Fathom sentiment/transcript,
    call-insight wins, notes) and expert copywriting when LLM is configured; else a short template fallback.
    """
    try:
        client_uuid = UUID(client_id)
    except ValueError:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid client ID format")
    org_id = getattr(current_user, "selected_org_id", current_user.org_id)
    check_sliding_window(
        f"ai_rec_email_draft:{current_user.id}:{org_id}",
        max_requests=40,
        window_seconds=300,
        db=db,
        audit_user=current_user,
        audit_request=request,
        endpoint_name="post_ai_recommendation_email_draft",
    )
    client = db.query(Client).filter(Client.id == client_uuid, Client.org_id == org_id).first()
    if not client:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Client not found")
    draft = build_recommendation_email_draft(db, client, action_id, org_id, sender_user=current_user)
    if draft is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Action not found or email draft not available for this recommendation",
        )
    return AIRecommendationEmailDraftResponse(
        subject=draft["subject"],
        body_plain=draft["body_plain"],
        body_html=draft["body_html"],
        source=draft.get("source", "template"),
    )


@router.get("/{client_id}/health-score", response_model=ClientHealthScoreResponse)
def get_client_health_score(
    request: Request,
    client_id: str,
    use_ai: bool = Query(
        False,
        description="Request AI overlay when LLM + FATHOM_API_KEY are configured; otherwise logic score with source_reason",
    ),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Get client/lead health score (0–100) and factors. Defaults to logic-based scoring.
    When use_ai=true and FATHOM_API_KEY + LLM are configured, may return source=\"ai\" with explanation;
    if Fathom is not configured, returns logic score and source_reason=fathom_not_configured (no error).
    Factors: show rate, email open rate (Brevo), failed payments, program timeline/tenure, days since last contact.
    """
    try:
        client_uuid = UUID(client_id)
    except ValueError:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid client ID format")

    org_id = getattr(current_user, "selected_org_id", current_user.org_id)
    win = getattr(settings, "HEALTH_SCORE_RATE_LIMIT_WINDOW_SEC", 300)
    max_req = (
        getattr(settings, "HEALTH_SCORE_AI_RATE_LIMIT_MAX", 25)
        if use_ai
        else getattr(settings, "HEALTH_SCORE_RATE_LIMIT_MAX", 120)
    )
    check_sliding_window(
        f"health_score:{current_user.id}:{org_id}:{'ai' if use_ai else 'base'}",
        max_requests=max_req,
        window_seconds=win,
        db=db,
        audit_user=current_user,
        audit_request=request,
        endpoint_name="get_client_health_score",
    )

    client = db.query(Client).filter(Client.id == client_uuid, Client.org_id == org_id).first()
    if not client:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Client not found")

    brevo_stats = brevo_merged_stats_for_client(db, org_id, current_user.id, client)

    result = resolve_health_score(
        db,
        client_uuid,
        org_id,
        brevo_email_stats=brevo_stats,
        use_ai=use_ai,
    )
    if not result:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Client not found")

    factors_out = [
        ClientHealthFactor(
            key=f.get("key"),
            label=f.get("label", ""),
            value=f.get("value"),
            raw=f.get("raw"),
            unit=f.get("unit"),
            description=f.get("description"),
        )
        for f in result.get("factors", [])
    ]
    return ClientHealthScoreResponse(
        client_id=result["client_id"],
        score=result["score"],
        grade=result["grade"],
        factors=factors_out,
        computed_at=result.get("computed_at"),
        source=result.get("source"),
        explanation=result.get("explanation"),
        source_reason=result.get("source_reason"),
    )

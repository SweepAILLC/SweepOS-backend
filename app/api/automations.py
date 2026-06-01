"""Automations REST API: rules CRUD, jobs list, dispatcher health, draft preview.

The request path here is intentionally lightweight — we never call the LLM or Brevo
on a sync request. Mutations write rule rows; previews build a draft in-process for
side-by-side comparison; sends are always handled by the worker after `enqueue_for_preview`
flips state to `ready`.
"""
from __future__ import annotations

import logging
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import desc
from sqlalchemy.orm import Session

from app.api.deps import get_current_user, require_admin_or_owner
from app.db.session import get_db
from app.models.automation import (
    AutomationEmailJob,
    AutomationRule,
    JobState,
    PLAYBOOK_VALUES,
    Playbook,
)
from app.models.client import Client
from app.models.user import User
from app.schemas.automation import (
    AutomationEmailJobListResponse,
    AutomationEmailJobRead,
    AutomationPreviewRequest,
    AutomationPreviewResponse,
    AutomationRuleRead,
    AutomationRuleUpdate,
    DispatcherHealth,
    JobStateUpdate,
)
from app.services.automation_dispatcher import read_dispatcher_health
from app.services.automation_drafts import build_automation_email_draft
from app.services.automation_engine import seed_default_rules

LOG = logging.getLogger(__name__)

router = APIRouter()


def _resolve_org_id(user: User) -> uuid.UUID:
    return getattr(user, "selected_org_id", user.org_id)


# ----- Rules ------------------------------------------------------------------

@router.get("/rules", response_model=List[AutomationRuleRead])
def list_rules(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """List all automation rules for the org. Seeds the four defaults on first call."""
    org_id = _resolve_org_id(current_user)
    seed_default_rules(db, org_id)
    db.commit()
    rows = (
        db.query(AutomationRule)
        .filter(AutomationRule.org_id == org_id)
        .order_by(AutomationRule.playbook)
        .all()
    )
    return [AutomationRuleRead.model_validate(r) for r in rows]


@router.put("/rules/{playbook}", response_model=AutomationRuleRead)
def update_rule(
    playbook: str,
    body: AutomationRuleUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin_or_owner),
):
    if playbook not in PLAYBOOK_VALUES:
        raise HTTPException(status_code=400, detail=f"unknown playbook '{playbook}'")
    org_id = _resolve_org_id(current_user)

    rule = (
        db.query(AutomationRule)
        .filter(AutomationRule.org_id == org_id, AutomationRule.playbook == playbook)
        .first()
    )
    if not rule:
        rule = AutomationRule(
            id=uuid.uuid4(),
            org_id=org_id,
            playbook=playbook,
        )
        db.add(rule)

    rule.enabled = body.enabled
    rule.delay_seconds = int(body.delay_seconds or 0)
    rule.content_mode = body.content_mode
    rule.subject_template = body.subject_template
    rule.html_template_ref = body.html_template_ref.model_dump() if body.html_template_ref else None
    rule.ai_content_system_prompt = body.ai_content_system_prompt
    rule.audience_filter = body.audience_filter.model_dump() if body.audience_filter else None
    rule.trigger_config = body.trigger_config.model_dump() if body.trigger_config else None
    rule.opportunity_priority = list(body.opportunity_priority) if body.opportunity_priority else None
    rule.combine_top_n = max(0, min(3, int(body.combine_top_n)))
    rule.require_approval = bool(body.require_approval)
    rule.approval_ttl_hours = int(body.approval_ttl_hours) if body.approval_ttl_hours else None
    rule.last_modified_by = current_user.id
    rule.updated_at = datetime.utcnow()
    db.commit()
    db.refresh(rule)
    return AutomationRuleRead.model_validate(rule)


# ----- Jobs -------------------------------------------------------------------

@router.get("/jobs", response_model=AutomationEmailJobListResponse)
def list_jobs(
    state: Optional[str] = Query(None),
    playbook: Optional[str] = Query(None),
    client_id: Optional[uuid.UUID] = Query(None),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    org_id = _resolve_org_id(current_user)
    q = db.query(AutomationEmailJob).filter(AutomationEmailJob.org_id == org_id)
    if state:
        q = q.filter(AutomationEmailJob.state == state)
    if playbook:
        q = q.filter(AutomationEmailJob.playbook == playbook)
    if client_id:
        q = q.filter(AutomationEmailJob.client_id == client_id)
    total = q.count()
    rows = (
        q.order_by(desc(AutomationEmailJob.created_at))
        .offset(offset)
        .limit(limit)
        .all()
    )
    return AutomationEmailJobListResponse(
        items=[AutomationEmailJobRead.model_validate(r) for r in rows],
        total=total,
    )


@router.patch("/jobs/{job_id}/state", response_model=AutomationEmailJobRead)
def update_job_state(
    job_id: uuid.UUID,
    body: JobStateUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin_or_owner),
):
    """Approve / cancel a job. Only allowed transitions are exposed in the UI."""
    org_id = _resolve_org_id(current_user)
    job = (
        db.query(AutomationEmailJob)
        .filter(AutomationEmailJob.org_id == org_id, AutomationEmailJob.id == job_id)
        .first()
    )
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    target = body.state
    allowed_transitions = {
        JobState.AWAITING_APPROVAL.value: {
            JobState.READY.value,    # approve
            JobState.CANCELED.value, # decline
        },
        JobState.SCHEDULED.value: {JobState.CANCELED.value},
        JobState.READY.value: {JobState.CANCELED.value},
        JobState.FAILED.value: {JobState.SCHEDULED.value},  # retry from "failed"
    }
    sources = allowed_transitions.get(job.state, set())
    if target not in sources:
        raise HTTPException(
            status_code=400,
            detail=f"cannot move from {job.state} to {target}",
        )
    job.state = target
    if target == JobState.SCHEDULED.value:
        job.scheduled_at = datetime.utcnow()
        job.error_text = None
    job.updated_at = datetime.utcnow()
    db.commit()
    db.refresh(job)
    return AutomationEmailJobRead.model_validate(job)


# ----- Dispatcher health ------------------------------------------------------

@router.get("/dispatcher/health", response_model=DispatcherHealth)
def get_dispatcher_health(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    info = read_dispatcher_health(db)
    return DispatcherHealth(**info)


# ----- Preview ---------------------------------------------------------------

@router.post("/preview", response_model=AutomationPreviewResponse)
def preview_draft(
    body: AutomationPreviewRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    org_id = _resolve_org_id(current_user)
    client = (
        db.query(Client)
        .filter(Client.id == body.client_id, Client.org_id == org_id)
        .first()
    )
    if not client:
        raise HTTPException(status_code=404, detail="Client not found")

    # Use existing rule for context but apply preview overrides so the user can see
    # the effect of their unsaved changes before persisting.
    rule = (
        db.query(AutomationRule)
        .filter(AutomationRule.org_id == org_id, AutomationRule.playbook == body.playbook)
        .first()
    )
    if not rule:
        rule = AutomationRule(
            id=uuid.uuid4(),
            org_id=org_id,
            playbook=body.playbook,
            enabled=False,
        )
        # not persisted; just used in-memory for preview
    if body.content_mode:
        rule.content_mode = body.content_mode
    if body.subject_template is not None:
        rule.subject_template = body.subject_template
    if body.html_template_ref is not None:
        rule.html_template_ref = body.html_template_ref.model_dump()
    if "ai_content_system_prompt" in body.model_fields_set:
        rule.ai_content_system_prompt = body.ai_content_system_prompt

    draft = build_automation_email_draft(db, rule=rule, client=client)
    return AutomationPreviewResponse(
        subject=draft.subject,
        body_plain=draft.body_plain,
        html=draft.html,
        chosen_opportunities=draft.chosen_opportunities,
        merge_tags_resolved=draft.merge_tags_resolved,
        notes=draft.notes,
    )

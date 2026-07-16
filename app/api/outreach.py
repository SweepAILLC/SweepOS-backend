"""Unified Outreach Inbox: automation-job approvals feed.

Performance / Priorities tasks were removed; this endpoint now returns
awaiting automation jobs only.
"""
from __future__ import annotations

import uuid
from datetime import datetime
from typing import List, Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy import desc
from sqlalchemy.orm import Session

from app.api.deps import get_current_user
from app.db.session import get_db
from app.models.automation import AutomationEmailJob, JobState
from app.models.client import Client
from app.models.user import User
from app.schemas.automation import OutreachInboxItem, OutreachInboxResponse

router = APIRouter()


def _client_lookup(db: Session, org_id: uuid.UUID, ids: List[uuid.UUID]) -> dict:
    if not ids:
        return {}
    rows = (
        db.query(Client)
        .filter(Client.org_id == org_id, Client.id.in_(ids))
        .all()
    )
    return {c.id: c for c in rows}


def _client_display_name(c: Optional[Client]) -> Optional[str]:
    if c is None:
        return None
    full = ((c.first_name or "") + " " + (c.last_name or "")).strip()
    return full or c.email or None


@router.get("/inbox", response_model=OutreachInboxResponse)
def get_inbox(
    include_performance: bool = Query(True),  # kept for API compat; ignored
    include_automations: bool = Query(True),
    limit: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    org_id = getattr(current_user, "selected_org_id", current_user.org_id)
    items: List[OutreachInboxItem] = []
    awaiting_count = 0

    if include_automations:
        rows = (
            db.query(AutomationEmailJob)
            .filter(
                AutomationEmailJob.org_id == org_id,
                AutomationEmailJob.state.in_(
                    (
                        JobState.AWAITING_APPROVAL.value,
                        JobState.SCHEDULED.value,
                        JobState.READY.value,
                    )
                ),
            )
            .order_by(desc(AutomationEmailJob.created_at))
            .limit(limit)
            .all()
        )
        client_ids = list({r.client_id for r in rows})
        clients = _client_lookup(db, org_id, client_ids)
        awaiting_count = sum(1 for r in rows if r.state == JobState.AWAITING_APPROVAL.value)
        for r in rows:
            client = clients.get(r.client_id)
            items.append(
                OutreachInboxItem(
                    id=f"automation:{r.id}",
                    source="automation_job",
                    client_id=r.client_id,
                    client_name=_client_display_name(client),
                    playbook=r.playbook,
                    title=_format_playbook_title(r.playbook, _client_display_name(client)),
                    summary=_summary_for_job(r),
                    state=r.state,
                    scheduled_at=r.scheduled_at,
                    created_at=r.created_at,
                    requires_approval=(r.state == JobState.AWAITING_APPROVAL.value),
                )
            )

    items.sort(
        key=lambda it: (
            0 if it.requires_approval else 1,
            -(it.created_at.timestamp() if it.created_at else 0),
        )
    )
    items = items[:limit]
    return OutreachInboxResponse(
        items=items,
        awaiting_approval_count=awaiting_count,
        performance_task_count=0,
    )


def _format_playbook_title(playbook: str, client_name: Optional[str]) -> str:
    label_map = {
        "pre_sale_post_booking": "Post-booking email",
        "pre_sale_pre_meeting": "Pre-meeting email",
        "first_payment_onboarding": "Onboarding email",
        "first_payment_referral": "First-payment referral ask",
        "win_combined_ask": "Combined ask after win",
        "offboarding_recap_ask": "Offboarding recap & ask",
    }
    label = label_map.get(playbook, playbook.replace("_", " "))
    if client_name:
        return f"{label} — {client_name}"
    return label


def _summary_for_job(r: AutomationEmailJob) -> str:
    subject = None
    if isinstance(r.payload, dict):
        subject = r.payload.get("subject")
    if subject:
        return str(subject)[:400]
    return f"Automation job ({r.state})"

"""Unified Outreach Inbox: Performance to-dos + automation-job approvals in one feed.

Backed by the same data the Performance tab and Automations tab already expose; this
endpoint exists so the OutreachDrawer can render a single chronologically-ordered list
without two round trips and inconsistent state.
"""
from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy import desc
from sqlalchemy.orm import Session

from app.api.deps import get_current_user
from app.db.session import get_db
from app.models.automation import AutomationEmailJob, JobState
from app.models.client import Client
from app.models.user import User
from app.schemas.automation import OutreachInboxItem, OutreachInboxResponse
from app.schemas.performance import performance_state_from_ai_profile
from app.services.performance_service import build_performance_snapshot

router = APIRouter()


def _client_lookup(db: Session, org_id: uuid.UUID, ids: List[uuid.UUID]) -> Dict[uuid.UUID, Client]:
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
    include_performance: bool = Query(True),
    include_automations: bool = Query(True),
    limit: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    org_id = getattr(current_user, "selected_org_id", current_user.org_id)
    items: List[OutreachInboxItem] = []
    awaiting_count = 0
    perf_count = 0

    if include_performance:
        user_row = db.query(User).filter(User.id == current_user.id).first()
        if user_row is not None:
            profile = getattr(user_row, "ai_profile", None) or {}
            pstate = performance_state_from_ai_profile(profile)
            completed_raw = pstate.get("completed_task_ids") or []
            completed_ids = {str(x) for x in completed_raw if x}
            try:
                snap = build_performance_snapshot(
                    db,
                    org_id,
                    user_ai_profile=profile,
                    completed_task_ids=list(completed_ids),
                )
            except Exception:
                snap = {"tasks": []}
            tasks = [t for t in snap.get("tasks") or [] if not t.get("completed")]
            perf_count = len(tasks)
            existing_drafts = pstate.get("drafts") if isinstance(pstate.get("drafts"), dict) else {}
            for t in tasks[:limit]:
                created_raw = t.get("created_at") or t.get("updated_at")
                created = _coerce_dt(created_raw) or datetime.utcnow()
                tid = str(t.get("id") or "")
                # Pull a client_id either from the top-level field or from
                # task.evidence.client_id (where build_performance_snapshot puts
                # it for client-scoped tasks). The UI uses this to enable the
                # per-row "Generate email" action.
                client_id = _uuid(t.get("client_id"))
                if client_id is None:
                    ev = t.get("evidence")
                    if isinstance(ev, dict):
                        client_id = _uuid(ev.get("client_id"))
                # recommended_actions can be heterogeneous; coerce to a list of
                # short strings so the FE never has to guard against mixed shapes.
                ra_raw = t.get("recommended_actions") or []
                rec_actions: List[str] = []
                if isinstance(ra_raw, list):
                    for a in ra_raw:
                        if isinstance(a, str) and a.strip():
                            rec_actions.append(a.strip()[:200])
                        elif isinstance(a, dict) and a.get("title"):
                            rec_actions.append(str(a["title"]).strip()[:200])
                impact_raw = t.get("impact_score")
                try:
                    impact_score = float(impact_raw) if impact_raw is not None else None
                except (TypeError, ValueError):
                    impact_score = None
                items.append(
                    OutreachInboxItem(
                        id=f"performance:{tid}",
                        source="performance_task",
                        client_id=client_id,
                        client_name=t.get("client_name"),
                        playbook=None,
                        title=str(t.get("title") or "Performance task"),
                        summary=str(t.get("why") or t.get("prescription") or ""),
                        state=None,
                        scheduled_at=None,
                        created_at=created,
                        requires_approval=False,
                        category=str(t.get("category"))[:80] if t.get("category") else None,
                        prescription=str(t.get("prescription"))[:1200] if t.get("prescription") else None,
                        next_step=str(t.get("next_step"))[:600] if t.get("next_step") else None,
                        recommended_actions=rec_actions or None,
                        impact_score=impact_score,
                        has_email_draft=bool(existing_drafts.get(tid)),
                    )
                )

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
        performance_task_count=perf_count,
    )


def _format_playbook_title(playbook: str, client_name: Optional[str]) -> str:
    label_map = {
        "first_payment_onboarding": "Onboarding email",
        "first_payment_referral": "First-payment referral ask",
        "win_combined_ask": "Combined ask after win",
        "offboarding_recap_ask": "Offboarding recap & ask",
    }
    label = label_map.get(playbook, playbook.replace("_", " "))
    if client_name:
        return f"{label} — {client_name}"
    return label


def _summary_for_job(job: AutomationEmailJob) -> str:
    if job.error_text:
        return f"Error: {job.error_text[:200]}"
    payload = job.payload_json if isinstance(job.payload_json, dict) else {}
    trigger = payload.get("trigger")
    if trigger:
        return f"Trigger: {trigger}"
    return f"State: {job.state}"


def _coerce_dt(v: Any) -> Optional[datetime]:
    if isinstance(v, datetime):
        return v
    if isinstance(v, str) and v:
        try:
            return datetime.fromisoformat(v.replace("Z", "+00:00"))
        except Exception:
            return None
    return None


def _uuid(v: Any) -> Optional[uuid.UUID]:
    if isinstance(v, uuid.UUID):
        return v
    if isinstance(v, str) and v:
        try:
            return uuid.UUID(v)
        except (ValueError, TypeError):
            return None
    return None

"""Performance tab: org-scoped snapshot, task persistence, optional LLM prescription."""

from __future__ import annotations

import json
import re
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

from fastapi import APIRouter, Depends, HTTPException, Request, status
from sqlalchemy.orm import Session
from sqlalchemy.orm.attributes import flag_modified

from app.api.deps import get_current_user
from app.db.session import get_db
from app.models.user import User
from app.core.config import settings
from app.core.rate_limit import check_sliding_window
from app.schemas.performance import (
    PerformanceEmailDraftsBody,
    PerformanceEmailDraftsResponse,
    PerformanceSnapshotResponse,
    PerformanceTaskEmailDraftOut,
    PerformanceTasksPatchBody,
    PerformanceTasksPatchResponse,
    PerformancePrescriptionBody,
    PerformancePrescriptionResponse,
    PerformancePrescriptionTaskOut,
    performance_state_from_ai_profile,
)
from app.services.performance_service import build_performance_snapshot
from app.services.llm_client import llm_available, chat_json
from app.services.offer_ladder import extract_offer_ladder, offer_ladder_for_llm
from app.services.ai_recommendation_email_draft import build_performance_task_email_draft

router = APIRouter()

MAX_COMPLETED_IDS = 500
MAX_PRESCRIPTION_TASKS = 18
MAX_EMAIL_DRAFT_TASKS = 12
EMAIL_DRAFT_BODY_MAX = 8000
EMAIL_DRAFT_SUBJECT_MAX = 200
FIELD_MAX = 1200

CLIENT_PERF_REC_RE = re.compile(r"^client\.([0-9a-fA-F-]{36})\.rec\.(.+)$")


def _sync_client_perf_recommendations(
    db: Session,
    org_id: uuid.UUID,
    old_ids: Set[str],
    new_ids: Set[str],
    user_id: uuid.UUID,
) -> None:
    """Mirror Performance checkboxes to Terminal client-card recommendation rows."""
    from app.models.client import Client
    from app.services.client_ai_recommendations_service import set_action_completed

    def parse(ids: Set[str]) -> Set[Tuple[uuid.UUID, str]]:
        out: Set[Tuple[uuid.UUID, str]] = set()
        for s in ids:
            m = CLIENT_PERF_REC_RE.match(s)
            if not m:
                continue
            try:
                cid = uuid.UUID(m.group(1))
                aid = m.group(2)
                out.add((cid, aid))
            except ValueError:
                continue
        return out

    old_p = parse(old_ids)
    new_p = parse(new_ids)
    for cid, aid in new_p - old_p:
        c = db.query(Client).filter(Client.id == cid, Client.org_id == org_id).first()
        if c:
            set_action_completed(db, c, aid, True, user_id=user_id)
    for cid, aid in old_p - new_p:
        c = db.query(Client).filter(Client.id == cid, Client.org_id == org_id).first()
        if c:
            set_action_completed(db, c, aid, False, user_id=user_id)


def _user_row(db: Session, user_id: uuid.UUID) -> User:
    u = db.query(User).filter(User.id == user_id).first()
    if not u:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")
    return u


def _normalize_completed_ids(incoming: List[str]) -> List[str]:
    out: List[str] = []
    seen: set[str] = set()
    for x in incoming:
        s = str(x).strip()
        if not s or len(s) > 200 or s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out[:MAX_COMPLETED_IDS]


def _drafts_from_state(pstate: Dict[str, Any]) -> List[PerformanceTaskEmailDraftOut]:
    """Pull persisted Performance email drafts out of performance_state.drafts."""
    raw = pstate.get("drafts") if isinstance(pstate, dict) else None
    if not isinstance(raw, dict):
        return []
    out: List[PerformanceTaskEmailDraftOut] = []
    for tid, d in raw.items():
        if not isinstance(d, dict):
            continue
        out.append(
            PerformanceTaskEmailDraftOut(
                task_id=str(tid)[:200],
                subject=str(d.get("subject") or "")[:EMAIL_DRAFT_SUBJECT_MAX],
                body_plain=str(d.get("body_plain") or "")[:EMAIL_DRAFT_BODY_MAX],
                body_html=str(d.get("body_html") or "")[:EMAIL_DRAFT_BODY_MAX * 2],
                source=str(d.get("source") or "llm")[:32],
                generated_at=str(d.get("generated_at") or ""),
                client_id=(str(d.get("client_id")) if d.get("client_id") else None),
                client_email=(str(d.get("client_email")) if d.get("client_email") else None),
                skipped_reason=(str(d.get("skipped_reason")) if d.get("skipped_reason") else None),
            )
        )
    return out


def _persist_draft(
    user_row: User,
    pstate: Dict[str, Any],
    task_id: str,
    payload: Dict[str, Any],
) -> None:
    """Write a draft back into performance_state.drafts (in-memory; caller commits)."""
    drafts = pstate.get("drafts") if isinstance(pstate.get("drafts"), dict) else {}
    drafts = dict(drafts)
    drafts[str(task_id)[:200]] = {
        "subject": str(payload.get("subject") or "")[:EMAIL_DRAFT_SUBJECT_MAX],
        "body_plain": str(payload.get("body_plain") or "")[:EMAIL_DRAFT_BODY_MAX],
        "body_html": str(payload.get("body_html") or "")[:EMAIL_DRAFT_BODY_MAX * 2],
        "source": str(payload.get("source") or "llm")[:32],
        "generated_at": payload.get("generated_at") or datetime.now(timezone.utc).isoformat(),
        "client_id": payload.get("client_id"),
        "client_email": payload.get("client_email"),
        "skipped_reason": payload.get("skipped_reason"),
    }
    pstate["drafts"] = drafts


@router.get("/snapshot", response_model=PerformanceSnapshotResponse)
def get_performance_snapshot(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    org_id = getattr(current_user, "selected_org_id", current_user.org_id)
    user_row = _user_row(db, current_user.id)
    profile = getattr(user_row, "ai_profile", None) or {}
    pstate = performance_state_from_ai_profile(profile)
    completed = pstate.get("completed_task_ids")
    completed_list = completed if isinstance(completed, list) else []
    snap = build_performance_snapshot(
        db,
        org_id,
        user_ai_profile=profile,
        completed_task_ids=[str(x) for x in completed_list if x],
    )
    snap["drafts"] = [d.model_dump() for d in _drafts_from_state(pstate)]
    return PerformanceSnapshotResponse(**snap)


@router.patch("/tasks", response_model=PerformanceTasksPatchResponse)
def patch_performance_tasks(
    body: PerformanceTasksPatchBody,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    org_id = getattr(current_user, "selected_org_id", current_user.org_id)
    user_row = _user_row(db, current_user.id)
    profile = dict(user_row.ai_profile) if isinstance(user_row.ai_profile, dict) else {}
    pstate = dict(performance_state_from_ai_profile(profile))
    old_raw = pstate.get("completed_task_ids") or []
    old_ids = {str(x) for x in old_raw if x}
    merged = _normalize_completed_ids(body.completed_task_ids)
    new_ids = set(merged)
    pstate["completed_task_ids"] = merged
    pstate["updated_at"] = datetime.now(timezone.utc).isoformat()
    profile["performance_state"] = pstate
    user_row.ai_profile = profile
    flag_modified(user_row, "ai_profile")
    db.commit()
    _sync_client_perf_recommendations(db, org_id, old_ids, new_ids, current_user.id)
    return PerformanceTasksPatchResponse(
        completed_task_ids=merged,
        updated_at=pstate.get("updated_at"),
    )


@router.post("/prescription", response_model=PerformancePrescriptionResponse)
def post_performance_prescription(
    request: Request,
    body: PerformancePrescriptionBody,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    org_id = getattr(current_user, "selected_org_id", current_user.org_id)
    win = getattr(settings, "HEALTH_SCORE_RATE_LIMIT_WINDOW_SEC", 300)
    max_rx = getattr(settings, "HEALTH_SCORE_AI_RATE_LIMIT_MAX", 25)
    check_sliding_window(
        f"performance_rx:{current_user.id}:{org_id}",
        max_requests=max(5, max_rx // 2),
        window_seconds=win,
        db=db,
        audit_user=current_user,
        audit_request=request,
        endpoint_name="post_performance_prescription",
    )

    user_row = _user_row(db, current_user.id)
    profile = getattr(user_row, "ai_profile", None) or {}
    pstate = performance_state_from_ai_profile(profile)
    completed_raw = pstate.get("completed_task_ids")
    completed_set: Set[str] = (
        {str(x) for x in completed_raw} if isinstance(completed_raw, list) else set()
    )

    snap = build_performance_snapshot(
        db,
        org_id,
        user_ai_profile=profile,
        completed_task_ids=list(completed_set),
    )
    tasks_in: List[Dict[str, Any]] = snap.get("tasks") or []
    open_tasks = [t for t in tasks_in if not t.get("completed")]
    id_set = {str(t["id"]) for t in open_tasks}

    if body.task_ids:
        wanted = [tid for tid in body.task_ids if tid in id_set]
    else:
        wanted = [t["id"] for t in open_tasks[:MAX_PRESCRIPTION_TASKS]]

    base = [t for t in open_tasks if str(t["id"]) in set(wanted)]
    if not base:
        return PerformancePrescriptionResponse(
            tasks=[],
            source="deterministic",
        )

    if not llm_available():
        out = [
            PerformancePrescriptionTaskOut(
                id=str(t["id"]),
                why=str(t.get("why") or ""),
                prescription=str(t.get("prescription") or ""),
                next_step=str(t.get("next_step") or ""),
            )
            for t in base
        ]
        return PerformancePrescriptionResponse(tasks=out, source="deterministic")

    allowed_ids = [str(t["id"]) for t in base]
    compact_tasks = [
        {
            "id": str(t["id"]),
            "title": str(t.get("title") or ""),
            "category": str(t.get("category") or ""),
            "evidence": t.get("evidence") or {},
            "recommended_actions": t.get("recommended_actions") or [],
        }
        for t in base
    ]
    profile_compact = {
        "pipeline_priorities": profile.get("pipeline_priorities"),
        "business_description": profile.get("business_description"),
        "target_audience": profile.get("target_audience"),
        "unique_selling_proposition": profile.get("unique_selling_proposition"),
        "sales_framework": profile.get("sales_framework"),
        "sales_tactics": profile.get("sales_tactics"),
        "marketing_strategy": profile.get("marketing_strategy"),
        "marketing_channels": profile.get("marketing_channels"),
        "writing_tone": profile.get("writing_tone"),
        "writing_style": profile.get("writing_style"),
        "coaching_style": profile.get("coaching_style"),
    }
    compact_ladder = offer_ladder_for_llm(extract_offer_ladder(profile))
    if compact_ladder:
        profile_compact["offer_ladder"] = compact_ladder

    system = """You personalize performance coaching copy for a SaaS coach/operator.
Return ONLY a JSON object with key "tasks" array. Each element: {"id": "<exact id>", "why": "...", "prescription": "...", "next_step": "..."}.
Rules:
- Use only the task ids provided; do not invent ids.
- why: 1-2 sentences grounded in evidence numbers.
- prescription: 1-2 sentences, actionable, aligned with the user's business profile when provided.
- next_step: one short imperative the user can do today.
- ROI ladder + pipeline priorities (REQUIRED): Every prescription and next_step must propose one concrete next move toward ROI
  drawn from user_ai_profile.offer_ladder when present (core, downsell, upsell, or referral as fits the lifecycle and the
  highest-ranked relevant entry of user_ai_profile.pipeline_priorities). Never invent offers outside the ladder.
- For roi_signal tasks, when evidence.offer_suggestion is present, name that offer in prescription and tailor language
  to evidence.offer_suggestion.script_hint. For client tasks, pick the rung that matches the action category
  (engagement -> testimonial/upsell, win_back -> revive offer, conversion -> core/downsell, referral -> referral path).
- If user_ai_profile.offer_ladder is absent, fall back to the highest-ranked relevant pipeline_priority and frame the move
  in that language (e.g. "ask for a referral", "open the upsell conversation").
- No markdown, no preface. English only."""

    user_payload = {
        "allowed_task_ids": allowed_ids,
        "tasks": compact_tasks,
        "user_ai_profile": profile_compact,
        "diagnosis": snap.get("diagnosis"),
    }
    user = "DATA:\n" + json.dumps(user_payload, default=str)[: min(36000, getattr(settings, "LLM_MAX_INPUT_CHARS_TOTAL", 48000))]

    try:
        raw = chat_json(system, user, temperature=0.35, timeout=45.0, org_id=org_id)
    except Exception:
        out = [
            PerformancePrescriptionTaskOut(
                id=str(t["id"]),
                why=str(t.get("why") or ""),
                prescription=str(t.get("prescription") or ""),
                next_step=str(t.get("next_step") or ""),
            )
            for t in base
        ]
        return PerformancePrescriptionResponse(tasks=out, source="deterministic")

    arr = raw.get("tasks")
    if not isinstance(arr, list):
        arr = []

    by_id: Dict[str, PerformancePrescriptionTaskOut] = {}
    allowed = set(allowed_ids)
    for item in arr:
        if not isinstance(item, dict):
            continue
        tid = str(item.get("id") or "").strip()
        if tid not in allowed:
            continue
        why = str(item.get("why") or "")[:FIELD_MAX]
        presc = str(item.get("prescription") or "")[:FIELD_MAX]
        nxt = str(item.get("next_step") or "")[:400]
        by_id[tid] = PerformancePrescriptionTaskOut(
            id=tid, why=why, prescription=presc, next_step=nxt
        )

    merged_out: List[PerformancePrescriptionTaskOut] = []
    for t in base:
        tid = str(t["id"])
        if tid in by_id:
            merged_out.append(by_id[tid])
        else:
            merged_out.append(
                PerformancePrescriptionTaskOut(
                    id=tid,
                    why=str(t.get("why") or "")[:FIELD_MAX],
                    prescription=str(t.get("prescription") or "")[:FIELD_MAX],
                    next_step=str(t.get("next_step") or "")[:400],
                )
            )

    return PerformancePrescriptionResponse(tasks=merged_out, source="llm")


# ---------------------------------------------------------------------------
# Performance email drafts
# ---------------------------------------------------------------------------


def _client_id_from_task(task: Dict[str, Any]) -> Optional[uuid.UUID]:
    ev = task.get("evidence")
    if not isinstance(ev, dict):
        return None
    cid_raw = ev.get("client_id")
    if not cid_raw:
        return None
    try:
        return uuid.UUID(str(cid_raw))
    except (ValueError, TypeError):
        return None


@router.post("/email-drafts", response_model=PerformanceEmailDraftsResponse)
def post_performance_email_drafts(
    request: Request,
    body: PerformanceEmailDraftsBody,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Generate (and persist) send-ready email drafts for one or more Performance tasks.

    Drafts live under user.ai_profile.performance_state.drafts[task_id] and are returned by
    GET /performance/snapshot, so the panel can render them inline without re-running the LLM.
    Skips tasks that are not tied to a specific client (org-level / funnel / revenue tasks).
    """
    org_id = getattr(current_user, "selected_org_id", current_user.org_id)
    win = getattr(settings, "HEALTH_SCORE_RATE_LIMIT_WINDOW_SEC", 300)
    base_max = getattr(settings, "HEALTH_SCORE_AI_RATE_LIMIT_MAX", 25)
    check_sliding_window(
        f"performance_email_drafts:{current_user.id}:{org_id}",
        max_requests=max(3, base_max // 3),
        window_seconds=win,
        db=db,
        audit_user=current_user,
        audit_request=request,
        endpoint_name="post_performance_email_drafts",
    )

    user_row = _user_row(db, current_user.id)
    profile_raw = getattr(user_row, "ai_profile", None) or {}
    profile = dict(profile_raw) if isinstance(profile_raw, dict) else {}
    pstate = dict(performance_state_from_ai_profile(profile))
    completed_raw = pstate.get("completed_task_ids")
    completed_set: Set[str] = (
        {str(x) for x in completed_raw} if isinstance(completed_raw, list) else set()
    )

    snap = build_performance_snapshot(
        db,
        org_id,
        user_ai_profile=profile,
        completed_task_ids=list(completed_set),
    )
    tasks_in: List[Dict[str, Any]] = snap.get("tasks") or []
    open_by_id: Dict[str, Dict[str, Any]] = {
        str(t["id"]): t for t in tasks_in if not t.get("completed")
    }

    # Default to the top open tasks when no ids were provided.
    if body.task_ids:
        wanted_ids = [tid for tid in body.task_ids if tid in open_by_id][:MAX_EMAIL_DRAFT_TASKS]
    else:
        ranked = sorted(open_by_id.values(), key=lambda t: -float(t.get("impact_score") or 0))
        wanted_ids = [str(t["id"]) for t in ranked[:MAX_EMAIL_DRAFT_TASKS]]

    if not wanted_ids:
        return PerformanceEmailDraftsResponse(drafts=[], skipped=[], source="deterministic")

    existing_drafts = pstate.get("drafts") if isinstance(pstate.get("drafts"), dict) else {}
    drafts_out: List[PerformanceTaskEmailDraftOut] = []
    skipped: List[str] = []

    from app.models.client import Client

    for tid in wanted_ids:
        task = open_by_id.get(tid)
        if not task:
            skipped.append(tid)
            continue
        if not body.force and isinstance(existing_drafts, dict) and tid in existing_drafts:
            saved = existing_drafts.get(tid) or {}
            drafts_out.append(
                PerformanceTaskEmailDraftOut(
                    task_id=tid,
                    subject=str(saved.get("subject") or "")[:EMAIL_DRAFT_SUBJECT_MAX],
                    body_plain=str(saved.get("body_plain") or "")[:EMAIL_DRAFT_BODY_MAX],
                    body_html=str(saved.get("body_html") or "")[:EMAIL_DRAFT_BODY_MAX * 2],
                    source=str(saved.get("source") or "llm")[:32],
                    generated_at=str(saved.get("generated_at") or ""),
                    client_id=(str(saved.get("client_id")) if saved.get("client_id") else None),
                    client_email=(str(saved.get("client_email")) if saved.get("client_email") else None),
                    skipped_reason=(str(saved.get("skipped_reason")) if saved.get("skipped_reason") else None),
                )
            )
            continue

        cid = _client_id_from_task(task)
        if not cid:
            skipped.append(tid)
            continue
        client = db.query(Client).filter(Client.id == cid, Client.org_id == org_id).first()
        if not client:
            skipped.append(tid)
            continue

        try:
            draft = build_performance_task_email_draft(
                db, client, task, org_id, sender_user=current_user
            )
        except Exception:
            draft = None
        if not draft or not draft.get("body_plain"):
            skipped.append(tid)
            continue

        client_email = (getattr(client, "email", None) or "").strip() or None
        payload = {
            "subject": draft.get("subject"),
            "body_plain": draft.get("body_plain"),
            "body_html": draft.get("body_html"),
            "source": draft.get("source") or "llm",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "client_id": str(cid),
            "client_email": client_email,
        }
        _persist_draft(user_row, pstate, tid, payload)
        drafts_out.append(
            PerformanceTaskEmailDraftOut(
                task_id=tid,
                subject=str(payload["subject"] or ""),
                body_plain=str(payload["body_plain"] or ""),
                body_html=str(payload["body_html"] or ""),
                source=str(payload["source"] or "llm"),
                generated_at=str(payload["generated_at"] or ""),
                client_id=payload["client_id"],
                client_email=payload["client_email"],
            )
        )

    if drafts_out:
        pstate["updated_at"] = datetime.now(timezone.utc).isoformat()
        profile["performance_state"] = pstate
        user_row.ai_profile = profile
        flag_modified(user_row, "ai_profile")
        db.commit()

    source = "llm" if any(d.source == "llm" for d in drafts_out) else "deterministic"
    return PerformanceEmailDraftsResponse(drafts=drafts_out, skipped=skipped, source=source)

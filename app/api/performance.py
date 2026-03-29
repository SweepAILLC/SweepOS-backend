"""Performance tab: org-scoped snapshot, task persistence, optional LLM prescription."""

from __future__ import annotations

import json
import re
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Set, Tuple

from fastapi import APIRouter, Depends, HTTPException, Request, status
from sqlalchemy.orm import Session
from sqlalchemy.orm.attributes import flag_modified

from app.api.deps import get_current_user
from app.db.session import get_db
from app.models.user import User
from app.core.config import settings
from app.core.rate_limit import check_sliding_window
from app.schemas.performance import (
    PerformanceSnapshotResponse,
    PerformanceTasksPatchBody,
    PerformanceTasksPatchResponse,
    PerformancePrescriptionBody,
    PerformancePrescriptionResponse,
    PerformancePrescriptionTaskOut,
    performance_state_from_ai_profile,
)
from app.services.performance_service import build_performance_snapshot
from app.services.llm_client import llm_available, chat_json

router = APIRouter()

MAX_COMPLETED_IDS = 500
MAX_PRESCRIPTION_TASKS = 18
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

    system = """You personalize performance coaching copy for a SaaS coach/operator.
Return ONLY a JSON object with key "tasks" array. Each element: {"id": "<exact id>", "why": "...", "prescription": "...", "next_step": "..."}.
Rules:
- Use only the task ids provided; do not invent ids.
- why: 1-2 sentences grounded in evidence numbers.
- prescription: 1-2 sentences, actionable, aligned with the user's business profile when provided.
- next_step: one short imperative the user can do today.
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

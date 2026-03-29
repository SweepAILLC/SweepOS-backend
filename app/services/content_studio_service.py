"""Content Studio: playbook sync, v2 bundle storage, transcript analysis."""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy.orm import Session

from app.models.content_studio_generation import ContentStudioGeneration
from app.models.content_studio_knowledge_item import ContentStudioKnowledgeItem
from app.models.content_studio_transcript_analysis import ContentStudioTranscriptAnalysis
from app.models.user import User
from app.services.llm_client import chat_json
from app.services.user_ai_profile_context import extract_ai_profile_for_llm

STAGE_SET = frozenset({"TOF", "MOF", "BOF"})
MAX_COMPLETED_IDS = 250


def content_studio_state_from_profile(ai_profile: Any) -> Tuple[str, List[str]]:
    if not isinstance(ai_profile, dict):
        return "", []
    raw = ai_profile.get("content_studio_state")
    if not isinstance(raw, dict):
        return "", []
    bid = str(raw.get("batch_id") or "").strip()
    done = raw.get("completed_idea_ids") or []
    if not isinstance(done, list):
        return bid, []
    out: List[str] = []
    seen: set[str] = set()
    for x in done:
        s = str(x).strip()
        if s and len(s) < 200 and s not in seen:
            seen.add(s)
            out.append(s)
    return bid, out[:MAX_COMPLETED_IDS]


def normalize_completed_idea_ids(incoming: List[str]) -> List[str]:
    out: List[str] = []
    seen: set[str] = set()
    for x in incoming:
        s = str(x).strip()
        if not s or len(s) > 200 or s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out[:MAX_COMPLETED_IDS]


def load_knowledge_grouped(db: Session, org_id: uuid.UUID) -> Dict[str, List[str]]:
    rows = (
        db.query(ContentStudioKnowledgeItem)
        .filter(ContentStudioKnowledgeItem.org_id == org_id)
        .order_by(ContentStudioKnowledgeItem.kind, ContentStudioKnowledgeItem.sort_order)
        .all()
    )
    out: Dict[str, List[str]] = {"objections": [], "closing": [], "reframes": []}
    key_map = {"objection": "objections", "closing": "closing", "reframe": "reframes"}
    for r in rows:
        k = key_map.get((r.kind or "").lower().strip())
        if not k:
            continue
        body = (r.body or "").strip()
        if body:
            out[k].append(body)
    return out


def replace_knowledge(
    db: Session,
    org_id: uuid.UUID,
    objections: List[str],
    closing: List[str],
    reframes: List[str],
) -> None:
    db.query(ContentStudioKnowledgeItem).filter(ContentStudioKnowledgeItem.org_id == org_id).delete(
        synchronize_session=False
    )

    def add(kind: str, lines: List[str]) -> None:
        for i, line in enumerate(lines):
            t = (line or "").strip()
            if not t:
                continue
            db.add(
                ContentStudioKnowledgeItem(
                    org_id=org_id,
                    kind=kind,
                    body=t[:8000],
                    sort_order=i,
                )
            )

    add("objection", objections)
    add("closing", closing)
    add("reframe", reframes)
    db.commit()


def get_latest_generation_row(db: Session, org_id: uuid.UUID) -> Optional[ContentStudioGeneration]:
    return (
        db.query(ContentStudioGeneration)
        .filter(ContentStudioGeneration.org_id == org_id)
        .first()
    )


def valid_idea_ids_from_ideas_json(raw: Any) -> set[str]:
    """Completion IDs for v2 bundle dict or legacy flat list."""
    from app.services.content_studio_bundle import BUNDLE_VERSION, flatten_bundle_idea_ids

    if isinstance(raw, dict) and int(raw.get("version") or 0) >= BUNDLE_VERSION:
        return set(flatten_bundle_idea_ids(raw))
    if isinstance(raw, list):
        return {str(x["id"]) for x in raw if isinstance(x, dict) and x.get("id")}
    return set()


def upsert_generation(
    db: Session,
    org_id: uuid.UUID,
    user_id: Optional[uuid.UUID],
    batch_id: uuid.UUID,
    ideas_json: Any,
) -> None:
    row = get_latest_generation_row(db, org_id)
    if row:
        row.batch_id = batch_id
        row.ideas_json = ideas_json
        row.created_by_user_id = user_id
        row.updated_at = datetime.utcnow()
    else:
        db.add(
            ContentStudioGeneration(
                org_id=org_id,
                batch_id=batch_id,
                ideas_json=ideas_json,
                created_by_user_id=user_id,
            )
        )
    db.commit()


def set_user_content_studio_batch_and_completions(
    db: Session,
    user_row: User,
    batch_id: str,
    completed_ids: List[str],
) -> str:
    profile = dict(user_row.ai_profile) if isinstance(user_row.ai_profile, dict) else {}
    cs = {}
    cs["batch_id"] = str(batch_id)
    cs["completed_idea_ids"] = normalize_completed_idea_ids(completed_ids)
    cs["updated_at"] = datetime.now(timezone.utc).isoformat()
    profile["content_studio_state"] = cs
    user_row.ai_profile = profile
    from sqlalchemy.orm.attributes import flag_modified

    flag_modified(user_row, "ai_profile")
    db.commit()
    return cs["updated_at"]


def analyze_transcript_llm(
    db: Session,
    org_id: uuid.UUID,
    transcript: str,
    purpose: str,
    mixed_note: Optional[str],
    user_row: User,
) -> Dict[str, Any]:
    profile = extract_ai_profile_for_llm(user_row)
    profile_block = json.dumps(profile, ensure_ascii=False) if profile else "{}"
    meta = f"Stated funnel purpose: {purpose}."
    if purpose == "mixed" and (mixed_note or "").strip():
        meta += f" Operator note: {(mixed_note or '').strip()[:1500]}"

    system = """You analyze sales or coaching call transcripts for conversion quality.
Return ONLY valid JSON (no markdown) with this exact structure:
{
  "components": [ { "label": "string", "summary": "string" } ],
  "strengths_for_conversion": [ { "point": "string", "evidence": "string" } ],
  "weaknesses_for_conversion": [ { "point": "string", "evidence": "string" } ],
  "purpose_alignment": "string — did the call content match the stated TOF/MOF/BOF intent? Explain.",
  "summary": "string — 2-4 sentences overall"
}
Rules:
- Ground claims in the transcript; paraphrase short quotes in evidence when possible.
- Be direct and actionable; no scoring gimmicks.
- TOF = discovery/education for cold audience; MOF = nurture, proof, objection handling mid-journey; BOF = close, terms, next step to pay/start."""

    user = f"""{meta}

INTELLIGENCE_PROFILE (optional context for the operator):
{profile_block}

TRANSCRIPT:
{transcript[:48000]}"""

    return chat_json(system, user, temperature=0.2, org_id=org_id)


def persist_transcript_analysis(
    db: Session,
    org_id: uuid.UUID,
    user_id: Optional[uuid.UUID],
    transcript: str,
    purpose: str,
    mixed_note: Optional[str],
    analysis: Dict[str, Any],
) -> uuid.UUID:
    row = ContentStudioTranscriptAnalysis(
        org_id=org_id,
        user_id=user_id,
        purpose=purpose,
        mixed_note=(mixed_note or "").strip()[:2000] or None,
        transcript_text=transcript[:50000],
        analysis_json=analysis,
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return row.id

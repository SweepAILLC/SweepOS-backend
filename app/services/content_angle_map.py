"""Content Angle Map: defaults, validation, LLM generation, regen locks."""
from __future__ import annotations

import hashlib
import json
import logging
import re
import threading
import uuid
from datetime import datetime, timedelta
from difflib import SequenceMatcher
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import func
from sqlalchemy.orm import Session
from sqlalchemy.orm.attributes import flag_modified

from app.models.client_call_insight import ClientCallInsight
from app.models.content_angle_map import ContentAngleMap
from app.models.organization import Organization
from app.models.user import User
from app.models.user_organization import UserOrganization
from app.schemas.content_angle_map import (
    ContentAngleItemOut,
    ContentAngleMapOut,
)

logger = logging.getLogger(__name__)

PROMPT_VERSION = "v0.1"
FEATURE_ICP = "content_angle_map_icp"
FEATURE_BRAND = "content_angle_map_brand"
AUTO_CALL_THRESHOLD = 5
AUTO_REFRESH_DAYS = 7
MIN_KEEP = 3
MAX_PHRASE_CHARS = 80
MIN_PHRASE_CHARS = 8
NEAR_DUPE_RATIO = 0.82
VERBATIM_RATIO = 0.90

DEFAULT_PILLS_TOF = [
    "B-roll + audio",
    "Vlog-style talking head",
    "Green screen reaction",
    "Humor talking head",
    "Rapid-fire info",
]
DEFAULT_PILLS_MOF = [
    "Document lifestyle",
    "Value talking head",
    "Value green screen",
    "Storytelling / mindset",
    "Emotional storytelling b-roll",
    "Value / storytelling carousel",
]
DEFAULT_PILLS_BOF = [
    "Case studies",
    "Testimonials",
    "Objections from not buying",
    "Documentation / proof",
]

ICP_PLACEHOLDERS = [
    "[Core insecurity or intimidation they feel]",
    "[Frustration they cannot name cleanly]",
    "[Identity tension — who they are vs who they want to be]",
    "[Anxiety about staying stuck]",
    "[Fear of wasting another year / another coach]",
    "[Belief that keeps them from buying]",
]
BRAND_PLACEHOLDERS = [
    "[Origin moment that made this personal]",
    "[Credibility they earned the hard way]",
    "[What they refused to keep doing]",
    "[How their approach is different]",
    "[Proof they have lived the client's problem]",
    "[The promise they actually stand behind]",
]

KAI_SYSTEM = """You are Kai, the AI Growth Engine for Sweep Coach OS. Your role is to help coaches build stronger relationships with their clients through personalized, empathetic, and data-driven communication.

Core principles:
- Be warm, professional, and coach-like in tone
- Prioritize client outcomes and relationship building
- Use data and context to inform recommendations
- Always cite sources and evidence for claims
- Flag uncertainty and request human review when appropriate
- Respect client boundaries and communication preferences
- Maintain consistency with Sweep brand voice (see BRAND.md)

You are NOT a replacement for human judgment. You are a tool to amplify coach effectiveness.
"""

CONSULTING_TIERS = frozenset({"pro_consulting", "core_consulting"})

_regen_locks: Dict[str, threading.Lock] = {}
_regen_meta_lock = threading.Lock()


def _org_lock(org_id: uuid.UUID) -> threading.Lock:
    key = str(org_id)
    with _regen_meta_lock:
        if key not in _regen_locks:
            _regen_locks[key] = threading.Lock()
        return _regen_locks[key]


def _norm(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip().lower())


def _ratio(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a, b).ratio()


def validate_angle_phrases(phrases: List[Any], source_text: str) -> List[str]:
    """Keep short distinct phrases that are not a near-copy of source_text."""
    source_norm = _norm(source_text)
    seen: List[str] = []
    out: List[str] = []
    for raw in phrases or []:
        text = str(raw or "").strip().strip("-–—• ").strip().strip("\"'")
        if text.endswith((".", "!", "?")):
            text = text[:-1].rstrip()
        if len(text) < MIN_PHRASE_CHARS or len(text) > MAX_PHRASE_CHARS:
            continue
        key = _norm(text)
        if not key or any(_ratio(key, s) >= NEAR_DUPE_RATIO for s in seen):
            continue
        if source_norm:
            if key in source_norm or source_norm in key:
                continue
            if _ratio(key, source_norm) >= VERBATIM_RATIO:
                continue
        seen.append(key)
        out.append(text)
        if len(out) >= 6:
            break
    return out


def merge_angles(
    existing: List[Dict[str, Any]],
    generated: List[Dict[str, Any]],
    *,
    full: bool = False,
) -> List[Dict[str, Any]]:
    """Replace unlocked items; keep manually_edited unless full=True."""
    if full or not existing:
        return generated[:6]
    locked_texts = [_norm(str(a.get("text") or "")) for a in existing if a.get("manually_edited")]
    fresh = [
        g
        for g in generated
        if _norm(str(g.get("text") or "")) not in locked_texts
        and not any(_ratio(_norm(str(g.get("text") or "")), t) >= NEAR_DUPE_RATIO for t in locked_texts)
    ]
    result: List[Dict[str, Any]] = []
    gi = 0
    for item in existing:
        if item.get("manually_edited"):
            result.append(item)
        elif gi < len(fresh):
            result.append(fresh[gi])
            gi += 1
        else:
            result.append(item)
    while len(result) < 6 and gi < len(fresh):
        result.append(fresh[gi])
        gi += 1
    return result[:6]


def compute_input_fingerprint(
    target_audience: str,
    personal_story: str,
    signal_digest: str,
) -> str:
    blob = json.dumps(
        {
            "ta": _norm(target_audience),
            "ps": _norm(personal_story),
            "sig": signal_digest,
        },
        sort_keys=True,
        ensure_ascii=False,
    )
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def should_auto_refresh(
    row: Optional[ContentAngleMap],
    *,
    current_call_count: int,
    new_fingerprint: str,
    now: Optional[datetime] = None,
) -> bool:
    if row is None:
        return True
    if (row.input_fingerprint or "") == new_fingerprint:
        return False
    now = now or datetime.utcnow()
    generated_at = row.last_generated_at
    age_ok = generated_at is None or (now - generated_at) >= timedelta(days=AUTO_REFRESH_DAYS)
    new_calls = current_call_count - int(row.calls_seen_at_generation or 0)
    return new_calls >= AUTO_CALL_THRESHOLD or age_ok


def _as_uuid(value: Any) -> uuid.UUID:
    return value if isinstance(value, uuid.UUID) else uuid.UUID(str(value))


def org_is_consulting(db: Session, org_id: uuid.UUID) -> bool:
    org = db.query(Organization).filter(Organization.id == org_id).first()
    return bool(org and getattr(org, "consulting_tier", None) in CONSULTING_TIERS)


def complete_insight_count(db: Session, org_id: uuid.UUID) -> int:
    return int(
        db.query(func.count(ClientCallInsight.id))
        .filter(ClientCallInsight.org_id == org_id, ClientCallInsight.status == "complete")
        .scalar()
        or 0
    )


def resolve_org_ai_profile_dict(db: Session, org_id: uuid.UUID) -> Dict[str, Any]:
    rows = (
        db.query(User)
        .filter(User.org_id == org_id)
        .order_by(User.created_at.asc())
        .all()
    )
    scored: List[Dict[str, Any]] = []
    for u in rows:
        raw = u.ai_profile if isinstance(u.ai_profile, dict) else None
        if raw:
            scored.append(raw)
    if not scored:
        link = (
            db.query(UserOrganization)
            .filter(UserOrganization.org_id == org_id)
            .order_by(UserOrganization.created_at.asc())
            .first()
        )
        if link and isinstance(link.ai_profile, dict):
            scored.append(link.ai_profile)
    for raw in scored:
        if str(raw.get("target_audience") or "").strip() or str(raw.get("personal_story") or "").strip():
            return raw
    return scored[0] if scored else {}


def seed_fields(profile: Optional[Dict[str, Any]]) -> Tuple[str, str]:
    raw = profile if isinstance(profile, dict) else {}
    return (
        str(raw.get("target_audience") or "").strip(),
        str(raw.get("personal_story") or "").strip(),
    )


def _normalize_item(raw: Any) -> Optional[Dict[str, Any]]:
    if isinstance(raw, str):
        text = raw.strip()
        if not text:
            return None
        return {"id": str(uuid.uuid4()), "text": text[:160], "manually_edited": False}
    if isinstance(raw, dict):
        text = str(raw.get("text") or "").strip()
        if not text:
            return None
        iid = str(raw.get("id") or uuid.uuid4())
        return {"id": iid, "text": text[:160], "manually_edited": bool(raw.get("manually_edited"))}
    return None


def normalize_angle_list(raw: Any) -> List[Dict[str, Any]]:
    if not isinstance(raw, list):
        return []
    out: List[Dict[str, Any]] = []
    for item in raw:
        norm = _normalize_item(item)
        if norm:
            out.append(norm)
    return out


PILL_FIELDS = {
    "tof": "format_pills_tof",
    "mof": "format_pills_mof",
    "bof": "format_pills_bof",
}


def normalize_pills(raw: Any) -> List[str]:
    if not isinstance(raw, list):
        return []
    out: List[str] = []
    seen = set()
    for item in raw:
        text = str(item or "").strip()[:80]
        key = text.lower()
        if not text or key in seen:
            continue
        seen.add(key)
        out.append(text)
        if len(out) >= 20:
            break
    return out


def _string_list(raw: Any, fallback: List[str]) -> List[str]:
    if not isinstance(raw, list):
        return list(fallback)
    return normalize_pills(raw)


def get_or_create_map(db: Session, org_id: uuid.UUID) -> ContentAngleMap:
    row = db.query(ContentAngleMap).filter(ContentAngleMap.org_id == org_id).first()
    if row:
        return row
    now = datetime.utcnow()
    row = ContentAngleMap(
        id=uuid.uuid4(),
        org_id=org_id,
        icp_angles=[],
        personal_brand_angles=[],
        format_pills_tof=list(DEFAULT_PILLS_TOF),
        format_pills_mof=list(DEFAULT_PILLS_MOF),
        format_pills_bof=list(DEFAULT_PILLS_BOF),
        calls_seen_at_generation=0,
        updated_at=now,
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return row


def _signal_digest(signals: Dict[str, Any]) -> str:
    themes = []
    for t in (signals.get("themes") or [])[:8]:
        if isinstance(t, dict):
            themes.append(str(t.get("label") or t.get("theme_key") or "")[:80])
    objections: List[str] = []
    for insight in (signals.get("insights") or [])[:8]:
        if not isinstance(insight, dict):
            continue
        for q in (insight.get("objection_quotes") or [])[:2]:
            objections.append(str(q)[:120])
    summaries = [str(s)[:100] for s in (signals.get("meeting_summaries") or [])[:4]]
    blob = json.dumps({"t": themes, "o": objections, "s": summaries}, ensure_ascii=False)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()[:32]


def _founder_snippets(signals: Dict[str, Any]) -> List[str]:
    out: List[str] = []
    for insight in (signals.get("insights") or [])[:10]:
        if not isinstance(insight, dict):
            continue
        synth = str(insight.get("client_state_synthesis") or "").strip()
        if synth:
            out.append(synth[:280])
        for w in (insight.get("wins") or [])[:2]:
            out.append(str(w)[:220])
    for s in (signals.get("meeting_summaries") or [])[:4]:
        out.append(str(s)[:220])
    return out[:10]


def _collect_signals(db: Session, org_id: uuid.UUID) -> Dict[str, Any]:
    try:
        from app.services.content_studio_fathom_context import collect_fathom_sales_signals

        return collect_fathom_sales_signals(db, org_id)
    except Exception as e:
        logger.warning("content_angle_map signals skipped: %s", e)
        return {"themes": [], "insights": [], "meeting_summaries": [], "has_any": False}


def map_to_out(
    row: ContentAngleMap,
    *,
    organization_name: str,
    can_generate_icp: bool,
    can_generate_brand: bool,
) -> ContentAngleMapOut:
    return ContentAngleMapOut(
        org_id=row.org_id,
        organization_name=organization_name,
        icp_angles=[ContentAngleItemOut(**a) for a in normalize_angle_list(row.icp_angles)],
        personal_brand_angles=[
            ContentAngleItemOut(**a) for a in normalize_angle_list(row.personal_brand_angles)
        ],
        format_pills_tof=_string_list(row.format_pills_tof, DEFAULT_PILLS_TOF),
        format_pills_mof=_string_list(row.format_pills_mof, DEFAULT_PILLS_MOF),
        format_pills_bof=_string_list(row.format_pills_bof, DEFAULT_PILLS_BOF),
        icp_placeholders=list(ICP_PLACEHOLDERS),
        brand_placeholders=list(BRAND_PLACEHOLDERS),
        last_generated_at=row.last_generated_at,
        last_generated_icp_at=row.last_generated_icp_at,
        last_generated_brand_at=row.last_generated_brand_at,
        can_generate_icp=can_generate_icp,
        can_generate_brand=can_generate_brand,
    )


def load_map_out(db: Session, org_id: uuid.UUID) -> ContentAngleMapOut:
    org = db.query(Organization).filter(Organization.id == org_id).first()
    row = get_or_create_map(db, org_id)
    ta, story = seed_fields(resolve_org_ai_profile_dict(db, org_id))
    return map_to_out(
        row,
        organization_name=(org.name if org else "") or "Organization",
        can_generate_icp=bool(ta),
        can_generate_brand=bool(story),
    )


def patch_angle(db: Session, org_id: uuid.UUID, card: str, item_id: str, text: str) -> ContentAngleMapOut:
    row = get_or_create_map(db, org_id)
    field = "icp_angles" if card == "icp" else "personal_brand_angles"
    items = normalize_angle_list(getattr(row, field))
    cleaned = text.strip()
    if not cleaned:
        raise ValueError("Angle text is required")
    found = False
    for item in items:
        if item["id"] == item_id:
            item["text"] = cleaned[:160]
            item["manually_edited"] = True
            found = True
            break
    if not found:
        items.append({"id": item_id, "text": cleaned[:160], "manually_edited": True})
    setattr(row, field, items)
    flag_modified(row, field)
    row.updated_at = datetime.utcnow()
    db.add(row)
    db.commit()
    db.refresh(row)
    return load_map_out(db, org_id)


def delete_angle(db: Session, org_id: uuid.UUID, card: str, item_id: str) -> ContentAngleMapOut:
    row = get_or_create_map(db, org_id)
    field = "icp_angles" if card == "icp" else "personal_brand_angles"
    items = [i for i in normalize_angle_list(getattr(row, field)) if i["id"] != item_id]
    setattr(row, field, items)
    flag_modified(row, field)
    row.updated_at = datetime.utcnow()
    db.add(row)
    db.commit()
    db.refresh(row)
    return load_map_out(db, org_id)


def replace_pills(db: Session, org_id: uuid.UUID, stage: str, pills: List[Any]) -> ContentAngleMapOut:
    field = PILL_FIELDS.get(stage)
    if not field:
        raise ValueError("Invalid stage")
    row = get_or_create_map(db, org_id)
    setattr(row, field, normalize_pills(pills))
    flag_modified(row, field)
    row.updated_at = datetime.utcnow()
    db.add(row)
    db.commit()
    db.refresh(row)
    return load_map_out(db, org_id)


def _phrases_to_items(phrases: List[str]) -> List[Dict[str, Any]]:
    return [{"id": str(uuid.uuid4()), "text": p, "manually_edited": False} for p in phrases]


def _draft_card_llm(
    db: Session,
    org_id: uuid.UUID,
    card: str,
    seed: str,
    extra_context: str,
) -> List[str]:
    from app.services.llm_client import chat_json, llm_available

    if not llm_available():
        raise RuntimeError("LLM is not configured")
    if card == "icp":
        task = (
            "Extract 5-6 distinct psychological messaging angles for this coach's ICP. "
            "Short phrases only (not full sentences): fears, frustrations, identity tensions, anxieties."
        )
        feature = FEATURE_ICP
    else:
        task = (
            "Extract 5-6 distinct personal-brand messaging angles from the founder story. "
            "Short phrases only (not full sentences): origin, credibility, difference, lived proof."
        )
        feature = FEATURE_BRAND
    user_prompt = (
        f"# version: {PROMPT_VERSION}\n"
        f"{task}\n"
        "Return ONLY JSON: {\"angles\": [\"phrase\", ...]}\n"
        "Rules: 5-6 items. No duplicates. Do not copy the source verbatim. "
        "No trailing punctuation. Each phrase under 80 characters.\n\n"
        f"SOURCE:\n{seed[:4000]}\n"
    )
    if extra_context:
        user_prompt += f"\nOPTIONAL CALL SIGNALS (refine, do not invent):\n{extra_context[:3500]}\n"
    parsed = chat_json(
        KAI_SYSTEM,
        user_prompt,
        temperature=0.7,
        timeout=60.0,
        org_id=org_id,
        feature=feature,
        max_tokens=600,
    )
    raw = parsed.get("angles") if isinstance(parsed, dict) else None
    if not isinstance(raw, list):
        raise RuntimeError("LLM returned no angles")
    return validate_angle_phrases(raw, seed)


def generate_card(
    db: Session,
    org_id: uuid.UUID,
    card: str,
    *,
    full: bool = False,
) -> ContentAngleMapOut:
    profile = resolve_org_ai_profile_dict(db, org_id)
    target_audience, personal_story = seed_fields(profile)
    seed = target_audience if card == "icp" else personal_story
    if not seed:
        raise ValueError(
            "Add your ideal client (target audience) in Intelligence first."
            if card == "icp"
            else "Add your personal story in Intelligence first."
        )
    signals = _collect_signals(db, org_id)
    if card == "icp":
        extra = json.dumps(
            {
                "themes": (signals.get("themes") or [])[:8],
                "objection_quotes": [
                    q
                    for ins in (signals.get("insights") or [])[:8]
                    if isinstance(ins, dict)
                    for q in (ins.get("objection_quotes") or [])[:2]
                ][:12],
            },
            ensure_ascii=False,
            default=str,
        )
    else:
        extra = json.dumps({"founder_relevant": _founder_snippets(signals)}, ensure_ascii=False)

    phrases = _draft_card_llm(db, org_id, card, seed, extra)
    if len(phrases) < MIN_KEEP:
        raise RuntimeError("Generated angles did not pass validation")

    row = get_or_create_map(db, org_id)
    field = "icp_angles" if card == "icp" else "personal_brand_angles"
    existing = normalize_angle_list(getattr(row, field))
    merged = merge_angles(existing, _phrases_to_items(phrases), full=full)
    setattr(row, field, merged)
    flag_modified(row, field)
    now = datetime.utcnow()
    row.updated_at = now
    row.last_generated_at = now
    if card == "icp":
        row.last_generated_icp_at = now
    else:
        row.last_generated_brand_at = now
    row.calls_seen_at_generation = complete_insight_count(db, org_id)
    row.input_fingerprint = compute_input_fingerprint(
        target_audience, personal_story, _signal_digest(signals)
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return load_map_out(db, org_id)


def generate_eligible_cards(
    db: Session,
    org_id: uuid.UUID,
    *,
    full: bool = False,
    only_empty: bool = False,
) -> None:
    profile = resolve_org_ai_profile_dict(db, org_id)
    target_audience, personal_story = seed_fields(profile)
    row = get_or_create_map(db, org_id)
    cards: List[str] = []
    if target_audience and (not only_empty or not normalize_angle_list(row.icp_angles)):
        cards.append("icp")
    if personal_story and (not only_empty or not normalize_angle_list(row.personal_brand_angles)):
        cards.append("personal_brand")
    for card in cards:
        try:
            generate_card(db, org_id, card, full=full)
        except Exception as e:
            logger.warning("content_angle_map generate %s org=%s failed: %s", card, org_id, e)


def generate_outside_session(
    org_id: uuid.UUID,
    *,
    card: Optional[str] = None,
    full: bool = False,
    only_empty: bool = False,
) -> None:
    from app.db.session import SessionLocal

    lock = _org_lock(org_id)
    if not lock.acquire(blocking=False):
        return
    db = SessionLocal()
    try:
        if card:
            generate_card(db, org_id, card, full=full)
        else:
            generate_eligible_cards(db, org_id, full=full, only_empty=only_empty)
    except Exception as e:
        logger.warning("content_angle_map background gen org=%s failed: %s", org_id, e)
    finally:
        db.close()
        lock.release()


def maybe_queue_initial_generation(org_id: Any) -> None:
    oid = _as_uuid(org_id)
    from app.db.session import SessionLocal

    db = SessionLocal()
    try:
        if not org_is_consulting(db, oid):
            return
        ta, story = seed_fields(resolve_org_ai_profile_dict(db, oid))
        if not ta and not story:
            return
        row = get_or_create_map(db, oid)
        empty_icp = not normalize_angle_list(row.icp_angles)
        empty_brand = not normalize_angle_list(row.personal_brand_angles)
        if not ((ta and empty_icp) or (story and empty_brand)):
            return
    finally:
        db.close()

    t = threading.Thread(
        target=generate_outside_session,
        args=(oid,),
        kwargs={"only_empty": True},
        daemon=True,
        name=f"cam-init-{oid}",
    )
    t.start()


def maybe_queue_fathom_refresh(org_id: Any) -> None:
    oid = _as_uuid(org_id)
    from app.db.session import SessionLocal

    db = SessionLocal()
    try:
        if not org_is_consulting(db, oid):
            return
        ta, story = seed_fields(resolve_org_ai_profile_dict(db, oid))
        if not ta and not story:
            return
        row = db.query(ContentAngleMap).filter(ContentAngleMap.org_id == oid).first()
        signals = _collect_signals(db, oid)
        fp = compute_input_fingerprint(ta, story, _signal_digest(signals))
        count = complete_insight_count(db, oid)
        if not should_auto_refresh(row, current_call_count=count, new_fingerprint=fp):
            return
    finally:
        db.close()

    t = threading.Thread(
        target=generate_outside_session,
        args=(oid,),
        kwargs={"full": False, "only_empty": False},
        daemon=True,
        name=f"cam-fathom-{oid}",
    )
    t.start()

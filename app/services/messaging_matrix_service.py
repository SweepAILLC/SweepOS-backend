"""Messaging Matrix: tag published content with messaging angles, score angles, explain them.

Pipeline (refresh_matrix):
1. Transcribe Meta ads over the spend floor (meta_ad_transcript_service).
2. Collect items: Instagram posts with a transcript, ads with a transcript.
3. Tag each new or changed item with one angle (LLM, prompt TAG_PROMPT_VERSION). Tags are
   cached by transcript hash; rejected tags are cached too, so nothing is re-billed.
4. Score every item and every angle in code (messaging_matrix_scoring).
5. Rewrite reasoning (LLM) only for angles whose score moved REASONING_REWRITE_DELTA+.

Recreate (recreate_angle) drafts hooks and an outline from an angle's best transcripts.
Drafts are stored with approved=False and are never posted.

LLM.md rules: SWEEPBOT_SYSTEM, versioned prompts, chat_json usage logging, evidence
quotes required, confidence < 0.6 rejected.
"""
from __future__ import annotations

import difflib
import hashlib
import json
import logging
import re
import threading
import uuid
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import func
from sqlalchemy.orm import Session

from app.core.ai_persona import SWEEPBOT_SYSTEM
from app.models.instagram_media import InstagramMedia
from app.models.messaging_angle import (
    AWARENESS_STAGES,
    SOURCE_INSTAGRAM,
    SOURCE_META_AD,
    MessagingAngle,
    MessagingAngleSource,
)
from app.models.meta_ad import MetaAd
from app.models.meta_ad_insight_daily import MetaAdInsightDaily
from app.services import messaging_matrix_scoring as scoring
from app.services import meta_ad_transcript_service as ad_tx

logger = logging.getLogger(__name__)

TAG_PROMPT_VERSION = "v0.1"
REASONING_PROMPT_VERSION = "v0.1"
RECREATE_PROMPT_VERSION = "v0.1"
FEATURE_TAG = "messaging_angle_tag"
FEATURE_REASONING = "messaging_angle_reasoning"
FEATURE_RECREATE = "messaging_angle_recreate"

MIN_CONFIDENCE = 0.6
ORGANIC_WINDOW_DAYS = 180
MAX_TAG_CALLS_PER_RUN = 100
MAX_ANGLES_IN_PROMPT = 60
NEAR_DUPE_RATIO = 0.82
REASONING_REWRITE_DELTA = 10.0
TRANSCRIPT_PROMPT_CHARS = 6000
HOOK_MECHANICS = ("callout", "contrarian", "story", "proof", "question", "how_to", "list", "mistake", "other")

# ----- access -----------------------------------------------------------------------------


def org_has_matrix_access(db: Session, org_id: uuid.UUID) -> bool:
    """Consulting-tier orgs, plus Sweep's own main org (it has no consulting tier but runs
    the same content and ads, the same way system owners see the org portal)."""
    from app.api.deps import MAIN_ORG_ID
    from app.services.content_angle_map import org_is_consulting

    return str(org_id) == str(MAIN_ORG_ID) or org_is_consulting(db, org_id)


# ----- refresh status + per-org lock ----------------------------------------------------

_local_status: Dict[str, Dict[str, Any]] = {}
_local_locks: Dict[str, threading.Lock] = {}
_locks_guard = threading.Lock()
_STATUS_TTL_SEC = 24 * 3600
_LOCK_TTL_SEC = 30 * 60


def _redis():
    try:
        from app.long_jobs import _get_redis

        return _get_redis()
    except Exception:  # noqa: BLE001
        return None


# A run that dies outside refresh_matrix (worker killed, redeploy, job import error)
# never writes "done"/"failed". Past these ages a queued/running status is treated as
# interrupted so the UI stops polling and Refresh works again.
_STALE_QUEUED_SEC = 15 * 60
_STALE_RUNNING_SEC = _LOCK_TTL_SEC + 5 * 60


def _mark_stale(status: Dict[str, Any], now: Optional[datetime] = None) -> Dict[str, Any]:
    state = status.get("state")
    if state not in ("queued", "running"):
        return status
    stamp = status.get("started_at") if state == "running" else status.get("queued_at")
    limit = _STALE_RUNNING_SEC if state == "running" else _STALE_QUEUED_SEC
    try:
        since = datetime.fromisoformat(str(stamp))
    except (TypeError, ValueError):
        return status
    if ((now or datetime.utcnow()) - since).total_seconds() <= limit:
        return status
    return {
        **status,
        "state": "failed",
        "error": "The last refresh was interrupted before it finished. Run it again.",
        "interrupted": True,
    }


def get_refresh_status(org_id: uuid.UUID) -> Dict[str, Any]:
    r = _redis()
    if r is not None:
        try:
            raw = r.get(f"mm:status:{org_id}")
            if raw:
                return _mark_stale(json.loads(raw))
        except Exception:  # noqa: BLE001
            logger.debug("messaging matrix status read failed", exc_info=True)
    return _mark_stale(dict(_local_status.get(str(org_id)) or {"state": "idle"}))


def _set_status(org_id: uuid.UUID, **status: Any) -> None:
    _local_status[str(org_id)] = status
    r = _redis()
    if r is not None:
        try:
            r.set(f"mm:status:{org_id}", json.dumps(status, default=str), ex=_STATUS_TTL_SEC)
        except Exception:  # noqa: BLE001
            logger.debug("messaging matrix status write failed", exc_info=True)


def _acquire(org_id: uuid.UUID) -> bool:
    r = _redis()
    if r is not None:
        try:
            return bool(r.set(f"mm:lock:{org_id}", "1", nx=True, ex=_LOCK_TTL_SEC))
        except Exception:  # noqa: BLE001
            pass
    with _locks_guard:
        lock = _local_locks.setdefault(str(org_id), threading.Lock())
    return lock.acquire(blocking=False)


def _release(org_id: uuid.UUID) -> None:
    r = _redis()
    if r is not None:
        try:
            r.delete(f"mm:lock:{org_id}")
            return
        except Exception:  # noqa: BLE001
            pass
    lock = _local_locks.get(str(org_id))
    if lock is not None and lock.locked():
        lock.release()


def mark_queued(org_id: uuid.UUID) -> None:
    _set_status(org_id, state="queued", queued_at=datetime.utcnow().isoformat())


def is_refresh_active(org_id: uuid.UUID) -> bool:
    return get_refresh_status(org_id).get("state") in ("queued", "running")


# ----- text helpers -----------------------------------------------------------------------


def transcript_hash(text: str) -> str:
    return hashlib.sha256((text or "").encode("utf-8", errors="ignore")).hexdigest()


def _norm(text: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^\w\s']", " ", (text or "").lower())).strip()


def quote_in_source(quote: str, source: str) -> bool:
    """True when the quote appears in the source, ignoring case, punctuation and spacing."""
    q = _norm(quote)
    return len(q) >= 8 and q in _norm(source)


def _clip(value: Any, n: int) -> Optional[str]:
    if value is None:
        return None
    s = str(value).strip()
    return s[:n] if s else None


# ----- items ------------------------------------------------------------------------------


class Item:
    """One piece of content to tag: an Instagram post or a Meta ad."""

    __slots__ = ("source_type", "source_id", "text", "metrics")

    def __init__(self, source_type: str, source_id: uuid.UUID, text: str, metrics: Dict[str, Any]):
        self.source_type = source_type
        self.source_id = source_id
        self.text = text
        self.metrics = metrics


def _organic_items(db: Session, org_id: uuid.UUID) -> Tuple[List[Item], Dict[uuid.UUID, float]]:
    """Transcribed posts in the window, plus percentile item scores. Percentiles are taken
    over every rankable post in the window, so untranscribed posts still set the bar."""
    from app.services.instagram_performance import rank_scored_posts

    since = datetime.utcnow() - timedelta(days=ORGANIC_WINDOW_DAYS)
    posts = db.query(InstagramMedia).filter(InstagramMedia.org_id == org_id, InstagramMedia.posted_at >= since).all()
    pct = scoring.percentiles({p.id: score for score, p in rank_scored_posts(posts)})
    items: List[Item] = []
    for p in posts:
        if p.transcript_status != "ok" or not p.transcript or p.id not in pct:
            continue
        text = f"{p.transcript}\n\nCAPTION: {p.caption}" if p.caption else p.transcript
        items.append(
            Item(
                SOURCE_INSTAGRAM,
                p.id,
                text,
                {
                    "views": p.views,
                    "reach": p.reach,
                    "saved": p.saved,
                    "shares": p.shares,
                    "comments": p.comments,
                    "engagement_rate_pct": p.engagement_rate_pct,
                    "posted_at": p.posted_at.isoformat() if p.posted_at else None,
                },
            )
        )
    return items, pct


def _ad_totals(db: Session, org_id: uuid.UUID, meta_ad_ids: List[str]) -> Dict[str, Dict[str, int]]:
    if not meta_ad_ids:
        return {}
    s = func.coalesce
    rows = (
        db.query(
            MetaAdInsightDaily.entity_id,
            s(func.sum(MetaAdInsightDaily.spend_cents), 0),
            s(func.sum(MetaAdInsightDaily.results), 0),
            s(func.sum(MetaAdInsightDaily.thruplays), 0),
            s(func.sum(MetaAdInsightDaily.video_3s_views), 0),
            s(func.sum(MetaAdInsightDaily.impressions), 0),
            s(func.sum(MetaAdInsightDaily.clicks), 0),
        )
        .filter(
            MetaAdInsightDaily.org_id == org_id,
            MetaAdInsightDaily.level == "ad",
            MetaAdInsightDaily.entity_id.in_(meta_ad_ids),
        )
        .group_by(MetaAdInsightDaily.entity_id)
        .all()
    )
    keys = ("spend_cents", "results", "thruplays", "video_3s_views", "impressions", "clicks")
    return {str(r[0]): {k: int(v) for k, v in zip(keys, r[1:])} for r in rows}


def _booked_calls_by_ad(db: Session, org_id: uuid.UUID, ads: List[MetaAd]) -> Dict[str, int]:
    if not ads:
        return {}
    try:
        from app.services.ad_attribution import attribute_ads

        result = attribute_ads(db, org_id, ads, None, date.today())
        return {mid: int(c.get("booked_calls") or 0) for mid, c in (result.get("by_ad") or {}).items()}
    except Exception:  # noqa: BLE001 - attribution is a bonus signal; scoring falls back to results
        logger.warning("messaging matrix: ad attribution failed org=%s", org_id, exc_info=True)
        return {}


def _paid_items(db: Session, org_id: uuid.UUID) -> Tuple[List[Item], Dict[uuid.UUID, float], str]:
    ads = ad_tx.eligible_ads(db, org_id, ad_tx.ad_spend_cents_by_meta_id(db, org_id))
    totals = _ad_totals(db, org_id, [a.meta_ad_id for a in ads])
    booked = _booked_calls_by_ad(db, org_id, ads)
    metrics = {a.id: {**totals.get(a.meta_ad_id, {}), "booked_calls": booked.get(a.meta_ad_id, 0)} for a in ads}
    pct, mode = scoring.paid_item_scores(metrics)
    items = [
        Item(
            SOURCE_META_AD,
            a.id,
            a.transcript,
            {**metrics[a.id], "transcript_source": a.transcript_source, "meta_ad_id": a.meta_ad_id},
        )
        for a in ads
        if a.transcript_status == "ok" and a.transcript
    ]
    return items, pct, mode


# ----- tagging (LLM) ------------------------------------------------------------------------


def angles_for_prompt(angles: List[MessagingAngle]) -> Tuple[str, List[MessagingAngle]]:
    active = [a for a in angles if a.status == "active"]
    active.sort(key=lambda a: (a.organic_samples or 0) + (a.paid_samples or 0), reverse=True)
    active = active[:MAX_ANGLES_IN_PROMPT]
    lines = [f"{i}. {a.name} | {a.awareness_stage or '?'} | {a.description or ''}" for i, a in enumerate(active, start=1)]
    return ("\n".join(lines) or "(none yet)"), active


def build_tag_prompt(item: Item, angle_lines: str) -> str:
    kind = "organic Instagram post" if item.source_type == SOURCE_INSTAGRAM else "paid Meta ad"
    return (
        f"# version: {TAG_PROMPT_VERSION}\n"
        f"Identify the ONE messaging angle this {kind} uses.\n"
        "An angle is the core persuasive claim, not the topic. Example: "
        "'Your offer is the bottleneck, not your ads' rather than 'offers'.\n\n"
        "EXISTING ANGLES (reuse one when the meaning matches, even if worded differently):\n"
        f"{angle_lines}\n\n"
        "Return ONLY JSON:\n"
        '{"existing_angle": <number from the list or null>,\n'
        ' "new_angle": null or {"name": "...", "description": "...", "awareness_stage": "...",\n'
        '   "persona": "...", "promise": "...", "hook_mechanic": "..."},\n'
        ' "quote": "<8-40 words copied exactly from the TRANSCRIPT that carry the angle>",\n'
        ' "confidence": <0.0-1.0>}\n'
        "Rules: set exactly one of existing_angle / new_angle. "
        f"awareness_stage is one of {', '.join(AWARENESS_STAGES)}. "
        f"hook_mechanic is one of {', '.join(HOOK_MECHANICS)}. "
        "name is a claim under 60 characters. description is one sentence. "
        "persona is who it speaks to. promise is the outcome plus the mechanism. "
        "If the content has no clear angle, use confidence below 0.5.\n\n"
        f"TRANSCRIPT:\n{item.text[:TRANSCRIPT_PROMPT_CHARS]}\n"
    )


def _validated_new_angle(raw: Any) -> Optional[Dict[str, Optional[str]]]:
    if not isinstance(raw, dict):
        return None
    name = _clip(raw.get("name"), 120)
    if not name or len(name) < 4:
        return None
    stage = _clip(raw.get("awareness_stage"), 24)
    hook = _clip(raw.get("hook_mechanic"), 32)
    return {
        "name": name.rstrip(".!"),
        "description": _clip(raw.get("description"), 400),
        "awareness_stage": stage if stage in AWARENESS_STAGES else None,
        "persona": _clip(raw.get("persona"), 120),
        "promise": _clip(raw.get("promise"), 400),
        "hook_mechanic": hook if hook in HOOK_MECHANICS else "other",
    }


def _confidence(parsed: Dict[str, Any]) -> float:
    try:
        return float(parsed.get("confidence") or 0.0)
    except (TypeError, ValueError):
        return 0.0


def parse_tag_response(
    parsed: Any, item_text: str, prompt_angles: List[MessagingAngle]
) -> Tuple[Optional[Any], Optional[str], float]:
    """-> (existing MessagingAngle | new-angle dict | None, quote, confidence). None = rejected."""
    if not isinstance(parsed, dict):
        return None, None, 0.0
    confidence = _confidence(parsed)
    quote = _clip(parsed.get("quote"), 600)
    if confidence < MIN_CONFIDENCE or not quote or not quote_in_source(quote, item_text):
        return None, quote, confidence
    idx = parsed.get("existing_angle")
    if isinstance(idx, (int, float)) and not isinstance(idx, bool) and 1 <= int(idx) <= len(prompt_angles):
        return prompt_angles[int(idx) - 1], quote, confidence
    new = _validated_new_angle(parsed.get("new_angle"))
    return (new, quote, confidence) if new else (None, quote, confidence)


def find_near_duplicate(name: str, angles: List[MessagingAngle]) -> Optional[MessagingAngle]:
    target = _norm(name)
    best, best_ratio = None, 0.0
    for a in angles:
        r = difflib.SequenceMatcher(None, target, _norm(a.name)).ratio()
        if r > best_ratio:
            best, best_ratio = a, r
    return best if best_ratio >= NEAR_DUPE_RATIO else None


def _tag_item(
    db: Session, org_id: uuid.UUID, item: Item, angles: List[MessagingAngle]
) -> Tuple[Optional[MessagingAngle], Optional[str], float]:
    from app.services.llm_client import chat_json

    angle_lines, prompt_angles = angles_for_prompt(angles)
    parsed = chat_json(
        SWEEPBOT_SYSTEM,
        build_tag_prompt(item, angle_lines),
        temperature=0.2,
        timeout=60.0,
        org_id=org_id,
        feature=FEATURE_TAG,
        max_tokens=500,
        prompt_version=TAG_PROMPT_VERSION,
    )
    result, quote, confidence = parse_tag_response(parsed, item.text, prompt_angles)
    if result is None or isinstance(result, MessagingAngle):
        return result, quote, confidence
    dupe = find_near_duplicate(result["name"], angles)
    if dupe is not None:
        return dupe, quote, confidence
    angle = MessagingAngle(org_id=org_id, status="active", organic_samples=0, paid_samples=0, **result)
    db.add(angle)
    db.flush()
    angles.append(angle)
    return angle, quote, confidence


# ----- reasoning (LLM) ----------------------------------------------------------------------


def build_reasoning_prompt(angle: MessagingAngle, sources: List[MessagingAngleSource], mode: str) -> str:
    ranked = sorted(sources, key=lambda s: s.item_score or 0, reverse=True)
    picked = ranked[:3] + (ranked[-1:] if len(ranked) > 3 else [])
    lines = []
    for s in picked:
        m = s.metrics_snapshot or {}
        keep = {k: m.get(k) for k in ("views", "engagement_rate_pct", "spend_cents", "booked_calls", "results") if m.get(k) is not None}
        lines.append(f'- [{s.source_type}, item score {s.item_score}] "{s.quote}" metrics={keep}')
    paid_basis = "cost per booked call" if mode == "booked_call" else "cost per result"
    return (
        f"# version: {REASONING_PROMPT_VERSION}\n"
        "Explain in 2-3 plain sentences why this messaging angle performs the way it does "
        "for this coach. Scores are 0-100 percentiles against the coach's own content "
        f"(organic: engagement vs their other posts; paid: {paid_basis} vs their other ads; 50 = average).\n"
        "Use ONLY numbers that appear below. Do not invent metrics. Refer to the quotes.\n"
        'Return ONLY JSON: {"reasoning": "...", "confidence": <0.0-1.0>}\n\n'
        f"ANGLE: {angle.name}\n"
        f"DESCRIPTION: {angle.description or ''}\n"
        f"AWARENESS: {angle.awareness_stage or ''} | HOOK: {angle.hook_mechanic or ''}\n"
        f"ORGANIC SCORE: {angle.organic_score} from {angle.organic_samples} posts\n"
        f"PAID SCORE: {angle.paid_score} from {angle.paid_samples} ads\n"
        f"OVERALL: {angle.overall_score}\n"
        "EVIDENCE:\n" + "\n".join(lines) + "\n"
    )


def _write_reasoning(
    db: Session, org_id: uuid.UUID, angle: MessagingAngle, sources: List[MessagingAngleSource], mode: str
) -> bool:
    from app.services.llm_client import chat_json

    parsed = chat_json(
        SWEEPBOT_SYSTEM,
        build_reasoning_prompt(angle, sources, mode),
        temperature=0.3,
        timeout=60.0,
        org_id=org_id,
        feature=FEATURE_REASONING,
        max_tokens=300,
        prompt_version=REASONING_PROMPT_VERSION,
    )
    if not isinstance(parsed, dict):
        return False
    text = _clip(parsed.get("reasoning"), 1200)
    if _confidence(parsed) < MIN_CONFIDENCE or not text:
        return False
    angle.reasoning = text
    angle.reasoning_score = angle.overall_score
    angle.reasoning_prompt_version = REASONING_PROMPT_VERSION
    return True


def needs_reasoning(angle: MessagingAngle) -> bool:
    if angle.overall_score is None:
        return False
    if not angle.reasoning or angle.reasoning_score is None:
        return True
    return abs(angle.overall_score - angle.reasoning_score) >= REASONING_REWRITE_DELTA


# ----- refresh ------------------------------------------------------------------------------


def _rescore(db: Session, org_id: uuid.UUID, items: List[Item], item_scores: Dict[uuid.UUID, float]) -> None:
    """Write item scores/metrics onto source rows, then aggregate every angle."""
    by_key = {(i.source_type, i.source_id): i for i in items}
    per_angle: Dict[uuid.UUID, Dict[str, List[float]]] = {}
    for s in db.query(MessagingAngleSource).filter(MessagingAngleSource.org_id == org_id).all():
        item = by_key.get((s.source_type, s.source_id))
        s.item_score = item_scores.get(s.source_id) if item else None
        if item:
            s.metrics_snapshot = item.metrics
        if s.angle_id is None or s.item_score is None:
            continue
        bucket = per_angle.setdefault(s.angle_id, {"organic": [], "paid": []})
        bucket["organic" if s.source_type == SOURCE_INSTAGRAM else "paid"].append(s.item_score)
    now = datetime.utcnow()
    for angle in db.query(MessagingAngle).filter(MessagingAngle.org_id == org_id).all():
        b = per_angle.get(angle.id, {"organic": [], "paid": []})
        for k, v in scoring.angle_scores(b["organic"], b["paid"]).items():
            setattr(angle, k, v)
        angle.scored_at = now


def refresh_matrix(db: Session, org_id: uuid.UUID) -> Dict[str, Any]:
    """Run the whole pipeline for one org. Safe to repeat: unchanged content costs no LLM
    calls. Never raises; errors land in the refresh status."""
    if not _acquire(org_id):
        return {"skipped": "already_running"}
    started = datetime.utcnow()
    _set_status(org_id, state="running", started_at=started.isoformat())
    stats: Dict[str, Any] = {"tagged": 0, "rejected": 0, "new_angles": 0, "reasoned": 0}
    try:
        from app.services.llm_client import llm_available

        stats["ads"] = ad_tx.transcribe_eligible_ads(db, org_id)
        organic, organic_pct = _organic_items(db, org_id)
        paid, paid_pct, mode = _paid_items(db, org_id)
        items = organic + paid
        stats["paid_score_basis"] = mode

        existing = {
            (s.source_type, s.source_id): s
            for s in db.query(MessagingAngleSource).filter(MessagingAngleSource.org_id == org_id).all()
        }
        angles = db.query(MessagingAngle).filter(MessagingAngle.org_id == org_id).all()
        before = len(angles)
        can_llm = llm_available()
        calls = 0
        for item in items:
            h = transcript_hash(item.text)
            row = existing.get((item.source_type, item.source_id))
            if row is not None and row.transcript_hash == h and row.prompt_version == TAG_PROMPT_VERSION:
                continue
            if not can_llm or calls >= MAX_TAG_CALLS_PER_RUN:
                stats["pending_items"] = stats.get("pending_items", 0) + 1
                continue
            calls += 1
            try:
                angle, quote, confidence = _tag_item(db, org_id, item, angles)
            except Exception:  # noqa: BLE001 - budget/slot/provider errors: retried next run
                logger.warning("messaging matrix tag failed org=%s item=%s", org_id, item.source_id, exc_info=True)
                continue
            if row is None:
                row = MessagingAngleSource(org_id=org_id, source_type=item.source_type, source_id=item.source_id)
                db.add(row)
            row.angle_id = angle.id if angle else None
            row.quote = quote if angle else None
            row.tag_confidence = confidence
            row.transcript_hash = h
            row.prompt_version = TAG_PROMPT_VERSION
            stats["tagged" if angle else "rejected"] += 1
            db.flush()
        stats["new_angles"] = len(angles) - before

        _rescore(db, org_id, items, {**organic_pct, **paid_pct})
        db.commit()

        if can_llm:
            by_angle: Dict[uuid.UUID, List[MessagingAngleSource]] = {}
            for s in db.query(MessagingAngleSource).filter(
                MessagingAngleSource.org_id == org_id, MessagingAngleSource.angle_id.isnot(None)
            ):
                by_angle.setdefault(s.angle_id, []).append(s)
            for angle in db.query(MessagingAngle).filter(
                MessagingAngle.org_id == org_id, MessagingAngle.status == "active"
            ).all():
                if not needs_reasoning(angle):
                    continue
                try:
                    if _write_reasoning(db, org_id, angle, by_angle.get(angle.id, []), mode):
                        stats["reasoned"] += 1
                except Exception:  # noqa: BLE001
                    logger.warning("messaging matrix reasoning failed angle=%s", angle.id, exc_info=True)
            db.commit()

        _set_status(
            org_id, state="done", started_at=started.isoformat(), finished_at=datetime.utcnow().isoformat(), stats=stats
        )
        return stats
    except Exception as exc:  # noqa: BLE001
        db.rollback()
        logger.exception("messaging matrix refresh failed org=%s", org_id)
        _set_status(
            org_id,
            state="failed",
            started_at=started.isoformat(),
            finished_at=datetime.utcnow().isoformat(),
            error=str(exc)[:300],
        )
        return {**stats, "error": str(exc)[:300]}
    finally:
        _release(org_id)


def refresh_matrix_job(org_id: str) -> Dict[str, Any]:
    """Background entry point (RQ or thread): opens its own DB session."""
    from app.db.session import SessionLocal

    db = SessionLocal()
    try:
        return refresh_matrix(db, uuid.UUID(str(org_id)))
    finally:
        db.close()


# ----- reads --------------------------------------------------------------------------------


def angle_out(a: MessagingAngle) -> Dict[str, Any]:
    return {
        "id": str(a.id),
        "name": a.name,
        "description": a.description,
        "awareness_stage": a.awareness_stage,
        "persona": a.persona,
        "promise": a.promise,
        "hook_mechanic": a.hook_mechanic,
        "organic_score": a.organic_score,
        "paid_score": a.paid_score,
        "overall_score": a.overall_score,
        "organic_samples": a.organic_samples or 0,
        "paid_samples": a.paid_samples or 0,
        "reasoning": a.reasoning,
        "status": a.status,
        "scored_at": a.scored_at.isoformat() if a.scored_at else None,
    }


def rank_angles(
    angles: List[MessagingAngle], *, channel: Optional[str] = None, min_samples: int = 1
) -> List[MessagingAngle]:
    score_attr = {"organic": "organic_score", "paid": "paid_score"}.get(channel or "", "overall_score")

    def samples(a: MessagingAngle) -> int:
        if channel == "organic":
            return a.organic_samples or 0
        if channel == "paid":
            return a.paid_samples or 0
        return (a.organic_samples or 0) + (a.paid_samples or 0)

    rows = [a for a in angles if samples(a) >= max(0, min_samples) and getattr(a, score_attr) is not None]
    rows.sort(key=lambda a: getattr(a, score_attr) or 0, reverse=True)
    return rows


def list_matrix(
    db: Session,
    org_id: uuid.UUID,
    *,
    channel: Optional[str] = None,
    awareness_stage: Optional[str] = None,
    min_samples: int = 1,
    include_archived: bool = False,
    limit: int = 50,
) -> Dict[str, Any]:
    q = db.query(MessagingAngle).filter(MessagingAngle.org_id == org_id)
    if not include_archived:
        q = q.filter(MessagingAngle.status == "active")
    if awareness_stage:
        q = q.filter(MessagingAngle.awareness_stage == awareness_stage)
    rows = rank_angles(q.all(), channel=channel, min_samples=min_samples)
    counts = dict(
        db.query(
            MessagingAngleSource.angle_id.is_(None),
            func.count(MessagingAngleSource.id),
        )
        .filter(MessagingAngleSource.org_id == org_id)
        .group_by(MessagingAngleSource.angle_id.is_(None))
        .all()
    )
    return {
        "angles": [angle_out(a) for a in rows[: max(1, min(limit, 200))]],
        "total_angles": len(rows),
        "tagged_items": int(counts.get(False, 0)),
        "rejected_items": int(counts.get(True, 0)),
        "refresh": get_refresh_status(org_id),
        "score_note": (
            "Scores are 0-100 percentiles against this org's own content, shrunk toward 50 "
            "when an angle has few samples. 50 = average."
        ),
    }


def _load_angle(db: Session, org_id: uuid.UUID, angle_id: uuid.UUID) -> MessagingAngle:
    angle = db.query(MessagingAngle).filter(MessagingAngle.id == angle_id, MessagingAngle.org_id == org_id).first()
    if angle is None:
        raise LookupError("Messaging angle not found")
    return angle


def angle_detail(db: Session, org_id: uuid.UUID, angle_id: uuid.UUID) -> Dict[str, Any]:
    angle = _load_angle(db, org_id, angle_id)
    sources = (
        db.query(MessagingAngleSource)
        .filter(MessagingAngleSource.org_id == org_id, MessagingAngleSource.angle_id == angle.id)
        .all()
    )
    ig_ids = [s.source_id for s in sources if s.source_type == SOURCE_INSTAGRAM]
    ad_ids = [s.source_id for s in sources if s.source_type == SOURCE_META_AD]
    posts = (
        {p.id: p for p in db.query(InstagramMedia).filter(InstagramMedia.org_id == org_id, InstagramMedia.id.in_(ig_ids))}
        if ig_ids
        else {}
    )
    ads = {a.id: a for a in db.query(MetaAd).filter(MetaAd.org_id == org_id, MetaAd.id.in_(ad_ids))} if ad_ids else {}
    out = []
    for s in sorted(sources, key=lambda s: s.item_score if s.item_score is not None else -1, reverse=True):
        if s.source_type == SOURCE_INSTAGRAM:
            p = posts.get(s.source_id)
            title = (p.hook_text or p.caption or "Instagram post")[:120] if p else "Instagram post"
            url, thumb = (p.permalink, p.thumbnail_url) if p else (None, None)
        else:
            a = ads.get(s.source_id)
            title = (a.name or "Meta ad") if a else "Meta ad"
            url, thumb = None, (a.creative_thumbnail_url if a else None)
        out.append(
            {
                "source_type": s.source_type,
                "source_id": str(s.source_id),
                "title": title,
                "url": url,
                "thumbnail_url": thumb,
                "quote": s.quote,
                "item_score": s.item_score,
                "metrics": s.metrics_snapshot or {},
            }
        )
    return {**angle_out(angle), "sources": out, "last_draft": angle.last_draft}


def set_angle_status(db: Session, org_id: uuid.UUID, angle_id: uuid.UUID, status: str) -> Dict[str, Any]:
    if status not in ("active", "archived"):
        raise ValueError("status must be active or archived")
    angle = _load_angle(db, org_id, angle_id)
    angle.status = status
    db.commit()
    return angle_out(angle)


# ----- recreate (LLM, draft only) -------------------------------------------------------------


def _source_texts(db: Session, org_id: uuid.UUID, sources: List[MessagingAngleSource]) -> List[Tuple[MessagingAngleSource, str]]:
    out: List[Tuple[MessagingAngleSource, str]] = []
    for s in sources:
        model = InstagramMedia if s.source_type == SOURCE_INSTAGRAM else MetaAd
        row = db.query(model).filter(model.org_id == org_id, model.id == s.source_id).first()
        if row is not None and row.transcript:
            out.append((s, row.transcript))
    return out


def build_recreate_prompt(angle: MessagingAngle, examples: List[Tuple[MessagingAngleSource, str]], fmt: str) -> str:
    blocks = [
        f"EXAMPLE {i} ({s.source_type}, item score {s.item_score}):\n{text[:3000]}"
        for i, (s, text) in enumerate(examples, start=1)
    ]
    return (
        f"# version: {RECREATE_PROMPT_VERSION}\n"
        f"Draft a new {fmt} script that uses this proven messaging angle. Keep the angle; "
        "change the story, examples and wording so it is not a copy.\n"
        'Return ONLY JSON: {"hooks": ["...", "...", "..."], "outline": ["beat 1", "..."], '
        '"cta": "...", "evidence": ["<short quote from an EXAMPLE that the draft builds on>"], '
        '"confidence": <0.0-1.0>}\n'
        "Rules: exactly 3 hooks, each under 20 words. Outline is 4-7 beats. "
        "Every evidence item must be copied from an EXAMPLE.\n\n"
        f"ANGLE: {angle.name}\nDESCRIPTION: {angle.description or ''}\n"
        f"AWARENESS: {angle.awareness_stage or ''}\nPERSONA: {angle.persona or ''}\n"
        f"PROMISE: {angle.promise or ''}\nHOOK MECHANIC: {angle.hook_mechanic or ''}\n\n"
        + "\n\n".join(blocks)
        + "\n"
    )


def parse_recreate_response(parsed: Any, corpus: str) -> Dict[str, Any]:
    """Validate a recreate response. Raises RuntimeError when it fails LLM.md's bar."""
    if not isinstance(parsed, dict):
        raise RuntimeError("The draft came back empty")
    hooks = [h for h in (_clip(x, 200) for x in (parsed.get("hooks") or [])) if h][:3]
    outline = [b for b in (_clip(x, 300) for x in (parsed.get("outline") or [])) if b][:7]
    evidence = [q for q in (_clip(x, 400) for x in (parsed.get("evidence") or [])) if q and quote_in_source(q, corpus)]
    confidence = _confidence(parsed)
    if confidence < MIN_CONFIDENCE or len(hooks) < 3 or len(outline) < 3 or not evidence:
        raise RuntimeError("The draft did not meet the quality bar (confidence or evidence). Try again.")
    return {
        "hooks": hooks,
        "outline": outline,
        "cta": _clip(parsed.get("cta"), 300),
        "evidence": evidence,
        "confidence": round(confidence, 2),
    }


def recreate_angle(
    db: Session,
    org_id: uuid.UUID,
    angle_id: uuid.UUID,
    *,
    channel: str = "organic",
    user_id: Optional[uuid.UUID] = None,
) -> Dict[str, Any]:
    """Draft three hooks and an outline in this angle, stored on the angle with
    approved=False. Nothing is posted. Raises LookupError / ValueError / RuntimeError."""
    from app.services.llm_client import chat_json, llm_available

    if channel not in ("organic", "paid"):
        raise ValueError("channel must be organic or paid")
    if not llm_available():
        raise RuntimeError("LLM is not configured")
    angle = _load_angle(db, org_id, angle_id)
    sources = (
        db.query(MessagingAngleSource)
        .filter(MessagingAngleSource.org_id == org_id, MessagingAngleSource.angle_id == angle.id)
        .all()
    )
    sources.sort(key=lambda s: s.item_score if s.item_score is not None else -1, reverse=True)
    examples = _source_texts(db, org_id, sources[:4])[:2]
    if not examples:
        raise ValueError("This angle has no transcripts to build from yet")
    parsed = chat_json(
        SWEEPBOT_SYSTEM,
        build_recreate_prompt(angle, examples, "Instagram reel" if channel == "organic" else "Meta video ad"),
        temperature=0.7,
        timeout=90.0,
        org_id=org_id,
        feature=FEATURE_RECREATE,
        max_tokens=900,
        prompt_version=RECREATE_PROMPT_VERSION,
    )
    body = parse_recreate_response(parsed, "\n".join(t for _, t in examples))
    draft = {
        "channel": channel,
        **body,
        "based_on": [{"source_type": s.source_type, "source_id": str(s.source_id)} for s, _ in examples],
        "approved": False,
        "prompt_version": RECREATE_PROMPT_VERSION,
        "created_at": datetime.utcnow().isoformat(),
        "created_by_user_id": str(user_id) if user_id else None,
    }
    angle.last_draft = draft
    db.commit()
    return draft

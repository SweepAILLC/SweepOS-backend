"""Orchestrate call insight generation, persistence, and rollup summary."""
from __future__ import annotations

import hashlib
import json
import logging
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import desc, nullslast
from sqlalchemy.orm import Session

from app.core.config import settings
from app.models.client import Client
from app.models.client_call_insight import ClientCallInsight, ClientInsightSummary
from app.models.fathom_call_record import FathomCallRecord
from app.services.call_insight_ai import compute_call_insight_json, headline_from_insight, validate_and_normalize_insight_json
from app.services.call_insight_context import assemble_context_pack, is_thin_transcript
from app.services.call_insight_correlation import link_fathom_to_checkin
from app.services.health_score_cache_service import resolve_health_score

logger = logging.getLogger(__name__)

_org_hour_lock = threading.Lock()
_org_hour_counts: Dict[str, int] = {}


def _org_bucket_key(org_id: uuid.UUID) -> str:
    hour = int(time.time() // 3600)
    return f"{org_id}:{hour}"


def check_org_insight_throttle(org_id: uuid.UUID) -> bool:
    """Return True if allowed, False if over hourly cap."""
    max_h = int(getattr(settings, "CALL_INSIGHT_ORG_MAX_PER_HOUR", 40) or 40)
    key = _org_bucket_key(org_id)
    with _org_hour_lock:
        c = _org_hour_counts.get(key, 0)
        if c >= max_h:
            return False
        _org_hour_counts[key] = c + 1
    # prune old keys occasionally
    if len(_org_hour_counts) > 5000:
        cutoff = int(time.time() // 3600) - 2
        stale = [k for k in _org_hour_counts if int(k.split(":")[-1]) < cutoff]
        for k in stale[:1000]:
            _org_hour_counts.pop(k, None)
    return True


def hash_context_pack(pack: Dict[str, Any]) -> str:
    canonical = json.dumps(pack, sort_keys=True, default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _lifecycle_str(client: Client) -> str:
    ls = client.lifecycle_state
    if hasattr(ls, "value"):
        return str(ls.value)
    return str(ls)


def run_call_insight_for_fathom_record(
    db: Session,
    org_id: uuid.UUID,
    fathom_call_record_id: uuid.UUID,
    *,
    bypass_cooldown: bool = False,
) -> Tuple[str, Optional[Dict[str, Any]]]:
    """
    Returns (status, detail) where status is ok|skipped|failed.
    """
    rec = (
        db.query(FathomCallRecord)
        .filter(
            FathomCallRecord.id == fathom_call_record_id,
            FathomCallRecord.org_id == org_id,
        )
        .first()
    )
    if not rec or not rec.client_id:
        logger.info(
            "call_insight skipped no_record_or_client org=%s record=%s",
            org_id,
            fathom_call_record_id,
        )
        return "skipped", {"reason": "no_record_or_client"}

    if rec.sentiment_status != "complete":
        logger.info(
            "call_insight skipped sentiment_not_complete org=%s record=%s",
            org_id,
            fathom_call_record_id,
        )
        return "skipped", {"reason": "sentiment_not_complete"}

    client = db.query(Client).filter(Client.id == rec.client_id, Client.org_id == org_id).first()
    if not client:
        logger.info(
            "call_insight skipped client_not_found org=%s record=%s",
            org_id,
            fathom_call_record_id,
        )
        return "skipped", {"reason": "client_not_found"}

    existing = (
        db.query(ClientCallInsight)
        .filter(ClientCallInsight.fathom_call_record_id == fathom_call_record_id)
        .first()
    )
    if existing and existing.status == "complete" and existing.insight_json:
        return "skipped", {"reason": "already_computed"}

    check_in_id = link_fathom_to_checkin(db, org_id, rec.client_id, rec.meeting_at)

    health = resolve_health_score(
        db, rec.client_id, org_id, brevo_email_stats=None, use_ai=False, persist_cache=False
    )
    if not health:
        return "skipped", {"reason": "no_health"}

    pack = assemble_context_pack(db, client, rec, check_in_id, health)
    input_hash = hash_context_pack(pack)

    if existing and existing.input_hash == input_hash and existing.status == "complete":
        logger.debug("call_insight skipped same_hash record=%s", fathom_call_record_id)
        return "skipped", {"reason": "same_hash"}

    if is_thin_transcript(pack):
        row = existing or ClientCallInsight(
            id=uuid.uuid4(),
            org_id=org_id,
            client_id=rec.client_id,
            fathom_call_record_id=fathom_call_record_id,
            check_in_id=check_in_id,
        )
        row.status = "skipped"
        row.failure_reason = "thin_transcript"
        row.computed_at = datetime.now(timezone.utc)
        row.input_hash = input_hash
        row.lifecycle_at_compute = _lifecycle_str(client)
        row.check_in_id = check_in_id
        if not existing:
            db.add(row)
        db.commit()
        logger.info(
            "call_insight skipped thin_transcript org=%s record=%s",
            org_id,
            fathom_call_record_id,
        )
        return "skipped", {"reason": "thin_transcript"}

    # Per-org hourly cap (before LLM). POST refresh may bypass (bypass_cooldown=True).
    if not bypass_cooldown and not check_org_insight_throttle(org_id):
        logger.warning(
            "call_insight org throttle skip org_id=%s record=%s",
            org_id,
            fathom_call_record_id,
        )
        row = existing or ClientCallInsight(
            id=uuid.uuid4(),
            org_id=org_id,
            client_id=rec.client_id,
            fathom_call_record_id=fathom_call_record_id,
            check_in_id=check_in_id,
        )
        row.status = "skipped"
        row.failure_reason = "org_throttle"
        row.computed_at = datetime.now(timezone.utc)
        row.input_hash = input_hash
        row.lifecycle_at_compute = _lifecycle_str(client)
        row.check_in_id = check_in_id
        row.insight_json = None
        if not existing:
            db.add(row)
        db.commit()
        return "skipped", {"reason": "org_throttle"}

    insight_json = compute_call_insight_json(
        context_pack=pack,
        lifecycle=_lifecycle_str(client),
        org_id=org_id,
    )
    if not insight_json:
        row = existing or ClientCallInsight(
            id=uuid.uuid4(),
            org_id=org_id,
            client_id=rec.client_id,
            fathom_call_record_id=fathom_call_record_id,
            check_in_id=check_in_id,
        )
        row.status = "failed"
        row.failure_reason = "llm_unavailable_or_failed"
        row.computed_at = datetime.now(timezone.utc)
        row.input_hash = input_hash
        row.lifecycle_at_compute = _lifecycle_str(client)
        row.model = getattr(settings, "HEALTH_SCORE_LLM_MODEL", None)
        if not existing:
            db.add(row)
        db.commit()
        logger.warning(
            "call_insight failed llm org=%s record=%s",
            org_id,
            fathom_call_record_id,
        )
        return "failed", {"reason": "llm"}

    insight_json = validate_and_normalize_insight_json(insight_json)

    row = existing or ClientCallInsight(
        id=uuid.uuid4(),
        org_id=org_id,
        client_id=rec.client_id,
        fathom_call_record_id=fathom_call_record_id,
        check_in_id=check_in_id,
    )
    row.insight_json = insight_json
    row.status = "complete"
    row.failure_reason = None
    row.computed_at = datetime.now(timezone.utc)
    row.input_hash = input_hash
    row.lifecycle_at_compute = _lifecycle_str(client)
    row.model = getattr(settings, "HEALTH_SCORE_LLM_MODEL", None)
    if not existing:
        db.add(row)

    _upsert_summary(db, org_id, client, rec, insight_json, health)
    db.commit()
    logger.info(
        "call_insight complete org=%s record=%s insight_id=%s",
        org_id,
        fathom_call_record_id,
        row.id,
    )
    try:
        from app.services.org_sales_theme_service import record_from_completed_insight

        record_from_completed_insight(db, row)
    except Exception as e:
        logger.warning("org_sales_theme record_from_completed_insight skipped: %s", e)

    try:
        from app.services.client_ai_recommendations_service import (
            merge_call_insight_actions_for_fathom_record,
            merge_prospect_voice_from_insight_into_client,
        )

        c2 = db.query(Client).filter(Client.id == client.id).first()
        if c2:
            merge_call_insight_actions_for_fathom_record(
                db,
                c2,
                fathom_call_record_id,
                insight_json.get("next_steps") or [],
            )
            merge_prospect_voice_from_insight_into_client(c2, insight_json.get("prospect_voice"))
            db.commit()
    except Exception as e:
        logger.exception("post_call_insight_merge failed client=%s: %s", client.id, e)
        db.rollback()

    return "ok", {"insight_id": str(row.id)}


def _upsert_summary(
    db: Session,
    org_id: uuid.UUID,
    client: Client,
    fathom_rec: FathomCallRecord,
    insight: Dict[str, Any],
    health: Dict[str, Any],
) -> None:
    summ = db.query(ClientInsightSummary).filter(ClientInsightSummary.client_id == client.id).first()
    if not summ:
        summ = ClientInsightSummary(client_id=client.id, org_id=org_id, tags=[])
        db.add(summ)

    low = bool(insight.get("low_signal"))
    tags = [] if low else list(dict.fromkeys(insight.get("opportunity_tags") or []))
    summ.headline = headline_from_insight(insight)
    summ.tags = tags[:12]
    summ.last_call_at = fathom_rec.meeting_at or fathom_rec.updated_at
    summ.last_insight_at = datetime.now(timezone.utc)
    summ.last_lifecycle_state = _lifecycle_str(client)
    summ.last_health_grade = str(health.get("grade") or "")[:8]
    try:
        summ.last_health_score = float(health.get("score")) if health.get("score") is not None else None
    except (TypeError, ValueError):
        summ.last_health_score = None


def run_call_insight_background(org_id_str: str, fathom_record_id_str: str) -> None:
    """New DB session for thread/BackgroundTasks."""
    from app.db.session import SessionLocal

    db = SessionLocal()
    try:
        oid = uuid.UUID(org_id_str)
        rid = uuid.UUID(fathom_record_id_str)
        status, detail = run_call_insight_for_fathom_record(db, oid, rid, bypass_cooldown=False)
        logger.info(
            "call_insight background done record=%s status=%s detail=%s",
            fathom_record_id_str,
            status,
            detail,
        )
    except Exception as e:
        logger.exception("call_insight background failed: %s", e)
    finally:
        db.close()


def refresh_latest_call_insight(
    db: Session, org_id: uuid.UUID, client_id: uuid.UUID
) -> Tuple[str, Optional[Dict[str, Any]]]:
    """Re-run LLM for the latest Fathom recording (manual refresh)."""
    rec = (
        db.query(FathomCallRecord)
        .filter(
            FathomCallRecord.org_id == org_id,
            FathomCallRecord.client_id == client_id,
            FathomCallRecord.sentiment_status == "complete",
        )
        .order_by(nullslast(desc(FathomCallRecord.meeting_at)), desc(FathomCallRecord.created_at))
        .first()
    )
    if not rec:
        return "skipped", {"reason": "no_fathom_recording"}
    db.query(ClientCallInsight).filter(ClientCallInsight.fathom_call_record_id == rec.id).delete()
    db.commit()
    return run_call_insight_for_fathom_record(db, org_id, rec.id, bypass_cooldown=True)


def _build_call_insights_rollup(db: Session, client: Client, rows: List[ClientCallInsight]) -> Dict[str, Any]:
    """Aggregate priorities, suggestions, clips, wins across completed insights (no per-call drill-down in UI)."""
    open_priorities: List[str] = []
    open_suggestions: List[Dict[str, str]] = []
    clips: List[Dict[str, Any]] = []
    wins: List[str] = []
    stories: List[str] = []
    seen_p: set = set()
    seen_s: set = set()
    # Single narrative paragraph: always from the most recent complete insight (rows are newest-first).
    latest_synthesis = ""

    for r in rows:
        if r.status != "complete" or not r.insight_json or not isinstance(r.insight_json, dict):
            continue
        ij = r.insight_json
        if not latest_synthesis:
            s = str(ij.get("client_state_synthesis") or "").strip()
            if s:
                latest_synthesis = s[:2500]
        fj = r.fathom_call_record
        mat = fj.meeting_at.isoformat() if fj and fj.meeting_at else None
        for p in ij.get("priorities") or []:
            ps = str(p).strip()[:500]
            if ps and ps not in seen_p:
                seen_p.add(ps)
                open_priorities.append(ps)
        for ns in ij.get("next_steps") or []:
            if not isinstance(ns, dict):
                continue
            t = str(ns.get("title") or "").strip()
            d = str(ns.get("detail") or "").strip()
            sig = (t[:120], d[:120])
            if sig in seen_s:
                continue
            seen_s.add(sig)
            open_suggestions.append(
                {
                    "title": t[:300],
                    "detail": d[:1200],
                    "meeting_at": mat or "",
                }
            )
        for c in (ij.get("clips") or [])[:10]:
            if isinstance(c, dict):
                cc = dict(c)
                cc["meeting_at"] = mat
                clips.append(cc)
        for w in ij.get("wins") or []:
            ws = str(w).strip()
            if ws and ws not in wins:
                wins.append(ws[:400])
        for st in ij.get("testimonial_stories") or []:
            ss = str(st).strip()
            if ss and ss not in stories:
                stories.append(ss[:600])

    prof: Dict[str, Any] = {}
    if isinstance(client.meta, dict):
        raw = client.meta.get("prospect_voice_profile")
        if isinstance(raw, dict):
            prof = raw

    out = {
        "client_state_synthesis": latest_synthesis,
        "accumulated_priorities": open_priorities[:25],
        "accumulated_call_suggestions": open_suggestions[:40],
        "accumulated_clips": clips[:35],
        "accumulated_wins": wins[:20],
        "accumulated_testimonial_stories": stories[:15],
        "prospect_voice_profile": prof,
        "org_validated_theme_keys": [],
    }
    try:
        from app.services.org_sales_theme_service import enrich_clips_org_validation, list_validated_theme_keys

        oid = client.org_id
        enrich_clips_org_validation(db, oid, out["accumulated_clips"])
        out["org_validated_theme_keys"] = list_validated_theme_keys(db, oid)
    except Exception:
        pass
    return out


def get_client_insights_response(db: Session, org_id: uuid.UUID, client_id: uuid.UUID, limit: int = 25) -> Dict[str, Any]:
    """Shape for GET /clients/{id}/call-insights."""
    client = db.query(Client).filter(Client.id == client_id, Client.org_id == org_id).first()
    if not client:
        return {}

    summ = db.query(ClientInsightSummary).filter(ClientInsightSummary.client_id == client_id).first()
    summary_out = None
    if summ:
        summary_out = {
            "headline": summ.headline,
            "tags": summ.tags or [],
            "last_call_at": summ.last_call_at.isoformat() if summ.last_call_at else None,
            "last_insight_at": summ.last_insight_at.isoformat() if summ.last_insight_at else None,
        }

    rows = (
        db.query(ClientCallInsight)
        .filter(ClientCallInsight.client_id == client_id, ClientCallInsight.org_id == org_id)
        .order_by(desc(ClientCallInsight.computed_at))
        .limit(limit)
        .all()
    )

    insights = []
    for r in rows:
        fj = r.fathom_call_record
        insights.append(
            {
                "id": str(r.id),
                "fathom_call_record_id": str(r.fathom_call_record_id),
                "fathom_recording_id": int(fj.fathom_recording_id) if fj else None,
                "meeting_at": fj.meeting_at.isoformat() if fj and fj.meeting_at else None,
                "status": r.status,
                "computed_at": r.computed_at.isoformat() if r.computed_at else None,
                "insight": r.insight_json,
                "failure_reason": r.failure_reason,
            }
        )

    rollup = _build_call_insights_rollup(db, client, rows)

    return {
        "client_id": str(client_id),
        "summary": summary_out,
        "insights": insights,
        "rollup": rollup,
    }


def get_call_insight_tags_batch(db: Session, org_id: uuid.UUID, client_ids: List[uuid.UUID]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    if not client_ids:
        return out
    rows = (
        db.query(ClientInsightSummary)
        .filter(
            ClientInsightSummary.org_id == org_id,
            ClientInsightSummary.client_id.in_(client_ids),
        )
        .all()
    )
    for r in rows:
        out[str(r.client_id)] = {
            "tags": r.tags or [],
            "headline": (r.headline or "")[:120],
        }
    return out

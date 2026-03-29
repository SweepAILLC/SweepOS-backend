"""
Persisted health score resolution: input hash, cache read/write, optional AI overlay, outcome snapshots.
"""
from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from sqlalchemy.orm import Session

from app.core.config import settings
from app.models.client import Client
from app.models.client_health_score_cache import ClientHealthScoreCache
from app.models.fathom_call_record import FathomCallRecord
from app.models.health_outcome_snapshot import HealthOutcomeSnapshot
from app.services.client_health_score import assemble_factors_and_logic_score
from app.services.fathom_client import resolve_fathom_api_key
from app.services.health_score_ai import compute_ai_health_score
from app.services.llm_client import llm_available
from app.services.similar_outcomes import build_feature_bucket, fetch_similar_past_outcomes


def _lifecycle_str(client: Client) -> str:
    ls = client.lifecycle_state
    if hasattr(ls, "value"):
        return str(ls.value)
    return str(ls)


def _prospect_fingerprint(meta: Optional[Dict[str, Any]]) -> str:
    if not meta or not isinstance(meta, dict):
        return ""
    p = meta.get("prospect")
    if not p or not isinstance(p, dict):
        return ""
    try:
        return hashlib.sha256(json.dumps(p, sort_keys=True, default=str).encode()).hexdigest()[:32]
    except Exception:
        return ""


def _fathom_signature_for_hash(db: Session, org_id: uuid.UUID, client_id: uuid.UUID) -> List[Any]:
    rows = (
        db.query(
            FathomCallRecord.fathom_recording_id,
            FathomCallRecord.sentiment_label,
            FathomCallRecord.sentiment_score,
        )
        .filter(
            FathomCallRecord.org_id == org_id,
            FathomCallRecord.client_id == client_id,
            FathomCallRecord.sentiment_status == "complete",
        )
        .order_by(FathomCallRecord.fathom_recording_id.desc())
        .limit(50)
        .all()
    )
    return [
        [r.fathom_recording_id, r.sentiment_label, round(float(r.sentiment_score or 0), 1)]
        for r in rows
    ]


def _brevo_key(stats: Optional[Dict[str, Any]]) -> Any:
    if stats is None:
        return None
    keys = (
        "campaign_open_rate",
        "campaign_click_rate",
        "messages_sent",
        "trans_open_rate",
        "trans_click_rate",
    )
    return {k: stats.get(k) for k in keys}


def _factors_fingerprint(factors: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out = []
    for f in factors:
        out.append(
            {
                "key": f.get("key"),
                "value": f.get("value"),
                "raw": f.get("raw"),
            }
        )
    return out


def compute_input_hash(
    org_id: uuid.UUID,
    client_id: uuid.UUID,
    factors: List[Dict[str, Any]],
    brevo_stats: Optional[Dict[str, Any]],
    fathom_sig: List[Any],
    lifecycle: str,
    prospect_fp: str,
    use_ai: bool,
    fathom_api_configured: bool,
) -> str:
    """Include fathom_api_configured so cache invalidates when FATHOM_API_KEY is added/removed."""
    payload = {
        "org_id": str(org_id),
        "client_id": str(client_id),
        "use_ai": use_ai,
        "fathom_api_configured": fathom_api_configured,
        "brevo": _brevo_key(brevo_stats),
        "factors": _factors_fingerprint(factors),
        "fathom": fathom_sig,
        "lifecycle": lifecycle,
        "prospect_fp": prospect_fp,
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True, default=str).encode()).hexdigest()


def get_latest_fathom_sentiment(
    db: Session, org_id: uuid.UUID, client_id: uuid.UUID
) -> Optional[Dict[str, Any]]:
    r = (
        db.query(FathomCallRecord)
        .filter(
            FathomCallRecord.org_id == org_id,
            FathomCallRecord.client_id == client_id,
            FathomCallRecord.sentiment_status == "complete",
        )
        .order_by(FathomCallRecord.created_at.desc())
        .first()
    )
    if not r:
        return None
    return {
        "sentiment_label": r.sentiment_label,
        "sentiment_score": r.sentiment_score,
        "snippet": r.sentiment_snippet,
        "meeting_at": r.meeting_at.isoformat() if r.meeting_at else None,
    }


def _prospect_context(client: Client) -> Dict[str, Any]:
    meta = client.meta if isinstance(client.meta, dict) else {}
    p = meta.get("prospect")
    if isinstance(p, dict):
        return {k: p.get(k) for k in ("source", "quiz_answers", "opt_in_data", "funnel_id", "funnel_step_reached", "captured_at") if k in p}
    return {}


def batch_read_cached_health_scores(
    db: Session,
    client_ids: List[uuid.UUID],
    org_id: uuid.UUID,
) -> Dict[uuid.UUID, Dict[str, Any]]:
    """
    Fast cache-only read for the board.  No Brevo / LLM / factor computation.
    Returns {client_id: {score, grade, source, computed_at}} for every client
    that has a cache row.  Clients without a cached score are simply omitted.
    """
    if not client_ids:
        return {}
    rows = (
        db.query(ClientHealthScoreCache)
        .filter(
            ClientHealthScoreCache.client_id.in_(client_ids),
            ClientHealthScoreCache.org_id == org_id,
        )
        .all()
    )
    out: Dict[uuid.UUID, Dict[str, Any]] = {}
    for r in rows:
        out[r.client_id] = {
            "score": r.score,
            "grade": r.grade,
            "source": r.source or "logic",
            "computed_at": r.computed_at.isoformat() if r.computed_at else None,
        }
    return out


def invalidate_health_score_cache(
    db: Session, client_id: uuid.UUID, org_id: Optional[uuid.UUID] = None, *, do_commit: bool = True
) -> None:
    q = db.query(ClientHealthScoreCache).filter(ClientHealthScoreCache.client_id == client_id)
    if org_id is not None:
        q = q.filter(ClientHealthScoreCache.org_id == org_id)
    row = q.first()
    if row:
        db.delete(row)
        if do_commit:
            db.commit()


def resolve_health_score(
    db: Session,
    client_id: uuid.UUID,
    org_id: uuid.UUID,
    brevo_email_stats: Optional[Dict[str, Any]],
    *,
    use_ai: bool = False,
    persist_cache: bool = True,
    record_outcome_snapshot: bool = True,
) -> Dict[str, Any]:
    """
    Returns same shape as get_health_score plus source, explanation (optional), source_reason (optional).
    Reads cache when input_hash matches; otherwise computes, persists cache + outcome snapshot.

    persist_cache=False: compute logic score in memory only (no DB read/write). Used by call-insight and
    email-draft so internal pipelines do not overwrite the persisted board cache.

    When persist_cache=True and use_ai=False, if the cache still holds a valid AI snapshot for current
    inputs, the row is left unchanged so board tags keep the last AI score until inputs change or
    use_ai=True recomputes.

    record_outcome_snapshot=False skips writing HealthOutcomeSnapshot (e.g. batch logic backfill).
    """
    client = db.query(Client).filter(Client.id == client_id, Client.org_id == org_id).first()
    if not client:
        return {}

    factors, logic_score, logic_grade = assemble_factors_and_logic_score(db, client, org_id, brevo_email_stats)

    if not persist_cache:
        now_iso = datetime.now(timezone.utc).isoformat()
        return {
            "client_id": str(client_id),
            "score": float(logic_score),
            "grade": logic_grade,
            "factors": factors,
            "computed_at": now_iso,
            "source": "logic",
            "explanation": None,
            "source_reason": None,
        }

    fathom_sig = _fathom_signature_for_hash(db, org_id, client_id)
    prospect_fp = _prospect_fingerprint(client.meta if isinstance(client.meta, dict) else None)
    fathom_api_configured = bool(resolve_fathom_api_key(db, org_id))
    h = compute_input_hash(
        org_id,
        client_id,
        factors,
        brevo_email_stats,
        fathom_sig,
        _lifecycle_str(client),
        prospect_fp,
        use_ai,
        fathom_api_configured,
    )

    cache_row = db.query(ClientHealthScoreCache).filter(ClientHealthScoreCache.client_id == client_id).first()
    if cache_row and cache_row.input_hash == h:
        return {
            "client_id": str(client_id),
            "score": cache_row.score,
            "grade": cache_row.grade,
            "factors": cache_row.factors_json or factors,
            "computed_at": cache_row.computed_at.isoformat() if cache_row.computed_at else datetime.now(timezone.utc).isoformat(),
            "source": cache_row.source or "logic",
            "explanation": cache_row.explanation,
            "source_reason": None,
        }

    fathom_sentiment = get_latest_fathom_sentiment(db, org_id, client_id)
    prospect_ctx = _prospect_context(client)

    source = "logic"
    explanation: Optional[str] = None
    source_reason: Optional[str] = None
    final_score = float(logic_score)
    final_grade = logic_grade

    # AI overlay only when Fathom is configured (env). Until then, logic score only (plan: graceful degradation).
    want_ai = (
        use_ai
        and settings.AI_HEALTH_SCORE_ENABLED
        and llm_available()
        and fathom_api_configured
    )
    if use_ai and not fathom_api_configured:
        source_reason = "fathom_not_configured"
    elif use_ai and not settings.AI_HEALTH_SCORE_ENABLED:
        source_reason = "ai_disabled"
    elif use_ai and not llm_available():
        source_reason = "ai_unavailable"

    similar: List[Dict[str, Any]] = []
    ai_result = None
    if want_ai:
        try:
            similar = fetch_similar_past_outcomes(
                db, org_id, client_id, factors, fathom_sentiment, limit=5
            )
            ai_result = compute_ai_health_score(
                factors=factors,
                brevo_summary=brevo_email_stats or {},
                fathom_sentiment=fathom_sentiment,
                prospect_context=prospect_ctx or None,
                similar_past_outcomes=similar or None,
                org_id=org_id,
            )
        except Exception:
            ai_result = None
            if want_ai:
                source_reason = source_reason or "ai_error"

    if ai_result:
        final_score = float(ai_result["score"])
        final_grade = ai_result["grade"]
        explanation = ai_result.get("explanation")
        source = "ai"
    else:
        if want_ai:
            source_reason = source_reason or "ai_failed"

    # Logic-only request: do not downgrade persisted AI cache while inputs still match that AI run.
    if (
        not use_ai
        and cache_row
        and (cache_row.source or "") == "ai"
    ):
        h_ai = compute_input_hash(
            org_id,
            client_id,
            factors,
            brevo_email_stats,
            fathom_sig,
            _lifecycle_str(client),
            prospect_fp,
            True,
            fathom_api_configured,
        )
        if cache_row.input_hash == h_ai:
            ca = cache_row.computed_at.isoformat() if cache_row.computed_at else datetime.now(timezone.utc).isoformat()
            return {
                "client_id": str(client_id),
                "score": float(logic_score),
                "grade": logic_grade,
                "factors": factors,
                "computed_at": ca,
                "source": "logic",
                "explanation": None,
                "source_reason": None,
            }

    now = datetime.now(timezone.utc)
    factors_to_store = json.loads(json.dumps(factors, default=str))

    if not cache_row:
        cache_row = ClientHealthScoreCache(client_id=client_id, org_id=org_id)
        db.add(cache_row)
    cache_row.org_id = org_id
    cache_row.score = final_score
    cache_row.grade = final_grade
    cache_row.source = source
    cache_row.explanation = explanation
    cache_row.factors_json = factors_to_store
    cache_row.input_hash = h
    cache_row.computed_at = now

    if record_outcome_snapshot:
        fb = build_feature_bucket(factors, fathom_sentiment)
        snap = HealthOutcomeSnapshot(
            org_id=org_id,
            client_id=client_id,
            score=final_score,
            grade=final_grade,
            lifecycle_phase=_lifecycle_str(client),
            feature_bucket=fb,
            recorded_at=now,
        )
        db.add(snap)
    db.commit()

    return {
        "client_id": str(client_id),
        "score": final_score,
        "grade": final_grade,
        "factors": factors,
        "computed_at": now.isoformat(),
        "source": source,
        "explanation": explanation,
        "source_reason": source_reason,
    }

"""Org-scoped retrieval of similar past health outcomes for RAG-style prompting (SQL / bucketing)."""
from __future__ import annotations

import uuid
from typing import Any, Dict, List, Optional

from sqlalchemy.orm import Session

from app.models.health_outcome_snapshot import HealthOutcomeSnapshot


def _bucket_from_factors(factors: List[Dict[str, Any]], fathom: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    fp = 0
    for f in factors:
        if f.get("key") == "failed_payments":
            raw = f.get("raw") or {}
            fp = int(raw.get("count") or 0)
    fp_band = "0" if fp == 0 else "1" if fp == 1 else "2+"

    sent = (fathom or {}).get("sentiment_label")
    if sent not in ("positive", "neutral", "negative"):
        sent = "neutral"

    show = None
    for f in factors:
        if f.get("key") == "show_rate":
            show = f.get("value")
    if show is None:
        sr = "none"
    elif show >= 70:
        sr = "high"
    elif show >= 40:
        sr = "mid"
    else:
        sr = "low"

    return {
        "failed_payments_band": fp_band,
        "sentiment_label": sent,
        "show_rate_band": sr,
    }


def fetch_similar_past_outcomes(
    db: Session,
    org_id: uuid.UUID,
    exclude_client_id: uuid.UUID,
    factors: List[Dict[str, Any]],
    fathom_sentiment: Optional[Dict[str, Any]],
    *,
    limit: int = 5,
) -> List[Dict[str, Any]]:
    target = _bucket_from_factors(factors, fathom_sentiment)
    q = (
        db.query(HealthOutcomeSnapshot)
        .filter(
            HealthOutcomeSnapshot.org_id == org_id,
            HealthOutcomeSnapshot.client_id != exclude_client_id,
        )
        .order_by(HealthOutcomeSnapshot.recorded_at.desc())
        .limit(200)
    )
    rows = q.all()
    scored: List[tuple] = []
    for r in rows:
        fb = r.feature_bucket or {}
        score = 0
        if fb.get("sentiment_label") == target.get("sentiment_label"):
            score += 2
        if fb.get("failed_payments_band") == target.get("failed_payments_band"):
            score += 1
        if fb.get("show_rate_band") == target.get("show_rate_band"):
            score += 1
        scored.append((score, r))

    def _ts(r: HealthOutcomeSnapshot) -> float:
        ra = r.recorded_at
        if ra is None:
            return 0.0
        return ra.timestamp()

    scored.sort(key=lambda x: (-x[0], -_ts(x[1])))
    out: List[Dict[str, Any]] = []
    for _, r in scored[:limit]:
        out.append(
            {
                "past_score_range": f"{r.score:.0f} grade {r.grade}",
                "lifecycle_phase": r.lifecycle_phase,
                "pattern_summary": f"Similar bucket: {r.feature_bucket}",
            }
        )
    return out


def build_feature_bucket(factors: List[Dict[str, Any]], fathom: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    return _bucket_from_factors(factors, fathom)

"""Derive structured sentiment from Fathom summary + transcript via LLM (required before health use)."""
from __future__ import annotations

import uuid
from typing import Any, Dict, Optional, Tuple

from app.core.config import settings
from app.services.llm_client import chat_json, llm_available, truncate_for_tokens

SYSTEM = (
    "You are a classifier. Given a meeting summary and a transcript excerpt, output ONLY valid JSON "
    'with no other text. Schema: {"sentiment_label": "positive"|"neutral"|"negative", '
    '"sentiment_score": number 0-100, "snippet": string max 200 chars}. '
    "Use sentiment_label and sentiment_score for sales/client relationship tone. "
    "snippet = one concise line for display. Output nothing else."
)


def derive_sentiment(
    summary_markdown: str,
    transcript_excerpt: str,
    org_id: Optional[uuid.UUID] = None,
) -> Tuple[str, Dict[str, Any]]:
    """
    Returns (status, payload). status is 'complete' or 'failed'.
    payload has sentiment_label, sentiment_score, sentiment_snippet on success.
    org_id enables per-org LLM budget; thin inputs skip the LLM (cost + noise).
    """
    if not llm_available():
        return "failed", {"error": "no_llm"}

    sm = truncate_for_tokens(summary_markdown or "", 8000)
    tr = truncate_for_tokens(transcript_excerpt or "", 12000)
    min_in = getattr(settings, "FATHOM_SENTIMENT_MIN_INPUT_CHARS", 80)
    if len(sm) + len(tr) < min_in:
        d = default_neutral()
        d["sentiment_snippet"] = "Insufficient call text for sentiment; defaulted to neutral"
        return "complete", {
            "sentiment_label": d["sentiment_label"],
            "sentiment_score": d["sentiment_score"],
            "sentiment_snippet": d["sentiment_snippet"],
        }

    user = "DATA - Summary:\n" + sm + "\n\nDATA - Transcript excerpt:\n" + tr
    try:
        raw = chat_json(SYSTEM, user, temperature=0.0, timeout=90.0, org_id=org_id)
    except RuntimeError as e:
        if "llm_budget" in str(e).lower():
            return "failed", {"error": "llm_budget_exceeded"}
        return "failed", {"error": "llm_call_failed"}
    except Exception:
        return "failed", {"error": "llm_call_failed"}

    label = (raw.get("sentiment_label") or "").lower()
    if label not in ("positive", "neutral", "negative"):
        label = "neutral"
    try:
        score = float(raw.get("sentiment_score", 50))
    except (TypeError, ValueError):
        score = 50.0
    score = max(0.0, min(100.0, score))
    snippet = str(raw.get("snippet") or "")[:200]

    return "complete", {
        "sentiment_label": label,
        "sentiment_score": score,
        "sentiment_snippet": snippet or f"{label} tone",
    }


def default_neutral() -> Dict[str, Any]:
    return {
        "sentiment_label": "neutral",
        "sentiment_score": 50.0,
        "sentiment_snippet": "Sentiment unavailable; defaulted to neutral",
    }

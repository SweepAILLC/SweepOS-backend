"""
Per-organization sliding-window budget for outbound LLM API calls (cost protection).

Uses Redis when REDIS_URL is set so web + worker processes share one cap.
Falls back to in-memory (per process) when Redis is unavailable.
"""
from __future__ import annotations

import uuid

from app.core.config import settings
from app.core.rate_limit import sliding_window_try_acquire


def consume_llm_budget(org_id: uuid.UUID) -> bool:
    """
    Record one LLM call for org if under budget. Returns False if limit would be exceeded (do not call LLM).
    If LLM_BUDGET_ENABLED is False, always returns True without recording.
    """
    if not getattr(settings, "LLM_BUDGET_ENABLED", True):
        return True
    max_calls = getattr(settings, "LLM_MAX_CALLS_PER_MINUTE_PER_ORG", 45)
    if max_calls <= 0:
        return False
    return sliding_window_try_acquire(f"llm_budget:{org_id}", int(max_calls), 60)


def peek_llm_budget_remaining(org_id: uuid.UUID) -> int:
    """Approximate remaining calls. Memory/Redis do not expose an exact remaining count cheaply."""
    if not getattr(settings, "LLM_BUDGET_ENABLED", True):
        return 999
    max_calls = int(getattr(settings, "LLM_MAX_CALLS_PER_MINUTE_PER_ORG", 45) or 45)
    # Best-effort: try one dry peek is not available; report configured max.
    return max_calls

"""
Per-organization sliding-window budget for outbound LLM API calls (cost protection).
In-memory; resets on process restart (acceptable for single-instance / small clusters).
"""
from __future__ import annotations

import threading
import time
import uuid
from collections import defaultdict
from typing import Dict, List

from app.core.config import settings

_lock = threading.Lock()
# org_id str -> list of unix timestamps (seconds) of consumed calls
_org_windows: Dict[str, List[float]] = defaultdict(list)

_WINDOW_SEC = 60.0


def _prune(org_key: str, now: float) -> None:
    cutoff = now - _WINDOW_SEC
    _org_windows[org_key] = [t for t in _org_windows[org_key] if t > cutoff]


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
    key = str(org_id)
    now = time.time()
    with _lock:
        _prune(key, now)
        if len(_org_windows[key]) >= max_calls:
            return False
        _org_windows[key].append(now)
        return True


def peek_llm_budget_remaining(org_id: uuid.UUID) -> int:
    """Approximate remaining calls in current window (for debugging/metrics)."""
    if not getattr(settings, "LLM_BUDGET_ENABLED", True):
        return 999
    max_calls = getattr(settings, "LLM_MAX_CALLS_PER_MINUTE_PER_ORG", 45)
    key = str(org_id)
    now = time.time()
    with _lock:
        _prune(key, now)
        return max(0, max_calls - len(_org_windows[key]))

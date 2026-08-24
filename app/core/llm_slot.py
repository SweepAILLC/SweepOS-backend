"""
Global in-flight cap for outbound LLM HTTP calls.

Redis when REDIS_URL is set (shared by web + all RQ workers). Process-local
semaphore otherwise. Prevents 50-user bursts from opening unbounded provider
connections and holding DB sessions behind them.
"""
from __future__ import annotations

import logging
import threading
import time
from contextlib import contextmanager
from typing import Iterator, Optional

from app.core.config import settings

logger = logging.getLogger(__name__)

_INFLIGHT_KEY = "llm:inflight"
_INFLIGHT_TTL_SEC = 180

_local_sem: Optional[threading.BoundedSemaphore] = None
_local_lock = threading.Lock()


def _max_inflight() -> int:
    return max(1, int(getattr(settings, "LLM_MAX_INFLIGHT", 6) or 6))


def _wait_sec() -> float:
    return float(getattr(settings, "LLM_SLOT_WAIT_SEC", 45) or 45)


def _local_semaphore() -> threading.BoundedSemaphore:
    global _local_sem
    with _local_lock:
        if _local_sem is None:
            _local_sem = threading.BoundedSemaphore(_max_inflight())
        return _local_sem


def _redis():
    try:
        from app.core.rate_limit import get_shared_redis

        return get_shared_redis()
    except Exception:
        return None


def _redis_try_acquire() -> Optional[bool]:
    r = _redis()
    if r is None:
        return None
    max_n = _max_inflight()
    try:
        n = int(r.incr(_INFLIGHT_KEY))
        r.expire(_INFLIGHT_KEY, _INFLIGHT_TTL_SEC)
        if n > max_n:
            r.decr(_INFLIGHT_KEY)
            return False
        return True
    except Exception as e:
        logger.warning("LLM slot Redis acquire failed, using process semaphore: %s", e)
        return None


def _redis_release() -> None:
    r = _redis()
    if r is None:
        return
    try:
        n = int(r.decr(_INFLIGHT_KEY))
        if n < 0:
            r.set(_INFLIGHT_KEY, 0, ex=_INFLIGHT_TTL_SEC)
    except Exception:
        logger.warning("LLM slot Redis release failed", exc_info=True)


def acquire_llm_slot(timeout: Optional[float] = None) -> bool:
    """Block up to timeout seconds for a slot. Returns False if not acquired."""
    deadline = time.monotonic() + (timeout if timeout is not None else _wait_sec())
    while True:
        rr = _redis_try_acquire()
        if rr is True:
            return True
        if rr is False:
            if time.monotonic() >= deadline:
                return False
            time.sleep(0.25)
            continue

        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return False
        sem = _local_semaphore()
        if sem.acquire(timeout=remaining):
            return True
        return False


def release_llm_slot() -> None:
    if _redis() is not None:
        _redis_release()
        return
    try:
        _local_semaphore().release()
    except ValueError:
        pass


@contextmanager
def llm_slot(timeout: Optional[float] = None) -> Iterator[None]:
    if not acquire_llm_slot(timeout):
        from app.core.llm_exceptions import LLMSlotUnavailableError
        raise LLMSlotUnavailableError()
    try:
        yield
    finally:
        release_llm_slot()

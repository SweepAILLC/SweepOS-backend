"""RQ-first scheduling, shared LLM budget, and in-flight slot."""
from __future__ import annotations

import threading
import time
import uuid
from unittest.mock import MagicMock, patch

from app.core import llm_slot as llm_slot_mod
from app.core.llm_budget import consume_llm_budget
from app.core.llm_slot import acquire_llm_slot, release_llm_slot
from app.core.rate_limit import sliding_window_try_acquire
from app.long_jobs import schedule_background_work


def _reset_local_slot():
    llm_slot_mod._local_sem = None


def test_schedule_prefers_rq_even_when_background_tasks_passed():
    q = MagicMock()
    bg = MagicMock()
    fn = MagicMock()
    with patch("app.long_jobs._get_queue", return_value=q):
        schedule_background_work(fn, bg, "org", "rec")
    q.enqueue.assert_called_once()
    bg.add_task.assert_not_called()
    fn.assert_not_called()


def test_schedule_uses_background_tasks_when_rq_off():
    bg = MagicMock()
    fn = MagicMock()
    with patch("app.long_jobs._get_queue", return_value=None):
        schedule_background_work(fn, bg, "org")
    bg.add_task.assert_called_once()


def test_llm_budget_caps_per_org():
    org = uuid.uuid4()
    allowed = 0
    with patch("app.core.config.settings.LLM_BUDGET_ENABLED", True):
        with patch("app.core.config.settings.LLM_MAX_CALLS_PER_MINUTE_PER_ORG", 3):
            for _ in range(5):
                if consume_llm_budget(org):
                    allowed += 1
    assert allowed == 3
    assert consume_llm_budget(uuid.uuid4()) is True


def test_sliding_window_memory_cap():
    key = f"test-sw-{uuid.uuid4()}"
    assert sliding_window_try_acquire(key, 2, 60) is True
    assert sliding_window_try_acquire(key, 2, 60) is True
    assert sliding_window_try_acquire(key, 2, 60) is False


def test_llm_slot_blocks_over_max():
    _reset_local_slot()
    with patch("app.core.llm_slot._redis", return_value=None):
        with patch("app.core.llm_slot._max_inflight", return_value=1):
            assert acquire_llm_slot(timeout=0.2) is True
            got = []

            def _try():
                got.append(acquire_llm_slot(timeout=0.3))

            t = threading.Thread(target=_try)
            t.start()
            t.join(timeout=2)
            assert got == [False]
            release_llm_slot()
            assert acquire_llm_slot(timeout=0.5) is True
            release_llm_slot()


def test_llm_slot_releases_for_next_waiter():
    _reset_local_slot()
    with patch("app.core.llm_slot._redis", return_value=None):
        with patch("app.core.llm_slot._max_inflight", return_value=1):
            assert acquire_llm_slot(timeout=0.2) is True
            released = threading.Event()

            def _hold_then_release():
                time.sleep(0.15)
                release_llm_slot()
                released.set()

            threading.Thread(target=_hold_then_release, daemon=True).start()
            assert acquire_llm_slot(timeout=1.0) is True
            assert released.is_set()
            release_llm_slot()

"""Optional RQ offload for heavy work so the web process stays responsive.

Enable with REDIS_URL and USE_RQ_LONG_JOBS=true; run a worker: python -m app.worker
"""
from __future__ import annotations

import logging
import threading
import time
from typing import Any, Callable

from app.core.config import settings

logger = logging.getLogger(__name__)

_rq_queue = None
_call_library_queue = None
_redis_conn = None
_rq_init_failed = False

CALL_LIBRARY_RQ_QUEUE = "sweep_call_library"
DEFAULT_RQ_QUEUE = "sweep_long"


def long_jobs_enabled() -> bool:
    return bool(settings.REDIS_URL and settings.USE_RQ_LONG_JOBS)


def _get_redis():
    global _redis_conn, _rq_init_failed
    if not long_jobs_enabled():
        return None
    if _rq_init_failed:
        return None
    if _redis_conn is not None:
        return _redis_conn
    try:
        from redis import Redis

        _redis_conn = Redis.from_url(
            settings.REDIS_URL,
            socket_connect_timeout=2.0,
            socket_timeout=5.0,
            health_check_interval=30,
        )
        _redis_conn.ping()
        return _redis_conn
    except Exception:
        logger.exception("RQ Redis init failed; using BackgroundTasks/thread fallback for this process")
        _rq_init_failed = True
        _redis_conn = None
        return None


def _get_queue(queue_name: str = DEFAULT_RQ_QUEUE):
    global _rq_queue, _call_library_queue
    conn = _get_redis()
    if conn is None:
        return None
    if queue_name == CALL_LIBRARY_RQ_QUEUE:
        if _call_library_queue is None:
            from rq import Queue

            _call_library_queue = Queue(CALL_LIBRARY_RQ_QUEUE, connection=conn)
        return _call_library_queue
    if _rq_queue is None:
        from rq import Queue

        _rq_queue = Queue(DEFAULT_RQ_QUEUE, connection=conn)
    return _rq_queue


def _run_after_delay(delay_sec: float, fn: Callable[..., Any], *args: Any) -> None:
    """RQ-safe delayed execution: sleep inside the worker job (no scheduler required)."""
    if delay_sec > 0:
        time.sleep(delay_sec)
    fn(*args)


def schedule_background_work(
    fn: Callable[..., Any],
    background_tasks: Any | None,
    *args: Any,
    prefer_rq: bool = True,
    job_timeout: int = 900,
    queue_name: str = DEFAULT_RQ_QUEUE,
    at_front: bool = False,
) -> None:
    """Schedule background work.

    Call Library batches pass a higher ``job_timeout`` — each report can take up to
    CALL_LIBRARY_LLM_TIMEOUT_SEC and batches run sequentially with stagger.
    """
    if background_tasks is not None:
        background_tasks.add_task(fn, *args)
        return
    if prefer_rq:
        q = _get_queue(queue_name)
        if q is not None:
            try:
                q.enqueue(
                    fn,
                    *args,
                    job_timeout=job_timeout,
                    result_ttl=300,
                    at_front=at_front,
                )
                return
            except Exception:
                logger.exception(
                    "RQ enqueue failed for %s; falling back",
                    getattr(fn, "__name__", fn),
                )
    threading.Thread(target=fn, args=args, daemon=True).start()


def schedule_call_library_work(
    fn: Callable[..., Any],
    background_tasks: Any | None,
    *args: Any,
    job_timeout: int = 900,
    delay_sec: float = 0,
) -> None:
    """Call Library jobs use a dedicated high-priority queue (not blocked by Fathom sync)."""
    if delay_sec > 0:
        schedule_delayed_background_work(
            fn,
            background_tasks,
            delay_sec,
            *args,
            prefer_rq=True,
            job_timeout=job_timeout,
            queue_name=CALL_LIBRARY_RQ_QUEUE,
            at_front=True,
        )
        return
    schedule_background_work(
        fn,
        background_tasks,
        *args,
        prefer_rq=True,
        job_timeout=job_timeout,
        queue_name=CALL_LIBRARY_RQ_QUEUE,
        at_front=True,
    )


def schedule_delayed_background_work(
    fn: Callable[..., Any],
    background_tasks: Any | None,
    delay_sec: float,
    *args: Any,
    prefer_rq: bool = True,
    job_timeout: int = 900,
    queue_name: str = DEFAULT_RQ_QUEUE,
    at_front: bool = False,
) -> None:
    """Schedule background work after delay_sec.

    Uses an in-job sleep when RQ is enabled so delayed jobs run without rq-scheduler.
    """
    if delay_sec <= 0:
        schedule_background_work(
            fn,
            background_tasks,
            *args,
            prefer_rq=prefer_rq,
            job_timeout=job_timeout,
            queue_name=queue_name,
            at_front=at_front,
        )
        return

    if prefer_rq:
        q = _get_queue(queue_name)
        if q is not None:
            try:
                q.enqueue(
                    _run_after_delay,
                    delay_sec,
                    fn,
                    *args,
                    job_timeout=int(job_timeout + delay_sec + 120),
                    result_ttl=300,
                    at_front=at_front,
                )
                return
            except Exception:
                logger.exception(
                    "RQ delayed enqueue failed for %s; falling back to Timer",
                    getattr(fn, "__name__", fn),
                )

    def _kick() -> None:
        schedule_background_work(
            fn,
            None,
            *args,
            prefer_rq=prefer_rq,
            job_timeout=job_timeout,
            queue_name=queue_name,
            at_front=at_front,
        )

    t = threading.Timer(delay_sec, _kick)
    t.daemon = True
    t.start()

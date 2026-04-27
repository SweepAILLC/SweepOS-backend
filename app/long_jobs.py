"""Optional RQ offload for heavy work so the web process stays responsive.

Enable with REDIS_URL and USE_RQ_LONG_JOBS=true; run a worker: python -m app.worker
"""
from __future__ import annotations

import logging
import threading
from typing import Any, Callable

from app.core.config import settings

logger = logging.getLogger(__name__)

_rq_queue = None
_redis_conn = None
_rq_init_failed = False


def long_jobs_enabled() -> bool:
    return bool(settings.REDIS_URL and settings.USE_RQ_LONG_JOBS)


def _get_queue():
    global _rq_queue, _redis_conn, _rq_init_failed
    if not long_jobs_enabled():
        return None
    if _rq_init_failed:
        return None
    if _rq_queue is not None:
        return _rq_queue
    try:
        from redis import Redis
        from rq import Queue

        _redis_conn = Redis.from_url(
            settings.REDIS_URL,
            socket_connect_timeout=2.0,
            socket_timeout=5.0,
            health_check_interval=30,
        )
        _redis_conn.ping()
        _rq_queue = Queue("sweep_long", connection=_redis_conn)
        return _rq_queue
    except Exception:
        logger.exception("RQ Redis init failed; using BackgroundTasks/thread fallback for this process")
        _rq_init_failed = True
        _redis_conn = None
        _rq_queue = None
        return None


def schedule_background_work(
    fn: Callable[..., Any],
    background_tasks: Any | None,
    *args: Any,
) -> None:
    """Prefer RQ when enabled; else FastAPI BackgroundTasks; else a daemon thread."""
    q = _get_queue()
    if q is not None:
        try:
            q.enqueue(fn, *args, job_timeout=900, result_ttl=300)
            return
        except Exception:
            logger.exception(
                "RQ enqueue failed for %s; falling back",
                getattr(fn, "__name__", fn),
            )
    if background_tasks is not None:
        background_tasks.add_task(fn, *args)
        return
    threading.Thread(target=fn, args=args, daemon=True).start()

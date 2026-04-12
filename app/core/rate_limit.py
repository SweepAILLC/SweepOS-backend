"""
Sliding-window rate limiting for API endpoints.

- **In-memory** (default): single-process; suitable for dev and single-worker uvicorn.
- **Redis** (set REDIS_URL): shared across workers/instances for production with Gunicorn or replicas.

Use check_sliding_window() inside handlers for precise control; @rate_limit for decorators.
"""
from __future__ import annotations

import logging
import threading
import time
import uuid
from collections import defaultdict
from datetime import datetime, timedelta
from functools import wraps
from typing import Callable, Optional

from fastapi import HTTPException, Request, status

_log = logging.getLogger(__name__)

# In-memory store: {identifier: [(timestamp, 1), ...]}
_rate_limit_store: dict = defaultdict(list)
_rate_limit_lock = threading.Lock()

_last_cleanup = datetime.utcnow()
_cleanup_interval = timedelta(minutes=5)

# None = not initialized; False = init failed (skip Redis until process restart)
_redis_client: object = None
_redis_lock = threading.Lock()


def _get_redis():
    """Lazy singleton sync Redis client (thread-safe pool)."""
    global _redis_client
    from app.core.config import settings

    if not getattr(settings, "REDIS_URL", None):
        return None
    if _redis_client is False:
        return None
    with _redis_lock:
        if _redis_client is False:
            return None
        if _redis_client is not None:
            return _redis_client
        try:
            import redis

            client = redis.from_url(
                settings.REDIS_URL,
                decode_responses=True,
                socket_connect_timeout=2.0,
                socket_timeout=2.0,
                health_check_interval=30,
            )
            client.ping()
            _redis_client = client
            return _redis_client
        except Exception as e:
            _log.warning("Redis unavailable for rate limiting, using in-memory store: %s", e)
            _redis_client = False
            return None


def _cleanup_old_entries():
    """Remove in-memory entries older than 1 hour."""
    global _last_cleanup, _rate_limit_store

    now = datetime.utcnow()
    if now - _last_cleanup < _cleanup_interval:
        return

    with _rate_limit_lock:
        _last_cleanup = now
        cutoff_time = now - timedelta(hours=1)

        for key in list(_rate_limit_store.keys()):
            _rate_limit_store[key] = [
                (ts, count) for ts, count in _rate_limit_store[key] if ts > cutoff_time
            ]
            if not _rate_limit_store[key]:
                del _rate_limit_store[key]


def _memory_try_acquire(identifier: str, max_requests: int, window_seconds: int) -> bool:
    now = datetime.utcnow()
    window_start = now - timedelta(seconds=window_seconds)

    with _rate_limit_lock:
        _rate_limit_store[identifier] = [
            (ts, c) for ts, c in _rate_limit_store[identifier] if ts > window_start
        ]
        recent = [ts for ts, _ in _rate_limit_store[identifier]]
        if len(recent) >= max_requests:
            return False
        _rate_limit_store[identifier].append((now, 1))
        return True


def _redis_try_acquire(identifier: str, max_requests: int, window_seconds: int) -> Optional[bool]:
    """
    Try Redis sliding window. Returns True/False if Redis worked, None if Redis unavailable.
    """
    r = _get_redis()
    if r is None:
        return None
    now = time.time()
    window_start = now - window_seconds
    key = f"rl:{identifier}"
    try:
        pipe = r.pipeline()
        pipe.zremrangebyscore(key, "-inf", window_start)
        pipe.zcard(key)
        _, n = pipe.execute()
        if n >= max_requests:
            return False
        pipe = r.pipeline()
        pipe.zadd(key, {str(uuid.uuid4()): now})
        pipe.expire(key, window_seconds + 60)
        pipe.execute()
        return True
    except Exception as e:
        _log.warning("Redis rate limit error, falling back to memory: %s", e)
        return None


def sliding_window_try_acquire(identifier: str, max_requests: int, window_seconds: int) -> bool:
    """
    Record one request for identifier. Returns True if under limit, False if exceeded.
    """
    if max_requests <= 0:
        return True

    rr = _redis_try_acquire(identifier, max_requests, window_seconds)
    if rr is not None:
        return rr

    _cleanup_old_entries()
    return _memory_try_acquire(identifier, max_requests, window_seconds)


def rate_limit(max_requests: int = 5, window_seconds: int = 300, identifier_func: Callable = None):
    """
    Rate limiting decorator for FastAPI endpoints.

    Identifiers:
    - If identifier_func is set: identifier_func(request, user)
    - Else if authenticated user: user_{id}_{org_id}
    - Else: ip_{client_ip} using X-Forwarded-For when TRUST_PROXY_HEADERS is set
    """

    def decorator(func: Callable):
        @wraps(func)
        def wrapper(*args, **kwargs):
            request = None
            user = None

            for arg in args:
                if isinstance(arg, Request):
                    request = arg
                    break

            for value in kwargs.values():
                if isinstance(value, Request):
                    request = value
                elif hasattr(value, "id") and hasattr(value, "org_id"):
                    user = value

            if identifier_func:
                identifier = identifier_func(request, user)
            elif user:
                identifier = f"user_{user.id}_{user.org_id}"
            elif request:
                from app.core.request_ip import get_client_ip

                identifier = f"ip_{get_client_ip(request)}"
            else:
                identifier = "unknown"

            if not sliding_window_try_acquire(identifier, max_requests, window_seconds):
                try:
                    from app.core.audit import log_security_event
                    from app.models.audit_log import AuditEventType
                    from app.core.request_ip import get_client_ip

                    db = kwargs.get("db")
                    if db and user and request:
                        log_security_event(
                            db=db,
                            event_type=AuditEventType.RATE_LIMIT_EXCEEDED,
                            org_id=user.org_id,
                            user_id=user.id,
                            resource_type="api_endpoint",
                            resource_id=func.__name__,
                            ip_address=get_client_ip(request),
                            user_agent=request.headers.get("user-agent") if request else None,
                            details={
                                "endpoint": func.__name__,
                                "max_requests": max_requests,
                                "window_seconds": window_seconds,
                            },
                        )
                except Exception:
                    pass

                raise HTTPException(
                    status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                    detail=(
                        f"Rate limit exceeded: {max_requests} requests per {window_seconds} seconds. "
                        "Please try again later."
                    ),
                )

            return func(*args, **kwargs)

        return wrapper

    return decorator


def check_sliding_window(
    identifier: str,
    max_requests: int,
    window_seconds: int,
    *,
    db: Optional[object] = None,
    audit_user: Optional[object] = None,
    audit_request: Optional[Request] = None,
    endpoint_name: str = "check_sliding_window",
) -> None:
    """
    Enforce sliding-window limit inside route handlers. Raises HTTPException 429 when exceeded.
    """
    if max_requests <= 0:
        return

    if sliding_window_try_acquire(identifier, max_requests, window_seconds):
        return

    try:
        if db is not None and audit_user is not None and audit_request is not None:
            from app.core.audit import log_security_event
            from app.models.audit_log import AuditEventType
            from app.core.request_ip import get_client_ip

            log_security_event(
                db=db,
                event_type=AuditEventType.RATE_LIMIT_EXCEEDED,
                org_id=getattr(audit_user, "org_id", None),
                user_id=getattr(audit_user, "id", None),
                resource_type="api_endpoint",
                resource_id=endpoint_name,
                ip_address=get_client_ip(audit_request) if audit_request else None,
                user_agent=audit_request.headers.get("user-agent") if audit_request else None,
                details={
                    "endpoint": endpoint_name,
                    "max_requests": max_requests,
                    "window_seconds": window_seconds,
                },
            )
    except Exception:
        pass

    raise HTTPException(
        status_code=status.HTTP_429_TOO_MANY_REQUESTS,
        detail=(
            f"Rate limit exceeded: {max_requests} requests per {window_seconds} seconds. "
            "Please try again later."
        ),
    )


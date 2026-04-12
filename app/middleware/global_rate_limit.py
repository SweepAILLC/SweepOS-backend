"""Optional global per-IP throttle (sliding 60s window). Disabled when GLOBAL_API_RATE_LIMIT_PER_MINUTE is 0."""
from __future__ import annotations

from starlette.concurrency import run_in_threadpool
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse


class GlobalRateLimitMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        from app.core.config import settings

        limit = getattr(settings, "GLOBAL_API_RATE_LIMIT_PER_MINUTE", 0) or 0
        if limit <= 0:
            return await call_next(request)

        if request.method == "OPTIONS":
            return await call_next(request)

        path = request.url.path
        if (
            path in ("/health", "/")
            or path.startswith("/docs")
            or path.startswith("/openapi")
            or path.startswith("/redoc")
            or path.startswith("/webhooks")
        ):
            return await call_next(request)

        from app.core.request_ip import get_client_ip
        from app.core.rate_limit import sliding_window_try_acquire

        ip = get_client_ip(request)
        # Avoid thread-pool hop when Redis is off (in-memory path is cheap).
        if getattr(settings, "REDIS_URL", None):
            ok = await run_in_threadpool(
                sliding_window_try_acquire,
                f"global:{ip}",
                limit,
                60,
            )
        else:
            ok = sliding_window_try_acquire(f"global:{ip}", limit, 60)
        if not ok:
            return JSONResponse(
                status_code=429,
                content={"detail": "Too many requests. Please slow down."},
                headers={"Retry-After": "60"},
            )
        return await call_next(request)

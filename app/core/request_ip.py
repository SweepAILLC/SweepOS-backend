"""
Client IP for rate limiting and security, with optional trust for reverse proxies.

When TRUST_PROXY_HEADERS is True (e.g. behind Render, AWS ALB, nginx), the first
hop in X-Forwarded-For is used. Only enable if the app is not directly exposed to
the internet without a proxy, or clients can spoof limits.
"""
from __future__ import annotations

from fastapi import Request

from app.core.config import settings


def get_client_ip(request: Request) -> str:
    if getattr(settings, "TRUST_PROXY_HEADERS", False):
        xff = request.headers.get("x-forwarded-for") or request.headers.get("X-Forwarded-For")
        if xff:
            return xff.split(",")[0].strip() or "unknown"
        xri = request.headers.get("x-real-ip") or request.headers.get("X-Real-IP")
        if xri:
            return xri.strip()
    if request.client and request.client.host:
        return request.client.host
    return "unknown"

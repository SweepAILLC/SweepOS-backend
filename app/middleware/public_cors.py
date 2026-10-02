"""Open CORS for the public, auth-less funnel tracking routes.

Tracking snippets run on client-owned domains (GHL funnel pages, custom landing
pages) that can't all be listed in ALLOWED_ORIGINS_EXTRA. These two routes take no
cookies or tokens and resolve the org from the funnel id, so any origin may call
them without credentials. Every other route keeps the allow-list CORSMiddleware.

Registered outermost (after CORSMiddleware) so it answers preflights before the
allow-list middleware can reject them, and rewrites the response headers on the way
out. Pure ASGI (no BaseHTTPMiddleware) so request bodies stream untouched.
"""
from __future__ import annotations

from typing import Iterable, List, Tuple

from starlette.types import ASGIApp, Message, Receive, Scope, Send

PUBLIC_CORS_PATHS = frozenset({"/funnels/events", "/funnels/leads"})

_ALLOW_METHODS = b"POST, OPTIONS"
_ALLOW_HEADERS = b"Content-Type"
_MAX_AGE = b"600"
# Headers the allow-list middleware may have set that must not leak through here.
_STRIP = frozenset(
    {
        b"access-control-allow-origin",
        b"access-control-allow-credentials",
        b"access-control-allow-methods",
        b"access-control-allow-headers",
        b"access-control-expose-headers",
        b"access-control-max-age",
    }
)


def is_public_cors_path(path: str) -> bool:
    return path.rstrip("/") in PUBLIC_CORS_PATHS


def public_cors_headers() -> List[Tuple[bytes, bytes]]:
    return [
        (b"access-control-allow-origin", b"*"),
        (b"access-control-allow-methods", _ALLOW_METHODS),
        (b"access-control-allow-headers", _ALLOW_HEADERS),
        (b"access-control-max-age", _MAX_AGE),
    ]


def _rewrite(headers: Iterable[Tuple[bytes, bytes]]) -> List[Tuple[bytes, bytes]]:
    kept = [(k, v) for k, v in headers if k.lower() not in _STRIP]
    return kept + public_cors_headers()


class PublicTrackingCorsMiddleware:
    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http" or not is_public_cors_path(scope.get("path", "")):
            await self.app(scope, receive, send)
            return

        if scope["method"] == "OPTIONS":
            await send({"type": "http.response.start", "status": 204, "headers": public_cors_headers()})
            await send({"type": "http.response.body", "body": b""})
            return

        async def send_with_cors(message: Message) -> None:
            if message["type"] == "http.response.start":
                message = {**message, "headers": _rewrite(message.get("headers", []))}
            await send(message)

        await self.app(scope, receive, send_with_cors)

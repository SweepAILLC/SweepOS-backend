"""Org-scoped Brevo transactional-email helper used by both the API and the worker.

Both the FastAPI handler `send_brevo_transactional_email` and the automation worker
must produce identical outbound payloads. This module is the single source of truth.
"""
from __future__ import annotations

import logging
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

import httpx
from sqlalchemy.orm import Session

from app.core.encryption import decrypt_token
from app.models.oauth_token import OAuthProvider, OAuthToken

LOG = logging.getLogger(__name__)


class BrevoNotConnectedError(Exception):
    pass


class BrevoSendError(Exception):
    """Wraps the upstream Brevo error so callers can decide retry/skip semantics."""

    def __init__(self, message: str, *, status_code: Optional[int] = None, retryable: bool = False):
        super().__init__(message)
        self.status_code = status_code
        self.retryable = retryable


def get_brevo_auth_headers(
    db: Session,
    org_id: uuid.UUID,
    user_id: Optional[uuid.UUID] = None,
) -> Dict[str, str]:
    """
    Resolve Brevo auth headers for the given org. Raises BrevoNotConnectedError if missing.

    Mirrors the request-path helper in app.api.integrations but is callable from background
    contexts (no FastAPI request) used by the automation worker.
    """
    brevo_token = (
        db.query(OAuthToken)
        .filter(
            OAuthToken.provider == OAuthProvider.BREVO,
            OAuthToken.org_id == org_id,
        )
        .first()
    )
    if not brevo_token:
        raise BrevoNotConnectedError("Brevo not connected for org")

    if brevo_token.expires_at and brevo_token.expires_at < datetime.utcnow():
        raise BrevoNotConnectedError("Brevo token expired; reconnect required")

    audit_ctx: Optional[Dict[str, Any]] = None
    if user_id is not None:
        audit_ctx = {
            "db": db,
            "org_id": org_id,
            "user_id": user_id,
            "resource_type": "brevo_token",
            "resource_id": str(brevo_token.id),
        }
    access_token = decrypt_token(brevo_token.access_token, audit_context=audit_ctx)

    is_api_key = brevo_token.scope == "api_key"
    headers = {"accept": "application/json", "content-type": "application/json"}
    if is_api_key:
        headers["api-key"] = access_token
    else:
        headers["Authorization"] = f"Bearer {access_token}"
    return headers


def _retryable_status(code: int) -> bool:
    if code == 429:
        return True
    if 500 <= code < 600:
        return True
    return False


def send_email(
    *,
    headers: Dict[str, str],
    sender: Dict[str, str],
    to: List[Dict[str, str]],
    subject: str,
    html_content: Optional[str] = None,
    text_content: Optional[str] = None,
    template_id: Optional[int] = None,
    params: Optional[Dict[str, Any]] = None,
    reply_to: Optional[Dict[str, str]] = None,
    tags: Optional[List[str]] = None,
    attachments: Optional[List[Dict[str, Any]]] = None,
    idempotency_key: Optional[str] = None,
    timeout_s: float = 30.0,
) -> Dict[str, Any]:
    """
    Send a single transactional email via Brevo. Returns the parsed JSON response on success.

    Pass ``idempotency_key`` to leverage Brevo's `Idempotency-Key` header, which makes
    network-level retries safe (worker may crash between commit and HTTP response).
    """
    if not sender or not isinstance(sender, dict):
        raise BrevoSendError("Missing sender")
    if not to:
        raise BrevoSendError("Missing recipients")

    payload: Dict[str, Any] = {
        "sender": sender,
        "subject": subject,
        "to": to[:100],
    }
    if template_id:
        payload["templateId"] = template_id
        if params:
            payload["params"] = params
    else:
        if text_content:
            payload["textContent"] = text_content
        if html_content:
            payload["htmlContent"] = html_content
    if reply_to:
        payload["replyTo"] = reply_to
    if tags:
        payload["tags"] = tags
    if attachments:
        payload["attachment"] = attachments

    req_headers = dict(headers)
    if idempotency_key:
        # Brevo accepts an `Idempotency-Key` header on POST /v3/smtp/email
        # (https://developers.brevo.com/docs/idempotent-requests)
        req_headers["Idempotency-Key"] = idempotency_key[:128]

    try:
        response = httpx.post(
            "https://api.brevo.com/v3/smtp/email",
            headers=req_headers,
            json=payload,
            timeout=timeout_s,
        )
    except httpx.RequestError as e:
        raise BrevoSendError(f"Network error contacting Brevo: {e}", retryable=True)

    if response.status_code in (200, 201):
        try:
            return response.json()
        except Exception:
            return {}

    try:
        body = response.json()
        message = body.get("message") or body.get("error") or response.text
    except Exception:
        message = response.text

    raise BrevoSendError(
        f"Brevo HTTP {response.status_code}: {message}",
        status_code=response.status_code,
        retryable=_retryable_status(response.status_code),
    )


def send_email_for_org(
    db: Session,
    org_id: uuid.UUID,
    *,
    user_id: Optional[uuid.UUID] = None,
    **kwargs: Any,
) -> Dict[str, Any]:
    """Convenience wrapper: resolve headers from the OAuth token, then call send_email."""
    headers = get_brevo_auth_headers(db, org_id, user_id=user_id)
    return send_email(headers=headers, **kwargs)

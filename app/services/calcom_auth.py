"""Resolve Cal.com API bearer token (org OAuth in prod; optional env key for local dev only)."""
from __future__ import annotations

import logging
import os
import uuid
from datetime import datetime
from typing import Optional

from fastapi import HTTPException
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.config import settings
from app.core.encryption import decrypt_token

_logger = logging.getLogger(__name__)


def calcom_api_key_configured() -> bool:
    key = getattr(settings, "CALCOM_API_KEY", None)
    return bool(key and str(key).strip())


def use_env_calcom_api_key_for_local_testing() -> bool:
    """True only in local/dev environments when CALCOM_API_KEY is set for manual testing."""
    env = (os.environ.get("ENVIRONMENT") or "").strip().lower()
    return env in ("development", "dev", "local") and calcom_api_key_configured()


def _load_org_calcom_token(db: Session, org_id: uuid.UUID, user_id: uuid.UUID) -> Optional[str]:
    """Decrypt org-scoped Cal.com API key from oauth_tokens, or None if not connected."""
    result = db.execute(
        text("""
            SELECT id, access_token, expires_at FROM oauth_tokens
            WHERE provider = CAST('calcom' AS oauthprovider)
            AND org_id = :org_id
            LIMIT 1
        """),
        {"org_id": org_id},
    ).first()

    if not result:
        return None

    token_id, access_token_encrypted, expires_at = result[0], result[1], result[2]
    if expires_at and expires_at < datetime.utcnow():
        raise HTTPException(
            status_code=401,
            detail="Cal.com token has expired. Please reconnect your account.",
        )

    try:
        return decrypt_token(
            access_token_encrypted,
            audit_context={
                "db": db,
                "org_id": org_id,
                "user_id": user_id,
                "resource_type": "calcom_token",
                "resource_id": str(token_id),
            },
        )
    except Exception as exc:
        raise HTTPException(
            status_code=401,
            detail="Cal.com token could not be decrypted. Please reconnect Cal.com.",
        ) from exc


def get_calcom_access_token(db: Session, org_id: uuid.UUID, user_id: uuid.UUID) -> str:
    """
    Bearer token for Cal.com v2 API calls.

    Org token from Integrations (oauth_tokens) always wins when present.
    ``CALCOM_API_KEY`` is a dev-only fallback when the org has no stored connection.
    """
    org_token = _load_org_calcom_token(db, org_id, user_id)
    if org_token:
        _logger.debug("Cal.com auth using org oauth_tokens for org_id=%s", org_id)
        return org_token

    if use_env_calcom_api_key_for_local_testing():
        _logger.debug("Cal.com auth using env CALCOM_API_KEY fallback for org_id=%s", org_id)
        return str(settings.CALCOM_API_KEY).strip()

    raise HTTPException(
        status_code=401,
        detail="Cal.com not connected. Connect Cal.com in Integrations.",
    )


def get_calcom_access_token_optional(db: Session, org_id: uuid.UUID, user_id: uuid.UUID) -> Optional[str]:
    """Like :func:`get_calcom_access_token` but returns None instead of raising."""
    try:
        return get_calcom_access_token(db, org_id, user_id)
    except HTTPException:
        return None

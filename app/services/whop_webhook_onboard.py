"""Ensure per-org Whop webhook endpoints stay registered and pointed at this backend."""
from __future__ import annotations

import logging
import os
import uuid
from typing import Any, Optional
from urllib.parse import urlparse

from app.db.session import SessionLocal

logger = logging.getLogger(__name__)


def whop_webhook_destination_for_org(org_id: uuid.UUID) -> str | None:
    from app.core.config import settings

    public_url = (getattr(settings, "BACKEND_PUBLIC_URL", None) or "").strip().rstrip("/")
    if not public_url:
        return None
    return f"{public_url}/webhooks/whop/org/{org_id}"


def _is_public_webhook_destination(url: str) -> bool:
    parsed = urlparse(url or "")
    host = (parsed.hostname or "").lower()
    if parsed.scheme != "https" or not host:
        return False
    return host not in {"localhost", "127.0.0.1", "0.0.0.0", "::1"} and not host.endswith(".local")


def _is_local_dev_environment() -> bool:
    env = (os.environ.get("ENVIRONMENT") or "").strip().lower()
    return env in {"development", "dev", "local"}


def _allow_whop_webhook_register() -> bool:
    return (os.environ.get("ALLOW_WHOP_WEBHOOK_REGISTER") or "").strip().lower() in {
        "1",
        "true",
        "yes",
    }


def _dev_must_skip_webhook_mutation(
    *,
    existing_wh_id: Optional[str],
    existing_has_secret: bool,
    destination: str,
) -> dict[str, Any] | None:
    if not _is_local_dev_environment() or _allow_whop_webhook_register():
        return None

    if existing_wh_id and existing_has_secret:
        logger.info(
            "whop_webhook_onboard: local dev preserving existing webhook endpoint=%s destination=%s",
            existing_wh_id,
            destination,
        )
        return {
            "success": True,
            "webhook_active": True,
            "skipped": True,
            "registration_skipped": True,
            "reason": "local_dev_preserve",
            "webhook_id": existing_wh_id,
            "destination_url": destination,
            "message": (
                "Existing Whop webhook preserved in local development. "
                "Set ALLOW_WHOP_WEBHOOK_REGISTER=true only if you intentionally want to retarget Whop."
            ),
        }

    logger.info(
        "whop_webhook_onboard: local dev skipping Whop webhook registration requested=%s",
        destination,
    )
    return {
        "success": True,
        "webhook_active": False,
        "skipped": True,
        "registration_skipped": True,
        "reason": "local_dev_skip",
        "destination_url": destination,
        "message": (
            "Whop connected. Webhook registration is skipped in local development "
            "so production destinations/secrets are not rotated. "
            "Set ALLOW_WHOP_WEBHOOK_REGISTER=true with a public tunnel URL to register from local."
        ),
    }


def ensure_whop_webhook_for_org(
    org_id: uuid.UUID,
    *,
    db=None,
    force: bool = False,
) -> dict[str, Any]:
    from app.core.encryption import decrypt_token, encrypt_token
    from app.models.oauth_token import OAuthProvider, OAuthToken
    from app.services import whop_client

    owns_db = db is None
    if owns_db:
        db = SessionLocal()

    try:
        token = (
            db.query(OAuthToken)
            .filter(OAuthToken.org_id == org_id, OAuthToken.provider == OAuthProvider.WHOP)
            .first()
        )
        if not token:
            return {"success": False, "webhook_active": False, "error": "Whop not connected"}

        destination = whop_webhook_destination_for_org(org_id)
        if not destination:
            return {
                "success": False,
                "webhook_active": False,
                "error": "BACKEND_PUBLIC_URL is not set; cannot register Whop webhook.",
            }

        skip = _dev_must_skip_webhook_mutation(
            existing_wh_id=token.webhook_endpoint_id,
            existing_has_secret=bool(token.webhook_secret),
            destination=destination,
        )
        if skip:
            return skip

        if not _is_public_webhook_destination(destination) and not _allow_whop_webhook_register():
            return {
                "success": False,
                "webhook_active": False,
                "error": "Whop webhook URL must be public HTTPS (not localhost).",
                "destination_url": destination,
            }

        api_key = decrypt_token(token.access_token)
        company_id = (token.account_id or "").strip()

        if not force and token.webhook_endpoint_id and token.webhook_secret:
            return {
                "success": True,
                "webhook_active": True,
                "webhook_id": token.webhook_endpoint_id,
                "destination_url": destination,
                "message": "Whop webhook already registered.",
            }

        existing_id = token.webhook_endpoint_id
        try:
            remote = whop_client.list_webhooks(api_key, company_id=company_id or None)
            for row in remote:
                if (row.get("url") or "").rstrip("/") == destination.rstrip("/"):
                    existing_id = row.get("id") or existing_id
                    break
        except Exception as e:
            logger.warning("whop_webhook_onboard: list webhooks failed org=%s: %s", org_id, e)

        if existing_id and (force or not token.webhook_secret):
            try:
                whop_client.delete_webhook(api_key, str(existing_id))
            except Exception as e:
                logger.warning("whop_webhook_onboard: delete old webhook %s failed: %s", existing_id, e)

        created = whop_client.create_webhook(
            api_key,
            url=destination,
            resource_id=company_id or None,
        )
        hook_id = created.get("id")
        secret = created.get("webhook_secret")
        if not hook_id or not secret:
            return {
                "success": False,
                "webhook_active": False,
                "error": "Whop created webhook but did not return id/secret.",
                "destination_url": destination,
            }

        token.webhook_endpoint_id = str(hook_id)
        token.webhook_secret = encrypt_token(str(secret))
        db.commit()
        logger.info("whop_webhook_onboard: registered webhook=%s org=%s", hook_id, org_id)
        return {
            "success": True,
            "webhook_active": True,
            "webhook_id": str(hook_id),
            "destination_url": destination,
            "message": "Whop webhook registered. New payments ingest automatically.",
        }
    except Exception as e:
        logger.exception("whop_webhook_onboard: failed org=%s", org_id)
        if owns_db:
            db.rollback()
        return {"success": False, "webhook_active": False, "error": str(e)}
    finally:
        if owns_db:
            db.close()


def delete_whop_webhook_for_org(org_id: uuid.UUID, *, db=None) -> None:
    from app.core.encryption import decrypt_token
    from app.models.oauth_token import OAuthProvider, OAuthToken
    from app.services import whop_client

    owns_db = db is None
    if owns_db:
        db = SessionLocal()
    try:
        token = (
            db.query(OAuthToken)
            .filter(OAuthToken.org_id == org_id, OAuthToken.provider == OAuthProvider.WHOP)
            .first()
        )
        if not token or not token.webhook_endpoint_id:
            return
        try:
            api_key = decrypt_token(token.access_token)
            whop_client.delete_webhook(api_key, token.webhook_endpoint_id)
        except Exception as e:
            logger.warning("whop_webhook_onboard: remote delete failed org=%s: %s", org_id, e)
    finally:
        if owns_db:
            db.close()

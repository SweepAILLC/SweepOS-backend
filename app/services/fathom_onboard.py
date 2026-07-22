"""Fathom webhook registration when the API key is saved or re-saved."""

from __future__ import annotations

import logging
import os
import uuid
from urllib.parse import urlparse
from typing import Any, Optional

from app.core.config import settings
from app.db.session import SessionLocal

logger = logging.getLogger(__name__)


def fathom_webhook_destination_for_org(org_id: uuid.UUID) -> str | None:
    public_url = (getattr(settings, "BACKEND_PUBLIC_URL", None) or "").strip().rstrip("/")
    if not public_url:
        return None
    return f"{public_url}/webhooks/fathom/{org_id}"


def _is_public_webhook_destination(url: str) -> bool:
    parsed = urlparse(url or "")
    host = (parsed.hostname or "").lower()
    if parsed.scheme != "https" or not host:
        return False
    return host not in {"localhost", "127.0.0.1", "0.0.0.0", "::1"} and not host.endswith(".local")


def _is_local_dev_environment() -> bool:
    env = (os.environ.get("ENVIRONMENT") or "").strip().lower()
    return env in {"development", "dev", "local"}


def _allow_fathom_webhook_register() -> bool:
    """Opt-in escape hatch for tunnels / intentional local Fathom destination changes."""
    return (os.environ.get("ALLOW_FATHOM_WEBHOOK_REGISTER") or "").strip().lower() in {
        "1",
        "true",
        "yes",
    }


def _dev_must_skip_webhook_mutation(
    *,
    existing_wh_id: Optional[str],
    existing_wh_url: str,
    destination: str,
) -> dict[str, Any] | None:
    """
    Never call Fathom's create/replace webhook API from local dev by default.

    Local .env often copies prod BACKEND_PUBLIC_URL. Registering would rotate the
    live webhook secret and write it only to the local DB — breaking production
    signature verification while leaving Fathom pointed at prod.
    """
    if not _is_local_dev_environment() or _allow_fathom_webhook_register():
        return None

    if existing_wh_id and existing_wh_url:
        logger.info(
            "fathom_onboard: local dev preserving existing Fathom webhook org destination=%s "
            "(set ALLOW_FATHOM_WEBHOOK_REGISTER=true to override)",
            existing_wh_url,
        )
        return {
            "success": True,
            "webhook_active": True,
            "skipped": True,
            "registration_skipped": True,
            "reason": "local_dev_preserve",
            "webhook_id": existing_wh_id,
            "destination_url": existing_wh_url,
            "requested_destination_url": destination,
            "message": (
                "Existing Fathom webhook preserved in local development. "
                "Inbound receive handlers still work; use Integrations sync or curl to test locally. "
                "Set ALLOW_FATHOM_WEBHOOK_REGISTER=true only if you intentionally want to retarget Fathom."
            ),
        }

    logger.info(
        "fathom_onboard: local dev skipping Fathom webhook registration requested=%s",
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
            "Fathom API key saved. Webhook registration is skipped in local development "
            "so production destinations/secrets are not rotated. "
            "Set ALLOW_FATHOM_WEBHOOK_REGISTER=true with a public tunnel URL to register from local."
        ),
    }


def register_fathom_webhook_for_org(org_id_str: str, *, force: bool = True) -> dict[str, Any]:
    """
    Register (or re-register) the Fathom webhook for an org. Runs synchronously so
    production saves return a definitive success/failure instead of fire-and-forget tasks.
    """
    from app.services.fathom_client import resolve_fathom_api_key

    db = SessionLocal()
    try:
        org_id = uuid.UUID(org_id_str)
        api_key = resolve_fathom_api_key(db, org_id)
        if not api_key:
            return {
                "success": False,
                "webhook_active": False,
                "error": "Fathom API key not configured for this organization.",
            }

        public_url = (getattr(settings, "BACKEND_PUBLIC_URL", None) or "").strip().rstrip("/")
        if not public_url:
            return {
                "success": False,
                "webhook_active": False,
                "error": "BACKEND_PUBLIC_URL is not set on the server; cannot create webhook destination URL.",
            }

        result = _register_webhook(db, org_id, api_key, force=force)
        db.commit()
        return result
    except Exception as exc:
        logger.exception("fathom_onboard: register failed org=%s", org_id_str)
        db.rollback()
        return {
            "success": False,
            "webhook_active": False,
            "error": str(exc) or "Webhook registration failed",
        }
    finally:
        db.close()


def bootstrap_fathom_for_org(org_id_str: str) -> None:
    """Background-safe entry point (no DB session in caller). Webhook only — no bulk sync."""
    register_fathom_webhook_for_org(org_id_str, force=False)


def setup_fathom_webhook_background(org_id_str: str) -> None:
    """Force webhook registration in the background (legacy async path)."""
    register_fathom_webhook_for_org(org_id_str, force=True)


def reconcile_fathom_webhooks_for_existing_orgs() -> dict[str, int]:
    """
    Ensure existing org-level Fathom API keys have a live webhook after deploys.

    This intentionally does not sync historical calls. It only creates/replaces
    the Fathom webhook destination so future Fathom-ready recordings flow in.
    """
    from app.models.organization import Organization
    from app.services.fathom_client import normalize_fathom_api_key

    destination_missing = fathom_webhook_destination_for_org(
        uuid.UUID("00000000-0000-0000-0000-000000000000")
    ) is None
    if destination_missing:
        logger.info("fathom_onboard: startup reconcile skipped; BACKEND_PUBLIC_URL unset")
        return {"checked": 0, "registered": 0, "failed": 0, "skipped": 0}

    db = SessionLocal()
    try:
        org_rows = (
            db.query(Organization.id, Organization.fathom_api_key)
            .filter(Organization.fathom_api_key.isnot(None))
            .all()
        )
    finally:
        db.close()

    checked = registered = failed = skipped = 0
    for org_id, fathom_api_key in org_rows:
        api_key = normalize_fathom_api_key(fathom_api_key)
        if not api_key:
            skipped += 1
            continue

        checked += 1
        result = register_fathom_webhook_for_org(str(org_id), force=False)
        if result.get("success") and result.get("skipped"):
            skipped += 1
        elif result.get("success") and result.get("webhook_active"):
            registered += 1
        else:
            failed += 1
            logger.warning(
                "fathom_onboard: startup reconcile failed org=%s error=%s",
                org_id,
                result.get("error"),
            )

    logger.info(
        "fathom_onboard: startup reconcile complete checked=%s registered=%s skipped=%s failed=%s",
        checked,
        registered,
        skipped,
        failed,
    )
    return {"checked": checked, "registered": registered, "failed": failed, "skipped": skipped}


def _register_webhook(db, org_id: uuid.UUID, api_key: str, *, force: bool) -> dict[str, Any]:
    from app.services.fathom_client import create_webhook
    from app.models.organization import Organization

    destination = fathom_webhook_destination_for_org(org_id) or ""

    org = db.query(Organization).filter(Organization.id == org_id).first()
    if not org:
        return {
            "success": False,
            "webhook_active": False,
            "error": "Organization not found",
            "destination_url": destination or None,
        }

    # Columns are managed by Alembic — do NOT run ALTER TABLE here.
    # Runtime DDL takes ACCESS EXCLUSIVE and can lock out all org reads (auth/me).

    existing_wh_url = getattr(org, "fathom_webhook_url", None) or ""
    existing_wh_id = getattr(org, "fathom_webhook_id", None)

    if not force and existing_wh_url == destination and existing_wh_id:
        logger.info(
            "fathom_onboard: webhook already registered for org=%s (%s), skipping",
            org_id,
            existing_wh_url,
        )
        return {
            "success": True,
            "webhook_active": True,
            "skipped": True,
            "webhook_id": existing_wh_id,
            "destination_url": existing_wh_url,
        }

    skipped = _dev_must_skip_webhook_mutation(
        existing_wh_id=existing_wh_id,
        existing_wh_url=existing_wh_url,
        destination=destination,
    )
    if skipped is not None:
        return skipped

    if not _is_public_webhook_destination(destination):
        if existing_wh_id and existing_wh_url:
            logger.info(
                "fathom_onboard: preserving existing webhook for local/non-public destination org=%s existing=%s requested=%s",
                org_id,
                existing_wh_url,
                destination,
            )
            return {
                "success": True,
                "webhook_active": True,
                "skipped": True,
                "registration_skipped": True,
                "reason": "non_public_destination",
                "webhook_id": existing_wh_id,
                "destination_url": existing_wh_url,
                "requested_destination_url": destination,
                "message": "Existing Fathom webhook preserved; local BACKEND_PUBLIC_URL is not publicly reachable.",
            }
        logger.info(
            "fathom_onboard: skipping webhook registration for local/non-public destination org=%s requested=%s",
            org_id,
            destination,
        )
        return {
            "success": True,
            "webhook_active": False,
            "skipped": True,
            "registration_skipped": True,
            "reason": "non_public_destination",
            "destination_url": destination,
            "message": "Fathom API key saved. Webhook registration is skipped in local dev unless BACKEND_PUBLIC_URL is a public HTTPS URL.",
        }

    try:
        wh = create_webhook(
            destination_url=destination,
            include_transcript=True,
            include_summary=True,
            include_action_items=True,
            triggered_for=[
                "my_recordings",
                "my_shared_with_team_recordings",
                "shared_external_recordings",
                "shared_team_recordings",
            ],
            api_key=api_key,
        )
    except Exception as exc:
        logger.exception("fathom_onboard: webhook create API call failed org=%s", org_id)
        detail = str(exc)
        resp = getattr(exc, "response", None)
        if resp is not None:
            try:
                body = resp.json()
                if isinstance(body, dict) and body.get("detail"):
                    detail = str(body["detail"])
            except Exception:
                pass
        return {
            "success": False,
            "webhook_active": bool(existing_wh_id and existing_wh_url),
            "error": detail or "Fathom webhook API call failed",
            "destination_url": destination,
        }

    webhook_id: Optional[str] = None
    webhook_secret: Optional[str] = None
    webhook_url: Optional[str] = None
    try:
        webhook_id = str(wh.get("id") or "")[:200] or None
        webhook_secret = str(wh.get("secret") or "")[:500] or None
        webhook_url = str(wh.get("url") or destination)[:2000] or destination
        org.fathom_webhook_id = webhook_id
        org.fathom_webhook_secret = webhook_secret
        org.fathom_webhook_url = webhook_url
    except Exception:
        pass

    logger.info(
        "fathom_onboard: webhook registered org=%s dest=%s id=%s",
        org_id,
        destination,
        webhook_id,
    )
    return {
        "success": True,
        "webhook_active": bool(webhook_id and webhook_url),
        "webhook_id": webhook_id,
        "destination_url": webhook_url or destination,
    }

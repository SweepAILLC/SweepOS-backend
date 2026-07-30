"""Ensure per-org Stripe webhook endpoints stay registered and pointed at this backend."""

from __future__ import annotations

import logging
import os
import uuid
from typing import Any, Optional
from urllib.parse import urlparse

from app.core.config import settings
from app.db.session import SessionLocal

logger = logging.getLogger(__name__)

STRIPE_WEBHOOK_EVENTS = [
    "invoice.payment_succeeded",
    "invoice.payment_failed",
    "invoice.paid",
    "charge.succeeded",
    "charge.failed",
    "charge.refunded",
    "payment_intent.succeeded",
    "payment_intent.payment_failed",
    "customer.subscription.created",
    "customer.subscription.updated",
    "customer.subscription.deleted",
    "customer.created",
    "customer.updated",
]


def stripe_webhook_destination_for_org(org_id: uuid.UUID) -> str | None:
    public_url = (getattr(settings, "BACKEND_PUBLIC_URL", None) or "").strip().rstrip("/")
    if not public_url:
        return None
    return f"{public_url}/webhooks/stripe/org/{org_id}"


def _is_public_webhook_destination(url: str) -> bool:
    parsed = urlparse(url or "")
    host = (parsed.hostname or "").lower()
    if parsed.scheme != "https" or not host:
        return False
    return host not in {"localhost", "127.0.0.1", "0.0.0.0", "::1"} and not host.endswith(".local")


def _is_local_dev_environment() -> bool:
    env = (os.environ.get("ENVIRONMENT") or "").strip().lower()
    return env in {"development", "dev", "local"}


def _allow_stripe_webhook_register() -> bool:
    return (os.environ.get("ALLOW_STRIPE_WEBHOOK_REGISTER") or "").strip().lower() in {
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
    """
    Avoid rotating live Stripe webhook secrets from local .env that points at prod URL.
    """
    if not _is_local_dev_environment() or _allow_stripe_webhook_register():
        return None

    if existing_wh_id and existing_has_secret:
        logger.info(
            "stripe_webhook_onboard: local dev preserving existing webhook endpoint=%s destination=%s",
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
                "Existing Stripe webhook preserved in local development. "
                "Set ALLOW_STRIPE_WEBHOOK_REGISTER=true only if you intentionally want to retarget Stripe."
            ),
        }

    logger.info(
        "stripe_webhook_onboard: local dev skipping Stripe webhook registration requested=%s",
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
            "Stripe connected. Webhook registration is skipped in local development "
            "so production destinations/secrets are not rotated. "
            "Set ALLOW_STRIPE_WEBHOOK_REGISTER=true with a public tunnel URL to register from local."
        ),
    }


def webhook_status_for_token(
    *,
    connected: bool,
    webhook_endpoint_id: Optional[str],
    webhook_secret: Optional[str],
    destination_url: Optional[str],
) -> dict[str, Any]:
    if not connected:
        return {
            "webhook_active": False,
            "webhook_status": "not_configured",
            "webhook_endpoint_id": None,
            "webhook_url": None,
        }
    active = bool(webhook_endpoint_id and webhook_secret)
    return {
        "webhook_active": active,
        "webhook_status": "active" if active else "not_registered",
        "webhook_endpoint_id": webhook_endpoint_id,
        "webhook_url": destination_url if active else destination_url,
    }


def ensure_stripe_webhook_for_org(
    org_id: uuid.UUID,
    *,
    db=None,
    force: bool = False,
) -> dict[str, Any]:
    """
    Create or repair the per-org Stripe WebhookEndpoint for API-key connections.

    Idempotent when force=False and endpoint already exists with a stored secret.
    When force=True or secret/endpoint is missing, creates (or recreates) the endpoint.
    """
    from app.core.encryption import decrypt_token, encrypt_token
    from app.models.oauth_token import OAuthToken, OAuthProvider

    owns_db = db is None
    if owns_db:
        db = SessionLocal()

    try:
        try:
            import stripe
        except ImportError:
            return {
                "success": False,
                "webhook_active": False,
                "error": "stripe library is not installed",
            }

        oauth_token = (
            db.query(OAuthToken)
            .filter(
                OAuthToken.provider == OAuthProvider.STRIPE,
                OAuthToken.org_id == org_id,
            )
            .first()
        )
        if not oauth_token or not oauth_token.access_token:
            return {
                "success": False,
                "webhook_active": False,
                "error": "Stripe not connected for this organization.",
            }

        destination = stripe_webhook_destination_for_org(org_id)
        if not destination:
            return {
                "success": False,
                "webhook_active": bool(oauth_token.webhook_endpoint_id and oauth_token.webhook_secret),
                "error": "BACKEND_PUBLIC_URL is not set on the server; cannot create webhook destination URL.",
            }

        existing_id = oauth_token.webhook_endpoint_id
        existing_has_secret = bool(oauth_token.webhook_secret)

        skip = _dev_must_skip_webhook_mutation(
            existing_wh_id=existing_id,
            existing_has_secret=existing_has_secret,
            destination=destination,
        )
        if skip is not None:
            return skip

        if not _is_public_webhook_destination(destination) and not _allow_stripe_webhook_register():
            # Allow http://localhost for intentional local stripe listen / tunnel setups only via escape hatch.
            if not _is_local_dev_environment():
                return {
                    "success": False,
                    "webhook_active": bool(existing_id and existing_has_secret),
                    "error": (
                        "BACKEND_PUBLIC_URL must be a public HTTPS URL for Stripe webhooks "
                        f"(got {destination})."
                    ),
                    "destination_url": destination,
                }

        if not force and existing_id and existing_has_secret:
            # Verify endpoint still exists and URL matches when possible.
            try:
                stripe.api_key = decrypt_token(oauth_token.access_token)
                we = stripe.WebhookEndpoint.retrieve(existing_id)
                current_url = getattr(we, "url", None) or ""
                disabled = bool(getattr(we, "status", None) == "disabled")
                if current_url.rstrip("/") == destination.rstrip("/") and not disabled:
                    return {
                        "success": True,
                        "webhook_active": True,
                        "skipped": True,
                        "webhook_id": existing_id,
                        "destination_url": destination,
                    }
                # URL drift or disabled — fall through to recreate
                force = True
                logger.info(
                    "stripe_webhook_onboard: endpoint %s needs repair url=%s expected=%s disabled=%s",
                    existing_id,
                    current_url,
                    destination,
                    disabled,
                )
            except Exception as retrieve_err:
                logger.warning(
                    "stripe_webhook_onboard: retrieve failed for %s (%s); recreating",
                    existing_id,
                    retrieve_err,
                )
                force = True

        if not force and existing_id and existing_has_secret:
            return {
                "success": True,
                "webhook_active": True,
                "skipped": True,
                "webhook_id": existing_id,
                "destination_url": destination,
            }

        stripe.api_key = decrypt_token(oauth_token.access_token)

        # Best-effort delete of stale endpoint before create (handles URL-already-exists).
        if existing_id:
            try:
                stripe.WebhookEndpoint.delete(existing_id)
            except Exception as del_err:
                logger.info(
                    "stripe_webhook_onboard: delete old endpoint %s failed (continuing): %s",
                    existing_id,
                    del_err,
                )

        # If Stripe still has an endpoint on this URL under another id, remove it.
        try:
            listed = stripe.WebhookEndpoint.list(limit=100)
            for item in getattr(listed, "data", []) or []:
                item_url = (getattr(item, "url", None) or "").rstrip("/")
                if item_url == destination.rstrip("/"):
                    try:
                        stripe.WebhookEndpoint.delete(item.id)
                    except Exception:
                        pass
        except Exception as list_err:
            logger.info("stripe_webhook_onboard: list endpoints failed (continuing): %s", list_err)

        we = stripe.WebhookEndpoint.create(
            url=destination,
            enabled_events=STRIPE_WEBHOOK_EVENTS,
            description=f"SweepOS org {org_id}",
            api_version="2024-06-20",
        )
        oauth_token.webhook_secret = encrypt_token(we.secret)
        oauth_token.webhook_endpoint_id = we.id
        db.commit()
        logger.info(
            "stripe_webhook_onboard: registered webhook %s for org %s → %s",
            we.id,
            org_id,
            destination,
        )
        return {
            "success": True,
            "webhook_active": True,
            "webhook_id": we.id,
            "destination_url": destination,
        }
    except Exception as exc:
        logger.exception("stripe_webhook_onboard: ensure failed org=%s", org_id)
        try:
            db.rollback()
        except Exception:
            pass
        return {
            "success": False,
            "webhook_active": False,
            "error": str(exc) or "Webhook registration failed",
            "destination_url": stripe_webhook_destination_for_org(org_id),
        }
    finally:
        if owns_db:
            db.close()


def register_stripe_webhook_for_org(org_id_str: str, *, force: bool = True) -> dict[str, Any]:
    try:
        org_id = uuid.UUID(org_id_str)
    except ValueError:
        return {
            "success": False,
            "webhook_active": False,
            "error": "Invalid organization id",
        }
    return ensure_stripe_webhook_for_org(org_id, force=force)


def reconcile_stripe_webhooks_for_existing_orgs() -> dict[str, int]:
    """Startup/deploy: ensure every connected Stripe org has a live per-org webhook."""
    from app.models.oauth_token import OAuthToken, OAuthProvider

    if stripe_webhook_destination_for_org(uuid.UUID("00000000-0000-0000-0000-000000000000")) is None:
        logger.info("stripe_webhook_onboard: startup reconcile skipped; BACKEND_PUBLIC_URL unset")
        return {"checked": 0, "registered": 0, "failed": 0, "skipped": 0}

    db = SessionLocal()
    try:
        org_ids = [
            row[0]
            for row in db.query(OAuthToken.org_id)
            .filter(
                OAuthToken.provider == OAuthProvider.STRIPE,
                OAuthToken.access_token.isnot(None),
            )
            .all()
        ]
    finally:
        db.close()

    checked = registered = failed = skipped = 0
    for org_id in org_ids:
        checked += 1
        result = ensure_stripe_webhook_for_org(org_id, force=False)
        if result.get("success") and result.get("skipped"):
            skipped += 1
        elif result.get("success") and result.get("webhook_active"):
            registered += 1
        else:
            failed += 1
            logger.warning(
                "stripe_webhook_onboard: startup reconcile failed org=%s error=%s",
                org_id,
                result.get("error"),
            )

    logger.info(
        "stripe_webhook_onboard: startup reconcile complete checked=%s registered=%s skipped=%s failed=%s",
        checked,
        registered,
        skipped,
        failed,
    )
    return {"checked": checked, "registered": registered, "failed": failed, "skipped": skipped}


def catchup_stripe_recent_for_all_orgs() -> dict[str, int]:
    """
    Safety-net incremental sync for all Stripe-connected orgs.
    Pulls recent charges/PIs/invoices (+ recent Treasury) so lag is bounded if webhooks miss.
    """
    from datetime import datetime, timedelta

    from app.models.oauth_token import OAuthToken, OAuthProvider
    from app.services.stripe_sync_v2 import sync_stripe_incremental, reconcile_stripe_data

    db = SessionLocal()
    try:
        tokens = (
            db.query(OAuthToken)
            .filter(
                OAuthToken.provider == OAuthProvider.STRIPE,
                OAuthToken.access_token.isnot(None),
            )
            .all()
        )
        org_ids = [t.org_id for t in tokens]
    finally:
        db.close()

    synced = failed = skipped = 0
    for org_id in org_ids:
        bg = SessionLocal()
        try:
            token = (
                bg.query(OAuthToken)
                .filter(
                    OAuthToken.provider == OAuthProvider.STRIPE,
                    OAuthToken.org_id == org_id,
                )
                .first()
            )
            if not token:
                skipped += 1
                continue

            # Avoid hammering Stripe right after a fresh webhook/sync.
            if token.last_webhook_processed_at and (
                datetime.utcnow() - token.last_webhook_processed_at
            ) < timedelta(minutes=8):
                skipped += 1
                continue

            token.last_sync_at = datetime.utcnow() - timedelta(hours=24)
            bg.commit()

            result = sync_stripe_incremental(bg, org_id=org_id, force_full=False, sync_recent=True)
            if result.get("error"):
                failed += 1
                logger.warning(
                    "stripe_webhook_onboard: catch-up sync failed org=%s error=%s",
                    org_id,
                    result.get("error"),
                )
                continue

            try:
                from app.services.stripe_treasury_sync import sync_treasury_transactions

                sync_treasury_transactions(
                    bg,
                    org_id=org_id,
                    created_since=datetime.utcnow() - timedelta(days=7),
                )
            except Exception as treasury_err:
                logger.info(
                    "stripe_webhook_onboard: treasury catch-up skipped org=%s (%s)",
                    org_id,
                    treasury_err,
                )

            try:
                reconcile_stripe_data(bg, org_id=org_id)
            except Exception:
                logger.exception("stripe_webhook_onboard: reconcile failed org=%s", org_id)

            token = (
                bg.query(OAuthToken)
                .filter(
                    OAuthToken.provider == OAuthProvider.STRIPE,
                    OAuthToken.org_id == org_id,
                )
                .first()
            )
            if token:
                token.last_webhook_processed_at = datetime.utcnow()
                bg.commit()
            synced += 1
        except Exception:
            failed += 1
            logger.exception("stripe_webhook_onboard: catch-up failed org=%s", org_id)
            try:
                bg.rollback()
            except Exception:
                pass
        finally:
            bg.close()

    logger.info(
        "stripe_webhook_onboard: catch-up complete synced=%s skipped=%s failed=%s",
        synced,
        skipped,
        failed,
    )
    return {"synced": synced, "skipped": skipped, "failed": failed, "checked": len(org_ids)}

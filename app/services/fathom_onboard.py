"""Fathom webhook registration on first connect.

Called as a background task when the Fathom API key is first saved on an org
with no existing call data. Registers a webhook so *future* meetings auto-sync.

Past meetings are never pulled here — use POST /integrations/fathom/sync (or the
UI "Sync Fathom now" button) so key save stays fast.
"""
from __future__ import annotations

import logging
import uuid

from app.core.config import settings
from app.db.session import SessionLocal

logger = logging.getLogger(__name__)


def bootstrap_fathom_for_org(org_id_str: str) -> None:
    """Background-safe entry point (no DB session in caller). Webhook only — no bulk sync."""
    from app.services.fathom_client import resolve_fathom_api_key

    db = SessionLocal()
    try:
        org_id = uuid.UUID(org_id_str)
        api_key = resolve_fathom_api_key(db, org_id)
        if not api_key:
            logger.info("fathom_onboard: no API key for org=%s, skipping", org_id)
            return

        _register_webhook(db, org_id, api_key, force=False)
        db.commit()
        logger.info("fathom_onboard: webhook bootstrap done org=%s", org_id)
    except Exception:
        logger.exception("fathom_onboard: bootstrap failed org=%s", org_id_str)
        db.rollback()
    finally:
        db.close()


def setup_fathom_webhook_background(org_id_str: str) -> None:
    """Force webhook registration in the background (used by manual setup endpoint)."""
    from app.services.fathom_client import resolve_fathom_api_key

    db = SessionLocal()
    try:
        org_id = uuid.UUID(org_id_str)
        api_key = resolve_fathom_api_key(db, org_id)
        if not api_key:
            logger.info("fathom_onboard: no API key for org=%s, skipping", org_id)
            return
        _register_webhook(db, org_id, api_key, force=True)
        db.commit()
        logger.info("fathom_onboard: webhook setup done org=%s", org_id)
    except Exception:
        logger.exception("fathom_onboard: setup failed org=%s", org_id_str)
        db.rollback()
    finally:
        db.close()


def _register_webhook(db, org_id: uuid.UUID, api_key: str, *, force: bool) -> None:
    from app.services.fathom_client import create_webhook
    from app.models.organization import Organization
    from sqlalchemy import text

    public_url = (getattr(settings, "BACKEND_PUBLIC_URL", None) or "").strip().rstrip("/")
    if not public_url:
        logger.warning(
            "fathom_onboard: BACKEND_PUBLIC_URL not set; skipping webhook registration for org=%s",
            org_id,
        )
        return

    org = db.query(Organization).filter(Organization.id == org_id).first()
    if not org:
        return

    # Idempotent column adds for fresh DBs
    try:
        db.execute(text("ALTER TABLE organizations ADD COLUMN IF NOT EXISTS fathom_webhook_id TEXT"))
        db.execute(text("ALTER TABLE organizations ADD COLUMN IF NOT EXISTS fathom_webhook_secret TEXT"))
        db.execute(text("ALTER TABLE organizations ADD COLUMN IF NOT EXISTS fathom_webhook_url TEXT"))
        db.commit()
    except Exception:
        db.rollback()

    existing_wh_url = getattr(org, "fathom_webhook_url", None) or ""
    destination = f"{public_url}/integrations/fathom/webhook/{org_id}"

    if not force and existing_wh_url == destination and getattr(org, "fathom_webhook_id", None):
        logger.info(
            "fathom_onboard: webhook already registered for org=%s (%s), skipping",
            org_id,
            existing_wh_url,
        )
        return

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
    except Exception:
        logger.exception("fathom_onboard: webhook create API call failed org=%s", org_id)
        return

    try:
        org.fathom_webhook_id = str(wh.get("id") or "")[:200] or None
        org.fathom_webhook_secret = str(wh.get("secret") or "")[:500] or None
        org.fathom_webhook_url = str(wh.get("url") or destination)[:2000] or destination
    except Exception:
        pass

    logger.info(
        "fathom_onboard: webhook registered org=%s dest=%s id=%s",
        org_id,
        destination,
        wh.get("id"),
    )

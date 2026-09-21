"""Register Cal.com / Calendly webhooks so bookings ingest at event time.

Mirrors stripe_webhook_onboard: per-org destination at BACKEND_PUBLIC_URL,
secret stored encrypted on oauth_tokens, local-dev skip to avoid rotating prod.
"""
from __future__ import annotations

import logging
import os
import re
import secrets
import uuid
from typing import Any, Optional
from urllib.parse import urlparse

import httpx
from sqlalchemy.orm import Session

from app.core.config import settings
from app.core.encryption import decrypt_token, encrypt_token
from app.db.session import SessionLocal
from app.models.oauth_token import OAuthProvider, OAuthToken

LOG = logging.getLogger("app.calendar_webhook_onboard")

CALCOM_TRIGGERS = ["BOOKING_CREATED", "BOOKING_CANCELLED", "BOOKING_RESCHEDULED"]
CALENDLY_EVENTS = ["invitee.created", "invitee.canceled"]


def calendar_webhook_destination(org_id: uuid.UUID, provider: str) -> Optional[str]:
    public_url = (getattr(settings, "BACKEND_PUBLIC_URL", None) or "").strip().rstrip("/")
    if not public_url:
        return None
    path = "calcom" if provider == "calcom" else "calendly"
    return f"{public_url}/webhooks/{path}/{org_id}"


def _is_public_destination(url: str) -> bool:
    parsed = urlparse(url or "")
    host = (parsed.hostname or "").lower()
    if parsed.scheme != "https" or not host:
        return False
    return host not in {"localhost", "127.0.0.1", "0.0.0.0", "::1"} and not host.endswith(".local")


def _is_local_dev() -> bool:
    env = (os.environ.get("ENVIRONMENT") or "").strip().lower()
    return env in {"development", "dev", "local"}


def _allow_calendar_webhook_register() -> bool:
    return (os.environ.get("ALLOW_CALENDAR_WEBHOOK_REGISTER") or "").strip().lower() in {
        "1",
        "true",
        "yes",
    }


def _dev_skip(
    *,
    existing_wh_id: Optional[str],
    existing_has_secret: bool,
    destination: str,
) -> Optional[dict[str, Any]]:
    if not _is_local_dev() or _allow_calendar_webhook_register():
        return None
    if existing_wh_id and existing_has_secret:
        return {
            "success": True,
            "webhook_active": True,
            "skipped": True,
            "reason": "local_dev_preserve",
            "webhook_id": existing_wh_id,
            "destination_url": destination,
            "message": "Existing calendar webhook preserved in local development.",
        }
    return {
        "success": True,
        "webhook_active": False,
        "skipped": True,
        "reason": "local_dev_skip",
        "destination_url": destination,
        "message": (
            "Calendar webhook registration skipped in local development. "
            "Set ALLOW_CALENDAR_WEBHOOK_REGISTER=true with a public tunnel URL to register."
        ),
    }


def _token_for(db: Session, org_id: uuid.UUID, provider: OAuthProvider) -> Optional[OAuthToken]:
    return (
        db.query(OAuthToken)
        .filter(OAuthToken.org_id == org_id, OAuthToken.provider == provider)
        .first()
    )


def ensure_calendar_webhook_for_org(
    org_id: uuid.UUID,
    provider: str,
    *,
    db: Optional[Session] = None,
    force: bool = False,
) -> dict[str, Any]:
    provider = (provider or "").strip().lower()
    if provider not in ("calcom", "calendly"):
        return {"success": False, "webhook_active": False, "error": "provider must be calcom or calendly"}

    destination = calendar_webhook_destination(org_id, provider)
    if not destination:
        return {
            "success": False,
            "webhook_active": False,
            "error": "BACKEND_PUBLIC_URL is not set; cannot register calendar webhook.",
        }

    owns_db = db is None
    if owns_db:
        db = SessionLocal()
    assert db is not None
    try:
        enum_provider = OAuthProvider.CALCOM if provider == "calcom" else OAuthProvider.CALENDLY
        token = _token_for(db, org_id, enum_provider)
        if not token:
            return {"success": False, "webhook_active": False, "error": f"{provider} not connected"}

        skip = _dev_skip(
            existing_wh_id=token.webhook_endpoint_id,
            existing_has_secret=bool(token.webhook_secret),
            destination=destination,
        )
        if skip:
            return skip

        if not force and token.webhook_endpoint_id and token.webhook_secret:
            if _is_public_destination(destination) or _allow_calendar_webhook_register():
                return {
                    "success": True,
                    "webhook_active": True,
                    "skipped": True,
                    "webhook_id": token.webhook_endpoint_id,
                    "destination_url": destination,
                    "message": "Calendar webhook already registered.",
                }

        access = decrypt_token(token.access_token)
        if provider == "calcom":
            result = _register_calcom(access, destination, token.webhook_endpoint_id if not force else None)
        else:
            result = _register_calendly(access, destination, token.webhook_endpoint_id if not force else None)

        if not result.get("success"):
            return result

        token.webhook_endpoint_id = str(result["webhook_id"])[:64]
        token.webhook_secret = encrypt_token(result["secret"])
        db.commit()
        LOG.info(
            "calendar_webhook_onboard: registered %s webhook=%s org=%s",
            provider,
            token.webhook_endpoint_id,
            org_id,
        )
        return {
            "success": True,
            "webhook_active": True,
            "webhook_id": token.webhook_endpoint_id,
            "destination_url": destination,
            "message": f"{provider} webhook active — new bookings ingest instantly.",
        }
    except Exception as exc:
        LOG.exception("calendar_webhook_onboard failed provider=%s org=%s", provider, org_id)
        try:
            db.rollback()
        except Exception:
            pass
        return {"success": False, "webhook_active": False, "error": str(exc)}
    finally:
        if owns_db:
            db.close()


def _register_calcom(access_token: str, destination: str, existing_id: Optional[str]) -> dict[str, Any]:
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "cal-api-version": "2024-08-13",
    }
    secret = secrets.token_urlsafe(32)
    body = {
        "subscriberUrl": destination,
        "triggers": CALCOM_TRIGGERS,
        "active": True,
        "secret": secret,
    }
    with httpx.Client(timeout=15.0) as client:
        if existing_id:
            resp = client.patch(
                f"https://api.cal.com/v2/webhooks/{existing_id}",
                headers=headers,
                json=body,
            )
            if resp.status_code in (200, 201):
                data = _unwrap_calcom(resp.json())
                return {
                    "success": True,
                    "webhook_id": str(data.get("id") or existing_id),
                    "secret": data.get("secret") or secret,
                }
        resp = client.post("https://api.cal.com/v2/webhooks", headers=headers, json=body)
        if resp.status_code not in (200, 201):
            return {
                "success": False,
                "webhook_active": False,
                "error": f"Cal.com webhook register HTTP {resp.status_code}: {resp.text[:300]}",
            }
        data = _unwrap_calcom(resp.json())
        return {
            "success": True,
            "webhook_id": str(data.get("id") or ""),
            "secret": data.get("secret") or secret,
        }


def _unwrap_calcom(payload: Any) -> dict:
    if isinstance(payload, dict) and isinstance(payload.get("data"), dict):
        return payload["data"]
    return payload if isinstance(payload, dict) else {}


_CALENDLY_API = "https://api.calendly.com"
_CALENDLY_SUBSCRIPTIONS = f"{_CALENDLY_API}/webhook_subscriptions"
_CALENDLY_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,64}$")


def _calendly_subscription_uri(existing_id: Optional[str]) -> Optional[str]:
    """Subscription URI on Calendly's own API host, or None if the stored id looks wrong.

    The id comes from our DB and is requested with the org's Bearer token, so anything
    other than a bare id or a URI under api.calendly.com is refused instead of fetched.
    """
    raw = (existing_id or "").strip()
    if raw.startswith(f"{_CALENDLY_SUBSCRIPTIONS}/"):
        raw = raw[len(_CALENDLY_SUBSCRIPTIONS) + 1 :].rstrip("/")
    return f"{_CALENDLY_SUBSCRIPTIONS}/{raw}" if _CALENDLY_ID_RE.match(raw) else None


def _register_calendly(access_token: str, destination: str, existing_id: Optional[str]) -> dict[str, Any]:
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }
    secret = secrets.token_urlsafe(32)
    old_uri = _calendly_subscription_uri(existing_id)
    with httpx.Client(timeout=15.0) as client:
        me = client.get(f"{_CALENDLY_API}/users/me", headers=headers)
        if me.status_code != 200:
            return {
                "success": False,
                "webhook_active": False,
                "error": f"Calendly /users/me HTTP {me.status_code}",
            }
        resource = (me.json() or {}).get("resource") or {}
        user_uri = resource.get("uri")
        org_uri = resource.get("current_organization")
        if not user_uri:
            return {"success": False, "webhook_active": False, "error": "Calendly user URI missing"}
        if not org_uri:
            # Calendly requires `organization` on every subscription, whatever the scope.
            return {"success": False, "webhook_active": False, "error": "Calendly organization URI missing"}

        def _delete_old() -> None:
            if not old_uri:
                return
            try:
                client.delete(old_uri, headers=headers)
            except Exception:
                LOG.warning("calendar_webhook_onboard: could not delete previous Calendly webhook")

        def _post(scope: str) -> httpx.Response:
            payload: dict[str, Any] = {
                "url": destination,
                "events": CALENDLY_EVENTS,
                "organization": org_uri,
                "scope": scope,
                "signing_key": secret,
            }
            if scope == "user":
                payload["user"] = user_uri
            return client.post(_CALENDLY_SUBSCRIPTIONS, headers=headers, json=payload)

        # Create first and only remove the previous webhook once a new one exists, so a
        # failed registration never leaves the org without a working webhook. The one
        # exception is Calendly's 409 for a duplicate url/scope, where our own previous
        # subscription has to go before the replacement can be created.
        deleted_old = False
        errors: list[str] = []
        resp: Optional[httpx.Response] = None
        for scope in ("organization", "user"):
            resp = _post(scope)
            if resp.status_code == 409 and old_uri and not deleted_old:
                deleted_old = True
                _delete_old()
                resp = _post(scope)
            if resp.status_code in (200, 201):
                break
            errors.append(f"{scope} scope HTTP {resp.status_code}: {resp.text[:200]}")
        else:
            return {
                "success": False,
                "webhook_active": False,
                "error": "Calendly webhook register failed: " + "; ".join(errors),
            }

        created = (resp.json() or {}).get("resource") or {}
        hook_uri = str(created.get("uri") or created.get("id") or "")
        hook_id = hook_uri.rstrip("/").rsplit("/", 1)[-1][:64]
        if old_uri and not deleted_old and old_uri.rsplit("/", 1)[-1] != hook_id:
            _delete_old()
        return {"success": True, "webhook_id": hook_id, "secret": secret}


def reconcile_calendar_webhooks_for_existing_orgs() -> dict[str, int]:
    if calendar_webhook_destination(uuid.UUID("00000000-0000-0000-0000-000000000000"), "calcom") is None:
        LOG.info("calendar_webhook_onboard: startup reconcile skipped; BACKEND_PUBLIC_URL unset")
        return {"checked": 0, "registered": 0, "failed": 0, "skipped": 0}

    db = SessionLocal()
    try:
        tokens = (
            db.query(OAuthToken.org_id, OAuthToken.provider)
            .filter(OAuthToken.provider.in_((OAuthProvider.CALCOM, OAuthProvider.CALENDLY)))
            .all()
        )
    finally:
        db.close()

    checked = registered = failed = skipped = 0
    for org_id, provider in tokens:
        checked += 1
        prov = provider.value if hasattr(provider, "value") else str(provider)
        result = ensure_calendar_webhook_for_org(org_id, prov, force=False)
        if result.get("success") and result.get("skipped"):
            skipped += 1
        elif result.get("success") and result.get("webhook_active"):
            registered += 1
        else:
            failed += 1
            LOG.warning(
                "calendar_webhook_onboard: reconcile failed org=%s provider=%s error=%s",
                org_id,
                prov,
                result.get("error"),
            )
    LOG.info(
        "calendar_webhook_onboard: reconcile checked=%s registered=%s skipped=%s failed=%s",
        checked,
        registered,
        skipped,
        failed,
    )
    return {"checked": checked, "registered": registered, "failed": failed, "skipped": skipped}


def resolve_calendar_webhook_secret(db: Session, org_id: uuid.UUID, provider: str) -> Optional[str]:
    """Per-org signing secret, then env fallback."""
    enum_provider = OAuthProvider.CALCOM if provider == "calcom" else OAuthProvider.CALENDLY
    token = _token_for(db, org_id, enum_provider)
    if token and token.webhook_secret:
        try:
            return decrypt_token(token.webhook_secret)
        except Exception:
            LOG.warning("calendar webhook secret decrypt failed org=%s provider=%s", org_id, provider)
    if provider == "calcom":
        return getattr(settings, "CALCOM_WEBHOOK_SECRET", None) or None
    return getattr(settings, "CALENDLY_WEBHOOK_SECRET", None) or None

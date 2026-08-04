"""
Thin Composio SDK wrapper for Instagram Performance Intel.

Keeps Composio SDK details out of API/UI layers. Each Sweep org stores its own
Composio API key + Instagram auth config ID (provider=composio). SweepOS org_id
is used as Composio user_id so connected accounts stay isolated per org.
"""
from __future__ import annotations

import hashlib
import logging
import uuid
from typing import Any, Dict, Optional, Tuple

from sqlalchemy.orm import Session

from app.core.encryption import decrypt_token, encrypt_token
from app.models.oauth_token import OAuthProvider, OAuthToken

logger = logging.getLogger(__name__)

COMPOSIO_CREDENTIALS_SCOPE = "composio_credentials"

# Small process-local cache: (org_id, key_fingerprint) -> Composio client
_client_cache: Dict[Tuple[str, str], Any] = {}


class ComposioConfigError(RuntimeError):
    """Org is missing Composio API key / Instagram auth config ID."""


class ComposioNotConnectedError(RuntimeError):
    """Org has no Instagram OAuthToken / connected account."""


class ComposioToolError(RuntimeError):
    """Composio tool execution failed or returned unsuccessful."""

    def __init__(self, message: str, *, slug: str | None = None, raw: Any = None):
        super().__init__(message)
        self.slug = slug
        self.raw = raw


def get_composio_token(db: Session, org_id: uuid.UUID) -> Optional[OAuthToken]:
    return (
        db.query(OAuthToken)
        .filter(
            OAuthToken.org_id == org_id,
            OAuthToken.provider == OAuthProvider.COMPOSIO,
        )
        .first()
    )


def get_composio_credentials(db: Session, org_id: uuid.UUID) -> Optional[Dict[str, str]]:
    """Return {api_key, auth_config_id} or None if not saved for this org."""
    row = get_composio_token(db, org_id)
    if row is None or not row.access_token or not (row.account_id or "").strip():
        return None
    try:
        api_key = decrypt_token(row.access_token)
    except Exception as e:
        logger.exception("composio decrypt failed org=%s", org_id)
        raise ComposioConfigError("Stored Composio API key could not be decrypted") from e
    api_key = (api_key or "").strip()
    auth_config_id = (row.account_id or "").strip()
    if not api_key or not auth_config_id:
        return None
    return {"api_key": api_key, "auth_config_id": auth_config_id}


def composio_configured(db: Session, org_id: uuid.UUID) -> bool:
    return get_composio_credentials(db, org_id) is not None


def upsert_composio_credentials(
    db: Session,
    org_id: uuid.UUID,
    *,
    api_key: str,
    auth_config_id: str,
) -> OAuthToken:
    key = (api_key or "").strip()
    cfg = (auth_config_id or "").strip()
    if not key:
        raise ComposioConfigError("Composio API key is required")
    if not cfg:
        raise ComposioConfigError("Composio Instagram auth config ID is required")

    enc = encrypt_token(key)
    row = get_composio_token(db, org_id)
    if row is None:
        row = OAuthToken(
            org_id=org_id,
            provider=OAuthProvider.COMPOSIO,
            account_id=cfg,
            access_token=enc,
            scope=COMPOSIO_CREDENTIALS_SCOPE,
        )
        db.add(row)
    else:
        row.access_token = enc
        row.account_id = cfg
        row.scope = COMPOSIO_CREDENTIALS_SCOPE
    db.commit()
    db.refresh(row)
    # Drop cached client for this org (key may have changed)
    _evict_org_client(org_id)
    return row


def delete_composio_credentials(db: Session, org_id: uuid.UUID) -> bool:
    row = get_composio_token(db, org_id)
    if row is None:
        return False
    db.delete(row)
    db.commit()
    _evict_org_client(org_id)
    return True


def _evict_org_client(org_id: uuid.UUID) -> None:
    prefix = str(org_id)
    for k in list(_client_cache.keys()):
        if k[0] == prefix:
            _client_cache.pop(k, None)


def _key_fingerprint(api_key: str) -> str:
    return hashlib.sha256(api_key.encode("utf-8")).hexdigest()[:16]


def composio_for_org(db: Session, org_id: uuid.UUID):
    """Composio client built from this org's stored API key."""
    creds = get_composio_credentials(db, org_id)
    if not creds:
        raise ComposioConfigError(
            "Add your Composio API key and Instagram auth config ID in Integrations first."
        )
    cache_key = (str(org_id), _key_fingerprint(creds["api_key"]))
    cached = _client_cache.get(cache_key)
    if cached is not None:
        return cached
    try:
        from composio import Composio
    except ImportError as e:
        raise ComposioConfigError(
            "composio package is not installed. Add composio to requirements and redeploy."
        ) from e
    client = Composio(api_key=creds["api_key"])
    _client_cache[cache_key] = client
    return client


def auth_config_id_for_org(db: Session, org_id: uuid.UUID) -> str:
    creds = get_composio_credentials(db, org_id)
    if not creds:
        raise ComposioConfigError(
            "Add your Composio API key and Instagram auth config ID in Integrations first."
        )
    return creds["auth_config_id"]


def start_instagram_link(
    db: Session,
    org_id: uuid.UUID,
    *,
    callback_url: str,
) -> Dict[str, Any]:
    """
    Start Composio hosted OAuth (connected_accounts.link) for Instagram.

    Returns { redirect_url, connection_request_id? }.
    """
    if not composio_configured(db, org_id):
        raise ComposioConfigError(
            "Add your Composio API key and Instagram auth config ID in Integrations first."
        )
    client = composio_for_org(db, org_id)
    user_id = str(org_id)
    auth_config_id = auth_config_id_for_org(db, org_id)
    # Prefer link() (required for Composio-managed OAuth after 2026-07-03).
    # allow_multiple=True: orgs may reconnect / retry after aborted OAuth; Composio
    # otherwise rejects when an ACTIVE account already exists for this user+auth config.
    try:
        link_fn = getattr(client.connected_accounts, "link", None)
        if callable(link_fn):
            try:
                req = link_fn(
                    user_id=user_id,
                    auth_config_id=auth_config_id,
                    callback_url=callback_url,
                    allow_multiple=True,
                    alias="instagram",
                )
            except TypeError:
                try:
                    req = link_fn(
                        user_id,
                        auth_config_id,
                        callback_url=callback_url,
                        allow_multiple=True,
                    )
                except TypeError:
                    # Older SDK signature variants
                    req = link_fn(user_id, auth_config_id, callback_url=callback_url)
        else:
            # Fallback for older SDKs / custom auth configs
            try:
                req = client.connected_accounts.initiate(
                    user_id=user_id,
                    auth_config_id=auth_config_id,
                    callback_url=callback_url,
                    allow_multiple=True,
                )
            except TypeError:
                req = client.connected_accounts.initiate(
                    user_id=user_id,
                    auth_config_id=auth_config_id,
                    callback_url=callback_url,
                )
    except ComposioToolError:
        raise
    except Exception as e:
        msg = str(e)
        lower = msg.lower()
        if "insufficient" in lower or "permission" in lower or "403" in lower:
            raise ComposioToolError(
                "Composio API key lacks permission to create connections. "
                "In Composio → Settings → API Keys, create a key with "
                "connected_accounts write access (not read-only), then re-save it in Sweep.",
                raw=e,
            ) from e
        raise ComposioToolError(
            f"Failed to start Instagram OAuth via Composio: {msg}",
            raw=e,
        ) from e

    redirect_url = (
        getattr(req, "redirect_url", None)
        or getattr(req, "redirectUrl", None)
        or (req.get("redirect_url") if isinstance(req, dict) else None)
        or (req.get("redirectUrl") if isinstance(req, dict) else None)
    )
    if not redirect_url:
        raise ComposioToolError(
            "Composio did not return a redirect_url for Instagram connect",
            raw=req,
        )
    req_id = (
        getattr(req, "id", None)
        or (req.get("id") if isinstance(req, dict) else None)
    )
    return {"redirect_url": str(redirect_url), "connection_request_id": req_id}


def get_instagram_token(db: Session, org_id: uuid.UUID) -> Optional[OAuthToken]:
    return (
        db.query(OAuthToken)
        .filter(
            OAuthToken.org_id == org_id,
            OAuthToken.provider == OAuthProvider.INSTAGRAM,
        )
        .first()
    )


def upsert_instagram_connection(
    db: Session,
    org_id: uuid.UUID,
    *,
    connected_account_id: str,
    ig_user_id: Optional[str] = None,
    scope: Optional[str] = None,
) -> OAuthToken:
    """Persist Composio connected_account_id (encrypted) + IG user id."""
    row = get_instagram_token(db, org_id)
    enc = encrypt_token(connected_account_id)
    if row is None:
        row = OAuthToken(
            org_id=org_id,
            provider=OAuthProvider.INSTAGRAM,
            account_id=ig_user_id,
            access_token=enc,
            scope=scope or "composio_instagram",
        )
        db.add(row)
    else:
        row.access_token = enc
        if ig_user_id:
            row.account_id = ig_user_id
        if scope:
            row.scope = scope
    db.commit()
    db.refresh(row)
    return row


def connected_account_id_for_org(db: Session, org_id: uuid.UUID) -> str:
    row = get_instagram_token(db, org_id)
    if row is None or not row.access_token:
        raise ComposioNotConnectedError("Instagram is not connected for this org")
    return decrypt_token(row.access_token)


def list_connected_account_ids(db: Session, org_id: uuid.UUID) -> list[str]:
    """List Composio connected account ids for this org (Instagram toolkit)."""
    client = composio_for_org(db, org_id)
    user_id = str(org_id)
    try:
        accounts = client.connected_accounts.list(user_ids=[user_id])
    except TypeError:
        try:
            accounts = client.connected_accounts.list(user_id=user_id)
        except Exception:
            accounts = client.connected_accounts.get(user_id=user_id)

    items = []
    if hasattr(accounts, "items"):
        items = list(accounts.items or [])
    elif isinstance(accounts, dict):
        items = accounts.get("items") or accounts.get("data") or []
    elif isinstance(accounts, list):
        items = accounts

    out: list[str] = []
    for a in items:
        status = str(
            getattr(a, "status", None)
            or (a.get("status") if isinstance(a, dict) else None)
            or ""
        ).upper()
        if status and status not in ("ACTIVE", "CONNECTED", "INITIATED"):
            continue
        aid = getattr(a, "id", None) or (a.get("id") if isinstance(a, dict) else None)
        # Prefer Instagram toolkit accounts when toolkit field is present
        toolkit = (
            getattr(a, "toolkit", None)
            or getattr(a, "appName", None)
            or getattr(a, "app_name", None)
            or (a.get("toolkit") if isinstance(a, dict) else None)
            or (a.get("appName") if isinstance(a, dict) else None)
        )
        if toolkit:
            tname = toolkit if isinstance(toolkit, str) else (
                getattr(toolkit, "slug", None)
                or getattr(toolkit, "name", None)
                or (toolkit.get("slug") if isinstance(toolkit, dict) else None)
                or ""
            )
            if tname and "instagram" not in str(tname).lower():
                continue
        if aid:
            out.append(str(aid))
    return out


def _unwrap_tool_result(result: Any, *, slug: str) -> Any:
    if result is None:
        raise ComposioToolError("Empty tool response", slug=slug)

    # Object-style response
    if hasattr(result, "successful") or hasattr(result, "data"):
        successful = getattr(result, "successful", True)
        error = getattr(result, "error", None)
        data = getattr(result, "data", result)
        if successful is False:
            raise ComposioToolError(
                str(error or f"{slug} failed"),
                slug=slug,
                raw=result,
            )
        return data

    if isinstance(result, dict):
        if result.get("successful") is False:
            raise ComposioToolError(
                str(result.get("error") or f"{slug} failed"),
                slug=slug,
                raw=result,
            )
        if "data" in result:
            return result["data"]
        return result

    return result


def execute(
    db: Session,
    org_id: uuid.UUID,
    slug: str,
    arguments: Optional[Dict[str, Any]] = None,
) -> Any:
    """Execute a Composio tool for the org's Instagram connected account."""
    if not composio_configured(db, org_id):
        raise ComposioConfigError(
            "Add your Composio API key and Instagram auth config ID in Integrations first."
        )
    ca_id = connected_account_id_for_org(db, org_id)
    client = composio_for_org(db, org_id)
    args = arguments or {}
    try:
        result = client.tools.execute(
            slug,
            arguments=args,
            user_id=str(org_id),
            connected_account_id=ca_id,
            dangerously_skip_version_check=True,
        )
    except TypeError:
        # Older SDK signature variants
        try:
            result = client.tools.execute(
                slug=slug,
                arguments=args,
                user_id=str(org_id),
                connected_account_id=ca_id,
            )
        except TypeError:
            result = client.tools.execute(
                slug,
                args,
                connected_account_id=ca_id,
                user_id=str(org_id),
            )
    except ComposioNotConnectedError:
        raise
    except ComposioConfigError:
        raise
    except Exception as e:
        logger.exception("composio tools.execute failed slug=%s org=%s", slug, org_id)
        raise ComposioToolError(str(e), slug=slug) from e

    return _unwrap_tool_result(result, slug=slug)

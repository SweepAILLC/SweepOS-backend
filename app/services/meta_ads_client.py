"""
Direct Meta Marketing API client (Graph API) for Meta Ads Performance Intel.

No Composio — Sweep calls Meta's Graph API directly using Sweep's own Meta
Business app (settings.META_ADS_APP_ID/SECRET) and a per-org access token
(an OAuth user token from the dialog flow below, or a Business Manager
System User token entered directly — see upsert_meta_ads_token), stored
encrypted under OAuthProvider.META_ADS in oauth_tokens. See
docs/features/META_ADS_PERFORMANCE_PRD.md "Core constraint".

Covers two use cases (lead capture was removed):
  - measure ad performance: list_campaigns/list_ad_sets/list_ads + get_insights
  - create/manage ads: create_campaign/create_ad_set/create_ad/update_entity_status
    (all created PAUSED by default, matching Meta's own Marketing API CLI convention)
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import logging
import time
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode

import httpx
from sqlalchemy.orm import Session

from app.core.config import settings
from app.core.encryption import decrypt_token, encrypt_token
from app.models.oauth_token import OAuthProvider, OAuthToken

logger = logging.getLogger(__name__)

GRAPH_API_VERSION_DEFAULT = "v26.0"
DEFAULT_SCOPES = "ads_management,ads_read,business_management"

# Meta Graph/Marketing API rate-limit error codes — 4 (app-level), 17
# (user-level), 32 (page-level), 613 (ads-API custom rate limit). Retried
# with backoff instead of failing the sync outright, since our app sits on
# the Standard (lowest) Marketing API access tier.
_RATE_LIMIT_CODES = {4, 17, 32, 613}
_RATE_LIMIT_BACKOFF_SECONDS = (5, 15, 30)


class MetaAdsConfigError(RuntimeError):
    """META_ADS_APP_ID/SECRET not configured, or org has no ad account selected."""


class MetaAdsNotConnectedError(RuntimeError):
    """Org has no connected Meta Ads token."""


class MetaAdsApiError(RuntimeError):
    def __init__(self, message: str, *, status_code: Optional[int] = None, raw: Any = None):
        super().__init__(message)
        self.status_code = status_code
        self.raw = raw


class MetaAdsAuthError(MetaAdsApiError):
    """Access token invalid/expired (OAuthException code 190) — org must reconnect."""


def _api_version() -> str:
    return getattr(settings, "META_ADS_API_VERSION", None) or GRAPH_API_VERSION_DEFAULT


def _base_url() -> str:
    return f"https://graph.facebook.com/{_api_version()}"


def _app_id() -> str:
    # Falls back to META_APP_ID (the Instagram app) if META_ADS_APP_ID is
    # unset — same shared-app-for-now convention as INSTAGRAM_APP_ID/SECRET
    # falling back to META_APP_ID/SECRET (see .env.example). Split them with
    # their own META_ADS_APP_ID/SECRET once Ads gets a dedicated Meta app.
    v = (
        getattr(settings, "META_ADS_APP_ID", "")
        or getattr(settings, "META_APP_ID", "")
        or ""
    ).strip()
    if not v:
        raise MetaAdsConfigError("META_ADS_APP_ID (or META_APP_ID) is not configured")
    return v


def _app_secret() -> str:
    v = (
        getattr(settings, "META_ADS_APP_SECRET", "")
        or getattr(settings, "META_APP_SECRET", "")
        or ""
    ).strip()
    if not v:
        raise MetaAdsConfigError("META_ADS_APP_SECRET (or META_APP_SECRET) is not configured")
    return v


def meta_ads_app_configured() -> bool:
    try:
        _app_id()
        _app_secret()
    except MetaAdsConfigError:
        return False
    return True


def oauth_dialog_url(*, redirect_uri: str, state: str) -> str:
    # This app is "Facebook Login for Business" (not classic Facebook Login) —
    # that product requires a Login Configuration ID (config_id) in the dialog
    # URL, or Meta rejects the request before even checking redirect_uri.
    # Find it under App Dashboard -> Facebook Login for Business -> Configurations.
    config_id = (getattr(settings, "META_ADS_LOGIN_CONFIG_ID", "") or "").strip()
    params = {
        "client_id": _app_id(),
        "redirect_uri": redirect_uri,
        "state": state,
        "response_type": "code",
    }
    if config_id:
        params["config_id"] = config_id
    else:
        scopes = getattr(settings, "META_ADS_OAUTH_SCOPES", None) or DEFAULT_SCOPES
        params["scope"] = scopes
    return f"https://www.facebook.com/{_api_version()}/dialog/oauth?{urlencode(params)}"


def _handle_response(resp: httpx.Response, path: str) -> Dict[str, Any]:
    try:
        body = resp.json()
    except Exception:
        body = {}
    if resp.status_code >= 400:
        err = (body or {}).get("error") or {}
        message = err.get("message") or f"Meta API error on {path} ({resp.status_code})"
        code = err.get("code")
        if code == 190:
            raise MetaAdsAuthError(message, status_code=resp.status_code, raw=body)
        raise MetaAdsApiError(message, status_code=resp.status_code, raw=body)
    return body if isinstance(body, dict) else {}


def _rate_limit_code(err: MetaAdsApiError) -> Optional[int]:
    raw = err.raw if isinstance(err.raw, dict) else {}
    return (raw.get("error") or {}).get("code")


def _get(path: str, *, params: Optional[Dict[str, Any]] = None, token: Optional[str] = None) -> Dict[str, Any]:
    p = dict(params or {})
    if token:
        p["access_token"] = token
    url = f"{_base_url()}/{path.lstrip('/')}"
    for attempt, backoff in enumerate((*_RATE_LIMIT_BACKOFF_SECONDS, None)):
        try:
            resp = httpx.get(url, params=p, timeout=30.0)
        except httpx.HTTPError as e:
            raise MetaAdsApiError(f"Network error calling Meta Graph API: {e}") from e
        try:
            return _handle_response(resp, path)
        except MetaAdsApiError as e:
            if backoff is None or _rate_limit_code(e) not in _RATE_LIMIT_CODES:
                raise
            logger.warning("meta_ads rate limited on GET %s, retry %d in %ds", path, attempt + 1, backoff)
            time.sleep(backoff)
    raise AssertionError("unreachable")


def _post(path: str, *, data: Optional[Dict[str, Any]] = None, token: Optional[str] = None) -> Dict[str, Any]:
    d = dict(data or {})
    if token:
        d["access_token"] = token
    url = f"{_base_url()}/{path.lstrip('/')}"
    for attempt, backoff in enumerate((*_RATE_LIMIT_BACKOFF_SECONDS, None)):
        try:
            resp = httpx.post(url, data=d, timeout=30.0)
        except httpx.HTTPError as e:
            raise MetaAdsApiError(f"Network error calling Meta Graph API: {e}") from e
        try:
            return _handle_response(resp, path)
        except MetaAdsApiError as e:
            if backoff is None or _rate_limit_code(e) not in _RATE_LIMIT_CODES:
                raise
            logger.warning("meta_ads rate limited on POST %s, retry %d in %ds", path, attempt + 1, backoff)
            time.sleep(backoff)
    raise AssertionError("unreachable")


# ---- OAuth: user token flow ----

def exchange_code_for_token(*, code: str, redirect_uri: str) -> Dict[str, Any]:
    """Short-lived user access token from an OAuth dialog `code`."""
    return _get(
        "oauth/access_token",
        params={
            "client_id": _app_id(),
            "client_secret": _app_secret(),
            "redirect_uri": redirect_uri,
            "code": code,
        },
    )


def exchange_for_long_lived_token(*, short_lived_token: str) -> Dict[str, Any]:
    """~60-day long-lived user token. Meta has no refresh-token for this flow —
    re-running the OAuth dialog (or switching to a non-expiring Business
    Manager System User token via upsert_meta_ads_token) is required before
    expiry."""
    return _get(
        "oauth/access_token",
        params={
            "grant_type": "fb_exchange_token",
            "client_id": _app_id(),
            "client_secret": _app_secret(),
            "fb_exchange_token": short_lived_token,
        },
    )


def list_ad_accounts(*, user_token: str) -> List[Dict[str, Any]]:
    body = _get(
        "me/adaccounts",
        params={"fields": "id,account_id,name,currency,account_status"},
        token=user_token,
    )
    return body.get("data") or []


def list_pages(*, user_token: str) -> List[Dict[str, Any]]:
    """Pages the authorizing user manages (pages_show_list) — used to pick which
    Page an ad's engagement data is read from (pages_read_engagement)."""
    body = _get(
        "me/accounts",
        params={"fields": "id,name"},
        token=user_token,
    )
    return body.get("data") or []


def get_page_engagement(*, page_id: str, token: str) -> Dict[str, Any]:
    """Basic Page engagement snapshot (pages_read_engagement)."""
    return _get(
        page_id,
        params={"fields": "id,name,engagement,fan_count"},
        token=token,
    )


# ---- Reads: campaigns / ad sets / ads / insights ----

def _act(ad_account_id: str) -> str:
    return ad_account_id if ad_account_id.startswith("act_") else f"act_{ad_account_id}"


def list_campaigns(*, ad_account_id: str, token: str) -> List[Dict[str, Any]]:
    body = _get(
        f"{_act(ad_account_id)}/campaigns",
        params={"fields": "id,name,objective,status,effective_status,daily_budget", "limit": 200},
        token=token,
    )
    return body.get("data") or []


def list_ad_sets(*, campaign_id: str, token: str) -> List[Dict[str, Any]]:
    body = _get(
        f"{campaign_id}/adsets",
        params={
            "fields": "id,name,optimization_goal,status,effective_status,daily_budget,learning_stage_info",
            "limit": 200,
        },
        token=token,
    )
    return body.get("data") or []


def list_ads(*, ad_set_id: str, token: str) -> List[Dict[str, Any]]:
    body = _get(
        f"{ad_set_id}/ads",
        params={
            "fields": "id,name,status,effective_status,created_time,creative{id,thumbnail_url}",
            "limit": 200,
        },
        token=token,
    )
    return body.get("data") or []


_INSIGHT_FIELDS = (
    "spend,actions,cost_per_action_type,impressions,clicks,cpm,ctr,reach,frequency,"
    "inline_link_click_ctr,video_play_actions,video_thruplay_watched_actions,date_start,date_stop"
)


def get_insights(*, object_id: str, level: str, since: str, until: str, token: str) -> List[Dict[str, Any]]:
    """level: account | campaign | adset | ad. Paginates via `paging.next`."""
    fields = _INSIGHT_FIELDS
    if level == "ad":
        fields += ",ad_id,adset_id,campaign_id"
    elif level == "adset":
        fields += ",adset_id,campaign_id"
    else:
        fields += ",campaign_id"
    params = {
        "level": level,
        "time_increment": 1,
        "time_range": f'{{"since":"{since}","until":"{until}"}}',
        "fields": fields,
        "limit": 500,
    }
    path = f"{object_id}/insights"
    body = _get(path, params=params, token=token)
    out: List[Dict[str, Any]] = list(body.get("data") or [])
    next_url = (body.get("paging") or {}).get("next")
    hops = 0
    while next_url and hops < 10:
        page = None
        for attempt, backoff in enumerate((*_RATE_LIMIT_BACKOFF_SECONDS, None)):
            try:
                resp = httpx.get(next_url, timeout=30.0)
            except httpx.HTTPError:
                break
            try:
                page = _handle_response(resp, path)
                break
            except MetaAdsApiError as e:
                if backoff is None or _rate_limit_code(e) not in _RATE_LIMIT_CODES:
                    break
                logger.warning("meta_ads rate limited on insights page %d, retry %d in %ds", hops, attempt + 1, backoff)
                time.sleep(backoff)
        if page is None:
            break
        out.extend(page.get("data") or [])
        next_url = (page.get("paging") or {}).get("next")
        hops += 1
    return out


# ---- Writes: create/manage ads (all PAUSED by default) ----

def create_campaign(
    *, ad_account_id: str, token: str, name: str, objective: str, status: str = "PAUSED"
) -> Dict[str, Any]:
    # Meta now requires this explicitly when the campaign isn't using campaign
    # budget optimization (we set per-ad-set budgets in create_ad_set) — a
    # missing value 400s with error_subcode 4834011.
    return _post(
        f"{_act(ad_account_id)}/campaigns",
        data={
            "name": name,
            "objective": objective,
            "status": status,
            "special_ad_categories": "[]",
            "is_adset_budget_sharing_enabled": "false",
        },
        token=token,
    )


def create_ad_set(
    *,
    ad_account_id: str,
    token: str,
    campaign_id: str,
    name: str,
    optimization_goal: str,
    billing_event: str,
    daily_budget_cents: int,
    targeting: Dict[str, Any],
    status: str = "PAUSED",
) -> Dict[str, Any]:
    return _post(
        f"{_act(ad_account_id)}/adsets",
        data={
            "name": name,
            "campaign_id": campaign_id,
            "optimization_goal": optimization_goal,
            "billing_event": billing_event,
            "daily_budget": daily_budget_cents,
            "targeting": _json_str(targeting),
            "status": status,
        },
        token=token,
    )


def create_ad(
    *, ad_account_id: str, token: str, name: str, ad_set_id: str, creative_id: str, status: str = "PAUSED"
) -> Dict[str, Any]:
    return _post(
        f"{_act(ad_account_id)}/ads",
        data={
            "name": name,
            "adset_id": ad_set_id,
            "creative": _json_str({"creative_id": creative_id}),
            "status": status,
        },
        token=token,
    )


def update_entity_status(*, entity_id: str, token: str, status: str) -> Dict[str, Any]:
    """Pause/activate/archive a campaign, ad set, or ad. status one of
    PAUSED/ACTIVE/ARCHIVED/DELETED."""
    return _post(entity_id, data={"status": status}, token=token)


def _json_str(obj: Dict[str, Any]) -> str:
    import json

    return json.dumps(obj)


# ---- OAuth CSRF state (sign org_id into the `state` param) ----

def sign_state(org_id: uuid.UUID) -> str:
    payload = str(org_id).encode("utf-8")
    sig = hmac.new(settings.SECRET_KEY.encode("utf-8"), payload, hashlib.sha256).hexdigest()[:32]
    return base64.urlsafe_b64encode(payload).decode("utf-8") + "." + sig


def verify_state(state: str) -> uuid.UUID:
    try:
        payload_b64, sig = state.split(".", 1)
        payload = base64.urlsafe_b64decode(payload_b64.encode("utf-8"))
    except Exception as e:
        raise MetaAdsConfigError("Invalid OAuth state") from e
    expected = hmac.new(settings.SECRET_KEY.encode("utf-8"), payload, hashlib.sha256).hexdigest()[:32]
    if not hmac.compare_digest(expected, sig):
        raise MetaAdsConfigError("OAuth state signature mismatch")
    return uuid.UUID(payload.decode("utf-8"))


# ---- Persistence (OAuthToken row under OAuthProvider.META_ADS) ----

def get_meta_ads_token(db: Session, org_id: uuid.UUID) -> Optional[OAuthToken]:
    return (
        db.query(OAuthToken)
        .filter(OAuthToken.org_id == org_id, OAuthToken.provider == OAuthProvider.META_ADS)
        .first()
    )


def access_token_for_org(db: Session, org_id: uuid.UUID) -> str:
    row = get_meta_ads_token(db, org_id)
    if row is None or not row.access_token:
        raise MetaAdsNotConnectedError("Meta Ads is not connected for this org")
    return decrypt_token(row.access_token)


def upsert_meta_ads_token(
    db: Session,
    org_id: uuid.UUID,
    *,
    access_token: str,
    ad_account_id: Optional[str] = None,
    expires_at: Optional[datetime] = None,
    scope: Optional[str] = None,
) -> OAuthToken:
    row = get_meta_ads_token(db, org_id)
    enc = encrypt_token(access_token)
    if row is None:
        row = OAuthToken(
            org_id=org_id,
            provider=OAuthProvider.META_ADS,
            account_id=ad_account_id,
            access_token=enc,
            expires_at=expires_at,
            scope=scope or "oauth_user_token",
        )
        db.add(row)
    else:
        row.access_token = enc
        if ad_account_id:
            row.account_id = ad_account_id
        row.expires_at = expires_at
        if scope:
            row.scope = scope
    db.commit()
    db.refresh(row)
    return row

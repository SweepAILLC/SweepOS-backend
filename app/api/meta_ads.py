"""Meta Ads Performance Intel API — direct Marketing API, no Composio.

Covers: measure ad performance (status/sync/campaigns), create/manage ads
(campaigns POST + pause/activate), and funnel<->campaign mapping so a
funnel's weekly ad spend auto-populates from synced insights instead of
manual entry. Lead capture was removed — see
docs/features/META_ADS_PERFORMANCE_PRD.md.
"""
from __future__ import annotations

import logging
import uuid
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.responses import RedirectResponse
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.api.deps import check_tab_access, get_current_user, get_db, require_admin_or_owner
from app.core.config import settings
from app.core.encryption import decrypt_token
from app.core.rate_limit import check_sliding_window
from app.long_jobs import schedule_background_work
from app.models.user import User
from app.services import meta_ads_client as mac
from app.services.meta_ads_client import MetaAdsAuthError, MetaAdsConfigError
from app.services.meta_ads_sync_service import sync_meta_ads_for_org

logger = logging.getLogger(__name__)
router = APIRouter()


class ConnectResponse(BaseModel):
    redirect_url: str


class SystemUserTokenIn(BaseModel):
    access_token: str = Field(..., min_length=20)
    ad_account_id: str = Field(..., min_length=3, description="act_<id> or bare <id>")


class StatusResponse(BaseModel):
    connected: bool
    app_configured: bool
    meta_ad_account_id: Optional[str] = None
    token_expires_at: Optional[str] = None
    last_sync_at: Optional[str] = None
    message: Optional[str] = None


class SyncResponse(BaseModel):
    ok: bool
    queued: bool
    message: str
    cooldown_seconds: int
    last_sync_at: Optional[str] = None


class CampaignIn(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    objective: str = Field(..., description="Meta campaign objective, e.g. OUTCOME_LEADS")


class EntityStatusIn(BaseModel):
    status: str = Field(..., pattern="^(PAUSED|ACTIVE|ARCHIVED)$")


def _org_id(user: User) -> uuid.UUID:
    raw = getattr(user, "selected_org_id", None) or user.org_id
    return raw if isinstance(raw, uuid.UUID) else uuid.UUID(str(raw))


def _require_content_studio(db: Session, user: User) -> None:
    # Ads tab lives inside Marketing Intel — same gate as Instagram Performance.
    if not check_tab_access("content_studio", user, db):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Marketing Intel is not enabled for your organization.",
        )


def _sync_single_campaign_on_demand(db: Session, org_id: uuid.UUID, meta_campaign_id: str):
    """Fetch one campaign live from Meta and upsert its row, for the case
    where the UI's live campaign list shows a campaign the periodic sync
    hasn't written to MetaCampaign yet."""
    from app.services.meta_ads_sync_service import _get_or_create_ad_account, _upsert_campaign

    token = mac.get_meta_ads_token(db, org_id)
    if token is None or not token.account_id:
        return None
    try:
        access_token = decrypt_token(token.access_token)
        campaigns = mac.list_campaigns(ad_account_id=token.account_id, token=access_token)
    except Exception:
        logger.exception(
            "meta_ads funnel-link: on-demand list_campaigns failed org=%s campaign=%s", org_id, meta_campaign_id
        )
        return None
    raw = next((c for c in campaigns if str(c.get("id")) == meta_campaign_id), None)
    if raw is None:
        return None
    ad_account = _get_or_create_ad_account(
        db, org_id, meta_ad_account_id=token.account_id, account_name=None, currency=None
    )
    campaign = _upsert_campaign(db, org_id, ad_account.id, raw)
    db.flush()
    return campaign


def _callback_url() -> str:
    base = (settings.BACKEND_PUBLIC_URL or "http://localhost:8000").rstrip("/")
    return f"{base}/marketing-intel/ads/callback"


def _frontend_integrations_url(*, connected: bool = False, error: Optional[str] = None) -> str:
    front = (settings.FRONTEND_URL or "http://localhost:3003").rstrip("/")
    qs = "tab=integrations&meta_ads="
    qs += "connected" if connected else "error"
    if error:
        qs += f"&reason={error[:120]}"
    return f"{front}/?{qs}"


def _sync_org_outside_session(org_id: uuid.UUID, full: bool = False) -> None:
    from app.db.session import SessionLocal

    db = SessionLocal()
    try:
        sync_meta_ads_for_org(db, org_id, full=full)
    except Exception:
        logger.exception("background meta_ads sync failed org=%s", org_id)
        try:
            db.rollback()
        except Exception:
            pass
    finally:
        db.close()


# ---- OAuth connect (short-lived user token flow) ----

@router.post("/connect", response_model=ConnectResponse)
def connect_meta_ads(
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin_or_owner),
):
    _require_content_studio(db, current_user)
    org_id = _org_id(current_user)
    if not mac.meta_ads_app_configured():
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="META_ADS_APP_ID/META_ADS_APP_SECRET are not configured on this server.",
        )
    state = mac.sign_state(org_id)
    url = mac.oauth_dialog_url(redirect_uri=_callback_url(), state=state)
    return ConnectResponse(redirect_url=url)


@router.get("/callback")
def meta_ads_oauth_callback(
    db: Session = Depends(get_db),
    code: Optional[str] = Query(None),
    state: Optional[str] = Query(None),
    error: Optional[str] = Query(None),
):
    """Meta redirects here after the OAuth dialog. Exchanges `code` for a
    short-lived user token, upgrades it to a long-lived one (~60 days — no
    refresh token exists for this flow; see StatusResponse.token_expires_at
    and PUT /system-user-token for the non-expiring production alternative),
    picks the first ad account, and kicks off the first sync in the
    background. Letting the org pick among several ad accounts is a
    follow-up, not scoped to this pass.
    """
    if error or not code or not state:
        return RedirectResponse(url=_frontend_integrations_url(connected=False, error=error or "missing_code"), status_code=303)

    try:
        org_id = mac.verify_state(state)
    except MetaAdsConfigError:
        return RedirectResponse(url=_frontend_integrations_url(connected=False, error="bad_state"), status_code=303)

    try:
        short = mac.exchange_code_for_token(code=code, redirect_uri=_callback_url())
        short_token = short.get("access_token")
        if not short_token:
            raise MetaAdsConfigError("Meta did not return an access_token")

        long = mac.exchange_for_long_lived_token(short_lived_token=short_token)
        long_token = long.get("access_token") or short_token
        expires_in = long.get("expires_in")
        expires_at = (
            datetime.now(timezone.utc) + timedelta(seconds=int(expires_in))
            if expires_in
            else None
        )

        accounts = mac.list_ad_accounts(user_token=long_token)
        if not accounts:
            return RedirectResponse(url=_frontend_integrations_url(connected=False, error="no_ad_account"), status_code=303)
        ad_account_id = accounts[0]["id"]

        mac.upsert_meta_ads_token(
            db,
            org_id,
            access_token=long_token,
            ad_account_id=ad_account_id,
            expires_at=expires_at,
            scope="oauth_user_token",
        )
        schedule_background_work(_sync_org_outside_session, None, org_id, True)
    except Exception:
        logger.exception("meta_ads callback failed")
        return RedirectResponse(url=_frontend_integrations_url(connected=False, error="callback_failed"), status_code=303)

    return RedirectResponse(url=_frontend_integrations_url(connected=True), status_code=303)


# ---- System User token (production alternative — non-expiring) ----

@router.put("/system-user-token")
def put_system_user_token(
    body: SystemUserTokenIn,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin_or_owner),
):
    """Enter a Business Manager System User token directly — Meta's
    recommended production path (does not expire, unlike the ~60-day OAuth
    user token from /connect). Generate it in Business Settings > Users >
    System Users, with Admin access to the ad account and leads_retrieval
    assigned to the relevant Pages."""
    _require_content_studio(db, current_user)
    org_id = _org_id(current_user)
    mac.upsert_meta_ads_token(
        db,
        org_id,
        access_token=body.access_token.strip(),
        ad_account_id=body.ad_account_id.strip(),
        expires_at=None,
        scope="system_user_token",
    )
    return {"ok": True}


@router.get("/status", response_model=StatusResponse)
def meta_ads_status(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    _require_content_studio(db, current_user)
    org_id = _org_id(current_user)
    app_configured = mac.meta_ads_app_configured()
    token = mac.get_meta_ads_token(db, org_id)
    if token is None:
        return StatusResponse(
            connected=False,
            app_configured=app_configured,
            message=None if app_configured else "META_ADS_APP_ID/SECRET are not configured on this server.",
        )
    return StatusResponse(
        connected=True,
        app_configured=app_configured,
        meta_ad_account_id=token.account_id,
        token_expires_at=token.expires_at.isoformat() if token.expires_at else None,
        last_sync_at=token.last_sync_at.isoformat() if token.last_sync_at else None,
    )


@router.delete("/disconnect")
def disconnect_meta_ads(
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin_or_owner),
):
    _require_content_studio(db, current_user)
    org_id = _org_id(current_user)
    token = mac.get_meta_ads_token(db, org_id)
    if token is None:
        return {"ok": True, "disconnected": False}
    db.delete(token)
    db.commit()
    return {"ok": True, "disconnected": True}


# ---- Ad account selection (connect picks the first one — this lets the org
# correct that, or switch to a different Business Manager portfolio later,
# without reconnecting) ----

class AdAccountLinkIn(BaseModel):
    ad_account_id: str


@router.get("/ad-accounts")
def list_meta_ad_accounts(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Every ad account this org's connected Meta login can access, so a
    coach can see whether the auto-picked one (on /connect) was the right
    business/portfolio and switch if not."""
    _require_content_studio(db, current_user)
    org_id = _org_id(current_user)
    token = mac.get_meta_ads_token(db, org_id)
    if token is None:
        raise HTTPException(status_code=400, detail="Meta Ads is not connected for this org")
    access_token = decrypt_token(token.access_token)
    try:
        accounts = mac.list_ad_accounts(user_token=access_token)
    except MetaAdsAuthError as e:
        raise HTTPException(status_code=401, detail=f"Meta token invalid, reconnect: {e}") from e
    except Exception as e:
        logger.exception("meta_ads list_ad_accounts failed org=%s", org_id)
        raise HTTPException(status_code=502, detail=f"Failed to list ad accounts: {e}") from e
    return {
        "accounts": [
            {
                "id": a.get("id"),
                "name": a.get("name"),
                "current": a.get("id") == token.account_id,
            }
            for a in accounts
            if a.get("id")
        ]
    }


@router.put("/ad-account")
def set_meta_ad_account(
    body: AdAccountLinkIn,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin_or_owner),
):
    """Switch which ad account this org's Meta Ads connection points to.
    Previously-synced campaigns from the old account are left in place
    (never deleted) but build_ads_dashboard already scopes to the current
    account only, so they stop showing once this switches."""
    _require_content_studio(db, current_user)
    org_id = _org_id(current_user)
    token = mac.get_meta_ads_token(db, org_id)
    if token is None:
        raise HTTPException(status_code=400, detail="Meta Ads is not connected for this org")
    new_account_id = body.ad_account_id.strip()
    if not new_account_id:
        raise HTTPException(status_code=400, detail="ad_account_id is required")

    access_token = decrypt_token(token.access_token)
    mac.upsert_meta_ads_token(
        db,
        org_id,
        access_token=access_token,
        ad_account_id=new_account_id,
        expires_at=token.expires_at,
        scope=token.scope,
    )
    schedule_background_work(_sync_org_outside_session, None, org_id, True)
    return {"ok": True, "ad_account_id": new_account_id}


@router.post("/sync", response_model=SyncResponse)
def sync_meta_ads(
    full: bool = Query(False),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Enqueue Meta Ads sync in the background (never blocks the HTTP request).

    Rate-limited: per-org cooldown + sliding window (defaults: 15 min / 3 per
    hour). A sync always re-pulls the trailing META_ADS_INSIGHTS_LOOKBACK_DAYS
    of insights regardless of `full` (attribution settling).
    """
    _require_content_studio(db, current_user)
    org_id = _org_id(current_user)
    cooldown = int(getattr(settings, "META_ADS_MANUAL_SYNC_COOLDOWN_SEC", 900) or 900)
    cooldown = max(60, min(cooldown, 86400))
    max_per_hour = int(getattr(settings, "META_ADS_MANUAL_SYNC_MAX_PER_HOUR", 3) or 3)
    max_per_hour = max(1, min(max_per_hour, 12))

    token = mac.get_meta_ads_token(db, org_id)
    if token is None:
        raise HTTPException(status_code=400, detail="Meta Ads is not connected for this org")

    last_sync = token.last_sync_at
    if last_sync is not None and cooldown > 0:
        last_naive = last_sync.replace(tzinfo=None) if getattr(last_sync, "tzinfo", None) else last_sync
        elapsed = (datetime.utcnow() - last_naive).total_seconds()
        remaining = int(cooldown - elapsed)
        if remaining > 0:
            mins = max(1, (remaining + 59) // 60)
            raise HTTPException(
                status_code=429,
                detail={
                    "message": f"Sync is rate-limited. Try again in about {mins} minute{'s' if mins != 1 else ''}.",
                    "cooldown_seconds": remaining,
                    "last_sync_at": last_naive.isoformat(),
                },
            )

    check_sliding_window(
        f"meta_ads_sync_{org_id}",
        max_requests=max_per_hour,
        window_seconds=3600,
        db=db,
        audit_user=current_user,
        endpoint_name="meta_ads_sync",
    )

    budget = int(getattr(settings, "META_ADS_SYNC_BUDGET_SEC", 120) or 120)
    schedule_background_work(
        _sync_org_outside_session,
        None,
        org_id,
        full,
        prefer_rq=True,
        job_timeout=max(180, budget + 120),
    )
    return SyncResponse(
        ok=True,
        queued=True,
        message="Meta Ads sync queued. Metrics will refresh in the background shortly.",
        cooldown_seconds=cooldown,
        last_sync_at=token.last_sync_at.isoformat() if token.last_sync_at else None,
    )


# ---- Create/manage ads (PAUSED by default) ----

@router.post("/campaigns")
def create_campaign(
    body: CampaignIn,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin_or_owner),
):
    _require_content_studio(db, current_user)
    org_id = _org_id(current_user)
    token = mac.get_meta_ads_token(db, org_id)
    if token is None or not token.account_id:
        raise HTTPException(status_code=400, detail="Meta Ads is not connected for this org")
    access_token = decrypt_token(token.access_token)
    try:
        result = mac.create_campaign(
            ad_account_id=token.account_id, token=access_token, name=body.name, objective=body.objective
        )
    except MetaAdsAuthError as e:
        raise HTTPException(status_code=401, detail=f"Meta token invalid, reconnect: {e}") from e
    except Exception as e:
        logger.exception("meta_ads create_campaign failed org=%s", org_id)
        raise HTTPException(status_code=502, detail=f"Failed to create campaign: {e}") from e
    return {"ok": True, "id": result.get("id")}


@router.post("/entities/{entity_id}/status")
def set_entity_status(
    entity_id: str,
    body: EntityStatusIn,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin_or_owner),
):
    """Pause/activate/archive a campaign, ad set, or ad by its Meta id."""
    _require_content_studio(db, current_user)
    org_id = _org_id(current_user)
    token = mac.get_meta_ads_token(db, org_id)
    if token is None:
        raise HTTPException(status_code=400, detail="Meta Ads is not connected for this org")
    if not _org_owns_meta_entity(db, org_id, entity_id):
        raise HTTPException(
            status_code=404,
            detail="That campaign, ad set, or ad is not in this org's synced Meta account.",
        )
    access_token = decrypt_token(token.access_token)
    try:
        mac.update_entity_status(entity_id=entity_id, token=access_token, status=body.status)
    except MetaAdsAuthError as e:
        raise HTTPException(status_code=401, detail=f"Meta token invalid, reconnect: {e}") from e
    except Exception as e:
        logger.exception("meta_ads set_entity_status failed entity=%s", entity_id)
        raise HTTPException(status_code=502, detail=f"Failed to update status: {e}") from e
    return {"ok": True, "id": entity_id, "status": body.status}


@router.get("/pages")
def list_pages(
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin_or_owner),
):
    """Facebook Pages the connected user manages (pages_show_list) — lets the
    org pick which Page's engagement to show alongside its ad performance."""
    _require_content_studio(db, current_user)
    org_id = _org_id(current_user)
    token = mac.get_meta_ads_token(db, org_id)
    if token is None:
        raise HTTPException(status_code=400, detail="Meta Ads is not connected for this org")
    access_token = decrypt_token(token.access_token)
    try:
        pages = mac.list_pages(user_token=access_token)
    except MetaAdsAuthError as e:
        raise HTTPException(status_code=401, detail=f"Meta token invalid, reconnect: {e}") from e
    except Exception as e:
        logger.exception("meta_ads list_pages failed org=%s", org_id)
        raise HTTPException(status_code=502, detail=f"Failed to list Pages: {e}") from e
    return {"pages": pages}


@router.get("/pages/{page_id}/engagement")
def page_engagement(
    page_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin_or_owner),
):
    """Basic engagement snapshot for a Page the connected user manages
    (pages_read_engagement)."""
    _require_content_studio(db, current_user)
    org_id = _org_id(current_user)
    token = mac.get_meta_ads_token(db, org_id)
    if token is None:
        raise HTTPException(status_code=400, detail="Meta Ads is not connected for this org")
    access_token = decrypt_token(token.access_token)
    try:
        data = mac.get_page_engagement(page_id=page_id, token=access_token)
    except MetaAdsAuthError as e:
        raise HTTPException(status_code=401, detail=f"Meta token invalid, reconnect: {e}") from e
    except Exception as e:
        logger.exception("meta_ads page_engagement failed org=%s page=%s", org_id, page_id)
        raise HTTPException(status_code=502, detail=f"Failed to read Page engagement: {e}") from e
    return data


# ---- Funnel <-> Meta campaign mapping (auto-populates weekly ad spend) ----
#
# Many campaigns can feed one funnel (confirmed real usage), so the link lives
# on the many side: MetaCampaign.funnel_id (migration 110). Funnel.meta_campaign_id
# (migration 106) is deprecated — left in place, no longer written to, kept only
# so nothing regresses for orgs that had a single mapping before this changed.

class CampaignFunnelLinkIn(BaseModel):
    funnel_id: Optional[str] = None
    auto_suggested: bool = False


@router.get("/funnel-mappings")
def list_funnel_meta_mappings(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Every funnel in this org plus the campaign(s) currently linked to it
    (a funnel can have more than one), and every campaign on the connected ad
    account annotated with which funnel (if any) it's linked to — one GET
    serves both the Funnels-tab display and the Ads-tab per-campaign picker."""
    from app.models.funnel import Funnel
    from app.models.meta_campaign import MetaCampaign

    _require_content_studio(db, current_user)
    org_id = _org_id(current_user)
    funnels = db.query(Funnel).filter(Funnel.org_id == org_id).order_by(Funnel.name.asc()).all()
    linked_rows = db.query(MetaCampaign).filter(MetaCampaign.org_id == org_id, MetaCampaign.funnel_id.isnot(None)).all()

    by_funnel: Dict[str, List[Dict[str, Any]]] = {}
    linked_by_meta_campaign_id: Dict[str, str] = {}
    for row in linked_rows:
        by_funnel.setdefault(str(row.funnel_id), []).append(
            {"id": str(row.id), "meta_campaign_id": row.meta_campaign_id, "name": row.name}
        )
        linked_by_meta_campaign_id[row.meta_campaign_id] = str(row.funnel_id)

    campaigns: List[Dict[str, Any]] = []
    token = mac.get_meta_ads_token(db, org_id)
    if token is not None and token.account_id:
        try:
            access_token = decrypt_token(token.access_token)
            campaigns = mac.list_campaigns(ad_account_id=token.account_id, token=access_token)
        except Exception:
            logger.exception("meta_ads list_funnel_meta_mappings: list_campaigns failed org=%s", org_id)
            campaigns = []

    return {
        "funnels": [
            {
                "funnel_id": str(f.id),
                "funnel_name": f.name,
                "linked_campaigns": by_funnel.get(str(f.id), []),
            }
            for f in funnels
        ],
        "campaigns": [
            {
                "id": c.get("id"),
                "name": c.get("name"),
                "funnel_id": linked_by_meta_campaign_id.get(c.get("id")),
            }
            for c in campaigns
            if c.get("id")
        ],
    }


@router.put("/campaigns/{meta_campaign_id}/funnel-link")
def set_campaign_funnel_link(
    meta_campaign_id: str,
    body: CampaignFunnelLinkIn,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin_or_owner),
):
    """Link (or clear) which funnel this campaign's spend feeds into the
    scorecard. Many campaigns may link to the same funnel; clearing a link
    does not delete already-synced spend rows."""
    from app.models.funnel import Funnel
    from app.models.meta_campaign import MetaCampaign

    _require_content_studio(db, current_user)
    org_id = _org_id(current_user)
    campaign = (
        db.query(MetaCampaign)
        .filter(MetaCampaign.org_id == org_id, MetaCampaign.meta_campaign_id == meta_campaign_id)
        .first()
    )
    if campaign is None:
        # Not synced yet — the link checkbox list comes from a live Graph API
        # call, which can show a campaign before the periodic sync has
        # written its row. Upsert it on demand instead of 404ing.
        campaign = _sync_single_campaign_on_demand(db, org_id, meta_campaign_id)
    if campaign is None:
        raise HTTPException(status_code=404, detail="Campaign not found")

    funnel_uuid: Optional[uuid.UUID] = None
    if body.funnel_id:
        try:
            funnel_uuid = uuid.UUID(body.funnel_id)
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid funnel_id")
        funnel = db.query(Funnel).filter(Funnel.id == funnel_uuid, Funnel.org_id == org_id).first()
        if funnel is None:
            raise HTTPException(status_code=404, detail="Funnel not found")

    campaign.funnel_id = funnel_uuid
    campaign.funnel_linked_at = datetime.now(timezone.utc) if funnel_uuid else None
    campaign.funnel_linked_by_user_id = current_user.id if funnel_uuid else None
    campaign.funnel_link_auto_suggested = bool(body.auto_suggested) if funnel_uuid else False
    db.commit()

    weeks_synced = 0
    try:
        from app.services.meta_ads_sync_service import sync_funnel_ad_spend_from_insights

        weeks_synced = sync_funnel_ad_spend_from_insights(db, org_id)
        db.commit()
    except Exception:
        logger.exception("meta_ads campaign funnel-link backfill failed campaign=%s", meta_campaign_id)

    return {
        "ok": True,
        "meta_campaign_id": meta_campaign_id,
        "funnel_id": str(funnel_uuid) if funnel_uuid else None,
        "weeks_synced": weeks_synced,
    }


# ---- Dashboard reads (Postgres only) + Sweep annotations ----

_SWEEP_STATUSES = {"testing", "foundational", "paused"}


def _org_owns_meta_entity(db: Session, org_id: uuid.UUID, entity_id: str) -> bool:
    from app.models.meta_ad import MetaAd
    from app.models.meta_ad_set import MetaAdSet
    from app.models.meta_campaign import MetaCampaign

    if db.query(MetaCampaign.id).filter(
        MetaCampaign.org_id == org_id, MetaCampaign.meta_campaign_id == entity_id
    ).first():
        return True
    if db.query(MetaAdSet.id).filter(MetaAdSet.org_id == org_id, MetaAdSet.meta_adset_id == entity_id).first():
        return True
    if db.query(MetaAd.id).filter(MetaAd.org_id == org_id, MetaAd.meta_ad_id == entity_id).first():
        return True
    return False


class AdsSettingsIn(BaseModel):
    target_cpr_cents: Optional[int] = Field(None, ge=0)
    min_roas: Optional[float] = Field(None, ge=0)


class SweepStatusIn(BaseModel):
    sweep_status: Optional[str] = None


def _ads_range(
    start: Optional[date],
    end: Optional[date],
    compare_start: Optional[date],
    compare_end: Optional[date],
) -> None:
    if start is not None and end is None:
        raise HTTPException(status_code=400, detail="end is required when start is set")
    if start is not None and end is not None and start > end:
        raise HTTPException(status_code=400, detail="start must be on or before end")
    if (compare_start is None) != (compare_end is None):
        raise HTTPException(status_code=400, detail="compare_start and compare_end must be set together")
    if compare_start is not None and compare_end is not None and compare_start > compare_end:
        raise HTTPException(status_code=400, detail="compare_start must be on or before compare_end")


@router.get("/dashboard")
def ads_dashboard(
    days: int = Query(28, ge=7, le=90),
    start: Optional[date] = Query(None),
    end: Optional[date] = Query(None),
    compare_start: Optional[date] = Query(None),
    compare_end: Optional[date] = Query(None),
    funnel_id: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Postgres-only. Never calls Meta on page render.

    `end` (and optional `start`) is the page calendar. Omit both to use trailing `days`.
    `funnel_id` narrows campaigns/KPIs/alerts to that funnel's linked campaigns —
    used by the Funnels tab's below-scorecard drill-down, not the main Ads tab.
    """
    from app.models.funnel import Funnel
    from app.services.ads_signal_engine import build_ads_dashboard

    _require_content_studio(db, current_user)
    _ads_range(start, end, compare_start, compare_end)
    org_id = _org_id(current_user)

    fid: Optional[uuid.UUID] = None
    if funnel_id:
        try:
            fid = uuid.UUID(funnel_id)
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid funnel_id")
        if not db.query(Funnel.id).filter(Funnel.id == fid, Funnel.org_id == org_id).first():
            raise HTTPException(status_code=404, detail="Funnel not found")

    if end is None:
        return build_ads_dashboard(db, org_id, days, funnel_id=fid)
    return build_ads_dashboard(
        db,
        org_id,
        start=start,
        end=end,
        compare_start=compare_start,
        compare_end=compare_end,
        funnel_id=fid,
    )


@router.get("/campaigns/{campaign_id}/breakdown")
def ads_campaign_breakdown(
    campaign_id: str,
    days: int = Query(28, ge=7, le=90),
    start: Optional[date] = Query(None),
    end: Optional[date] = Query(None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    from app.services.ads_signal_engine import build_campaign_breakdown

    _require_content_studio(db, current_user)
    _ads_range(start, end, None, None)
    try:
        cid = uuid.UUID(campaign_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid campaign id")
    if end is None:
        payload = build_campaign_breakdown(db, _org_id(current_user), cid, days)
    else:
        payload = build_campaign_breakdown(db, _org_id(current_user), cid, start=start, end=end)
    if payload is None:
        raise HTTPException(status_code=404, detail="Campaign not found")
    return payload


@router.get("/funnels/{funnel_id}/creative-grid")
def funnel_creative_grid(
    funnel_id: str,
    days: int = Query(28, ge=7, le=90),
    start: Optional[date] = Query(None),
    end: Optional[date] = Query(None),
    campaign_id: Optional[str] = Query(None, description="Narrow to one linked campaign, or omit for all"),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Per-creative (ad) rows across this funnel's linked campaign(s) — the
    funnel scorecard's replacement for the 23-metric sheet when a funnel has
    campaigns linked. Postgres-only, same as /dashboard."""
    from app.models.funnel import Funnel
    from app.services.ads_signal_engine import build_funnel_creative_grid

    _require_content_studio(db, current_user)
    _ads_range(start, end, None, None)
    org_id = _org_id(current_user)
    try:
        fid = uuid.UUID(funnel_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid funnel_id")
    if not db.query(Funnel.id).filter(Funnel.id == fid, Funnel.org_id == org_id).first():
        raise HTTPException(status_code=404, detail="Funnel not found")
    cid: Optional[uuid.UUID] = None
    if campaign_id:
        try:
            cid = uuid.UUID(campaign_id)
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid campaign_id")

    if end is None:
        return build_funnel_creative_grid(db, org_id, fid, days, campaign_id=cid)
    return build_funnel_creative_grid(db, org_id, fid, start=start, end=end, campaign_id=cid)


@router.get("/alerts")
def list_ads_alerts(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    from app.services.ads_signal_engine import _alert_out, _load_alerts

    _require_content_studio(db, current_user)
    org_id = _org_id(current_user)
    return {"alerts": [_alert_out(row) for row in _load_alerts(db, org_id)]}


@router.post("/alerts/{alert_id}/ack")
def ack_ads_alert(
    alert_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    from app.models.meta_ad_alert import MetaAdAlert
    from app.services.ads_signal_engine import _alert_out

    _require_content_studio(db, current_user)
    org_id = _org_id(current_user)
    try:
        aid = uuid.UUID(alert_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid alert id")
    row = db.query(MetaAdAlert).filter(MetaAdAlert.id == aid, MetaAdAlert.org_id == org_id).first()
    if row is None:
        raise HTTPException(status_code=404, detail="Alert not found")
    if row.status == "resolved":
        raise HTTPException(status_code=400, detail="Alert is already resolved")
    row.status = "acknowledged"
    db.commit()
    db.refresh(row)
    return _alert_out(row)


@router.put("/campaigns/{campaign_id}/sweep-status")
def set_campaign_sweep_status(
    campaign_id: str,
    body: SweepStatusIn,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    from app.models.meta_campaign import MetaCampaign

    _require_content_studio(db, current_user)
    org_id = _org_id(current_user)
    try:
        cid = uuid.UUID(campaign_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid campaign id")
    raw = (body.sweep_status or "").strip() or None
    if raw is not None and raw not in _SWEEP_STATUSES:
        raise HTTPException(status_code=400, detail="sweep_status must be testing, foundational, or paused")
    camp = db.query(MetaCampaign).filter(MetaCampaign.id == cid, MetaCampaign.org_id == org_id).first()
    if camp is None:
        raise HTTPException(status_code=404, detail="Campaign not found")
    camp.sweep_status = raw
    db.commit()
    return {"ok": True, "id": campaign_id, "sweep_status": camp.sweep_status}


@router.put("/settings")
def put_ads_settings(
    body: AdsSettingsIn,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin_or_owner),
):
    from app.models.meta_ad_account import MetaAdAccount

    _require_content_studio(db, current_user)
    org_id = _org_id(current_user)
    token = mac.get_meta_ads_token(db, org_id)
    account = None
    if token is not None and token.account_id:
        account = (
            db.query(MetaAdAccount)
            .filter(MetaAdAccount.org_id == org_id, MetaAdAccount.meta_ad_account_id == token.account_id)
            .first()
        )
    if account is None:
        account = (
            db.query(MetaAdAccount).filter(MetaAdAccount.org_id == org_id).order_by(MetaAdAccount.updated_at.desc()).first()
        )
    if account is None:
        raise HTTPException(status_code=400, detail="Sync Meta Ads before setting a target CPR or minimum ROAS")
    account.target_cpr_cents = body.target_cpr_cents
    account.min_roas = body.min_roas
    db.commit()
    return {
        "ok": True,
        "target_cpr_cents": account.target_cpr_cents,
        "min_roas": account.min_roas,
    }

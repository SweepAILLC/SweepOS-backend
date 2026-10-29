"""
Sync Meta Ads campaigns/ad sets/ads/insights via the direct Graph API into Postgres.

Never call Meta on page render — this service is for the worker (and the
manual /marketing-intel/ads/sync enqueue), mirroring instagram_sync_service.py's
shape. No Composio: all calls go through app/services/meta_ads_client.py
straight to graph.facebook.com.
"""
from __future__ import annotations

import logging
import time
import uuid
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.core.config import settings
from app.models.meta_ad import MetaAd
from app.models.meta_ad_account import MetaAdAccount
from app.models.meta_ad_insight_daily import MetaAdInsightDaily
from app.models.meta_ad_set import MetaAdSet
from app.models.meta_campaign import MetaCampaign
from app.models.oauth_token import OAuthProvider, OAuthToken
from app.services import meta_ads_client as mac
from app.services.meta_ads_client import (
    MetaAdsAuthError,
    MetaAdsConfigError,
    MetaAdsNotConnectedError,
)

logger = logging.getLogger(__name__)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _as_int(v: Any) -> Optional[int]:
    try:
        return int(v) if v is not None else None
    except (TypeError, ValueError):
        return None


def _as_cents(v: Any) -> Optional[int]:
    """Meta reports spend/cost as a decimal string in the account's major unit."""
    try:
        return int(round(float(v) * 100)) if v is not None else None
    except (TypeError, ValueError):
        return None


def _as_float(v: Any) -> Optional[float]:
    try:
        return float(v) if v is not None else None
    except (TypeError, ValueError):
        return None


def _unwrap_list(data: Any) -> List[Dict[str, Any]]:
    if isinstance(data, dict):
        items = data.get("data") or data.get("items") or []
        return items if isinstance(items, list) else []
    return data if isinstance(data, list) else []


def _get_or_create_ad_account(
    db: Session, org_id: uuid.UUID, *, meta_ad_account_id: str, account_name: Optional[str], currency: Optional[str]
) -> MetaAdAccount:
    row = (
        db.query(MetaAdAccount)
        .filter(MetaAdAccount.org_id == org_id, MetaAdAccount.meta_ad_account_id == meta_ad_account_id)
        .first()
    )
    if row is None:
        row = MetaAdAccount(
            org_id=org_id,
            meta_ad_account_id=meta_ad_account_id,
            account_name=account_name,
            currency=currency,
        )
        db.add(row)
        db.flush()
    else:
        if account_name:
            row.account_name = account_name
        if currency:
            row.currency = currency
    return row


def _assign_budget(row: Any, raw: Dict[str, Any]) -> None:
    """Store daily budget (already minor units) and stamp budget_updated_at on change."""
    new_budget = _as_int(raw.get("daily_budget"))
    if new_budget is not None and row.daily_budget_cents is not None and new_budget != row.daily_budget_cents:
        row.budget_updated_at = _utcnow()
    if new_budget is not None:
        row.daily_budget_cents = new_budget
    if raw.get("effective_status"):
        row.effective_status = raw.get("effective_status")


def _learning_stage(raw: Dict[str, Any]) -> Optional[str]:
    info = raw.get("learning_stage_info")
    if isinstance(info, dict) and info.get("status"):
        return str(info.get("status"))[:32]
    if isinstance(info, str) and info:
        return info[:32]
    return None


def _upsert_campaign(db: Session, org_id: uuid.UUID, ad_account_id: uuid.UUID, raw: Dict[str, Any]) -> MetaCampaign:
    meta_campaign_id = str(raw.get("id") or raw.get("campaign_id"))
    row = (
        db.query(MetaCampaign)
        .filter(MetaCampaign.org_id == org_id, MetaCampaign.meta_campaign_id == meta_campaign_id)
        .first()
    )
    if row is None:
        row = MetaCampaign(org_id=org_id, ad_account_id=ad_account_id, meta_campaign_id=meta_campaign_id)
        db.add(row)
        db.flush()
    row.name = raw.get("name") or row.name
    row.objective = raw.get("objective") or row.objective
    row.status = raw.get("effective_status") or raw.get("status") or row.status
    _assign_budget(row, raw)
    return row


def _upsert_ad_set(db: Session, org_id: uuid.UUID, campaign_id: uuid.UUID, raw: Dict[str, Any]) -> MetaAdSet:
    meta_adset_id = str(raw.get("id") or raw.get("adset_id"))
    row = (
        db.query(MetaAdSet)
        .filter(MetaAdSet.org_id == org_id, MetaAdSet.meta_adset_id == meta_adset_id)
        .first()
    )
    if row is None:
        row = MetaAdSet(org_id=org_id, campaign_id=campaign_id, meta_adset_id=meta_adset_id)
        db.add(row)
        db.flush()
    row.name = raw.get("name") or row.name
    row.optimization_goal = raw.get("optimization_goal") or row.optimization_goal
    _assign_budget(row, raw)
    stage = _learning_stage(raw)
    if stage:
        row.learning_stage = stage
    return row


def _upsert_ad(db: Session, org_id: uuid.UUID, ad_set_id: uuid.UUID, raw: Dict[str, Any]) -> MetaAd:
    meta_ad_id = str(raw.get("id") or raw.get("ad_id"))
    row = db.query(MetaAd).filter(MetaAd.org_id == org_id, MetaAd.meta_ad_id == meta_ad_id).first()
    if row is None:
        row = MetaAd(org_id=org_id, ad_set_id=ad_set_id, meta_ad_id=meta_ad_id)
        db.add(row)
        db.flush()
    row.name = raw.get("name") or row.name
    if raw.get("effective_status"):
        row.effective_status = raw.get("effective_status")
    creative = raw.get("creative") or {}
    if isinstance(creative, dict):
        row.creative_id = creative.get("id") or row.creative_id
        row.creative_thumbnail_url = creative.get("thumbnail_url") or row.creative_thumbnail_url
    created_time_str = raw.get("created_time")
    if created_time_str and row.created_time is None:
        try:
            row.created_time = datetime.fromisoformat(str(created_time_str))
        except ValueError:
            pass
    return row


# Action types that count as "the" result, in priority order, keyed by the ad
# set optimization_goal (preferred) or the campaign objective. Awareness goals
# map to an empty list so we store null instead of a random actions[0] (often
# a video view or link click that is not the optimized event).
_DEFAULT_ACTION_PRIORITY = [
    "lead",
    "onsite_conversion.lead_grouped",
    "offsite_conversion.fb_pixel_lead",
    "offsite_conversion.fb_pixel_purchase",
    "omni_purchase",
    "purchase",
    "complete_registration",
    "offsite_conversion.fb_pixel_complete_registration",
    "schedule",
    "offsite_conversion.fb_pixel_schedule",
    "onsite_conversion.messaging_conversation_started_7d",
    "link_click",
    "landing_page_view",
]
_GOAL_ACTION_PRIORITY: Dict[str, List[str]] = {
    "LEAD_GENERATION": ["lead", "onsite_conversion.lead_grouped", "offsite_conversion.fb_pixel_lead", "complete_registration", "schedule"],
    "QUALITY_LEAD": ["lead", "offsite_conversion.fb_pixel_lead", "onsite_conversion.lead_grouped"],
    "OFFSITE_CONVERSIONS": [
        "offsite_conversion.fb_pixel_lead",
        "lead",
        "offsite_conversion.fb_pixel_purchase",
        "purchase",
        "omni_purchase",
        "complete_registration",
        "offsite_conversion.fb_pixel_complete_registration",
        "schedule",
        "offsite_conversion.fb_pixel_schedule",
    ],
    "VALUE": ["offsite_conversion.fb_pixel_purchase", "omni_purchase", "purchase"],
    "CONVERSATIONS": ["onsite_conversion.messaging_conversation_started_7d", "onsite_conversion.messaging_first_reply"],
    "LINK_CLICKS": ["link_click"],
    "LANDING_PAGE_VIEWS": ["landing_page_view", "link_click"],
    "POST_ENGAGEMENT": ["post_engagement"],
    "THRUPLAY": ["video_view"],
    "REACH": [],
    "IMPRESSIONS": [],
    "AD_RECALL_LIFT": [],
}
_OBJECTIVE_ACTION_PRIORITY: Dict[str, List[str]] = {
    "OUTCOME_LEADS": ["lead", "onsite_conversion.lead_grouped", "offsite_conversion.fb_pixel_lead", "complete_registration", "schedule"],
    "OUTCOME_SALES": ["purchase", "omni_purchase", "offsite_conversion.fb_pixel_purchase"],
    "OUTCOME_ENGAGEMENT": ["post_engagement", "onsite_conversion.messaging_conversation_started_7d"],
    "OUTCOME_TRAFFIC": ["link_click", "landing_page_view"],
    "LEAD_GENERATION": ["lead", "offsite_conversion.fb_pixel_lead"],
    "CONVERSIONS": ["offsite_conversion.fb_pixel_purchase", "purchase", "offsite_conversion.fb_pixel_lead", "lead"],
    "LINK_CLICKS": ["link_click"],
    "MESSAGES": ["onsite_conversion.messaging_conversation_started_7d"],
}


def _preferred_action_types(optimization_goal: Optional[str], objective: Optional[str]) -> Optional[List[str]]:
    if optimization_goal and optimization_goal in _GOAL_ACTION_PRIORITY:
        return _GOAL_ACTION_PRIORITY[optimization_goal]
    if objective and objective in _OBJECTIVE_ACTION_PRIORITY:
        return _OBJECTIVE_ACTION_PRIORITY[objective]
    return None


def _pick_action_value(items: Any, preferred: Optional[List[str]]) -> Any:
    if not isinstance(items, list):
        return None
    by_type: Dict[str, Any] = {}
    ordered: List[Dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        ordered.append(item)
        action_type = item.get("action_type")
        if action_type and action_type not in by_type:
            by_type[action_type] = item.get("value")
    if preferred is not None:
        for action_type in preferred:
            if action_type in by_type:
                return by_type[action_type]
        return None
    for action_type in _DEFAULT_ACTION_PRIORITY:
        if action_type in by_type:
            return by_type[action_type]
    if ordered:
        return ordered[0].get("value")
    return None


def _extract_result_metrics(
    raw: Dict[str, Any],
    *,
    optimization_goal: Optional[str] = None,
    objective: Optional[str] = None,
) -> tuple[Optional[int], Optional[int]]:
    """(results, cost_per_result_cents) for the optimization event, not actions[0]."""
    preferred = _preferred_action_types(optimization_goal, objective)
    results = _as_int(_pick_action_value(raw.get("actions"), preferred))
    cost_per_result = _as_cents(_pick_action_value(raw.get("cost_per_action_type"), preferred))
    return results, cost_per_result


def _first_action_count(raw: Dict[str, Any], key: str) -> Optional[int]:
    items = raw.get(key)
    if isinstance(items, list) and items and isinstance(items[0], dict):
        return _as_int(items[0].get("value"))
    return None


def _upsert_insight_row(
    db: Session,
    org_id: uuid.UUID,
    *,
    level: str,
    entity_id: str,
    day: date,
    raw: Dict[str, Any],
    optimization_goal: Optional[str] = None,
    objective: Optional[str] = None,
) -> MetaAdInsightDaily:
    """Upsert on (org_id, level, entity_id, date) — insights get revised for
    up to 28 days after the report date, so this is never append-only.
    """
    results, cost_per_result = _extract_result_metrics(
        raw, optimization_goal=optimization_goal, objective=objective
    )
    row = (
        db.query(MetaAdInsightDaily)
        .filter(
            MetaAdInsightDaily.org_id == org_id,
            MetaAdInsightDaily.level == level,
            MetaAdInsightDaily.entity_id == entity_id,
            MetaAdInsightDaily.date == day,
        )
        .first()
    )
    if row is None:
        row = MetaAdInsightDaily(org_id=org_id, level=level, entity_id=entity_id, date=day)
        db.add(row)
    row.spend_cents = _as_cents(raw.get("spend"))
    row.results = results
    row.cost_per_result_cents = cost_per_result
    row.impressions = _as_int(raw.get("impressions"))
    row.clicks = _as_int(raw.get("clicks"))
    row.cpm_cents = _as_cents(raw.get("cpm"))
    row.ctr = _as_float(raw.get("ctr"))
    row.reach = _as_int(raw.get("reach"))
    row.frequency = _as_float(raw.get("frequency"))
    row.link_ctr = _as_float(raw.get("inline_link_click_ctr"))
    row.video_3s_views = _first_action_count(raw, "video_play_actions")
    row.thruplays = _first_action_count(raw, "video_thruplay_watched_actions")
    try:
        db.flush()
    except IntegrityError:
        # Concurrent sync for the same (org, level, entity, date) — safe no-op retry.
        db.rollback()
        logger.info(
            "meta_ads insight upsert race, retrying level=%s entity=%s date=%s org=%s",
            level, entity_id, day, org_id,
        )
        row = (
            db.query(MetaAdInsightDaily)
            .filter(
                MetaAdInsightDaily.org_id == org_id,
                MetaAdInsightDaily.level == level,
                MetaAdInsightDaily.entity_id == entity_id,
                MetaAdInsightDaily.date == day,
            )
            .first()
        )
    return row


def sync_meta_ads_for_org(db: Session, org_id: uuid.UUID, *, full: bool = False) -> Dict[str, Any]:
    """Pull campaigns/ad sets/ads + daily insights for one org.

    Always re-pulls the trailing META_ADS_INSIGHTS_LOOKBACK_DAYS (default 28)
    of insights regardless of `full` — Meta revises insights for up to 28
    days after the report date, so a pure incremental sync would silently
    miss those revisions. `full` controls whether individual ads are
    re-listed. Ad set budget, status, and learning stage are always refreshed
    so the fatigue/scale rules see current delivery.
    """
    token_row = mac.get_meta_ads_token(db, org_id)
    if token_row is None:
        raise MetaAdsNotConnectedError("Meta Ads is not connected for this org")

    started = time.monotonic()
    budget = float(getattr(settings, "META_ADS_SYNC_BUDGET_SEC", 120) or 120)
    budget = max(30.0, min(budget, 900.0))

    def _budget_ok(min_left: float = 10.0) -> bool:
        return (budget - (time.monotonic() - started)) > min_left

    meta_ad_account_id = token_row.account_id
    if not meta_ad_account_id:
        raise MetaAdsConfigError("No Meta ad account selected for this org yet")

    access_token = mac.access_token_for_org(db, org_id)

    ad_account = _get_or_create_ad_account(
        db, org_id, meta_ad_account_id=meta_ad_account_id, account_name=None, currency=None
    )

    campaigns_synced = 0
    ad_sets_synced = 0
    ads_synced = 0
    insight_rows_synced = 0

    campaigns_raw = _unwrap_list(
        mac.list_campaigns(ad_account_id=meta_ad_account_id, token=access_token)
    )
    campaign_rows: Dict[str, MetaCampaign] = {}
    for c in campaigns_raw:
        if not _budget_ok():
            break
        campaign = _upsert_campaign(db, org_id, ad_account.id, c)
        campaign_rows[campaign.meta_campaign_id] = campaign
        campaigns_synced += 1

        if _budget_ok():
            ad_sets_raw = _unwrap_list(
                mac.list_ad_sets(campaign_id=campaign.meta_campaign_id, token=access_token)
            )
            for a in ad_sets_raw:
                if not _budget_ok():
                    break
                ad_set = _upsert_ad_set(db, org_id, campaign.id, a)
                ad_sets_synced += 1

                if not full:
                    continue
                ads_raw = _unwrap_list(
                    mac.list_ads(ad_set_id=ad_set.meta_adset_id, token=access_token)
                )
                for ad in ads_raw:
                    if not _budget_ok():
                        break
                    _upsert_ad(db, org_id, ad_set.id, ad)
                    ads_synced += 1

    lookback_days = int(getattr(settings, "META_ADS_INSIGHTS_LOOKBACK_DAYS", 28) or 28)
    lookback_days = max(1, min(lookback_days, 90))
    since = (_utcnow() - timedelta(days=lookback_days)).date()
    until = _utcnow().date()

    adset_goals = {
        row.meta_adset_id: row.optimization_goal
        for row in db.query(MetaAdSet).filter(MetaAdSet.org_id == org_id).all()
    }

    for meta_campaign_id, campaign in campaign_rows.items():
        if not _budget_ok():
            break
        insights_raw = _unwrap_list(
            mac.get_insights(
                object_id=meta_campaign_id,
                level="campaign",
                since=since.isoformat(),
                until=until.isoformat(),
                token=access_token,
            )
        )
        for raw_row in insights_raw:
            day_str = raw_row.get("date_start") or raw_row.get("date")
            if not day_str:
                continue
            day = date.fromisoformat(str(day_str)[:10])
            _upsert_insight_row(
                db,
                org_id,
                level="campaign",
                entity_id=meta_campaign_id,
                day=day,
                raw=raw_row,
                objective=campaign.objective,
            )
            insight_rows_synced += 1

        if _budget_ok():
            adset_insights_raw = _unwrap_list(
                mac.get_insights(
                    object_id=meta_campaign_id,
                    level="adset",
                    since=since.isoformat(),
                    until=until.isoformat(),
                    token=access_token,
                )
            )
            for raw_row in adset_insights_raw:
                day_str = raw_row.get("date_start") or raw_row.get("date")
                adset_id = raw_row.get("adset_id")
                if not day_str or not adset_id:
                    continue
                day = date.fromisoformat(str(day_str)[:10])
                _upsert_insight_row(
                    db,
                    org_id,
                    level="adset",
                    entity_id=str(adset_id),
                    day=day,
                    raw=raw_row,
                    optimization_goal=adset_goals.get(str(adset_id)),
                    objective=campaign.objective,
                )
                insight_rows_synced += 1

        if not _budget_ok():
            break
        ad_insights_raw = _unwrap_list(
            mac.get_insights(
                object_id=meta_campaign_id,
                level="ad",
                since=since.isoformat(),
                until=until.isoformat(),
                token=access_token,
            )
        )
        for raw_row in ad_insights_raw:
            day_str = raw_row.get("date_start") or raw_row.get("date")
            ad_id = raw_row.get("ad_id")
            if not day_str or not ad_id:
                continue
            day = date.fromisoformat(str(day_str)[:10])
            _upsert_insight_row(
                db,
                org_id,
                level="ad",
                entity_id=str(ad_id),
                day=day,
                raw=raw_row,
                optimization_goal=adset_goals.get(str(raw_row.get("adset_id"))),
                objective=campaign.objective,
            )
            insight_rows_synced += 1

    funnel_spend_weeks_synced = sync_funnel_ad_spend_from_insights(db, org_id)

    try:
        with db.begin_nested():
            from app.services.ads_signal_engine import evaluate_org_alerts

            evaluate_org_alerts(db, org_id)
    except Exception:
        logger.exception("ads signal evaluation failed org=%s", org_id)

    ad_account.last_synced_at = _utcnow()
    token_row.last_sync_at = _utcnow()
    db.commit()

    return {
        "org_id": str(org_id),
        "campaigns_synced": campaigns_synced,
        "ad_sets_synced": ad_sets_synced,
        "ads_synced": ads_synced,
        "insight_rows_synced": insight_rows_synced,
        "funnel_spend_weeks_synced": funnel_spend_weeks_synced,
    }


def sync_funnel_ad_spend_from_insights(db: Session, org_id: uuid.UUID) -> int:
    """Auto-populate FunnelAdSpend weekly rows — spend from already-synced
    campaign-level daily insights, summed across every campaign linked to a
    funnel (a funnel can have more than one — see migration 110, which moved
    the link from Funnel.meta_campaign_id's 1:1 column onto MetaCampaign.funnel_id).
    "ads_deployed" is the count of ads under those campaigns whose Meta
    created_time falls in the week. No extra Graph API calls — reuses data
    sync_meta_ads_for_org already pulled. Never overwrites a week someone set
    directly via PUT /ad-spend (source other than "meta_ads_auto") — auto-sync
    only owns weeks it wrote itself."""
    from app.models.funnel import Funnel
    from app.models.funnel_ad_spend import FunnelAdSpend
    from app.services.funnel_dashboard import upsert_ad_spend, week_start_for

    linked = (
        db.query(MetaCampaign.funnel_id, MetaCampaign.meta_campaign_id)
        .filter(MetaCampaign.org_id == org_id, MetaCampaign.funnel_id.isnot(None))
        .all()
    )
    if not linked:
        return 0
    campaign_ids_by_funnel: Dict[uuid.UUID, List[str]] = {}
    for funnel_id, meta_campaign_id in linked:
        campaign_ids_by_funnel.setdefault(funnel_id, []).append(meta_campaign_id)

    funnels = (
        db.query(Funnel)
        .filter(Funnel.org_id == org_id, Funnel.id.in_(list(campaign_ids_by_funnel.keys())))
        .all()
    )

    weeks_written = 0
    for funnel in funnels:
        campaign_ids = campaign_ids_by_funnel.get(funnel.id, [])
        if not campaign_ids:
            continue
        insight_rows = (
            db.query(MetaAdInsightDaily)
            .filter(
                MetaAdInsightDaily.org_id == org_id,
                MetaAdInsightDaily.level == "campaign",
                MetaAdInsightDaily.entity_id.in_(campaign_ids),
            )
            .all()
        )
        weekly_cents: Dict[date, int] = {}
        for row in insight_rows:
            wk = week_start_for(row.date)
            weekly_cents[wk] = weekly_cents.get(wk, 0) + int(row.spend_cents or 0)

        ad_created_times = (
            db.query(MetaAd.created_time)
            .join(MetaAdSet, MetaAd.ad_set_id == MetaAdSet.id)
            .join(MetaCampaign, MetaAdSet.campaign_id == MetaCampaign.id)
            .filter(
                MetaCampaign.org_id == org_id,
                MetaCampaign.meta_campaign_id.in_(campaign_ids),
                MetaAd.created_time.isnot(None),
            )
            .all()
        )
        weekly_ads_deployed: Dict[date, int] = {}
        for (created_time,) in ad_created_times:
            wk = week_start_for(created_time.date())
            weekly_ads_deployed[wk] = weekly_ads_deployed.get(wk, 0) + 1

        for wk in set(weekly_cents) | set(weekly_ads_deployed):
            existing = (
                db.query(FunnelAdSpend)
                .filter(
                    FunnelAdSpend.org_id == org_id,
                    FunnelAdSpend.funnel_id == funnel.id,
                    FunnelAdSpend.week_start == wk,
                )
                .first()
            )
            if existing is not None and existing.source not in (None, "meta_ads_auto"):
                continue
            upsert_ad_spend(
                db,
                org_id,
                funnel.id,
                wk,
                weekly_cents.get(wk, 0),
                None,
                ads_deployed=weekly_ads_deployed.get(wk),
                source="meta_ads_auto",
            )
            weeks_written += 1
    return weeks_written


def sync_meta_ads_all_orgs(db: Session) -> Dict[str, Any]:
    """Worker entry: sync orgs whose last_sync_at is older than interval."""
    interval = int(getattr(settings, "META_ADS_SYNC_INTERVAL_SEC", 86400) or 86400)
    cutoff = _utcnow() - timedelta(seconds=max(interval, 300))
    tokens = (
        db.query(OAuthToken)
        .filter(OAuthToken.provider == OAuthProvider.META_ADS)
        .all()
    )
    synced = 0
    failed = 0
    skipped_fresh = 0
    skipped_not_connected = 0
    for tok in tokens:
        last_sync = tok.last_sync_at
        if last_sync is not None:
            last_aware = last_sync if last_sync.tzinfo else last_sync.replace(tzinfo=timezone.utc)
            if last_aware > cutoff:
                skipped_fresh += 1
                continue
        try:
            sync_meta_ads_for_org(db, tok.org_id, full=True)
            synced += 1
        except (MetaAdsNotConnectedError, MetaAdsConfigError) as e:
            skipped_not_connected += 1
            logger.info("meta_ads sync skip org=%s: %s", tok.org_id, e)
        except MetaAdsAuthError as e:
            logger.warning("meta_ads sync needs reconnect org=%s: %s", tok.org_id, e)
        except Exception:
            failed += 1
            logger.exception("meta_ads sync failed org=%s", tok.org_id)
            try:
                db.rollback()
            except Exception:
                pass
    return {
        "synced": synced,
        "failed": failed,
        "skipped_fresh": skipped_fresh,
        "skipped_not_connected": skipped_not_connected,
        "candidates": len(tokens),
    }


def sync_meta_ads_all_orgs_job() -> Dict[str, Any]:
    from app.db.session import SessionLocal

    db = SessionLocal()
    try:
        return sync_meta_ads_all_orgs(db)
    finally:
        db.close()

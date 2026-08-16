"""
Sync Instagram media + insights via Composio into Postgres.

Never call Meta/Composio on page render — this service is for the worker
(and optional background /instagram/sync enqueue). Graceful degradation when
insights are unavailable (follower floor, metric deprecations, rate limits).
"""
from __future__ import annotations

import logging
import re
import time
import uuid
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple

from sqlalchemy.orm import Session

from app.core.config import settings
from app.models.instagram_account_snapshot import InstagramAccountSnapshot
from app.models.instagram_media import InstagramMedia
from app.models.oauth_token import OAuthProvider, OAuthToken
from app.services import composio_client as cc
from app.services.composio_client import (
    ComposioConfigError,
    ComposioNotConnectedError,
    ComposioToolError,
)

logger = logging.getLogger(__name__)

SETTLE_HOURS = 48
# Instagram often returns core insights well before the full 48h settle window.
# Still attempt pulls after this age so "this week" dashboards aren't blank.
INSIGHTS_MIN_AGE_HOURS = 2

CORE_METRICS = ["views", "reach", "saved", "total_interactions"]
FEED_METRICS = [
    "views",
    "reach",
    "saved",
    "likes",
    "comments",
    "shares",
    "total_interactions",
    "reposts",
]
REELS_METRICS = FEED_METRICS + [
    "ig_reels_avg_watch_time",
    "ig_reels_video_view_total_time",
    "reels_skip_rate",
]

_QUESTION_RE = re.compile(r"\?")
_NUMBER_RE = re.compile(r"^\s*\d")
_HOWTO_RE = re.compile(r"\b(how to|how i|here'?s how|step[- ]?by[- ]?step)\b", re.I)
_STORY_RE = re.compile(r"\b(i used to|my client|story time|confession)\b", re.I)
_CONTRA_RE = re.compile(r"\b(stop|don'?t|never|myth|wrong|truth about)\b", re.I)
_CTA_RE = re.compile(r"\b(comment|dm me|link in bio|save this|follow for)\b", re.I)


def _utcnow() -> datetime:
    return datetime.utcnow()


def _parse_posted_at(raw: Any) -> Optional[datetime]:
    if raw is None:
        return None
    if isinstance(raw, datetime):
        return raw.replace(tzinfo=None) if raw.tzinfo else raw
    s = str(raw).strip()
    if not s:
        return None
    try:
        # Instagram returns ISO 8601 UTC
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        return dt.replace(tzinfo=None) if dt.tzinfo else dt
    except ValueError:
        return None


def _unwrap_list(data: Any) -> List[Dict[str, Any]]:
    """Normalize Composio/Graph nesting (data vs data.data)."""
    if data is None:
        return []
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    if not isinstance(data, dict):
        return []
    inner = data.get("data")
    if isinstance(inner, list):
        return [x for x in inner if isinstance(x, dict)]
    if isinstance(inner, dict):
        nested = inner.get("data")
        if isinstance(nested, list):
            return [x for x in nested if isinstance(x, dict)]
        # single object
        return [inner]
    # Sometimes the media payload is the dict itself with id
    if data.get("id"):
        return [data]
    return []


def _paging_after(data: Any) -> Optional[str]:
    if not isinstance(data, dict):
        return None
    paging = data.get("paging") or {}
    if isinstance(paging, dict):
        cursors = paging.get("cursors") or {}
        if isinstance(cursors, dict) and cursors.get("after"):
            return str(cursors["after"])
        if paging.get("next") and cursors.get("after"):
            return str(cursors["after"])
    # Nested under data
    inner = data.get("data")
    if isinstance(inner, dict):
        return _paging_after(inner)
    return None


def _insight_map(data: Any) -> Dict[str, float]:
    """Parse insights response into {metric_name: value}."""
    out: Dict[str, float] = {}
    rows = _unwrap_list(data)
    if not rows and isinstance(data, dict) and isinstance(data.get("data"), list):
        rows = [x for x in data["data"] if isinstance(x, dict)]
    for row in rows:
        name = str(row.get("name") or "").strip()
        if not name:
            continue
        val: Optional[float] = None
        values = row.get("values")
        if isinstance(values, list) and values:
            last = values[-1]
            if isinstance(last, dict) and last.get("value") is not None:
                try:
                    val = float(last["value"])
                except (TypeError, ValueError):
                    val = None
        tv = row.get("total_value")
        if val is None and isinstance(tv, dict) and tv.get("value") is not None:
            try:
                val = float(tv["value"])
            except (TypeError, ValueError):
                val = None
        if val is None and row.get("value") is not None:
            try:
                val = float(row["value"])
            except (TypeError, ValueError):
                val = None
        if val is not None:
            out[name] = val
    return out


def _hook_text(caption: Optional[str]) -> str:
    if not caption:
        return ""
    first = caption.strip().split("\n", 1)[0].strip()
    return first[:500]


def classify_hook_pattern(caption: Optional[str]) -> str:
    hook = _hook_text(caption)
    if not hook:
        return "none"
    if _QUESTION_RE.search(hook):
        return "question"
    if _NUMBER_RE.search(hook):
        return "listicle_number"
    if _HOWTO_RE.search(hook):
        return "howto"
    if _STORY_RE.search(hook):
        return "story"
    if _CONTRA_RE.search(hook):
        return "contrarian"
    if _CTA_RE.search(hook):
        return "cta"
    if len(hook) <= 40:
        return "short_punch"
    return "statement"


def format_bucket(media_type: Optional[str], product_type: Optional[str]) -> str:
    pt = (product_type or "").upper()
    mt = (media_type or "").upper()
    if pt == "REELS" or mt == "REELS":
        return "reel"
    if pt == "STORY":
        return "story"
    if mt == "CAROUSEL_ALBUM":
        return "carousel"
    if mt == "VIDEO":
        return "video"
    if mt == "IMAGE":
        return "image"
    return (pt or mt or "unknown").lower()


def guess_funnel_stage(caption: Optional[str], hook_pattern: str) -> str:
    text = (caption or "").lower()
    if any(w in text for w in ("link in bio", "book a call", "dm me", "apply", "offer", "close")):
        return "BOF"
    if any(w in text for w in ("how to", "framework", "mistake", "case study", "vs ", "compare")):
        return "MOF"
    if hook_pattern in ("story", "question", "short_punch"):
        return "TOF"
    return "TOF"


def caption_len_bucket(n: Optional[int]) -> str:
    if n is None:
        return "unknown"
    if n < 80:
        return "short"
    if n < 300:
        return "medium"
    return "long"


def match_theme_keys(
    caption: Optional[str],
    themes: Sequence[Dict[str, Any]],
) -> List[str]:
    if not caption or not themes:
        return []
    lower = caption.lower()
    hits: List[str] = []
    for t in themes:
        key = str(t.get("theme_key") or "")
        label = str(t.get("label") or "").lower()
        if not key:
            continue
        # Match label words or theme_key tokens in caption
        tokens = [w for w in re.split(r"[\s_/:-]+", label or key) if len(w) >= 4]
        if any(tok in lower for tok in tokens[:6]):
            hits.append(key)
        else:
            for q in (t.get("sample_quotes") or [])[:2]:
                snippet = str(q).lower()[:40]
                words = [w for w in snippet.split() if len(w) >= 5][:3]
                if words and all(w in lower for w in words[:1]):
                    hits.append(key)
                    break
    return hits[:8]


def compute_derived(
    *,
    reach: Optional[int],
    saved: Optional[int],
    shares: Optional[int],
    total_interactions: Optional[int],
    likes: Optional[int],
    comments: Optional[int],
    followers: Optional[int],
) -> Dict[str, Optional[float]]:
    ti = total_interactions
    if ti is None and (likes is not None or comments is not None):
        ti = int(likes or 0) + int(comments or 0) + int(saved or 0) + int(shares or 0)

    eng = None
    save_r = None
    share_r = None
    reach_vs = None
    denom_reach = reach if reach and reach > 0 else None
    denom_followers = followers if followers and followers > 0 else None

    if ti is not None and denom_reach:
        eng = round((ti / denom_reach) * 100.0, 3)
    elif ti is not None and denom_followers:
        # Fallback when insights unavailable: engagement vs followers
        eng = round((ti / denom_followers) * 100.0, 3)

    if saved is not None and denom_reach:
        save_r = round((saved / denom_reach) * 100.0, 3)
    if shares is not None and denom_reach:
        share_r = round((shares / denom_reach) * 100.0, 3)
    if denom_reach and denom_followers:
        reach_vs = round((denom_reach / denom_followers) * 100.0, 3)

    return {
        "engagement_rate_pct": eng,
        "save_rate_pct": save_r,
        "share_rate_pct": share_r,
        "reach_vs_followers_pct": reach_vs,
        "total_interactions": float(ti) if ti is not None else None,
    }


def _as_int(v: Any) -> Optional[int]:
    if v is None:
        return None
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return None


def _as_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _cover_image_url(item: Dict[str, Any]) -> Optional[str]:
    """Pick a still image URL suitable for <img> (never a video media_url)."""
    thumb = item.get("thumbnail_url") or item.get("thumb_url") or item.get("cover_url")
    if isinstance(thumb, str) and thumb.strip().startswith("http"):
        return thumb.strip()[:4000]
    media_type = str(item.get("media_type") or "").upper()
    product = str(item.get("media_product_type") or "").upper()
    media_url = item.get("media_url") or item.get("url")
    if not isinstance(media_url, str) or not media_url.strip().startswith("http"):
        return None
    url = media_url.strip()
    # Video/Reel media_url is usually an .mp4 — unusable as an <img> cover.
    if product == "REELS" or media_type in ("VIDEO", "REELS"):
        return None
    lower = url.lower().split("?", 1)[0]
    if lower.endswith((".mp4", ".mov", ".m3u8", ".webm")):
        return None
    return url[:4000]


def fetch_user_info(db: Session, org_id: uuid.UUID) -> Dict[str, Any]:
    data = cc.execute(db, org_id, "INSTAGRAM_GET_USER_INFO", {"ig_user_id": "me"})
    rows = _unwrap_list(data)
    if rows:
        return rows[0]
    if isinstance(data, dict):
        return data
    return {}


def fetch_media_page(
    db: Session,
    org_id: uuid.UUID,
    *,
    ig_user_id: str = "me",
    after: Optional[str] = None,
    limit: int = 50,
) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    # Composio/Meta require ig_user_id (numeric id or "me"); omitting it fails the page.
    args: Dict[str, Any] = {
        "ig_user_id": (ig_user_id or "me").strip() or "me",
        "limit": min(max(limit, 1), 100),
    }
    if after:
        args["after"] = after
    data = cc.execute(db, org_id, "INSTAGRAM_GET_IG_USER_MEDIA", args)
    items = _unwrap_list(data)
    nxt = _paging_after(data)
    return items, nxt


def fetch_media_insights(
    db: Session,
    org_id: uuid.UUID,
    ig_media_id: str,
    *,
    product_type: Optional[str],
) -> Tuple[Dict[str, float], str, Optional[str]]:
    """
    Returns (metrics, insights_status, error_message).

    Degradation ladder:
      1) full metric set for product type
      2) core subset → partial
      3) unavailable
    """
    pt = (product_type or "").upper()
    full = REELS_METRICS if pt == "REELS" else FEED_METRICS
    try:
        data = cc.execute(
            db,
            org_id,
            "INSTAGRAM_GET_IG_MEDIA_INSIGHTS",
            {"ig_media_id": ig_media_id, "metric": full},
        )
        return _insight_map(data), "ok", None
    except ComposioToolError as e1:
        logger.info(
            "instagram insights full failed media=%s: %s — retrying core",
            ig_media_id,
            e1,
        )
        try:
            data = cc.execute(
                db,
                org_id,
                "INSTAGRAM_GET_IG_MEDIA_INSIGHTS",
                {"ig_media_id": ig_media_id, "metric": CORE_METRICS},
            )
            m = _insight_map(data)
            if m:
                return m, "partial", str(e1)[:500]
        except ComposioToolError as e2:
            return {}, "unavailable", str(e2)[:500]
        return {}, "unavailable", str(e1)[:500]


def resolve_capabilities(
    *,
    followers_count: Optional[int],
    insights_ok_count: int,
    insights_attempted: int,
) -> Dict[str, Any]:
    insights = True
    reason = None
    if followers_count is not None and followers_count < 1000:
        insights = False
        reason = (
            "Instagram media insights typically require 1,000+ followers. "
            f"This account has {followers_count}. Showing likes/comments and "
            "follower-normalized engagement instead."
        )
    elif insights_attempted > 0 and insights_ok_count == 0:
        insights = False
        reason = (
            "Media insights are unavailable for this account right now "
            "(permission, follower floor, or Meta API error). "
            "Using public like/comment counts as a fallback."
        )
    elif insights_attempted > 0 and insights_ok_count < insights_attempted:
        reason = (
            f"Partial insights: {insights_ok_count}/{insights_attempted} posts "
            "returned full metrics. Some Reels-only or deprecated metrics were skipped."
        )
    return {"insights": insights, "reason": reason, "followers_count": followers_count}


def sync_instagram_for_org(
    db: Session,
    org_id: uuid.UUID,
    *,
    full: bool = False,
) -> Dict[str, Any]:
    """Pull media + insights for one org. Returns sync stats dict.

    Incremental syncs:
      - page only *new* posts newer than the newest cached row
      - refresh insights for a capped set of recent/unsettled rows
      - stop early when INSTAGRAM_SYNC_BUDGET_SEC is exhausted

    This avoids the previous behavior of re-fetching insights for an entire
    media page on every sync (the main cause of HTTP/proxy timeouts).
    """
    token = cc.get_instagram_token(db, org_id)
    if token is None:
        raise ComposioNotConnectedError("Instagram is not connected for this org")

    started = time.monotonic()
    budget = float(getattr(settings, "INSTAGRAM_SYNC_BUDGET_SEC", 90) or 90)
    budget = max(20.0, min(budget, 600.0))

    def _budget_left() -> float:
        return budget - (time.monotonic() - started)

    def _budget_ok(min_left: float = 5.0) -> bool:
        return _budget_left() > min_left

    max_posts = int(getattr(settings, "INSTAGRAM_MAX_POSTS_PER_SYNC", 100) or 100)
    max_posts = max(1, min(max_posts, 500))
    refresh_limit = int(getattr(settings, "INSTAGRAM_INSIGHTS_REFRESH_LIMIT", 25) or 25)
    refresh_limit = max(1, min(refresh_limit, 100))

    # Themes for classification
    themes: List[Dict[str, Any]] = []
    try:
        from app.services.org_sales_theme_service import list_validated_themes_payload

        themes = list_validated_themes_payload(db, org_id)
    except Exception:
        logger.exception("instagram sync: theme load failed org=%s", org_id)

    # Incremental watermark
    newest_existing: Optional[datetime] = None
    if not full:
        newest_existing = (
            db.query(InstagramMedia.posted_at)
            .filter(InstagramMedia.org_id == org_id, InstagramMedia.posted_at.isnot(None))
            .order_by(InstagramMedia.posted_at.desc())
            .limit(1)
            .scalar()
        )

    user_info = fetch_user_info(db, org_id)
    ig_user_id = str(user_info.get("id") or user_info.get("ig_id") or token.account_id or "")
    username = str(user_info.get("username") or "")
    followers = _as_int(user_info.get("followers_count"))

    if ig_user_id and ig_user_id != token.account_id:
        token.account_id = ig_user_id

    # Account snapshot for today
    today = date.today()
    snap = (
        db.query(InstagramAccountSnapshot)
        .filter(
            InstagramAccountSnapshot.org_id == org_id,
            InstagramAccountSnapshot.snapshot_date == today,
        )
        .first()
    )
    if snap is None:
        snap = InstagramAccountSnapshot(org_id=org_id, snapshot_date=today)
        db.add(snap)
    snap.followers_count = followers
    snap.updated_at = _utcnow()

    # Optional account insights (best-effort)
    if _budget_ok(15.0):
        try:
            since = int((datetime.now(timezone.utc) - timedelta(days=1)).timestamp())
            until = int(datetime.now(timezone.utc).timestamp())
            acct = cc.execute(
                db,
                org_id,
                "INSTAGRAM_GET_USER_INSIGHTS",
                {
                    "metric": [
                        "reach",
                        "views",
                        "profile_views",
                        "accounts_engaged",
                        "follows_and_unfollows",
                    ],
                    "period": "day",
                    "since": since,
                    "until": until,
                    "metric_type": "total_value",
                },
            )
            amap = _insight_map(acct)
            if "reach" in amap:
                snap.reach = int(amap["reach"])
            if "views" in amap:
                snap.views = int(amap["views"])
            if "profile_views" in amap:
                snap.profile_views = int(amap["profile_views"])
            if "accounts_engaged" in amap:
                snap.accounts_engaged = int(amap["accounts_engaged"])
            if "follows_and_unfollows" in amap:
                n = int(amap["follows_and_unfollows"])
                if n >= 0:
                    snap.follows = n
                else:
                    snap.unfollows = abs(n)
        except ComposioToolError as e:
            logger.info("instagram account insights skipped org=%s: %s", org_id, e)

    collected: List[Dict[str, Any]] = []
    after: Optional[str] = None
    budget_hit = False
    while len(collected) < max_posts and _budget_ok(10.0):
        page_limit = min(50, max_posts - len(collected))
        try:
            items, after = fetch_media_page(
                db,
                org_id,
                ig_user_id=ig_user_id or "me",
                after=after,
                limit=page_limit,
            )
        except ComposioToolError as e:
            logger.warning("instagram media page failed org=%s: %s", org_id, e)
            break
        if not items:
            break
        stop_incremental = False
        for item in items:
            posted = _parse_posted_at(item.get("timestamp") or item.get("posted_at"))
            # Incremental: only keep posts newer than the watermark.
            if not full and newest_existing and posted and posted <= newest_existing:
                stop_incremental = True
                break
            collected.append(item)
            if len(collected) >= max_posts:
                break
        if stop_incremental or not after:
            break
        time.sleep(0.2)

    if not _budget_ok(8.0):
        budget_hit = True

    by_id: Dict[str, Dict[str, Any]] = {}
    for item in collected:
        mid = str(item.get("id") or "")
        if mid:
            by_id[mid] = item

    # Refresh insights for recent / unsettled known posts (even when nothing new was posted).
    # Previously refresh_ids was computed and never used — that left metrics stale after ~48h.
    refresh_rows: List[InstagramMedia] = []
    if not full and _budget_ok(8.0):
        cutoff = _utcnow() - timedelta(days=10)
        refresh_rows = (
            db.query(InstagramMedia)
            .filter(
                InstagramMedia.org_id == org_id,
                InstagramMedia.posted_at >= cutoff,
            )
            .order_by(InstagramMedia.posted_at.desc())
            .limit(refresh_limit * 2)
            .all()
        )
        # Prefer unsettled / incomplete insights first.
        refresh_rows.sort(
            key=lambda r: (
                0 if not r.metrics_settled else 1,
                0 if r.insights_status not in ("ok",) else 1,
                -(r.posted_at.timestamp() if r.posted_at else 0),
            )
        )
        refresh_rows = refresh_rows[:refresh_limit]

    insights_ok = 0
    insights_attempted = 0
    upserted = 0
    refreshed = 0
    now = _utcnow()

    def _upsert_from_item(
        item: Dict[str, Any],
        *,
        fetch_insights: bool,
        existing: Optional[InstagramMedia] = None,
    ) -> None:
        nonlocal insights_ok, insights_attempted, upserted, refreshed, budget_hit
        mid = str(item.get("id") or (existing.ig_media_id if existing else "") or "")
        if not mid:
            return
        posted = _parse_posted_at(item.get("timestamp")) or (existing.posted_at if existing else None)
        settled = True
        if posted:
            settled = (now - posted) >= timedelta(hours=SETTLE_HOURS)

        product_type = str(item.get("media_product_type") or (existing.media_product_type if existing else "") or "")
        media_type = str(item.get("media_type") or (existing.media_type if existing else "") or "")
        if not product_type and media_type.upper() == "VIDEO":
            product_type = "REELS"
        caption = item.get("caption")
        if caption is None and existing is not None:
            caption = existing.caption
        caption_s = str(caption) if caption is not None else None
        likes = _as_int(item.get("like_count") or item.get("likes"))
        comments = _as_int(item.get("comments_count") or item.get("comments"))

        insights: Dict[str, float] = {}
        status = "unavailable"
        err: Optional[str] = None
        age_hours = None
        if posted:
            age_hours = (now - posted).total_seconds() / 3600.0
        insights_ready_enough = age_hours is None or age_hours >= INSIGHTS_MIN_AGE_HOURS

        if fetch_insights and insights_ready_enough and _budget_ok(6.0):
            insights_attempted += 1
            insights, status, err = fetch_media_insights(
                db, org_id, mid, product_type=product_type
            )
            if status in ("ok", "partial"):
                insights_ok += 1
            # Brand-new posts may still return empty/error; keep a settling note.
            if not settled and status in ("unavailable", "partial") and not insights:
                err = err or "Metrics still settling (Instagram data can lag ~48h)"
                status = "partial"
            time.sleep(0.15)
        elif fetch_insights and insights_ready_enough and not _budget_ok(6.0):
            budget_hit = True
            status = existing.insights_status if existing and existing.insights_status else "partial"
            err = "Sync budget exhausted — will retry on next scheduled sync"
        elif fetch_insights and not insights_ready_enough:
            status = "partial"
            err = "Posted too recently for reliable insights — retrying on next sync"
        elif existing is not None and not fetch_insights:
            status = existing.insights_status or "unavailable"
            err = existing.insights_error
        elif not settled:
            status = "partial"
            err = "Metrics still settling (Instagram data can lag ~48h)"

        views = _as_int(insights.get("views"))
        reach = _as_int(insights.get("reach"))
        saved = _as_int(insights.get("saved"))
        shares = _as_int(insights.get("shares"))
        reposts = _as_int(insights.get("reposts"))
        ti = _as_int(insights.get("total_interactions"))
        if likes is None:
            likes = _as_int(insights.get("likes"))
        if comments is None:
            comments = _as_int(insights.get("comments"))

        # Preserve prior insight metrics when we skipped the insights call.
        if existing is not None and not insights:
            views = views if views is not None else existing.views
            reach = reach if reach is not None else existing.reach
            saved = saved if saved is not None else existing.saved
            shares = shares if shares is not None else existing.shares
            reposts = reposts if reposts is not None else existing.reposts
            ti = ti if ti is not None else existing.total_interactions
            likes = likes if likes is not None else existing.likes
            comments = comments if comments is not None else existing.comments

        derived = compute_derived(
            reach=reach,
            saved=saved,
            shares=shares,
            total_interactions=ti,
            likes=likes,
            comments=comments,
            followers=followers,
        )
        if derived["total_interactions"] is not None and ti is None:
            ti = int(derived["total_interactions"])

        hook = _hook_text(caption_s)
        pattern = classify_hook_pattern(caption_s)
        fbucket = format_bucket(media_type, product_type)
        stage = guess_funnel_stage(caption_s, pattern)
        theme_keys = match_theme_keys(caption_s, themes)
        clen = len(caption_s) if caption_s else 0

        row = existing or (
            db.query(InstagramMedia)
            .filter(InstagramMedia.org_id == org_id, InstagramMedia.ig_media_id == mid)
            .first()
        )
        if row is None:
            row = InstagramMedia(org_id=org_id, ig_media_id=mid)
            db.add(row)

        if item.get("permalink") or not row.permalink:
            row.permalink = item.get("permalink") or row.permalink
        row.media_type = media_type or row.media_type
        row.media_product_type = product_type or row.media_product_type
        row.posted_at = posted or row.posted_at
        if caption_s is not None:
            row.caption = caption_s
        thumb = _cover_image_url(item)
        if thumb:
            row.thumbnail_url = thumb
        elif row.thumbnail_url and (
            ".mp4" in (row.thumbnail_url or "").lower()
            or ".mov" in (row.thumbnail_url or "").lower()
        ):
            # Clear previously stored video URLs that break <img> covers.
            row.thumbnail_url = None
        if insights or existing is None:
            row.views = views
            row.reach = reach
            row.saved = saved
            row.likes = likes
            row.comments = comments
            row.shares = shares
            row.total_interactions = ti
            row.reposts = reposts
            if insights:
                row.avg_watch_time_sec = _as_float(insights.get("ig_reels_avg_watch_time"))
                aw = row.avg_watch_time_sec
                if aw is not None and aw > 3600:
                    row.avg_watch_time_sec = round(aw / 1000.0, 3)
                row.total_watch_time_sec = _as_float(insights.get("ig_reels_video_view_total_time"))
                tw = row.total_watch_time_sec
                if tw is not None and tw > 1_000_000:
                    row.total_watch_time_sec = round(tw / 1000.0, 3)
                row.skip_rate_pct = _as_float(insights.get("reels_skip_rate"))
            row.engagement_rate_pct = derived["engagement_rate_pct"]
            row.save_rate_pct = derived["save_rate_pct"]
            row.share_rate_pct = derived["share_rate_pct"]
            row.reach_vs_followers_pct = derived["reach_vs_followers_pct"]
            row.insights_status = status
            row.insights_error = err
        row.format_bucket = fbucket
        row.hook_text = hook or None
        row.hook_pattern = pattern
        row.theme_keys = theme_keys or None
        row.funnel_stage = stage
        row.caption_len = clen
        if posted:
            row.posted_dow = posted.weekday()
            row.posted_hour = posted.hour
        row.metrics_settled = settled
        row.last_synced_at = now
        row.updated_at = now
        upserted += 1

    # New / listed media from pages
    for item in by_id.values():
        if not _budget_ok(5.0):
            budget_hit = True
            break
        _upsert_from_item(item, fetch_insights=True)

    # Refresh known recent/unsettled posts that were not in the new-posts set
    for row in refresh_rows:
        if not _budget_ok(5.0):
            budget_hit = True
            break
        mid = str(row.ig_media_id)
        if mid in by_id:
            continue
        # Need insights only — reuse cached metadata as the item shell.
        shell = {
            "id": mid,
            "media_type": row.media_type,
            "media_product_type": row.media_product_type,
            "caption": row.caption,
            "permalink": row.permalink,
            "thumbnail_url": row.thumbnail_url,
            "timestamp": row.posted_at.isoformat() if row.posted_at else None,
            "like_count": row.likes,
            "comments_count": row.comments,
        }
        need_insights = (not row.metrics_settled) or row.insights_status not in ("ok",)
        # Once settled with ok insights, still refresh a few recent ones for freshness.
        if row.metrics_settled and row.insights_status == "ok":
            need_insights = refreshed < max(5, refresh_limit // 3)
        if not need_insights:
            continue
        _upsert_from_item(shell, fetch_insights=True, existing=row)
        refreshed += 1

    token.last_sync_at = now
    db.commit()

    linked = 0
    try:
        linked = link_media_to_concepts(db, org_id)
    except Exception:
        logger.exception("instagram concept linking failed org=%s", org_id)

    caps = resolve_capabilities(
        followers_count=followers,
        insights_ok_count=insights_ok,
        insights_attempted=insights_attempted,
    )

    elapsed = round(time.monotonic() - started, 1)
    return {
        "org_id": str(org_id),
        "username": username,
        "ig_user_id": ig_user_id,
        "followers_count": followers,
        "upserted": upserted,
        "new_media": len(by_id),
        "insights_refreshed": refreshed,
        "insights_ok": insights_ok,
        "insights_attempted": insights_attempted,
        "concepts_linked": linked,
        "capabilities": caps,
        "last_synced_at": now.isoformat(),
        "full": full,
        "elapsed_sec": elapsed,
        "budget_sec": budget,
        "budget_exhausted": budget_hit,
    }


def sync_instagram_all_orgs(db: Session) -> Dict[str, Any]:
    """Worker entry: sync orgs whose last_sync_at is older than interval."""
    interval = int(getattr(settings, "INSTAGRAM_SYNC_INTERVAL_SEC", 86400) or 86400)
    cutoff = _utcnow() - timedelta(seconds=max(interval, 300))
    tokens = (
        db.query(OAuthToken)
        .filter(OAuthToken.provider == OAuthProvider.INSTAGRAM)
        .all()
    )
    synced = 0
    failed = 0
    skipped_fresh = 0
    skipped_no_creds = 0
    for tok in tokens:
        if tok.last_sync_at and tok.last_sync_at > cutoff:
            skipped_fresh += 1
            continue
        if not cc.composio_configured(db, tok.org_id):
            skipped_no_creds += 1
            continue
        try:
            sync_instagram_for_org(db, tok.org_id, full=False)
            synced += 1
        except (ComposioNotConnectedError, ComposioConfigError) as e:
            logger.info("instagram sync skip org=%s: %s", tok.org_id, e)
        except Exception:
            failed += 1
            logger.exception("instagram sync failed org=%s", tok.org_id)
            try:
                db.rollback()
            except Exception:
                pass
    return {
        "synced": synced,
        "failed": failed,
        "skipped_fresh": skipped_fresh,
        "skipped_no_creds": skipped_no_creds,
        "candidates": len(tokens),
    }


def sync_instagram_all_orgs_job() -> Dict[str, Any]:
    """RQ/thread entrypoint — opens its own DB session."""
    from app.db.session import SessionLocal

    db = SessionLocal()
    try:
        stats = sync_instagram_all_orgs(db)
        logger.info("instagram sync job %s", stats)
        return stats
    except Exception:
        logger.exception("instagram sync job failed")
        try:
            db.rollback()
        except Exception:
            pass
        raise
    finally:
        db.close()


def purge_instagram_cache(db: Session, org_id: uuid.UUID) -> None:
    db.query(InstagramMedia).filter(InstagramMedia.org_id == org_id).delete()
    db.query(InstagramAccountSnapshot).filter(
        InstagramAccountSnapshot.org_id == org_id
    ).delete()
    db.commit()


def link_media_to_concepts(db: Session, org_id: uuid.UUID) -> int:
    """
    Best-effort match published posts to generated content-studio concepts
    via caption/hook overlap. Skips rows that already have linked_concept_id.
    """
    from app.services import content_studio_service as css

    gen = css.get_latest_generation_row(db, org_id)
    if not gen or not isinstance(gen.ideas_json, dict):
        return 0
    concepts: List[Dict[str, Any]] = []
    for stage in gen.ideas_json.get("stages") or []:
        if not isinstance(stage, dict):
            continue
        for c in stage.get("concepts") or []:
            if isinstance(c, dict) and c.get("id"):
                concepts.append(c)
    if not concepts:
        return 0

    rows = (
        db.query(InstagramMedia)
        .filter(
            InstagramMedia.org_id == org_id,
            InstagramMedia.linked_concept_id.is_(None),
            InstagramMedia.caption.isnot(None),
        )
        .order_by(InstagramMedia.posted_at.desc())
        .limit(80)
        .all()
    )
    linked = 0
    for row in rows:
        cap = (row.caption or "").lower()
        hook_row = (row.hook_text or "").lower()
        best_id = None
        best_score = 0
        for c in concepts:
            title = str(c.get("title") or "").lower()
            hook = str(c.get("hook") or "").lower()
            score = 0
            if hook and len(hook) >= 12 and (hook in cap or hook in hook_row):
                score += 3
            # Token overlap on title words
            tokens = [t for t in re.split(r"\W+", title) if len(t) >= 5][:6]
            hits = sum(1 for t in tokens if t in cap)
            score += hits
            if score > best_score:
                best_score = score
                best_id = str(c["id"])
        if best_id and best_score >= 3:
            row.linked_concept_id = best_id
            linked += 1
    if linked:
        db.commit()
    return linked

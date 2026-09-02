"""Instagram + Marketing Intel helpers for Claude MCP (read-only over Postgres cache)."""
from __future__ import annotations

import uuid
from typing import Any, Dict, List, Optional

from sqlalchemy.orm import Session

from app.services.instagram_performance import (
    build_instagram_performance,
    bottom_posts_for_mcp,
    top_posts_for_mcp,
)


def _post_for_reference(p: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize a post card so Claude can cite Instagram links + numerical performance."""
    permalink = p.get("permalink")
    return {
        "ig_media_id": p.get("ig_media_id"),
        "instagram_url": permalink,
        "permalink": permalink,
        "thumbnail_url": p.get("thumbnail_url"),
        "hook_text": p.get("hook_text"),
        "caption_excerpt": (p.get("caption") or "")[:280] or None,
        "format_bucket": p.get("format_bucket"),
        "funnel_stage": p.get("funnel_stage"),
        "theme_keys": p.get("theme_keys") or [],
        "posted_at": p.get("posted_at"),
        "metrics": {
            "reach": p.get("reach"),
            "views": p.get("views"),
            "saved": p.get("saved"),
            "likes": p.get("likes"),
            "comments": p.get("comments"),
            "shares": p.get("shares"),
            "total_interactions": p.get("total_interactions"),
            "engagement_rate_pct": p.get("engagement_rate_pct"),
            "save_rate_pct": p.get("save_rate_pct"),
            "avg_watch_time_sec": p.get("avg_watch_time_sec"),
        },
        "insights_status": p.get("insights_status"),
        "metrics_settled": p.get("metrics_settled"),
    }


def get_instagram_performance_for_mcp(
    db: Session,
    org_id: uuid.UUID,
    *,
    days: int = 90,
) -> Dict[str, Any]:
    """Period KPIs, weekly trend patterns, and top/underperformer posts with links."""
    payload = build_instagram_performance(db, org_id, days=days)
    summary = payload.get("summary") or {}
    top = [_post_for_reference(p) for p in (payload.get("top_posts") or [])[:5]]
    bottom = [_post_for_reference(p) for p in (payload.get("bottom_posts") or [])[:5]]
    return {
        "connected": payload.get("connected"),
        "org_id": str(org_id),
        "days": payload.get("days") or days,
        "range_start": payload.get("range_start"),
        "range_end": payload.get("range_end"),
        "last_synced_at": payload.get("last_synced_at"),
        "username": payload.get("username"),
        "capabilities": payload.get("capabilities"),
        "unsettled_post_count": payload.get("unsettled_post_count"),
        "period_comparison": {
            "label": summary.get("comparison_label"),
            "posts": summary.get("posts"),
            "prev_posts": summary.get("prev_period_posts"),
            "reach": summary.get("reach"),
            "prev_reach": summary.get("prev_reach"),
            "reach_delta_pct": summary.get("reach_delta_pct"),
            "views": summary.get("views"),
            "prev_views": summary.get("prev_views"),
            "views_delta_pct": summary.get("views_delta_pct"),
            "saved": summary.get("saved"),
            "prev_saved": summary.get("prev_saved"),
            "saved_delta_pct": summary.get("saved_delta_pct"),
            "engagement_rate_pct": summary.get("engagement_rate_pct"),
            "prev_engagement_rate_pct": summary.get("prev_engagement_rate_pct"),
            "engagement_rate_delta_pct": summary.get("engagement_rate_delta_pct"),
            "followers_count": summary.get("followers_count"),
            "follower_growth": summary.get("follower_growth"),
            "prev_range_start": summary.get("prev_range_start"),
            "prev_range_end": summary.get("prev_range_end"),
        },
        "weekly_trend": payload.get("trend") or [],
        "trend_flags": payload.get("flags") or [],
        "top_posts": top,
        "underperformers": bottom,
        "usage": (
            "Marketing Intel Instagram read model. Use period_comparison for how metrics moved "
            "vs the prior equal window. Use weekly_trend for pattern trends over time. "
            "Identify and cite top_posts / underperformers by instagram_url (permalink) — the "
            "stable identifier — and their metrics (reach, views, saved, engagement_rate_pct). "
            "caption_excerpt is display-only; never use it to match, dedupe, or refer to a post "
            "(captions get edited/truncated/duplicated). last_synced_at shows how current this "
            "data is (syncs run roughly every 24h) — surface it if the user asks. Prefer "
            "observed numbers and links over invented advice. If connected=false, ask the user "
            "to connect Instagram in SweepOS → Integrations."
        ),
    }


def get_instagram_top_posts_for_mcp(
    db: Session,
    org_id: uuid.UUID,
    *,
    days: int = 90,
    format_bucket: Optional[str] = None,
    theme_key: Optional[str] = None,
    limit: int = 10,
) -> Dict[str, Any]:
    raw = top_posts_for_mcp(
        db,
        org_id,
        days=days,
        format_bucket=format_bucket,
        theme_key=theme_key,
        limit=limit,
    )
    posts = [_post_for_reference(p) for p in (raw.get("posts") or [])]
    return {
        "org_id": str(org_id),
        "days": days,
        "count": len(posts),
        "posts": posts,
        "capabilities": raw.get("capabilities"),
        "last_synced_at": raw.get("last_synced_at"),
        "usage": (
            "Top Instagram posts by engagement rate. Identify each post by instagram_url "
            "(permalink) — it is the stable identifier; caption_excerpt is display-only and "
            "can be edited/truncated, do not use it to match or dedupe posts. Each post "
            "includes numerical metrics for citation. last_synced_at shows when this org's "
            "Instagram data was last refreshed (syncs run roughly every 24h) — surface it if "
            "the user asks how current the numbers are."
        ),
    }


def get_instagram_underperforming_posts_for_mcp(
    db: Session,
    org_id: uuid.UUID,
    *,
    days: int = 90,
    format_bucket: Optional[str] = None,
    limit: int = 5,
) -> Dict[str, Any]:
    raw = bottom_posts_for_mcp(
        db,
        org_id,
        days=days,
        format_bucket=format_bucket,
        limit=limit,
    )
    posts = [_post_for_reference(p) for p in (raw.get("posts") or [])]
    return {
        "org_id": str(org_id),
        "days": days,
        "count": len(posts),
        "posts": posts,
        "capabilities": raw.get("capabilities"),
        "last_synced_at": raw.get("last_synced_at"),
        "usage": (
            "Lowest-engagement Instagram posts in the window. Identify each post by "
            "instagram_url (permalink), not caption_excerpt (display-only, can be edited/"
            "truncated). Cite instagram_url + metrics when contrasting against top performers; "
            "do not invent reasons not present in the data. last_synced_at shows when this "
            "org's Instagram data was last refreshed (syncs run roughly every 24h)."
        ),
    }


def get_marketing_ideas_for_mcp(
    db: Session,
    org_id: uuid.UUID,
) -> Dict[str, Any]:
    """Latest drafted TOF/MOF/BOF Marketing Intel ideas (content studio bundle)."""
    from app.services import content_studio_service as css
    from app.services.content_studio_bundle import BUNDLE_VERSION

    gen_row = css.get_latest_generation_row(db, org_id)
    if not gen_row or not isinstance(gen_row.ideas_json, dict):
        return {
            "org_id": str(org_id),
            "has_ideas": False,
            "bundle": None,
            "hint": (
                "No Marketing Intel idea bundle yet. Open Marketing Intel Overview in SweepOS "
                "or wait for concepts to generate from Fathom + Instagram performance."
            ),
        }
    ideas = gen_row.ideas_json
    version = int(ideas.get("version") or 0)
    stages_out: List[Dict[str, Any]] = []
    for stage in ideas.get("stages") or []:
        if not isinstance(stage, dict):
            continue
        concepts = []
        for c in stage.get("concepts") or []:
            if not isinstance(c, dict):
                continue
            concepts.append(
                {
                    "id": c.get("id"),
                    "format": c.get("format"),
                    "title": c.get("title"),
                    "hook": c.get("hook"),
                    "bullets": c.get("bullets") or [],
                    "why_for_icp": c.get("why_for_icp"),
                    "funnel_path_to_sale": c.get("funnel_path_to_sale"),
                }
            )
        stages_out.append(
            {
                "id": stage.get("id"),
                "title": stage.get("title"),
                "intro": stage.get("intro"),
                "concepts": concepts,
            }
        )
    return {
        "org_id": str(org_id),
        "has_ideas": True,
        "version": version,
        "bundle_current": version >= BUNDLE_VERSION,
        "batch_id": str(gen_row.batch_id) if gen_row.batch_id else None,
        "generated_at": ideas.get("generated_at"),
        "source": ideas.get("source"),
        "stages": stages_out,
        "usage": (
            "Drafted Marketing Intel ideas (TOF / MOF / BOF). Refine or extend these concepts; "
            "cross-check against get_instagram_performance top/underperformers and "
            "get_org_sales_signals before inventing new angles."
        ),
    }

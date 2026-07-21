"""
Org-level Marketing Intel packages for MCP (Claude custom connector).

Read-only wrappers around Content Studio / call-insight / ICP data so Claude can
ideate content from real objections, struggles, stories, and wins without
triggering expensive reanalyze jobs.
"""
from __future__ import annotations

import threading
import time
import uuid
from typing import Any, Dict, List, Optional

from sqlalchemy import desc
from sqlalchemy.orm import Session

_marketing_cache: dict[str, tuple[float, Dict[str, Any]]] = {}
_marketing_cache_lock = threading.Lock()
_MARKETING_CACHE_TTL_SEC = 60.0

from app.models.client import Client
from app.models.client_call_insight import ClientCallInsight
from app.models.user import User
from app.services import content_studio_service as css
from app.services.content_sop import content_ideation_sop_block, marketing_intel_knowledge_block
from app.services.content_studio_bundle import BUNDLE_VERSION
from app.services.content_studio_fathom_context import (
    build_sales_playbook_for_studio,
    collect_fathom_sales_signals,
)
from app.services.offer_ladder import (
    extract_offer_ladder,
    offer_ladder_for_llm,
    resolve_org_offer_ladder,
)
from app.services.org_intelligence_profile import get_org_ai_profile
from app.services.org_sales_theme_service import (
    ensure_org_sales_content_themes_table,
    list_validated_themes_payload,
)


def _trim_profile_for_marketing(profile: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Keep ICP / content-relevant fields; drop noisy or unrelated keys."""
    if not isinstance(profile, dict):
        return {}
    keep_keys = (
        "ideal_client",
        "icp",
        "target_audience",
        "positioning",
        "brand_voice",
        "voice",
        "offer",
        "offers",
        "offer_ladder",
        "pipeline_priorities",
        "content_studio",
        "value_props",
        "differentiators",
        "niches",
        "geo",
        "price_range",
        "notes",
    )
    out: Dict[str, Any] = {}
    for k in keep_keys:
        if k in profile and profile[k] is not None:
            if k == "offer_ladder":
                ladder = offer_ladder_for_llm(extract_offer_ladder(profile))
                if ladder:
                    out[k] = ladder
            else:
                out[k] = profile[k]
    # Also pass through common free-text fields if present
    for k, v in profile.items():
        if k in out:
            continue
        if isinstance(v, str) and v.strip() and len(k) < 40:
            out[k] = v[:2000]
    return out


_BUSINESS_CONTEXT_KEYS = (
    "business_description",
    "target_audience",
    "unique_selling_proposition",
    "coaching_style",
    "client_management_philosophy",
    "marketing_strategy",
    "marketing_channels",
)

_SALES_APPROACH_KEYS = (
    "sales_framework",
    "sales_tactics",
)


def _full_intelligence_for_mcp(profile: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Full sanitized Intelligence bank export for Claude: business context, sales
    approach, priorities, and asset links. Writing samples are reduced to
    metadata to stay within MCP payload limits.
    """
    if not isinstance(profile, dict):
        return {}
    out: Dict[str, Any] = {}

    business: Dict[str, Any] = {}
    for k in _BUSINESS_CONTEXT_KEYS:
        v = profile.get(k)
        if isinstance(v, str) and v.strip():
            business[k] = v.strip()[:2000]
    if business:
        out["business_context"] = business

    sales: Dict[str, Any] = {}
    for k in _SALES_APPROACH_KEYS:
        v = profile.get(k)
        if isinstance(v, str) and v.strip():
            sales[k] = v.strip()[:2000]
    if sales:
        out["sales_approach"] = sales

    priorities = profile.get("pipeline_priorities")
    if isinstance(priorities, list):
        cleaned = [str(x).strip() for x in priorities if isinstance(x, str) and str(x).strip()]
        if cleaned:
            out["pipeline_priorities"] = cleaned[:10]

    assets = profile.get("asset_links")
    if isinstance(assets, list):
        links = [
            {"label": str(a.get("label", "")), "url": str(a.get("url", ""))}
            for a in assets
            if isinstance(a, dict) and a.get("url")
        ][:20]
        if links:
            out["asset_links"] = links

    voice: Dict[str, Any] = {}
    for k in ("writing_style", "writing_tone", "brand_voice", "voice"):
        v = profile.get(k)
        if isinstance(v, str) and v.strip():
            voice[k] = v.strip()[:1000]
    if voice:
        out["brand_voice"] = voice

    samples = profile.get("writing_samples")
    if isinstance(samples, list) and samples:
        out["writing_samples_available"] = [
            {"kind": str(s.get("kind") or "other"), "title": str(s.get("title") or "")[:120]}
            for s in samples
            if isinstance(s, dict)
        ][:12]

    return out


def get_org_intelligence_for_mcp(
    db: Session,
    org_id: uuid.UUID,
    *,
    user_id: Optional[uuid.UUID] = None,
) -> Dict[str, Any]:
    """Full Intelligence bank + offer ladder: ICP, business context, sales approach."""
    profile: Optional[Dict[str, Any]] = None
    if user_id:
        user = db.query(User).filter(User.id == user_id).first()
        if user is not None:
            # Overlay selected org so multi-org users get the right bank
            try:
                user.selected_org_id = org_id  # type: ignore[attr-defined]
            except Exception:
                pass
            profile = get_org_ai_profile(db, user, selected_org_id=org_id)

    if profile is None:
        # Fall back: first user in org with a non-empty ai_profile
        row = (
            db.query(User)
            .filter(User.org_id == org_id, User.ai_profile.isnot(None))
            .order_by(User.created_at.asc())
            .first()
        )
        if row and isinstance(row.ai_profile, dict):
            profile = row.ai_profile

    ladder = resolve_org_offer_ladder(db, org_id)
    out = {
        "org_id": str(org_id),
        "ai_profile": _trim_profile_for_marketing(profile),
        "intelligence_profile": _full_intelligence_for_mcp(profile),
        "offer_ladder": offer_ladder_for_llm(ladder),
        "usage": (
            "intelligence_profile carries the org's business context (description, USP, "
            "target audience, coaching style, marketing strategy), sales_approach "
            "(framework + tactics), pipeline_priorities, and brand_voice. offer_ladder is "
            "the configured offer/pricing ladder. Ground offer positioning and business "
            "advice in these fields; ai_profile is a marketing-trimmed subset kept for "
            "backward compatibility."
        ),
    }
    if not out["intelligence_profile"] and not out["offer_ladder"]:
        out["hint"] = (
            "No Intelligence profile configured for this org yet. Fill in the Intelligence "
            "tab in SweepOS (business description, offers, ICP) to unlock business context."
        )
    return out


def get_org_sales_signals_for_mcp(db: Session, org_id: uuid.UUID) -> Dict[str, Any]:
    """
    Sales signals used by Marketing Intel / Content Studio:
    recurring themes, recent call insights (objections, wins, stories, struggles),
    active-client friction, meeting summaries.
    """
    signals = collect_fathom_sales_signals(db, org_id)
    # Rename priorities → struggles for Claude clarity (same underlying field)
    insights = []
    for ins in signals.get("insights") or []:
        if not isinstance(ins, dict):
            continue
        row = dict(ins)
        row["struggles"] = list(row.get("priorities") or [])
        insights.append(row)
    active = []
    for ins in signals.get("active_client_insights") or []:
        if not isinstance(ins, dict):
            continue
        row = dict(ins)
        row["struggles"] = list(row.get("priorities") or [])
        active.append(row)
    return {
        "org_id": str(org_id),
        "themes": signals.get("themes") or [],
        "insights": insights,
        "active_client_insights": active,
        "meeting_summaries": signals.get("meeting_summaries") or [],
        "has_any": bool(signals.get("has_any")),
        "field_guide": {
            "themes": "Validated/recurring objection themes across the org (sample_quotes = prospect language).",
            "insights[].objection_quotes": "Verbatim-style objection language from sales calls.",
            "insights[].struggles": "Client priorities / pain points from call analysis (closest to 'struggles').",
            "insights[].wins": "Documented wins / outcomes from calls.",
            "insights[].testimonial_stories": "Story-shaped proof for content and BOF.",
            "insights[].phrases_that_resonated / avoid_phrasing": "Prospect voice guidance.",
            "active_client_insights": "Delivery/retention friction from ACTIVE clients.",
        },
    }


def list_org_sales_themes_for_mcp(
    db: Session,
    org_id: uuid.UUID,
    *,
    validated_only: bool = False,
    limit: int = 25,
) -> Dict[str, Any]:
    try:
        ensure_org_sales_content_themes_table(db)
    except Exception:
        return {"org_id": str(org_id), "themes": [], "validated_only": validated_only}

    themes = list_validated_themes_payload(db, org_id)
    if not themes and not validated_only:
        # Reuse collect path which already falls back to top recurring themes
        themes = collect_fathom_sales_signals(db, org_id).get("themes") or []
    lim = max(1, min(int(limit or 25), 50))
    return {
        "org_id": str(org_id),
        "validated_only": validated_only,
        "themes": themes[:lim],
        "count": min(len(themes), lim),
    }


def get_marketing_intel_bootstrap_for_mcp(
    db: Session,
    org_id: uuid.UUID,
    *,
    user_id: Optional[uuid.UUID] = None,
    include_sop: bool = True,
) -> Dict[str, Any]:
    """
    Same conceptual surface as GET /content-studio/bootstrap, plus sales signals and ICP,
    optimized for Claude to ideate TOF/MOF/BOF content autonomously.
    Does not kick off reanalyze / LLM regen.
    """
    cache_key = f"{org_id}:sop={1 if include_sop else 0}"
    now = time.monotonic()
    with _marketing_cache_lock:
        hit = _marketing_cache.get(cache_key)
        if hit and now - hit[0] <= _MARKETING_CACHE_TTL_SEC:
            cached = dict(hit[1])
            cached["cache"] = {"hit": True, "ttl_seconds": _MARKETING_CACHE_TTL_SEC}
            return cached

    knowledge = css.load_knowledge_grouped(db, org_id)
    sp_source, sp_paragraphs = build_sales_playbook_for_studio(db, org_id, use_llm_synthesis=False)
    signals = get_org_sales_signals_for_mcp(db, org_id)
    intel = get_org_intelligence_for_mcp(db, org_id, user_id=user_id)

    content_bundle: Optional[Dict[str, Any]] = None
    batch_id: Optional[str] = None
    gen_row = css.get_latest_generation_row(db, org_id)
    if gen_row and isinstance(gen_row.ideas_json, dict):
        ideas = gen_row.ideas_json
        if int(ideas.get("version") or 0) >= BUNDLE_VERSION:
            content_bundle = ideas
            batch_id = str(gen_row.batch_id) if gen_row.batch_id else None

    out: Dict[str, Any] = {
        "org_id": str(org_id),
        "knowledge": knowledge,
        "sales_playbook": {"source": sp_source, "paragraphs": sp_paragraphs},
        "sales_signals": signals,
        "intelligence": intel,
        "content_bundle": content_bundle,
        "batch_id": batch_id,
        "usage": (
            "Use sales_signals (objections, struggles, wins, stories, themes) + knowledge "
            "(operator objections/closings/reframes) + intelligence (ICP/offer ladder) to "
            "ideate short-form TOF→MOF→BOF content. Prefer prospect language from sample_quotes "
            "and objection_quotes. If content_bundle is present, treat it as the last drafted "
            "Marketing Intel concepts and refine or extend it rather than ignoring it."
        ),
        "cache": {"hit": False, "ttl_seconds": _MARKETING_CACHE_TTL_SEC},
    }
    if include_sop:
        # Cap SOP size for MCP payload limits
        sop = marketing_intel_knowledge_block(db, org_id)
        if len(sop) > 24_000:
            sop = content_ideation_sop_block()[:12_000] + "\n\n…[sop truncated]"
        out["content_ideation_guidance"] = sop[:24_000]

    with _marketing_cache_lock:
        _marketing_cache[cache_key] = (time.monotonic(), out)
        if len(_marketing_cache) > 64:
            oldest = sorted(_marketing_cache.items(), key=lambda kv: kv[1][0])[:16]
            for k, _ in oldest:
                _marketing_cache.pop(k, None)
    return out


def search_sales_clips_for_mcp(
    db: Session,
    org_id: uuid.UUID,
    *,
    kind: Optional[str] = None,
    query: Optional[str] = None,
    limit: int = 40,
) -> Dict[str, Any]:
    """
    Search recent call-insight clips across the org.
    kind: objection | win | testimonial | other (optional)
    """
    kind_norm = (kind or "").strip().lower() or None
    q_norm = (query or "").strip().lower() or None
    lim = max(1, min(int(limit or 40), 100))

    rows = (
        db.query(ClientCallInsight)
        .filter(ClientCallInsight.org_id == org_id, ClientCallInsight.status == "complete")
        .order_by(desc(ClientCallInsight.computed_at))
        .limit(80)
        .all()
    )

    hits: List[Dict[str, Any]] = []
    for r in rows:
        ij = r.insight_json if isinstance(r.insight_json, dict) else {}
        if ij.get("low_signal"):
            continue
        clips = ij.get("clips") or []
        if not isinstance(clips, list):
            continue
        client_name = None
        try:
            c = db.query(Client).filter(Client.id == r.client_id).first()
            client_name = c.name if c else None
        except Exception:
            client_name = None

        for clip in clips:
            if not isinstance(clip, dict):
                continue
            ckind = str(clip.get("kind") or "").lower().strip()
            if kind_norm and ckind != kind_norm:
                continue
            quote = str(clip.get("quote") or "").strip()
            label = str(clip.get("label") or "").strip()
            rationale = str(clip.get("rationale") or "").strip()
            if q_norm:
                blob = f"{quote} {label} {rationale}".lower()
                if q_norm not in blob:
                    continue
            hits.append(
                {
                    "client_id": str(r.client_id),
                    "client_name": client_name,
                    "insight_id": str(r.id),
                    "kind": ckind or "other",
                    "label": label[:200],
                    "quote": quote[:500],
                    "rationale": rationale[:400],
                    "computed_at": r.computed_at.isoformat() if r.computed_at else None,
                }
            )
            if len(hits) >= lim:
                break
        if len(hits) >= lim:
            break

    # Also surface wins / stories as synthetic clips when kind requests them
    if kind_norm in (None, "win", "testimonial") and len(hits) < lim:
        for r in rows:
            ij = r.insight_json if isinstance(r.insight_json, dict) else {}
            if kind_norm in (None, "win"):
                for w in ij.get("wins") or []:
                    text = str(w or "").strip()
                    if not text:
                        continue
                    if q_norm and q_norm not in text.lower():
                        continue
                    hits.append(
                        {
                            "client_id": str(r.client_id),
                            "kind": "win",
                            "label": "win",
                            "quote": text[:500],
                            "source": "wins[]",
                        }
                    )
                    if len(hits) >= lim:
                        break
            if kind_norm in (None, "testimonial") and len(hits) < lim:
                for s in ij.get("testimonial_stories") or []:
                    text = str(s or "").strip()
                    if not text:
                        continue
                    if q_norm and q_norm not in text.lower():
                        continue
                    hits.append(
                        {
                            "client_id": str(r.client_id),
                            "kind": "testimonial",
                            "label": "testimonial_story",
                            "quote": text[:500],
                            "source": "testimonial_stories[]",
                        }
                    )
                    if len(hits) >= lim:
                        break
            if len(hits) >= lim:
                break

    return {
        "org_id": str(org_id),
        "kind": kind_norm,
        "query": query,
        "clips": hits[:lim],
        "count": min(len(hits), lim),
    }


def get_client_call_insights_for_mcp(
    db: Session,
    org_id: uuid.UUID,
    client_id: uuid.UUID,
    *,
    limit: int = 10,
) -> Dict[str, Any]:
    """Lighter call-insight package than full client profile — for content mining one client."""
    from app.services.call_insight_service import get_client_insights_response

    raw = get_client_insights_response(db, org_id, client_id, limit=max(1, min(int(limit or 10), 25)))
    if not raw:
        return {"error": "client not found", "client_id": str(client_id)}

    # Trim bulky insight_json fields for MCP
    slim_insights = []
    for item in raw.get("insights") or []:
        if not isinstance(item, dict):
            continue
        ij = item.get("insight") if isinstance(item.get("insight"), dict) else {}
        clips = []
        for c in (ij.get("clips") or [])[:12]:
            if isinstance(c, dict):
                clips.append(
                    {
                        "kind": c.get("kind"),
                        "label": str(c.get("label") or "")[:160],
                        "quote": str(c.get("quote") or "")[:420],
                    }
                )
        slim_insights.append(
            {
                "id": item.get("id"),
                "meeting_at": item.get("meeting_at"),
                "status": item.get("status"),
                "client_state_synthesis": str(ij.get("client_state_synthesis") or "")[:900],
                "struggles": [str(x)[:320] for x in (ij.get("priorities") or [])[:6] if x],
                "wins": [str(x)[:320] for x in (ij.get("wins") or [])[:6] if x],
                "testimonial_stories": [str(x)[:400] for x in (ij.get("testimonial_stories") or [])[:4] if x],
                "opportunity_tags": ij.get("opportunity_tags") or [],
                "clips": clips,
                "prospect_voice": ij.get("prospect_voice") if isinstance(ij.get("prospect_voice"), dict) else {},
            }
        )

    rollup = raw.get("rollup") if isinstance(raw.get("rollup"), dict) else {}
    return {
        "client_id": str(client_id),
        "org_id": str(org_id),
        "summary": raw.get("summary"),
        "roi_state": raw.get("roi_state"),
        "rollup": {
            "accumulated_wins": (rollup.get("accumulated_wins") or [])[:12],
            "accumulated_testimonial_stories": (rollup.get("accumulated_testimonial_stories") or [])[:10],
            "org_validated_theme_keys": rollup.get("org_validated_theme_keys") or [],
        },
        "insights": slim_insights,
        "count": len(slim_insights),
    }

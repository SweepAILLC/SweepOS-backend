"""Shared extraction of user ai_profile fields for LLM prompts (Intelligence tab)."""
from __future__ import annotations

import uuid
from typing import Any, Dict, List, Optional, Set

from sqlalchemy.orm import Session

from app.services.offer_ladder import extract_offer_ladder, offer_ladder_for_llm


# Sales-lens fields the call-insight analyzer uses to ground next_steps,
# objections, and the framework critique. Mirrors what's editable in the
# Intelligence tab's Sales + Business sections plus relational coaching context.
_SALES_LENS_KEYS = (
    "sales_framework",
    "sales_tactics",
    "target_audience",
    "unique_selling_proposition",
    "business_description",
    "coaching_style",
    "client_management_philosophy",
)

_WRITING_SAMPLE_KINDS = frozenset(
    {
        "email",
        "message",
        "other",
        # Campaign-style samples (carry HTML template + auto-resolve in playbooks).
        "onboarding_email",
        "referral_campaign",
        "upsell_campaign",
        "re_sign_campaign",
    }
)
_MAX_WRITING_SAMPLES = 12
_MAX_WRITING_SAMPLE_BODY = 3500
_MAX_WRITING_SAMPLE_HTML = 12000
_MAX_WRITING_SAMPLE_TITLE = 120


def normalize_writing_samples_for_llm(raw: Any) -> Optional[List[Dict[str, str]]]:
    """
    Sanitize user-provided writing examples for prompt injection.

    Each item: kind (email | message | other | onboarding_email | referral_campaign |
    upsell_campaign | re_sign_campaign), optional title, body and/or html_template
    (at least one required).
    """
    if not isinstance(raw, list):
        return None
    out: List[Dict[str, str]] = []
    for item in raw:
        if len(out) >= _MAX_WRITING_SAMPLES:
            break
        if not isinstance(item, dict):
            continue
        kind = str(item.get("kind") or "other").strip().lower()
        if kind not in _WRITING_SAMPLE_KINDS:
            kind = "other"
        body = str(item.get("body") or "").strip()
        html_t = str(item.get("html_template") or "").strip()
        if not body and not html_t:
            continue
        title = str(item.get("title") or "").strip()[:_MAX_WRITING_SAMPLE_TITLE]
        rec: Dict[str, str] = {"kind": kind}
        if body:
            rec["body"] = body[:_MAX_WRITING_SAMPLE_BODY]
        if html_t:
            rec["html_template"] = html_t[:_MAX_WRITING_SAMPLE_HTML]
        if title:
            rec["title"] = title
        out.append(rec)
    return out or None


def resolve_performance_campaign_templates_for_task(
    ai_profile: Any,
    roi_tags: Any,
    *,
    lifecycle: Optional[str] = None,
) -> List[Dict[str, str]]:
    """
    Pick Intelligence writing samples that match Performance ROI tags / lifecycle for campaign HTML tools.

    referral tag → referral_campaign sample; upsell → upsell_campaign;
    offboarding lifecycle (re-commit / re-sign) → re_sign_campaign.
    At most one sample per campaign kind (first in saved order wins).
    """
    if not isinstance(ai_profile, dict):
        return []
    samples = normalize_writing_samples_for_llm(ai_profile.get("writing_samples"))
    if not samples:
        return []
    tags: Set[str] = set()
    if isinstance(roi_tags, list):
        tags = {str(t).lower().strip() for t in roi_tags if str(t).strip()}
    ls = (lifecycle or "").lower().strip()
    out: List[Dict[str, str]] = []
    seen: Set[str] = set()

    def take(kind: str) -> None:
        if kind in seen:
            return
        for s in samples:
            if s.get("kind") == kind:
                out.append(dict(s))
                seen.add(kind)
                break

    if "referral" in tags:
        take("referral_campaign")
    if "upsell" in tags:
        take("upsell_campaign")
    # Re-sign / renewal / recommit — ops often save templates under re_sign_campaign
    if ls == "offboarding":
        take("re_sign_campaign")

    return out[:3]


_SALES_LENS_CAPS = {
    "sales_framework": 200,
    "sales_tactics": 1200,
    "target_audience": 600,
    "unique_selling_proposition": 600,
    "business_description": 800,
    "coaching_style": 200,
    "client_management_philosophy": 800,
}


def extract_sales_lens_for_llm(ai_profile: Any) -> Optional[Dict[str, Any]]:
    """
    Pull only the sales-relevant Intelligence fields, capped for prompt safety.

    Returns None when the user hasn't filled any of them so callers can skip
    injection cleanly.
    """
    if not isinstance(ai_profile, dict):
        return None
    out: Dict[str, Any] = {}
    for k in _SALES_LENS_KEYS:
        v = ai_profile.get(k)
        if isinstance(v, str) and v.strip():
            out[k] = v.strip()[: _SALES_LENS_CAPS.get(k, 600)]
    return out or None


def resolve_org_sales_lens(db: Session, org_id: uuid.UUID) -> Optional[Dict[str, Any]]:
    """
    Resolve the org's primary sales lens for jobs without a current user
    (e.g. Fathom call-insight processing).

    Picks the first user with usable sales-lens fields, preferring OWNER -> ADMIN -> MEMBER.
    """
    from app.models.user import User, UserRole

    role_order = {UserRole.OWNER: 0, UserRole.ADMIN: 1, UserRole.MEMBER: 2}
    users = (
        db.query(User)
        .filter(User.org_id == org_id, User.ai_profile.isnot(None))
        .all()
    )
    if not users:
        return None
    users.sort(key=lambda u: (role_order.get(u.role, 99), u.created_at or 0))
    for u in users:
        lens = extract_sales_lens_for_llm(u.ai_profile)
        if lens:
            return lens
    return None


def extract_intelligence_profile_for_automation_llm(user: Any) -> Optional[Dict[str, Any]]:
    """Per-org Intelligence export for automation email AI drafts (worker + preview).

    Includes the same sanitized fields as :func:`extract_ai_profile_for_llm`.
    """
    return extract_ai_profile_for_llm(user)


def extract_ai_profile_for_llm(user: Any) -> Optional[Dict[str, Any]]:
    """Return a sanitized dict of Intelligence personalization fields, or None if empty."""
    if not user:
        return None
    raw = getattr(user, "ai_profile", None)
    if not raw or not isinstance(raw, dict):
        return None
    keys = (
        "writing_style",
        "writing_tone",
        "coaching_style",
        "client_management_philosophy",
        "business_description",
        "target_audience",
        "unique_selling_proposition",
        "sales_framework",
        "sales_tactics",
        "marketing_strategy",
        "marketing_channels",
        "pipeline_priorities",
        "asset_links",
    )
    out: Dict[str, Any] = {}
    for k in keys:
        v = raw.get(k)
        if v:
            if k == "asset_links" and isinstance(v, list):
                out[k] = [
                    {"label": str(a.get("label", "")), "url": str(a.get("url", ""))}
                    for a in v
                    if isinstance(a, dict) and a.get("url")
                ][:20]
            elif k == "pipeline_priorities" and isinstance(v, list):
                out[k] = [str(x) for x in v if isinstance(x, str)][:10]
            elif isinstance(v, str) and v.strip():
                out[k] = v.strip()[:1000]
    ladder = offer_ladder_for_llm(extract_offer_ladder(raw))
    if ladder:
        out["offer_ladder"] = ladder
    samples = normalize_writing_samples_for_llm(raw.get("writing_samples"))
    if samples:
        out["writing_samples"] = samples
    return out if out else None

"""Shared extraction of user ai_profile fields for LLM prompts (Intelligence tab)."""
from __future__ import annotations

import uuid
from typing import Any, Dict, Optional

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
    return out if out else None

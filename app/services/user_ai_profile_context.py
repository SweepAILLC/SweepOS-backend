"""Shared extraction of user ai_profile fields for LLM prompts (Intelligence tab)."""
from __future__ import annotations

from typing import Any, Dict, List, Optional


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
    return out if out else None

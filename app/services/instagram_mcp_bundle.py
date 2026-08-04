"""Instagram performance helpers for Claude MCP (read-only over Postgres cache)."""
from __future__ import annotations

import uuid
from typing import Any, Dict, Optional

from sqlalchemy.orm import Session

from app.services.instagram_performance import (
    build_instagram_performance,
    top_posts_for_mcp,
)


def get_instagram_performance_for_mcp(
    db: Session,
    org_id: uuid.UUID,
    *,
    days: int = 90,
) -> Dict[str, Any]:
    payload = build_instagram_performance(db, org_id, days=days)
    payload["usage"] = (
        "Instagram Performance Intel for the connected org. Prefer verdicts and "
        "what_works (verdict=double_down|stop) for tactical advice. top_posts are "
        "ranked by engagement rate. If connected=false, ask the user to connect "
        "Instagram in SweepOS → Integrations."
    )
    return payload


def get_instagram_top_posts_for_mcp(
    db: Session,
    org_id: uuid.UUID,
    *,
    days: int = 90,
    format_bucket: Optional[str] = None,
    theme_key: Optional[str] = None,
    limit: int = 10,
) -> Dict[str, Any]:
    return top_posts_for_mcp(
        db,
        org_id,
        days=days,
        format_bucket=format_bucket,
        theme_key=theme_key,
        limit=limit,
    )

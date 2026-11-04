"""Tally which Instagram posts clients pick most often in the post-sale
onboarding survey's content-gallery step (Q4), to feed back into content
ideation as a "proven format" reference.

See docs/features/ONBOARDING_SURVEY_CONTENT_ATTRIBUTION_PRD.md.
"""
from __future__ import annotations

import uuid
from typing import Any, Dict, List

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.models.instagram_media import InstagramMedia


def get_top_selected_content(db: Session, org_id: uuid.UUID, limit: int = 5) -> List[Dict[str, Any]]:
    """Rank an org's Instagram posts by onboarding-survey selection frequency, most-picked first.

    ``content_selected_ids`` stores ``InstagramMedia.id`` (the internal UUID row id set by
    ``onboarding_survey_service.build_onboarding_survey_meta``), not the raw ``ig_media_id`` —
    so the tally is grouped by that id and joined back to ``instagram_media`` for display fields.
    """
    rows = db.execute(
        text(
            """
            SELECT unnested.instagram_media_id, COUNT(*) AS selection_count
            FROM onboarding_survey_responses,
                 unnest(content_selected_ids) AS unnested(instagram_media_id)
            WHERE org_id = :org_id
              AND content_selected_ids IS NOT NULL
            GROUP BY unnested.instagram_media_id
            ORDER BY selection_count DESC
            LIMIT :limit
            """
        ),
        {"org_id": str(org_id), "limit": limit},
    ).fetchall()

    if not rows:
        return []

    tally = {row.instagram_media_id: row.selection_count for row in rows}
    media_ids = list(tally.keys())

    media_rows = (
        db.query(InstagramMedia)
        .filter(InstagramMedia.id.in_(media_ids), InstagramMedia.org_id == org_id)
        .all()
    )
    media_by_id = {m.id: m for m in media_rows}

    results: List[Dict[str, Any]] = []
    for media_id in media_ids:
        media = media_by_id.get(media_id)
        if not media:
            # Post was deleted/unsynced since being selected — skip rather than error.
            continue
        results.append(
            {
                "instagram_media_id": str(media_id),
                "selection_count": tally[media_id],
                "ig_media_id": media.ig_media_id,
                "permalink": media.permalink,
                "thumbnail_url": media.thumbnail_url,
                "caption": media.caption,
                "transcript": media.transcript,
                "posted_at": media.posted_at.isoformat() if media.posted_at else None,
            }
        )
    return results

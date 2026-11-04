"""Transcribe synced Instagram reels via OpenAI Whisper.

Callers: auto_transcribe_top_performers (after sync, for posts in
build_instagram_performance's rank_score top_posts), auto_transcribe_priority_reference_posts
(after sync, for posts clients picked most in the onboarding survey — see
content_conversion_service.get_top_selected_content), and transcribe_media (on-demand,
one post, from the UI "Transcribe" button). All write onto instagram_media so
content_studio_bundle's angle/ideation block can pull transcript text alongside hook_text.
"""
from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Optional

import httpx
from sqlalchemy.orm import Session

from app.core.config import settings
from app.models.instagram_media import InstagramMedia
from app.services.llm_usage import record_llm_usage

logger = logging.getLogger(__name__)

WHISPER_MODEL = "whisper-1"
WHISPER_PRICE_PER_MINUTE_USD = 0.006
# OpenAI's hard per-file limit for whisper-1 (no audio-extraction step here, so the
# whole mp4 counts against it).
OPENAI_AUDIO_MAX_BYTES = 25 * 1024 * 1024
DOWNLOAD_TIMEOUT = httpx.Timeout(120.0, connect=15.0)
TRANSCRIBE_TIMEOUT = httpx.Timeout(180.0, connect=15.0)


class TranscriptUnavailableError(Exception):
    """Reel can't be transcribed (no media_url, too large, no API key, etc.)."""


def _download_media(url: str) -> bytes:
    with httpx.stream("GET", url, timeout=DOWNLOAD_TIMEOUT, follow_redirects=True) as resp:
        resp.raise_for_status()
        content_length = resp.headers.get("content-length")
        if content_length and int(content_length) > OPENAI_AUDIO_MAX_BYTES:
            raise TranscriptUnavailableError(
                f"Reel file is {int(content_length) / 1_000_000:.1f}MB — over Whisper's 25MB limit"
            )
        chunks = []
        total = 0
        for chunk in resp.iter_bytes():
            total += len(chunk)
            if total > OPENAI_AUDIO_MAX_BYTES:
                raise TranscriptUnavailableError("Reel file exceeds Whisper's 25MB limit")
            chunks.append(chunk)
        return b"".join(chunks)


def _whisper_transcribe(audio_bytes: bytes, filename: str, org_id: uuid.UUID) -> str:
    api_key = settings.OPENAI_API_KEY
    if not api_key:
        raise TranscriptUnavailableError("OPENAI_API_KEY not configured")

    resp = httpx.post(
        "https://api.openai.com/v1/audio/transcriptions",
        headers={"Authorization": f"Bearer {api_key}"},
        files={"file": (filename, audio_bytes, "video/mp4")},
        data={"model": WHISPER_MODEL, "response_format": "verbose_json"},
        timeout=TRANSCRIBE_TIMEOUT,
    )
    resp.raise_for_status()
    payload = resp.json()
    text = (payload.get("text") or "").strip()
    duration_sec = float(payload.get("duration") or 0.0)

    cost = round((duration_sec / 60.0) * WHISPER_PRICE_PER_MINUTE_USD, 6) if duration_sec else None
    record_llm_usage(
        org_id=org_id,
        provider="openai",
        model=WHISPER_MODEL,
        feature="instagram_reel_transcript",
        cost_usd_override=cost,
    )
    return text


def transcribe_media(db: Session, org_id: uuid.UUID, media_id: uuid.UUID) -> InstagramMedia:
    """Transcribe one org-scoped reel. Idempotent — returns cached transcript if already ok."""
    media = (
        db.query(InstagramMedia)
        .filter(InstagramMedia.id == media_id, InstagramMedia.org_id == org_id)
        .first()
    )
    if media is None:
        raise ValueError("Instagram media not found")
    if media.transcript_status == "ok" and media.transcript:
        return media

    if not media.media_url:
        media.transcript_status = "unavailable"
        media.transcript_error = "No media_url on this post"
        media.transcribed_at = datetime.now(timezone.utc)
        db.commit()
        db.refresh(media)
        return media

    try:
        audio_bytes = _download_media(media.media_url)
        text = _whisper_transcribe(audio_bytes, f"{media.ig_media_id}.mp4", org_id)
        media.transcript = text[:50000]
        media.transcript_status = "ok"
        media.transcript_error = None
    except TranscriptUnavailableError as exc:
        media.transcript_status = "unavailable"
        media.transcript_error = str(exc)[:500]
    except Exception as exc:
        logger.exception("instagram reel transcription failed media=%s", media_id)
        media.transcript_status = "failed"
        media.transcript_error = str(exc)[:500]
    media.transcribed_at = datetime.now(timezone.utc)
    db.commit()
    db.refresh(media)
    return media


def auto_transcribe_top_performers(db: Session, org_id: uuid.UUID, *, days: int = 90) -> int:
    """Transcribe reels currently ranked in build_instagram_performance's top_posts.

    Skips anything already transcribed ok or already marked unavailable/failed — a
    sync-triggered retry storm on a post Whisper can't handle gains nothing.
    Returns count of posts newly transcribed (any status).
    """
    from app.services.instagram_performance import build_instagram_performance

    perf = build_instagram_performance(db, org_id, days=days)
    limit = max(1, int(getattr(settings, "INSTAGRAM_TOP_POSTS_TRANSCRIBE_LIMIT", 5) or 5))
    top_ids = [
        p.get("ig_media_id") for p in (perf.get("top_posts") or [])[:limit] if p.get("ig_media_id")
    ]
    if not top_ids:
        return 0

    rows = (
        db.query(InstagramMedia)
        .filter(
            InstagramMedia.org_id == org_id,
            InstagramMedia.ig_media_id.in_(top_ids),
            InstagramMedia.transcript_status == "unavailable",
            InstagramMedia.transcript.is_(None),
        )
        .all()
    )
    count = 0
    for row in rows:
        try:
            transcribe_media(db, org_id, row.id)
            count += 1
        except Exception:
            logger.exception("auto transcribe failed media=%s", row.id)
    return count


def auto_transcribe_priority_reference_posts(db: Session, org_id: uuid.UUID, *, limit: int = 5) -> int:
    """Transcribe reels currently ranked by onboarding-survey content-gallery selections.

    Same shape as auto_transcribe_top_performers, but sourced from client picks
    (content_conversion_service.get_top_selected_content) rather than engagement rank —
    these are the posts content_studio_bundle's PRIORITY_REFERENCE_POSTS block cites as
    proven-to-convert formats, so they need transcripts even if they aren't also top
    engagement performers. Skips anything already transcribed ok or already marked
    unavailable/failed. Returns count of posts newly transcribed (any status).
    """
    from app.services.content_conversion_service import get_top_selected_content

    top = get_top_selected_content(db, org_id, limit=limit)
    media_ids = [uuid.UUID(entry["instagram_media_id"]) for entry in top]
    if not media_ids:
        return 0

    rows = (
        db.query(InstagramMedia)
        .filter(
            InstagramMedia.org_id == org_id,
            InstagramMedia.id.in_(media_ids),
            InstagramMedia.transcript_status == "unavailable",
            InstagramMedia.transcript.is_(None),
        )
        .all()
    )
    count = 0
    for row in rows:
        try:
            transcribe_media(db, org_id, row.id)
            count += 1
        except Exception:
            logger.exception("auto transcribe (priority reference) failed media=%s", row.id)
    return count

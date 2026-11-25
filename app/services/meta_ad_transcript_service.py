"""Transcripts for Meta ads, the paid half of the Messaging Matrix input.

Order of preference per ad (cheapest first):
1. The ad is a boosted Instagram post that is already synced: reuse (or create) that
   post's transcript via instagram_transcript_service.transcribe_media.
2. The ad has a video: fetch Meta's short-lived `source` URL, download, Whisper.
3. Image ad, or the video can't be read: use the ad copy (headline + primary text).

Only ads with at least AD_SPEND_FLOOR_CENTS of synced spend are transcribed, and at most
MAX_NEW_TRANSCRIPTS_PER_RUN per call, so Whisper cost stays bounded.
"""
from __future__ import annotations

import logging
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy import func
from sqlalchemy.orm import Session

from app.models.instagram_media import InstagramMedia
from app.models.meta_ad import MetaAd
from app.models.meta_ad_insight_daily import MetaAdInsightDaily
from app.services import instagram_transcript_service as its
from app.services import meta_ads_client as mac

logger = logging.getLogger(__name__)

AD_SPEND_FLOOR_CENTS = 5000  # $50 lifetime synced spend
MAX_NEW_TRANSCRIPTS_PER_RUN = 40
WHISPER_FEATURE = "meta_ad_transcript"

SOURCE_INSTAGRAM = "instagram"
SOURCE_WHISPER = "whisper"
SOURCE_AD_COPY = "ad_copy"


def ad_spend_cents_by_meta_id(db: Session, org_id: uuid.UUID) -> Dict[str, int]:
    """Total synced spend per ad (meta_ad_id -> cents), one grouped query."""
    rows = (
        db.query(MetaAdInsightDaily.entity_id, func.coalesce(func.sum(MetaAdInsightDaily.spend_cents), 0))
        .filter(MetaAdInsightDaily.org_id == org_id, MetaAdInsightDaily.level == "ad")
        .group_by(MetaAdInsightDaily.entity_id)
        .all()
    )
    return {str(entity_id): int(total or 0) for entity_id, total in rows}


def _find_instagram_media(db: Session, org_id: uuid.UUID, ig_media_id: str) -> Optional[InstagramMedia]:
    return (
        db.query(InstagramMedia)
        .filter(InstagramMedia.org_id == org_id, InstagramMedia.ig_media_id == ig_media_id)
        .first()
    )


def _set(ad: MetaAd, *, text: Optional[str], source: Optional[str], status: str, error: Optional[str] = None) -> None:
    ad.transcript = text[:50000] if text else None
    ad.transcript_source = source
    ad.transcript_status = status
    ad.transcript_error = error[:500] if error else None
    ad.transcribed_at = datetime.utcnow()


def _from_instagram(db: Session, org_id: uuid.UUID, ad: MetaAd) -> Optional[str]:
    if not ad.instagram_media_id:
        return None
    media = _find_instagram_media(db, org_id, ad.instagram_media_id)
    if media is None:
        return None
    if not (media.transcript_status == "ok" and media.transcript):
        if media.transcript_status != "unavailable" or not media.media_url:
            return None
        media = its.transcribe_media(db, org_id, media.id)
    return media.transcript if media.transcript_status == "ok" and media.transcript else None


def _from_video(org_id: uuid.UUID, ad: MetaAd, token: str) -> Optional[str]:
    if not ad.video_id:
        return None
    url = mac.get_video_source_url(video_id=ad.video_id, token=token)
    if not url:
        return None
    audio = its._download_media(url)  # the URL is signed and short-lived: used once, never stored
    text = its._whisper_transcribe(audio, f"ad_{ad.meta_ad_id}.mp4", org_id, feature=WHISPER_FEATURE)
    return text or None


def transcribe_ad(db: Session, org_id: uuid.UUID, ad: MetaAd, token: str) -> MetaAd:
    """Fill one ad's transcript. Idempotent: an ad already `ok` is returned as is."""
    if ad.org_id != org_id:
        raise ValueError("Ad does not belong to this org")
    if ad.transcript_status == "ok" and ad.transcript:
        return ad

    if not (ad.video_id or ad.instagram_media_id or ad.ad_copy):
        # Synced before the creative fields were widened: fetch them once.
        from app.services.meta_ads_sync_service import apply_creative_fields

        try:
            apply_creative_fields(ad, mac.get_ad_creative(meta_ad_id=ad.meta_ad_id, token=token))
        except Exception as exc:  # noqa: BLE001 - one bad ad must not stop the batch
            logger.warning("meta ad creative fetch failed ad=%s: %s", ad.id, exc)

    video_error: Optional[str] = None
    try:
        text = _from_instagram(db, org_id, ad)
        if text:
            _set(ad, text=text, source=SOURCE_INSTAGRAM, status="ok")
            db.commit()
            return ad
        text = _from_video(org_id, ad, token)
        if text:
            _set(ad, text=text, source=SOURCE_WHISPER, status="ok")
            db.commit()
            return ad
    except its.TranscriptUnavailableError as exc:
        video_error = str(exc)
    except Exception as exc:  # noqa: BLE001
        logger.exception("meta ad transcription failed ad=%s", ad.id)
        video_error = str(exc)

    if ad.ad_copy:
        _set(ad, text=ad.ad_copy, source=SOURCE_AD_COPY, status="ok", error=video_error)
    elif video_error:
        _set(ad, text=None, source=None, status="failed", error=video_error)
    else:
        _set(ad, text=None, source=None, status="unavailable", error="No video, Instagram post, or ad copy")
    db.commit()
    return ad


def eligible_ads(db: Session, org_id: uuid.UUID, spend_by_id: Dict[str, int]) -> List[MetaAd]:
    """Ads over the spend floor, highest spend first."""
    over = {mid for mid, cents in spend_by_id.items() if cents >= AD_SPEND_FLOOR_CENTS}
    if not over:
        return []
    ads = db.query(MetaAd).filter(MetaAd.org_id == org_id, MetaAd.meta_ad_id.in_(over)).all()
    return sorted(ads, key=lambda a: spend_by_id.get(a.meta_ad_id, 0), reverse=True)


def transcribe_eligible_ads(db: Session, org_id: uuid.UUID, *, limit: int = MAX_NEW_TRANSCRIPTS_PER_RUN) -> Dict[str, Any]:
    """Transcribe up to `limit` untranscribed ads over the spend floor. Skips ads that
    already failed or were unavailable, so a sync-triggered run never retries forever."""
    spend = ad_spend_cents_by_meta_id(db, org_id)
    todo = [
        a
        for a in eligible_ads(db, org_id, spend)
        if a.transcript_status == "unavailable" and a.transcribed_at is None
    ][: max(0, limit)]
    if not todo:
        return {"attempted": 0, "ok": 0}
    try:
        token = mac.access_token_for_org(db, org_id)
    except mac.MetaAdsNotConnectedError:
        return {"attempted": 0, "ok": 0, "error": "meta_ads_not_connected"}
    ok = 0
    for ad in todo:
        try:
            transcribe_ad(db, org_id, ad, token)
            ok += 1 if ad.transcript_status == "ok" else 0
        except Exception:  # noqa: BLE001
            db.rollback()
            logger.exception("meta ad transcription crashed ad=%s", ad.id)
    return {"attempted": len(todo), "ok": ok}

"""Ingest Fathom meetings/webhooks: match clients, store records, run required sentiment step."""
from __future__ import annotations

import re
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy.orm import Session
from sqlalchemy import text

from app.models.client import find_client_by_email
from app.models.fathom_call_record import FathomCallRecord
from app.services.fathom_attendee_clients import resolve_clients_for_meeting
from app.services.fathom_client import resolve_fathom_api_key, get_recording_summary, get_recording_transcript
from app.services.fathom_sentiment import default_neutral, derive_sentiment
from app.services.health_score_cache_service import invalidate_health_score_cache
from app.core.config import settings as app_settings
from app.services.llm_client import llm_available, truncate_for_tokens


def _norm_email(e: Optional[str]) -> Optional[str]:
    if not e or not isinstance(e, str):
        return None
    return re.sub(r"\s+", "", e.lower().strip()) or None


def transcript_to_text(transcript: Any) -> str:
    if not transcript:
        return ""
    if isinstance(transcript, str):
        return transcript
    if isinstance(transcript, list):
        parts = []
        for item in transcript:
            if not isinstance(item, dict):
                continue
            t = item.get("text") or ""
            sp = item.get("speaker") or {}
            name = sp.get("display_name") or ""
            if name:
                parts.append(f"{name}: {t}")
            else:
                parts.append(t)
        return "\n".join(parts)
    return str(transcript)


def summary_to_markdown(summary_payload: Any) -> str:
    if not summary_payload:
        return ""
    if isinstance(summary_payload, dict):
        if "markdown_formatted" in summary_payload:
            return str(summary_payload.get("markdown_formatted") or "")
        if "summary" in summary_payload:
            inner = summary_payload["summary"]
            if isinstance(inner, dict):
                return str(inner.get("markdown_formatted") or "")
    return str(summary_payload)


_fathom_media_columns_ensured = False


def _ensure_fathom_media_columns(db: Session) -> None:
    """Idempotent ADD COLUMN for deploys where DB migrations haven't been applied yet."""
    global _fathom_media_columns_ensured
    if _fathom_media_columns_ensured:
        return
    try:
        db.execute(text("ALTER TABLE fathom_call_records ADD COLUMN IF NOT EXISTS share_url TEXT"))
        db.execute(text("ALTER TABLE fathom_call_records ADD COLUMN IF NOT EXISTS video_url TEXT"))
        db.commit()
        _fathom_media_columns_ensured = True
    except Exception:
        db.rollback()
        # Soft-fail: the app can still ingest without these columns.
        _fathom_media_columns_ensured = True


def _extract_media_urls(meeting: Dict[str, Any]) -> Dict[str, Optional[str]]:
    """Best-effort pull of share/video URLs from list API or webhook payloads."""
    share = None
    video = None
    for k in ("share_link_url", "share_url", "shareUrl", "shareLinkUrl"):
        v = meeting.get(k)
        if isinstance(v, str) and v.strip().startswith("http"):
            share = v.strip()[:2000]
            break
    for k in ("video_url", "videoUrl", "recording_video_url", "recordingVideoUrl"):
        v = meeting.get(k)
        if isinstance(v, str) and v.strip().startswith("http"):
            video = v.strip()[:2000]
            break
    return {"share_url": share, "video_url": video}


def _action_items_to_markdown(action_items: Any) -> str:
    if not isinstance(action_items, list) or not action_items:
        return ""
    lines: List[str] = ["## Action items"]
    for it in action_items[:20]:
        if not isinstance(it, dict):
            continue
        desc = str(it.get("description") or "").strip()
        if not desc:
            continue
        ts = str(it.get("recording_timestamp") or "").strip()
        url = str(it.get("recording_playback_url") or "").strip()
        meta = " · ".join([x for x in (ts, url) if x])
        if meta:
            lines.append(f"- {desc} ({meta})")
        else:
            lines.append(f"- {desc}")
    return "\n".join(lines).strip()


def find_client_for_invitees(db: Session, org_id: uuid.UUID, meeting: Dict[str, Any]) -> Optional[uuid.UUID]:
    emails: List[str] = []
    for inv in meeting.get("calendar_invitees") or []:
        if isinstance(inv, dict) and inv.get("email"):
            emails.append(inv["email"])
    for block in meeting.get("transcript") or []:
        if not isinstance(block, dict):
            continue
        sp = block.get("speaker") or {}
        if isinstance(sp, dict) and sp.get("matched_calendar_invitee_email"):
            emails.append(sp["matched_calendar_invitee_email"])
    seen = set()
    for raw in emails:
        e = _norm_email(raw)
        if not e or e in seen:
            continue
        seen.add(e)
        c = find_client_by_email(db, org_id, raw)
        if c:
            return c.id
    return None


def upsert_call_record(
    db: Session,
    org_id: uuid.UUID,
    client_id: Optional[uuid.UUID],
    recording_id: int,
    summary_md: str,
    transcript_text: str,
    meeting_at: Optional[datetime],
    *,
    recording_url: Optional[str] = None,
    share_url: Optional[str] = None,
    video_url: Optional[str] = None,
    attendees_json: Optional[List[Dict[str, Any]]] = None,
    related_client_ids: Optional[List[str]] = None,
) -> FathomCallRecord:
    _ensure_fathom_media_columns(db)
    row = (
        db.query(FathomCallRecord)
        .filter(
            FathomCallRecord.org_id == org_id,
            FathomCallRecord.fathom_recording_id == recording_id,
        )
        .first()
    )
    if not row:
        row = FathomCallRecord(
            org_id=org_id,
            client_id=client_id,
            fathom_recording_id=recording_id,
        )
        db.add(row)

    row.client_id = client_id
    row.summary_text = summary_md[:50000] if summary_md else None
    row.transcript_snippet = truncate_for_tokens(transcript_text, 24000) if transcript_text else None
    row.meeting_at = meeting_at
    if recording_url:
        row.recording_url = recording_url[:2000]
    if share_url:
        try:
            row.share_url = share_url[:2000]
        except Exception:
            pass
    if video_url:
        try:
            row.video_url = video_url[:2000]
        except Exception:
            pass
    if attendees_json is not None:
        row.attendees_json = attendees_json
    if related_client_ids is not None:
        row.related_client_ids = related_client_ids
    row.sentiment_status = "pending"
    row.updated_at = datetime.now(timezone.utc)
    if not row.created_at:
        row.created_at = datetime.now(timezone.utc)
    return row


def apply_sentiment_to_record(db: Session, record: FathomCallRecord) -> None:
    summary = record.summary_text or ""
    trans = record.transcript_snippet or ""
    status, payload = derive_sentiment(summary, trans, org_id=record.org_id)
    if status == "complete":
        record.sentiment_status = "complete"
        record.sentiment_label = payload["sentiment_label"]
        record.sentiment_score = payload["sentiment_score"]
        record.sentiment_snippet = payload.get("sentiment_snippet")
    else:
        err = payload.get("error") if isinstance(payload, dict) else None
        if err == "llm_budget_exceeded":
            # Graceful degradation: keep pipeline usable for health score
            d = default_neutral()
            record.sentiment_status = "complete"
            record.sentiment_label = d["sentiment_label"]
            record.sentiment_score = d["sentiment_score"]
            record.sentiment_snippet = "Sentiment deferred (LLM budget); defaulted to neutral"
        elif llm_available():
            record.sentiment_status = "failed"
        else:
            # No LLM: default neutral so pipeline can complete (health uses neutral)
            d = default_neutral()
            record.sentiment_status = "complete"
            record.sentiment_label = d["sentiment_label"]
            record.sentiment_score = d["sentiment_score"]
            record.sentiment_snippet = d["sentiment_snippet"]
    record.updated_at = datetime.now(timezone.utc)


def ingest_meeting_payload(
    db: Session, org_id: uuid.UUID, meeting: Dict[str, Any]
) -> Tuple[str, Optional[uuid.UUID], Optional[uuid.UUID]]:
    """
    Process a Fathom Meeting JSON object (webhook or list API).

    **Order:** resolve org client by invitee / transcript emails first.  Only matched
    clients get sentiment analysis (LLM) and health-score invalidation.
    Unmatched meetings are still persisted so they appear in the Call Library.
    """
    recording_id = meeting.get("recording_id")
    if recording_id is None:
        return "no_recording_id", None, None
    try:
        rid = int(recording_id)
    except (TypeError, ValueError):
        return "bad_recording_id", None, None

    primary_cid, attendees_payload, related_strs, recording_url = resolve_clients_for_meeting(db, org_id, meeting)
    client_id = primary_cid
    if client_id is None:
        client_id = find_client_for_invitees(db, org_id, meeting)

    # --- Parse common fields (needed for all calls, matched or not) ---
    summary_md = ""
    if meeting.get("default_summary"):
        summary_md = summary_to_markdown(meeting["default_summary"])
    action_items_md = _action_items_to_markdown(meeting.get("action_items"))
    if action_items_md:
        summary_md = (summary_md.strip() + "\n\n" + action_items_md).strip() if summary_md else action_items_md
    transcript_text = transcript_to_text(meeting.get("transcript"))
    media = _extract_media_urls(meeting)

    meeting_at = None
    for key in ("recording_start_time", "scheduled_start_time", "created_at"):
        raw = meeting.get(key)
        if raw:
            try:
                s = str(raw).replace("Z", "+00:00")
                meeting_at = datetime.fromisoformat(s)
                if meeting_at.tzinfo is None:
                    meeting_at = meeting_at.replace(tzinfo=timezone.utc)
                break
            except Exception:
                pass

    fathom_key = resolve_fathom_api_key(db, org_id)
    if (not summary_md or not transcript_text) and fathom_key:
        try:
            if not summary_md:
                s = get_recording_summary(rid, api_key=fathom_key)
                summary_md = summary_to_markdown(s.get("summary") or s)
            if not transcript_text:
                t = get_recording_transcript(rid, api_key=fathom_key)
                transcript_text = transcript_to_text(t.get("transcript") or t)
        except Exception:
            pass

    common_upsert_kwargs = dict(
        recording_url=recording_url or None,
        share_url=media.get("share_url"),
        video_url=media.get("video_url"),
        attendees_json=attendees_payload,
        related_client_ids=related_strs,
    )

    if client_id is None:
        # No org client matched any attendee emails: do not ingest into library.
        return "no_client_match", None, None

    # --- Matched client: run sentiment and health-score invalidation ---
    rec = upsert_call_record(
        db, org_id, client_id, rid, summary_md, transcript_text, meeting_at, **common_upsert_kwargs
    )
    apply_sentiment_to_record(db, rec)
    invalidate_health_score_cache(db, client_id, org_id, do_commit=False)
    db.commit()
    db.refresh(rec)

    return "ok", client_id, rec.id


def sync_recent_meetings_for_org(
    db: Session,
    org_id: uuid.UUID,
    max_pages: Optional[int] = None,
    *,
    user: Optional[Any] = None,
) -> Dict[str, Any]:
    """Poll Fathom list meetings (when API key set). Session-independent.

    Only meetings whose invitee/transcript emails match an org client are ingested and analyzed;
    others are skipped without per-recording API calls or LLM use.

    API key: logged-in user's Settings key, or any org member's key, or FATHOM_API_KEY env.
    """
    from app.services import fathom_client

    api_key = resolve_fathom_api_key(db, org_id, user=user)
    if not api_key:
        return {"skipped": True, "reason": "no_fathom_key"}

    if max_pages is None:
        max_pages = int(getattr(app_settings, "FATHOM_SYNC_MAX_PAGES", 5) or 5)
    delay_ms = int(getattr(app_settings, "FATHOM_SYNC_DELAY_MS", 0) or 0)

    cursor = None
    ingested = 0
    skipped_no_client = 0
    total_seen = 0
    pending_insight_record_ids: List[uuid.UUID] = []
    pending_library_record_ids: List[uuid.UUID] = []  # all calls for library reports
    max_seconds = int(getattr(app_settings, "FATHOM_SYNC_MAX_SECONDS", 90) or 90)
    started_at = time.time()

    for _ in range(max_pages):
        # Hard wall-clock guard: stop the sync if we've been running too long
        if max_seconds > 0 and time.time() - started_at > max_seconds:
            break
        data = fathom_client.list_meetings(
            cursor=cursor,
            include_summary=True,
            include_transcript=True,
            api_key=api_key,
        )
        items = data.get("items") or []
        for m in items:
            total_seen += 1
            status, _cid, fathom_row_id = ingest_meeting_payload(db, org_id, m)
            if status == "ok" and fathom_row_id:
                pending_insight_record_ids.append(fathom_row_id)
            if status == "ok":
                ingested += 1
            elif status == "no_client_match":
                skipped_no_client += 1
            # Queue call library report only for matched calls.
            if fathom_row_id and status == "ok":
                pending_library_record_ids.append(fathom_row_id)
            if delay_ms > 0:
                time.sleep(delay_ms / 1000.0)
        cursor = data.get("next_cursor")
        if not cursor:
            break
    return {
        "skipped": False,
        "ingested": ingested,
        "processed": ingested,  # alias for backward compatibility
        "skipped_no_client_match": skipped_no_client,
        "meetings_seen": total_seen,
        "pending_insight_record_ids": [str(x) for x in pending_insight_record_ids],
        "call_insights_queued": len(pending_insight_record_ids),
        "pending_library_record_ids": [str(x) for x in pending_library_record_ids],
        "library_reports_queued": len(pending_library_record_ids),
    }


def queue_fathom_sync_followups(background_tasks: Any, org_id: uuid.UUID, sync_result: Dict[str, Any]) -> None:
    """Queue call-insight and call-library background jobs after sync (shared with integrations + Content Studio)."""
    from app.long_jobs import schedule_background_work
    from app.services.call_insight_service import run_call_insight_background
    from app.services.call_library_service import run_call_library_report_background

    oid_str = str(org_id)
    for rid in sync_result.get("pending_insight_record_ids") or []:
        schedule_background_work(run_call_insight_background, background_tasks, oid_str, str(rid))
    for rid in sync_result.get("pending_library_record_ids") or []:
        schedule_background_work(run_call_library_report_background, background_tasks, oid_str, str(rid))

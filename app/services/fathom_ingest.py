"""Ingest Fathom meetings/webhooks: match clients, store records, run required sentiment step."""
from __future__ import annotations

import logging
import re
import threading
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

logger = logging.getLogger(__name__)


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
    db: Session,
    org_id: uuid.UUID,
    meeting: Dict[str, Any],
    *,
    bulk_sync: bool = False,
) -> Tuple[str, Optional[uuid.UUID], Optional[uuid.UUID]]:
    """
    Process a Fathom Meeting JSON object (webhook or list API).

    All meetings are stored for marketing insights. Client-linked calls also get
    health-score invalidation; unlinked calls relink when a matching client appears.
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
    if (not summary_md or not transcript_text) and fathom_key and not bulk_sync:
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

    rec = upsert_call_record(
        db, org_id, client_id, rid, summary_md, transcript_text, meeting_at, **common_upsert_kwargs
    )
    if bulk_sync:
        rec.sentiment_status = rec.sentiment_status or "pending"
    else:
        apply_sentiment_to_record(db, rec)
    if client_id is not None:
        invalidate_health_score_cache(db, client_id, org_id, do_commit=False)
    db.commit()
    db.refresh(rec)

    try:
        from app.services.call_library_service import ensure_pending_call_library_report

        ensure_pending_call_library_report(db, org_id, rec.id)
    except Exception:
        logger.exception(
            "call_library pending row failed org=%s recording_id=%s",
            org_id,
            rid,
        )

    if client_id is None:
        return "ok_unlinked", None, rec.id
    return "ok", client_id, rec.id


def sync_recent_meetings_for_org(
    db: Session,
    org_id: uuid.UUID,
    max_pages: Optional[int] = None,
    *,
    user: Optional[Any] = None,
    max_seconds: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Poll Fathom list meetings (when API key set). Session-independent.

    All meetings are ingested for marketing insights (Content Studio, Call Library).
    Calls link to pipeline clients when attendee emails match; unlinked calls are
    relinked automatically when clients are added later or at the end of each sync.

    API key: logged-in user's Settings key, or any org member's key, or FATHOM_API_KEY env.
    """
    from app.services import fathom_client

    api_key = resolve_fathom_api_key(db, org_id, user=user)
    if not api_key:
        return {"skipped": True, "reason": "no_fathom_key"}

    if max_pages is None:
        max_pages = int(getattr(app_settings, "FATHOM_SYNC_MAX_PAGES", 5) or 5)
    delay_ms = int(getattr(app_settings, "FATHOM_SYNC_DELAY_MS", 0) or 0)
    page_delay_ms = int(getattr(app_settings, "FATHOM_SYNC_PAGE_DELAY_MS", 1100) or 1100)

    cursor = None
    ingested = 0
    ingested_unlinked = 0
    total_seen = 0
    pending_insight_record_ids: List[uuid.UUID] = []
    pending_library_record_ids: List[uuid.UUID] = []
    pending_enrichment_record_ids: List[uuid.UUID] = []
    ingest_errors = 0
    if max_seconds is None:
        max_seconds = int(getattr(app_settings, "FATHOM_SYNC_MAX_SECONDS", 90) or 90)
    started_at = time.time()

    for _ in range(max_pages):
        # Hard wall-clock guard: stop the sync if we've been running too long
        if max_seconds > 0 and time.time() - started_at > max_seconds:
            break
        data = fathom_client.list_meetings_for_bulk_sync(cursor=cursor, api_key=api_key)
        items = data.get("items") or []
        for m in items:
            total_seen += 1
            try:
                status, _cid, fathom_row_id = ingest_meeting_payload(
                    db, org_id, m, bulk_sync=True
                )
            except Exception:
                ingest_errors += 1
                logger.exception(
                    "fathom ingest failed org=%s recording_id=%s",
                    org_id,
                    m.get("recording_id"),
                )
                continue
            if status in ("ok", "ok_unlinked") and fathom_row_id:
                pending_library_record_ids.append(fathom_row_id)
                pending_enrichment_record_ids.append(fathom_row_id)
                if status == "ok":
                    pending_insight_record_ids.append(fathom_row_id)
            if status == "ok":
                ingested += 1
            elif status == "ok_unlinked":
                ingested += 1
                ingested_unlinked += 1
            if delay_ms > 0:
                time.sleep(delay_ms / 1000.0)
        cursor = data.get("next_cursor")
        if cursor and page_delay_ms > 0:
            time.sleep(page_delay_ms / 1000.0)
        if not cursor:
            break

    from app.services.fathom_client_link import relink_orphan_fathom_records_for_org

    relinked = relink_orphan_fathom_records_for_org(db, org_id)
    if relinked:
        db.commit()
        for rec_id, _client_id in relinked:
            if rec_id not in pending_insight_record_ids:
                pending_insight_record_ids.append(rec_id)
            if rec_id not in pending_library_record_ids:
                pending_library_record_ids.append(rec_id)
            if rec_id not in pending_enrichment_record_ids:
                pending_enrichment_record_ids.append(rec_id)

    return {
        "skipped": False,
        "ingested": ingested,
        "processed": ingested,
        "ingested_unlinked": ingested_unlinked,
        "relinked_to_clients": len(relinked),
        "ingest_errors": ingest_errors,
        "skipped_no_client_match": 0,
        "meetings_seen": total_seen,
        "pending_insight_record_ids": [str(x) for x in pending_insight_record_ids],
        "call_insights_queued": len(pending_insight_record_ids),
        "pending_library_record_ids": [str(x) for x in pending_library_record_ids],
        "library_reports_queued": len(pending_library_record_ids),
        "pending_enrichment_record_ids": [str(x) for x in pending_enrichment_record_ids],
    }


def run_fathom_sync_background(org_id_str: str) -> None:
    """Run Fathom list + ingest off the HTTP thread (retries / rate limits won't 503 the browser)."""
    from app.db.session import SessionLocal

    db = SessionLocal()
    try:
        org_id = uuid.UUID(org_id_str)
        bg_max = int(getattr(app_settings, "FATHOM_SYNC_BACKGROUND_MAX_SECONDS", 300) or 300)
        result = sync_recent_meetings_for_org(db, org_id, max_seconds=bg_max)
        if result.get("skipped"):
            logger.info("fathom background sync skipped org=%s reason=%s", org_id, result.get("reason"))
            return
        queue_fathom_sync_followups(None, org_id, result)
        logger.info(
            "fathom background sync done org=%s ingested=%s seen=%s relinked=%s errors=%s",
            org_id,
            result.get("ingested"),
            result.get("meetings_seen"),
            result.get("relinked_to_clients"),
            result.get("ingest_errors"),
        )
    except Exception:
        logger.exception("fathom background sync failed org=%s", org_id_str)
    finally:
        db.close()


def queue_fathom_sync_followups(background_tasks: Any, org_id: uuid.UUID, sync_result: Dict[str, Any]) -> None:
    """Queue call-insight and call-library background jobs after sync (shared with integrations + Content Studio)."""
    from app.long_jobs import schedule_background_work
    from app.services.call_insight_service import run_call_insight_background
    from app.services.call_library_service import run_call_library_report_background

    oid_str = str(org_id)
    enrichment_ids = {str(x) for x in (sync_result.get("pending_enrichment_record_ids") or [])}
    for rid in sync_result.get("pending_insight_record_ids") or []:
        if str(rid) in enrichment_ids:
            continue
        schedule_background_work(run_call_insight_background, background_tasks, oid_str, str(rid))
    for rid in sync_result.get("pending_library_record_ids") or []:
        if str(rid) in enrichment_ids:
            continue
        schedule_background_work(run_call_library_report_background, background_tasks, oid_str, str(rid))
    for i, rid in enumerate(sorted(enrichment_ids)):
        delay_sec = i * float(getattr(app_settings, "FATHOM_ENRICHMENT_STAGGER_SEC", 3) or 3)
        if delay_sec > 0:

            def _kick(
                off: str = oid_str,
                record_id: str = str(rid),
                attempt: str = "1",
            ) -> None:
                schedule_background_work(
                    run_fathom_webhook_enrichment_and_followups,
                    None,
                    off,
                    record_id,
                    attempt,
                )

            t = threading.Timer(delay_sec, _kick)
            t.daemon = True
            t.start()
        else:
            schedule_background_work(
                run_fathom_webhook_enrichment_and_followups,
                background_tasks,
                oid_str,
                str(rid),
                "1",
            )


def _refresh_fathom_row_from_api(db: Session, rec: FathomCallRecord, api_key: Optional[str]) -> bool:
    """Pull summary/transcript from Fathom API when combined text still looks thin. Returns True if row text changed."""
    from app.services.call_insight_context import is_meeting_snapshot_thin

    if not api_key:
        return False
    prev_sum = (rec.summary_text or "").strip()
    prev_tr = (rec.transcript_snippet or "").strip()
    if not is_meeting_snapshot_thin(prev_sum, prev_tr):
        return False
    rid = int(rec.fathom_recording_id)
    changed = False
    try:
        s = get_recording_summary(rid, api_key=api_key)
        new_md = summary_to_markdown(s.get("summary") or s).strip()
        if len(new_md) > len(prev_sum):
            rec.summary_text = new_md[:50000]
            changed = True
        time.sleep(float(getattr(app_settings, "FATHOM_RECORDINGS_CALL_GAP_SEC", 2.5) or 2.5))
        t = get_recording_transcript(rid, api_key=api_key)
        tt_full = transcript_to_text(t.get("transcript") or t)
        tt_trunc = truncate_for_tokens(tt_full, 24000) if tt_full else ""
        if len(tt_trunc.strip()) > len(prev_tr):
            rec.transcript_snippet = tt_trunc or None
            changed = True
    except Exception:
        logger.exception(
            "Fathom webhook enrichment API pull failed recording_id=%s org=%s",
            rec.fathom_recording_id,
            rec.org_id,
        )
    if changed:
        rec.updated_at = datetime.now(timezone.utc)
    return changed


def run_fathom_webhook_enrichment_and_followups(
    org_id_str: str, fathom_record_uuid_str: str, attempt_str: str = "1"
) -> None:
    """
    Webhooks often arrive before full summary/transcript exist. Re-fetch with bounded retries,
    then enqueue call insight + call library jobs (one recording at a time; avoids hammering list-sync).
    """
    from app.db.session import SessionLocal
    from app.long_jobs import schedule_background_work
    from app.services.call_insight_context import is_meeting_snapshot_thin
    from app.services.call_insight_service import run_call_insight_background
    from app.services.call_library_service import run_call_library_report_background

    max_att = int(getattr(app_settings, "FATHOM_WEBHOOK_ENRICH_MAX_ATTEMPTS", 8) or 8)
    delay = float(getattr(app_settings, "FATHOM_WEBHOOK_ENRICH_DELAY_SEC", 90) or 90)

    try:
        org_id = uuid.UUID(org_id_str)
        rec_id = uuid.UUID(fathom_record_uuid_str)
        attempt = max(1, int(attempt_str or "1"))
    except ValueError:
        logger.warning(
            "fathom webhook enrichment bad uuid org=%s record=%s",
            org_id_str,
            fathom_record_uuid_str,
        )
        return

    db = SessionLocal()
    try:
        rec = (
            db.query(FathomCallRecord)
            .filter(FathomCallRecord.id == rec_id, FathomCallRecord.org_id == org_id)
            .first()
        )
        if not rec:
            return

        api_key = resolve_fathom_api_key(db, org_id)
        _refresh_fathom_row_from_api(db, rec, api_key)
        apply_sentiment_to_record(db, rec)
        if rec.client_id:
            invalidate_health_score_cache(db, rec.client_id, org_id, do_commit=False)
        db.commit()
        db.refresh(rec)

        still_thin = is_meeting_snapshot_thin(rec.summary_text, rec.transcript_snippet)
        # Only wait while Fathom hasn't finished processing transcript/summary.
        # Sentiment must not block Call Library — reports run on summary + transcript alone.
        defer = still_thin and attempt < max_att

        if defer:
            # Faster early retries while Fathom is still processing on their end.
            retry_delay = min(delay, 15.0 * attempt)
            logger.info(
                "fathom webhook enrichment defer org=%s record=%s attempt=%s/%s thin=%s delay_s=%s",
                org_id,
                rec_id,
                attempt,
                max_att,
                still_thin,
                retry_delay,
            )

            def _reschedule() -> None:
                schedule_background_work(
                    run_fathom_webhook_enrichment_and_followups,
                    None,
                    org_id_str,
                    fathom_record_uuid_str,
                    str(attempt + 1),
                )

            t = threading.Timer(retry_delay, _reschedule)
            t.daemon = True
            t.start()
            return

        from app.models.call_library_report import CallLibraryReport

        lib_row = (
            db.query(CallLibraryReport)
            .filter(CallLibraryReport.fathom_call_record_id == rec_id)
            .first()
        )
        if not lib_row or lib_row.status != "complete":
            # queue_fathom_webhook_record_followups fast-paths library when content is already
            # complete on attempt 1 — avoid double LLM runs.
            skip_library = attempt == 1 and not still_thin
            if not skip_library:
                schedule_background_work(
                    run_call_library_report_background, None, org_id_str, fathom_record_uuid_str
                )
        if rec.client_id:
            schedule_background_work(run_call_insight_background, None, org_id_str, fathom_record_uuid_str)
        logger.info(
            "fathom webhook followups queued org=%s record=%s attempts_used=%s linked=%s",
            org_id,
            rec_id,
            attempt,
            bool(rec.client_id),
        )
    except Exception:
        logger.exception(
            "fathom webhook enrichment failed org=%s record=%s", org_id_str, fathom_record_uuid_str
        )
    finally:
        db.close()


def queue_fathom_webhook_record_followups(
    background_tasks: Any, org_id: uuid.UUID, fathom_call_record_id: uuid.UUID
) -> None:
    """Prefer this for single-meeting webhooks instead of immediate queue_fathom_sync_followups."""
    from app.db.session import SessionLocal
    from app.long_jobs import schedule_background_work
    from app.services.call_insight_context import is_meeting_snapshot_thin
    from app.services.call_library_service import run_call_library_report_background

    oid_str = str(org_id)
    rid_str = str(fathom_call_record_id)

    with SessionLocal() as db:
        rec = (
            db.query(FathomCallRecord)
            .filter(
                FathomCallRecord.id == fathom_call_record_id,
                FathomCallRecord.org_id == org_id,
            )
            .first()
        )
        fast_path = bool(
            rec
            and not is_meeting_snapshot_thin(rec.summary_text, rec.transcript_snippet)
        )

    if fast_path:
        # Webhook payload already has enough content — start library LLM immediately.
        schedule_background_work(
            run_call_library_report_background, background_tasks, oid_str, rid_str
        )

    schedule_background_work(
        run_fathom_webhook_enrichment_and_followups,
        background_tasks,
        oid_str,
        rid_str,
        "1",
    )

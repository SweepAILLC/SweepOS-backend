"""Ingest Fathom meetings/webhooks: match clients, store records, run required sentiment step."""
from __future__ import annotations

import re
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy.orm import Session

from app.models.client import find_client_by_email
from app.models.fathom_call_record import FathomCallRecord
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
) -> FathomCallRecord:
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

    **Order:** resolve org client by invitee / transcript emails first. Only if a client profile
    matches do we pull recording details from Fathom (extra API calls) and run sentiment (LLM).
    Unmatched meetings return early — no per-recording Fathom fetches and no analysis spend.
    """
    recording_id = meeting.get("recording_id")
    if recording_id is None:
        return "no_recording_id", None, None
    try:
        rid = int(recording_id)
    except (TypeError, ValueError):
        return "bad_recording_id", None, None

    client_id = find_client_for_invitees(db, org_id, meeting)
    if client_id is None:
        return "no_client_match", None, None

    # --- Matched client: safe to fetch recording content and analyze ---
    summary_md = ""
    if meeting.get("default_summary"):
        summary_md = summary_to_markdown(meeting["default_summary"])
    transcript_text = transcript_to_text(meeting.get("transcript"))

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

    rec = upsert_call_record(db, org_id, client_id, rid, summary_md, transcript_text, meeting_at)
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
    for _ in range(max_pages):
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
    }

"""Assemble context pack for call-insight LLM (client, health, check-in, call text)."""
from __future__ import annotations

import json
import re
import uuid
from typing import Any, Dict, List, Optional

from sqlalchemy.orm import Session

from app.core.config import settings
from app.models.client import Client
from app.models.client_checkin import ClientCheckIn
from app.models.fathom_call_record import FathomCallRecord
from app.services.llm_client import truncate_for_tokens


def _lifecycle_str(client: Client) -> str:
    ls = client.lifecycle_state
    if hasattr(ls, "value"):
        return str(ls.value)
    return str(ls)


def _truncate_notes(notes: Optional[str], max_len: int = 1200) -> str:
    if not notes:
        return ""
    s = notes.strip()
    if len(s) <= max_len:
        return s
    return s[: max_len - 3] + "..."


def _parse_raw_event(raw: Optional[str]) -> Dict[str, Any]:
    if not raw:
        return {}
    try:
        return json.loads(raw) if isinstance(raw, str) else {}
    except Exception:
        return {}


def extract_booking_fields(raw_event: Dict[str, Any]) -> Dict[str, Any]:
    """Cal.com bookingFieldsResponses / Calendly-style custom answers (best-effort)."""
    out: Dict[str, Any] = {}
    bfr = raw_event.get("bookingFieldsResponses") or raw_event.get("booking_fields_responses")
    if isinstance(bfr, dict):
        for k, v in list(bfr.items())[:40]:
            if isinstance(v, (str, int, float, bool)):
                out[str(k)[:80]] = str(v)[:500]
            elif isinstance(v, dict):
                out[str(k)[:80]] = str(v)[:500]
    # Calendly invitee questions sometimes under different keys
    inv = raw_event.get("invitee") or raw_event.get("questions_and_answers")
    if isinstance(inv, list):
        for item in inv[:20]:
            if isinstance(item, dict):
                q = item.get("question") or item.get("name")
                a = item.get("answer") or item.get("text")
                if q and a:
                    out[str(q)[:80]] = str(a)[:500]
    return out


def build_check_in_context(db: Session, check_in_id: Optional[uuid.UUID]) -> Dict[str, Any]:
    if not check_in_id:
        return {}
    ci = db.query(ClientCheckIn).filter(ClientCheckIn.id == check_in_id).first()
    if not ci:
        return {}
    raw = _parse_raw_event(ci.raw_event_data)
    fields = extract_booking_fields(raw)
    return {
        "title": (ci.title or "")[:300],
        "start_time": ci.start_time.isoformat() if ci.start_time else None,
        "end_time": ci.end_time.isoformat() if ci.end_time else None,
        "completed": ci.completed,
        "no_show": getattr(ci, "no_show", False),
        "provider": ci.provider,
        "is_sales_call": ci.is_sales_call,
        "booking_fields_excerpt": fields,
    }


def assemble_context_pack(
    db: Session,
    client: Client,
    fathom_record: FathomCallRecord,
    check_in_id: Optional[uuid.UUID],
    health_snapshot: Dict[str, Any],
) -> Dict[str, Any]:
    """Structured pack for hashing + LLM user payload."""
    meta = client.meta if isinstance(client.meta, dict) else {}
    prospect = meta.get("prospect") if isinstance(meta.get("prospect"), dict) else {}

    check_ctx = build_check_in_context(db, check_in_id)
    summary = fathom_record.summary_text or ""
    trans = fathom_record.transcript_snippet or ""
    combined_len = len(summary) + len(trans)

    pack = {
        "client": {
            "lifecycle_state": _lifecycle_str(client),
            "notes_excerpt": _truncate_notes(client.notes),
            "program_start": client.program_start_date.isoformat() if client.program_start_date else None,
            "program_end": client.program_end_date.isoformat() if client.program_end_date else None,
            "program_progress_percent": float(client.program_progress_percent)
            if client.program_progress_percent is not None
            else None,
            "prospect_excerpt": truncate_for_tokens(json.dumps(prospect, default=str), 2000),
        },
        "health": {
            "score": health_snapshot.get("score"),
            "grade": health_snapshot.get("grade"),
            "source": health_snapshot.get("source"),
            "factors_excerpt": _factors_excerpt(health_snapshot.get("factors") or []),
        },
        "fathom": {
            "sentiment_label": fathom_record.sentiment_label,
            "sentiment_score": fathom_record.sentiment_score,
            "meeting_at": fathom_record.meeting_at.isoformat() if fathom_record.meeting_at else None,
        },
        "check_in": check_ctx,
        "call_text": {
            "summary": truncate_for_tokens(summary, 12000),
            "transcript": truncate_for_tokens(trans, 20000),
            "combined_len": combined_len,
        },
        "fathom_recording_id": int(fathom_record.fathom_recording_id),
    }
    return pack


def _factors_excerpt(factors: List[Dict[str, Any]], limit: int = 12) -> List[Dict[str, Any]]:
    out = []
    for f in factors[:limit]:
        if not isinstance(f, dict):
            continue
        out.append(
            {
                "key": f.get("key"),
                "label": f.get("label"),
                "value": f.get("value"),
            }
        )
    return out


def is_thin_transcript(pack: Dict[str, Any]) -> bool:
    """Cheap pre-check: skip LLM when there is not enough conversational signal."""
    ct = pack.get("call_text") or {}
    summary = (ct.get("summary") or "").strip()
    trans = (ct.get("transcript") or "").strip()
    combined = summary + trans
    min_chars = int(getattr(settings, "CALL_INSIGHT_MIN_INPUT_CHARS", 400) or 400)
    min_lines = int(getattr(settings, "CALL_INSIGHT_MIN_TRANSCRIPT_LINES", 3) or 3)
    if len(combined) < min_chars:
        return True
    # Fathom often returns transcript as one long block; a substantive summary is enough signal.
    summary_rich = len(summary) >= 240
    if summary_rich and len(combined) >= min_chars:
        return False
    lines = [ln for ln in re.split(r"[\r\n]+", trans) if ln.strip()]
    if len(lines) < min_lines and len(combined) < min_chars * 2:
        return True
    return False

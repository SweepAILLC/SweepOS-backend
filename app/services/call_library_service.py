"""Orchestrate Call Library report generation and persistence."""
from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Dict, List, Optional
from uuid import UUID

from sqlalchemy import desc, func, text
from sqlalchemy.orm import Session, joinedload

if TYPE_CHECKING:
    from fastapi import BackgroundTasks

from app.models.call_library_report import CallLibraryReport
from app.models.client import Client
from app.models.fathom_call_record import FathomCallRecord
from app.services.call_library_ai import generate_call_library_report
from app.services.fathom_attendee_clients import client_display_name

logger = logging.getLogger(__name__)

# ORM includes call_title_override; DB may lag migrations — add column once per process if missing.
_call_title_override_column_ensured = False
_call_media_columns_ensured = False


def _ensure_call_title_override_column(db: Session) -> None:
    """Idempotent ADD COLUMN for deploys where alembic 039 has not been applied yet."""
    global _call_title_override_column_ensured
    if _call_title_override_column_ensured:
        return
    try:
        db.execute(
            text(
                "ALTER TABLE call_library_reports ADD COLUMN IF NOT EXISTS call_title_override TEXT"
            )
        )
        db.commit()
        _call_title_override_column_ensured = True
    except Exception as e:
        db.rollback()
        logger.warning("call_library: ensure call_title_override column failed: %s", e)


def _ensure_call_media_columns(db: Session) -> None:
    """Idempotent ADD COLUMN for share/video URLs on call_library_reports."""
    global _call_media_columns_ensured
    if _call_media_columns_ensured:
        return
    try:
        db.execute(text("ALTER TABLE call_library_reports ADD COLUMN IF NOT EXISTS share_url TEXT"))
        db.execute(text("ALTER TABLE call_library_reports ADD COLUMN IF NOT EXISTS video_url TEXT"))
        db.commit()
        _call_media_columns_ensured = True
    except Exception as e:
        db.rollback()
        logger.warning("call_library: ensure media columns failed: %s", e)
        _call_media_columns_ensured = True


def _derive_call_title(rec: FathomCallRecord, db: Session, org_id: uuid.UUID) -> str:
    """Build a human-readable title from the call record."""
    if rec.client_id:
        client = db.query(Client).filter(Client.id == rec.client_id).first()
        if client:
            dn = client_display_name(client)
            date_str = rec.meeting_at.strftime("%b %d, %Y") if rec.meeting_at else ""
            return f"Call with {dn}" + (f" — {date_str}" if date_str else "")

    if rec.meeting_at:
        return f"Call on {rec.meeting_at.strftime('%B %d, %Y at %I:%M %p UTC')}"
    return f"Call #{rec.fathom_recording_id}"


def generate_and_persist_report(
    db: Session,
    org_id: uuid.UUID,
    fathom_record_id: uuid.UUID,
) -> str:
    """
    Generate the AI call library report for a fathom record and persist it.
    Returns status: 'ok' | 'skipped' | 'failed'.
    """
    _ensure_call_title_override_column(db)
    _ensure_call_media_columns(db)
    rec = (
        db.query(FathomCallRecord)
        .filter(
            FathomCallRecord.id == fathom_record_id,
            FathomCallRecord.org_id == org_id,
        )
        .first()
    )
    if not rec:
        logger.info("call_library skipped: no record org=%s record=%s", org_id, fathom_record_id)
        return "skipped"

    transcript = rec.transcript_snippet or ""
    summary = rec.summary_text or ""
    call_title = _derive_call_title(rec, db, org_id)

    if not transcript and not summary:
        _upsert_report(
            db,
            org_id,
            fathom_record_id,
            rec,
            "failed",
            None,
            "no_content",
            call_title=call_title,
            call_score=None,
        )
        logger.info("call_library failed: no_content record=%s", fathom_record_id)
        return "failed"

    report_json = generate_call_library_report(
        transcript=transcript,
        summary=summary,
        org_id=org_id,
    )

    if not report_json:
        _upsert_report(
            db,
            org_id,
            fathom_record_id,
            rec,
            "failed",
            None,
            "llm_failed",
            call_title=call_title,
            call_score=None,
        )
        logger.warning("call_library AI failed record=%s", fathom_record_id)
        return "failed"

    cs = report_json.get("call_score")
    try:
        call_score_f = float(cs) if cs is not None else None
        if call_score_f is not None:
            call_score_f = max(0.0, min(100.0, call_score_f))
    except (TypeError, ValueError):
        call_score_f = None

    _upsert_report(
        db,
        org_id,
        fathom_record_id,
        rec,
        "complete",
        report_json,
        None,
        call_title=call_title,
        call_score=call_score_f,
    )
    logger.info("call_library complete record=%s", fathom_record_id)

    # Refresh AI call insights for the primary client now that the call is fully processed.
    if rec.client_id and rec.sentiment_status == "complete":
        try:
            from app.services.call_insight_service import run_call_insight_for_fathom_record

            run_call_insight_for_fathom_record(
                db, org_id, fathom_record_id, bypass_cooldown=True
            )
        except Exception:
            logger.exception("call_insight refresh after library failed record=%s", fathom_record_id)

    return "ok"


def _upsert_report(
    db: Session,
    org_id: uuid.UUID,
    fathom_record_id: uuid.UUID,
    rec: FathomCallRecord,
    status: str,
    report_json: Optional[Dict[str, Any]],
    failure_reason: Optional[str],
    call_title: Optional[str] = None,
    *,
    call_score: Optional[float] = None,
) -> CallLibraryReport:
    row = (
        db.query(CallLibraryReport)
        .filter(CallLibraryReport.fathom_call_record_id == fathom_record_id)
        .first()
    )
    if not row:
        row = CallLibraryReport(
            id=uuid.uuid4(),
            org_id=org_id,
            fathom_call_record_id=fathom_record_id,
        )
        db.add(row)

    row.status = status
    row.report_json = report_json
    row.failure_reason = failure_reason
    row.call_title = call_title or row.call_title
    row.call_score = call_score
    row.recording_url = (rec.recording_url or "")[:2000] or None
    try:
        row.share_url = (getattr(rec, "share_url", None) or "")[:2000] or None
        row.video_url = (getattr(rec, "video_url", None) or "")[:2000] or None
    except Exception:
        pass
    row.attendees_json = rec.attendees_json
    row.computed_at = datetime.now(timezone.utc)
    row.updated_at = datetime.now(timezone.utc)
    db.commit()
    db.refresh(row)
    return row


def run_call_library_report_background(org_id_str: str, fathom_record_id_str: str) -> None:
    """Thread-safe background entry point for FastAPI BackgroundTasks."""
    from app.db.session import SessionLocal

    db = SessionLocal()
    try:
        oid = uuid.UUID(org_id_str)
        rid = uuid.UUID(fathom_record_id_str)
        status = generate_and_persist_report(db, oid, rid)
        logger.info(
            "call_library background done record=%s status=%s", fathom_record_id_str, status
        )
    except Exception as e:
        logger.exception("call_library background failed record=%s: %s", fathom_record_id_str, e)
    finally:
        db.close()


def get_call_library_for_org(
    db: Session,
    org_id: uuid.UUID,
    limit: int = 25,
    offset: int = 0,
) -> Dict[str, Any]:
    """Return paginated call library items for the org."""
    _ensure_call_title_override_column(db)
    _ensure_call_media_columns(db)
    total = (
        db.query(func.count(CallLibraryReport.id))
        .filter(CallLibraryReport.org_id == org_id)
        .scalar()
        or 0
    )
    rows = (
        db.query(CallLibraryReport)
        .options(joinedload(CallLibraryReport.fathom_call_record))
        .filter(CallLibraryReport.org_id == org_id)
        .order_by(desc(CallLibraryReport.created_at))
        .offset(offset)
        .limit(limit)
        .all()
    )

    client_ids = {
        r.fathom_call_record.client_id
        for r in rows
        if r.fathom_call_record is not None and r.fathom_call_record.client_id is not None
    }
    clients_by_id: Dict[UUID, Client] = {}
    if client_ids:
        for c in db.query(Client).filter(Client.id.in_(client_ids)).all():
            clients_by_id[c.id] = c

    items: List[Dict[str, Any]] = []
    for row in rows:
        fathom_rec = row.fathom_call_record
        client_name: Optional[str] = None
        if fathom_rec and fathom_rec.client_id:
            client = clients_by_id.get(fathom_rec.client_id)
            if client:
                client_name = client_display_name(client)

        attendees = row.attendees_json if row.attendees_json is not None else (
            fathom_rec.attendees_json if fathom_rec else None
        )
        recording_url = row.recording_url or (fathom_rec.recording_url if fathom_rec else None)
        share_url = getattr(row, "share_url", None) or (getattr(fathom_rec, "share_url", None) if fathom_rec else None)
        video_url = getattr(row, "video_url", None) or (getattr(fathom_rec, "video_url", None) if fathom_rec else None)

        derived_title = row.call_title or f"Call #{fathom_rec.fathom_recording_id if fathom_rec else '?'}"
        display_title = (getattr(row, "call_title_override", None) or "").strip() or derived_title

        items.append(
            {
                "id": str(row.id),
                "fathom_recording_id": int(fathom_rec.fathom_recording_id) if fathom_rec else None,
                "call_title": display_title,
                "meeting_at": fathom_rec.meeting_at.isoformat() if fathom_rec and fathom_rec.meeting_at else None,
                "status": row.status,
                "failure_reason": row.failure_reason,
                "client_name": client_name,
                "call_score": row.call_score,
                "recording_url": recording_url,
                "share_url": share_url,
                "video_url": video_url,
                "attendees": attendees,
                "report": row.report_json,
                "computed_at": row.computed_at.isoformat() if row.computed_at else None,
            }
        )

    return {"items": items, "total": total}


def update_call_library_title(
    db: Session,
    org_id: uuid.UUID,
    report_id: uuid.UUID,
    title: str,
) -> Optional[CallLibraryReport]:
    """Set user-visible title override (empty string clears override)."""
    _ensure_call_title_override_column(db)
    row = (
        db.query(CallLibraryReport)
        .filter(CallLibraryReport.id == report_id, CallLibraryReport.org_id == org_id)
        .first()
    )
    if not row:
        return None
    t = (title or "").strip()
    row.call_title_override = t[:500] if t else None
    row.updated_at = datetime.now(timezone.utc)
    db.commit()
    db.refresh(row)
    return row


def requeue_llm_failed_reports(
    db: Session,
    org_id: uuid.UUID,
    background_tasks: "BackgroundTasks",
) -> int:
    """
    Re-run report generation for rows where the LLM failed. Matches the Call Library UI:
    failure_reason == 'llm_failed', status == 'failed' (shows the “Analyzing…” chip while not complete).
    """
    _ensure_call_title_override_column(db)
    rows = (
        db.query(CallLibraryReport)
        .filter(
            CallLibraryReport.org_id == org_id,
            CallLibraryReport.failure_reason == "llm_failed",
            CallLibraryReport.status == "failed",
        )
        .all()
    )
    if not rows:
        return 0
    for row in rows:
        row.status = "pending"
        row.failure_reason = None
        row.updated_at = datetime.now(timezone.utc)
    db.commit()
    oid_str = str(org_id)
    for row in rows:
        fid = row.fathom_call_record_id
        background_tasks.add_task(run_call_library_report_background, oid_str, str(fid))
    return len(rows)

"""Orchestrate Call Library report generation and persistence."""
from __future__ import annotations

import hashlib
import logging
import uuid
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from sqlalchemy import desc, func, text
from sqlalchemy.orm import Session, joinedload

if TYPE_CHECKING:
    from fastapi import BackgroundTasks

from app.models.call_library_report import CallLibraryReport
from app.models.fathom_call_record import FathomCallRecord
from app.services.call_library_ai import generate_call_library_report
from app.services.fathom_call_labels import (
    derive_call_library_title,
    primary_external_attendee_label,
)

logger = logging.getLogger(__name__)

# ORM includes call_title_override; DB may lag migrations — add column once per process if missing.
_call_title_override_column_ensured = False
_call_media_columns_ensured = False
_call_deal_columns_ensured = False
_fathom_meeting_title_column_ensured = False


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


def _ensure_call_deal_columns(db: Session) -> None:
    """Idempotent ADD COLUMN for the deal outcome fields surfaced next to call_score."""
    global _call_deal_columns_ensured
    if _call_deal_columns_ensured:
        return
    try:
        db.execute(
            text(
                "ALTER TABLE call_library_reports "
                "ADD COLUMN IF NOT EXISTS deal_closed BOOLEAN NOT NULL DEFAULT false"
            )
        )
        db.execute(
            text(
                "ALTER TABLE call_library_reports ADD COLUMN IF NOT EXISTS deal_value_cents BIGINT"
            )
        )
        db.execute(
            text(
                "ALTER TABLE call_library_reports ADD COLUMN IF NOT EXISTS deal_currency VARCHAR(8)"
            )
        )
        db.execute(
            text(
                "ALTER TABLE call_library_reports ADD COLUMN IF NOT EXISTS deal_billing VARCHAR(32)"
            )
        )
        db.commit()
        _call_deal_columns_ensured = True
    except Exception as e:
        db.rollback()
        logger.warning("call_library: ensure deal columns failed: %s", e)
        _call_deal_columns_ensured = True


def _ensure_fathom_meeting_title_column(db: Session) -> None:
    """Idempotent ADD COLUMN for meeting_title on fathom_call_records."""
    global _fathom_meeting_title_column_ensured
    if _fathom_meeting_title_column_ensured:
        return
    try:
        db.execute(text("ALTER TABLE fathom_call_records ADD COLUMN IF NOT EXISTS meeting_title TEXT"))
        db.commit()
        _fathom_meeting_title_column_ensured = True
    except Exception as e:
        db.rollback()
        logger.warning("call_library: ensure meeting_title column failed: %s", e)
        _fathom_meeting_title_column_ensured = True


def _coerce_deal_outcome(report_json: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Pull the persisted columns out of an LLM report's deal_outcome block."""
    fallback = {
        "closed": False,
        "value_cents": None,
        "currency": None,
        "billing": None,
    }
    if not isinstance(report_json, dict):
        return fallback
    raw = report_json.get("deal_outcome")
    if not isinstance(raw, dict):
        return fallback

    closed = bool(raw.get("closed"))
    if not closed:
        return fallback

    amount = raw.get("amount")
    value_cents: Optional[int] = None
    try:
        if amount is not None and amount != "":
            amt = float(amount)
            if amt > 0:
                value_cents = int(round(amt * 100))
    except (TypeError, ValueError):
        value_cents = None

    currency_raw = str(raw.get("currency") or "USD").upper().strip()
    currency = currency_raw if 2 <= len(currency_raw) <= 8 and currency_raw.isalpha() else "USD"

    billing_raw = str(raw.get("billing") or "").lower().strip()
    billing = billing_raw if billing_raw in {"one_time", "recurring_monthly", "recurring_annual"} else None

    return {
        "closed": True,
        "value_cents": value_cents,
        "currency": currency,
        "billing": billing,
    }


def _hash_call_inputs(summary: str, transcript: str) -> str:
    """Stable hash for skipping identical LLM work."""
    payload = f"{summary}\n---\n{transcript}".encode("utf-8", errors="ignore")
    return hashlib.sha256(payload).hexdigest()


def _derive_call_title(rec: FathomCallRecord, db: Session, org_id: uuid.UUID) -> str:
    """Build display title from Fathom meeting metadata (not linked CRM client)."""
    attendees = rec.attendees_json if isinstance(rec.attendees_json, list) else None
    meeting_title = getattr(rec, "meeting_title", None)
    return derive_call_library_title(
        meeting_title=meeting_title,
        attendees_json=attendees,
        meeting_at=rec.meeting_at,
        recording_id=int(rec.fathom_recording_id) if rec.fathom_recording_id is not None else None,
    )


def ensure_pending_call_library_report(
    db: Session,
    org_id: uuid.UUID,
    fathom_record_id: uuid.UUID,
) -> Optional[CallLibraryReport]:
    """Create or refresh a pending library row so the UI can show 'Analyzing…' immediately."""
    _ensure_call_title_override_column(db)
    _ensure_call_media_columns(db)
    _ensure_call_deal_columns(db)
    _ensure_fathom_meeting_title_column(db)
    rec = (
        db.query(FathomCallRecord)
        .filter(
            FathomCallRecord.id == fathom_record_id,
            FathomCallRecord.org_id == org_id,
        )
        .first()
    )
    if not rec:
        return None

    row = (
        db.query(CallLibraryReport)
        .filter(CallLibraryReport.fathom_call_record_id == fathom_record_id)
        .first()
    )
    call_title = _derive_call_title(rec, db, org_id)
    if not row:
        row = CallLibraryReport(
            id=uuid.uuid4(),
            org_id=org_id,
            fathom_call_record_id=fathom_record_id,
            status="pending",
            call_title=call_title,
        )
        db.add(row)
    elif row.status == "complete":
        # Already analyzed — leave the finished report untouched.
        return row
    else:
        was_pending = row.status == "pending"
        row.status = "pending"
        row.failure_reason = None
        row.call_title = call_title or row.call_title
        # Do not bump updated_at when already pending — otherwise bulk re-ingest
        # resets the stuck-pending drain timer forever.
        if not was_pending:
            row.updated_at = datetime.now(timezone.utc)

    if row.status == "pending" and row.updated_at is None:
        row.updated_at = datetime.now(timezone.utc)

    row.recording_url = (rec.recording_url or "")[:2000] or None
    try:
        row.share_url = (getattr(rec, "share_url", None) or "")[:2000] or None
        row.video_url = (getattr(rec, "video_url", None) or "")[:2000] or None
    except Exception:
        pass
    row.attendees_json = rec.attendees_json
    if not row.updated_at:
        row.updated_at = datetime.now(timezone.utc)
    db.commit()
    db.refresh(row)
    return row


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
    _ensure_call_deal_columns(db)
    _ensure_fathom_meeting_title_column(db)
    existing = (
        db.query(CallLibraryReport)
        .filter(CallLibraryReport.fathom_call_record_id == fathom_record_id)
        .first()
    )
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
        if existing and existing.status == "pending":
            existing.status = "failed"
            existing.failure_reason = "orphan_fathom_record"
            existing.updated_at = datetime.now(timezone.utc)
            db.commit()
        return "skipped"

    transcript = rec.transcript_snippet or ""
    summary = rec.summary_text or ""
    call_title = _derive_call_title(rec, db, org_id)
    input_hash = _hash_call_inputs(summary, transcript)

    if existing and existing.status == "complete":
        prev_hash = None
        if isinstance(existing.report_json, dict):
            prev_hash = existing.report_json.get("_input_hash")
        if prev_hash == input_hash:
            if call_title and existing.call_title != call_title:
                existing.call_title = call_title
                existing.updated_at = datetime.now(timezone.utc)
                db.commit()
            return "skipped"

    if not transcript and not summary:
        # Content can be briefly absent during a Fathom re-ingest/enrichment window.
        # Never clobber an already-complete report with no_content — keep the good
        # report and let a later run refresh it once content is back.
        if existing and existing.status == "complete":
            logger.info(
                "call_library skip no_content: preserving complete report record=%s",
                fathom_record_id,
            )
            return "skipped"
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

    try:
        from app.services.resource_documents import get_document_content
        from app.services.call_library_ai import (
            DISCOVERY_AUDIT_SOP,
            PITCHING_SOP,
            OBJECTION_HANDLING_SOP,
        )

        discovery_sop = get_document_content(
            db, org_id, "discovery-audit-sop", fallback=DISCOVERY_AUDIT_SOP
        )
        pitching_sop = get_document_content(
            db, org_id, "pitching-sop", fallback=PITCHING_SOP
        )
        objection_sop = get_document_content(
            db, org_id, "objection-handling-sop", fallback=OBJECTION_HANDLING_SOP
        )
        report_json = generate_call_library_report(
            transcript=transcript,
            summary=summary,
            org_id=org_id,
            discovery_audit_sop=discovery_sop,
            pitching_sop=pitching_sop,
            objection_handling_sop=objection_sop,
        )
    except RuntimeError as e:
        if "llm_budget" in str(e).lower():
            _upsert_report(
                db,
                org_id,
                fathom_record_id,
                rec,
                "pending",
                None,
                "budget_deferred",
                call_title=call_title,
                call_score=None,
            )
            from app.services.call_library_queue import schedule_budget_retry

            schedule_budget_retry(str(org_id), str(fathom_record_id))
            logger.info("call_library budget deferred record=%s", fathom_record_id)
            return "deferred"
        report_json = None

    if isinstance(report_json, dict):
        report_json["_input_hash"] = input_hash

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

    # Hoist the closed-deal metric out of the LLM report so the list view can
    # filter / sort on it without re-parsing report_json. Reset on failed runs
    # so a previously closed deal does not stick around with stale data.
    deal = _coerce_deal_outcome(report_json) if status == "complete" else {
        "closed": False,
        "value_cents": None,
        "currency": None,
        "billing": None,
    }
    try:
        row.deal_closed = bool(deal["closed"])
        row.deal_value_cents = deal["value_cents"]
        row.deal_currency = deal["currency"] if deal["closed"] else None
        row.deal_billing = deal["billing"]
    except Exception as e:
        # Schema may still be migrating in older deploys; log and continue.
        logger.warning("call_library: persist deal_outcome failed: %s", e)

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
    _ensure_call_deal_columns(db)
    _ensure_fathom_meeting_title_column(db)
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

    items: List[Dict[str, Any]] = []
    for row in rows:
        fathom_rec = row.fathom_call_record

        attendees = row.attendees_json if row.attendees_json is not None else (
            fathom_rec.attendees_json if fathom_rec else None
        )
        client_name = primary_external_attendee_label(
            attendees if isinstance(attendees, list) else None
        )

        recording_url = row.recording_url or (fathom_rec.recording_url if fathom_rec else None)
        share_url = getattr(row, "share_url", None) or (getattr(fathom_rec, "share_url", None) if fathom_rec else None)
        video_url = getattr(row, "video_url", None) or (getattr(fathom_rec, "video_url", None) if fathom_rec else None)

        fathom_meeting_title = getattr(fathom_rec, "meeting_title", None) if fathom_rec else None
        derived_title = derive_call_library_title(
            meeting_title=fathom_meeting_title,
            attendees_json=attendees if isinstance(attendees, list) else None,
            meeting_at=fathom_rec.meeting_at if fathom_rec else None,
            recording_id=int(fathom_rec.fathom_recording_id) if fathom_rec and fathom_rec.fathom_recording_id is not None else None,
        )
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
                "deal_closed": bool(getattr(row, "deal_closed", False) or False),
                "deal_value_cents": getattr(row, "deal_value_cents", None),
                "deal_currency": getattr(row, "deal_currency", None),
                "deal_billing": getattr(row, "deal_billing", None),
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
    Re-run report generation for rows where the LLM failed or was budget-deferred.
    """
    _ensure_call_title_override_column(db)
    rows = (
        db.query(CallLibraryReport)
        .filter(
            CallLibraryReport.org_id == org_id,
            CallLibraryReport.status.in_(("failed", "pending")),
            CallLibraryReport.failure_reason.in_(("llm_failed", "budget_deferred")),
        )
        .all()
    )
    if not rows:
        return 0
    ids: List[uuid.UUID] = []
    for row in rows:
        row.status = "pending"
        row.failure_reason = None
        row.updated_at = datetime.now(timezone.utc)
        if row.fathom_call_record_id:
            ids.append(row.fathom_call_record_id)
    db.commit()
    from app.services.call_library_queue import schedule_call_library_reports

    schedule_call_library_reports(org_id, ids, background_tasks)
    return len(ids)


def requeue_stuck_pending_reports(
    db: Session,
    org_id: uuid.UUID,
    background_tasks: "BackgroundTasks",
) -> int:
    from app.services.call_library_queue import requeue_stuck_pending_reports as _requeue

    return _requeue(db, org_id, background_tasks)

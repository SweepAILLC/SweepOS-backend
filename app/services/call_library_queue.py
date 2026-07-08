"""Staggered Call Library LLM scheduling and recovery for stuck pending rows."""
from __future__ import annotations

import logging
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, List, Optional

from sqlalchemy.orm import Session

from app.core.config import settings
from app.models.call_library_report import CallLibraryReport

logger = logging.getLogger(__name__)

_org_drain_last: dict[str, float] = {}
_org_drain_lock = threading.Lock()
_DRAIN_COOLDOWN_SEC = 90.0


def _library_stagger_sec() -> float:
    return float(getattr(settings, "CALL_LIBRARY_STAGGER_SEC", 1.5) or 1.5)


def _stuck_pending_minutes() -> int:
    return int(getattr(settings, "CALL_LIBRARY_STUCK_PENDING_MINUTES", 8) or 8)


def schedule_call_library_reports(
    org_id: uuid.UUID,
    record_ids: List[uuid.UUID],
    background_tasks: Any | None,
    *,
    start_index: int = 0,
) -> int:
    """
    Queue LLM report jobs with stagger so we stay under per-org LLM budget (~45/min)
    without flooding BackgroundTasks/RQ.
    """
    if not record_ids:
        return 0
    from app.long_jobs import schedule_delayed_background_work
    from app.services.call_library_service import run_call_library_report_background

    oid = str(org_id)
    stagger = _library_stagger_sec()
    for i, rid in enumerate(record_ids):
        delay = max(0.0, (start_index + i) * stagger)
        schedule_delayed_background_work(
            run_call_library_report_background,
            background_tasks,
            delay,
            oid,
            str(rid),
        )
    logger.info(
        "call_library queued org=%s count=%s stagger_s=%s",
        org_id,
        len(record_ids),
        stagger,
    )
    return len(record_ids)


def find_stuck_pending_report_ids(db: Session, org_id: uuid.UUID) -> List[uuid.UUID]:
    """Pending rows older than threshold — job likely never ran or was starved."""
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=_stuck_pending_minutes())
    rows = (
        db.query(CallLibraryReport.fathom_call_record_id)
        .filter(
            CallLibraryReport.org_id == org_id,
            CallLibraryReport.status == "pending",
            CallLibraryReport.updated_at < cutoff,
        )
        .order_by(CallLibraryReport.updated_at.asc())
        .limit(int(getattr(settings, "CALL_LIBRARY_STUCK_REQUEUE_BATCH", 25) or 25))
        .all()
    )
    return [r[0] for r in rows if r[0] is not None]


def mark_orphan_pending_reports_failed(db: Session, org_id: uuid.UUID) -> int:
    """Pending rows whose fathom_call_record was removed cannot be analyzed."""
    from app.models.fathom_call_record import FathomCallRecord

    pending = (
        db.query(CallLibraryReport)
        .filter(
            CallLibraryReport.org_id == org_id,
            CallLibraryReport.status == "pending",
        )
        .all()
    )
    if not pending:
        return 0
    fathom_ids = {
        r[0]
        for r in db.query(FathomCallRecord.id)
        .filter(
            FathomCallRecord.org_id == org_id,
            FathomCallRecord.id.in_([p.fathom_call_record_id for p in pending if p.fathom_call_record_id]),
        )
        .all()
    }
    now = datetime.now(timezone.utc)
    n = 0
    for row in pending:
        if row.fathom_call_record_id and row.fathom_call_record_id in fathom_ids:
            continue
        row.status = "failed"
        row.failure_reason = "orphan_fathom_record"
        row.updated_at = now
        n += 1
    if n:
        db.commit()
        logger.info("call_library marked orphan pending failed org=%s count=%s", org_id, n)
    return n


def requeue_stuck_pending_reports(
    db: Session,
    org_id: uuid.UUID,
    background_tasks: Any | None,
) -> int:
    mark_orphan_pending_reports_failed(db, org_id)
    ids = find_stuck_pending_report_ids(db, org_id)
    if not ids:
        return 0
    for row in (
        db.query(CallLibraryReport)
        .filter(
            CallLibraryReport.org_id == org_id,
            CallLibraryReport.fathom_call_record_id.in_(ids),
        )
        .all()
    ):
        row.failure_reason = None
        row.updated_at = datetime.now(timezone.utc)
    db.commit()
    schedule_call_library_reports(org_id, ids, background_tasks)
    logger.info("call_library requeued stuck pending org=%s count=%s", org_id, len(ids))
    return len(ids)


def maybe_drain_stuck_pending_on_read(
    db: Session,
    org_id: uuid.UUID,
    background_tasks: Any | None,
) -> None:
    """Light self-heal when the Call Library tab is open (rate-limited per org)."""
    key = str(org_id)
    now = time.time()
    with _org_drain_lock:
        if now - _org_drain_last.get(key, 0.0) < _DRAIN_COOLDOWN_SEC:
            return
        _org_drain_last[key] = now
    try:
        n = requeue_stuck_pending_reports(db, org_id, background_tasks)
        if n:
            logger.info("call_library auto-drain on read org=%s requeued=%s", org_id, n)
    except Exception:
        logger.exception("call_library auto-drain failed org=%s", org_id)


def schedule_budget_retry(org_id_str: str, record_id_str: str) -> None:
    """Re-run a single report after LLM budget window rolls (≈65s)."""
    delay = float(getattr(settings, "CALL_LIBRARY_BUDGET_RETRY_SEC", 65) or 65)

    def _kick() -> None:
        from app.long_jobs import schedule_background_work
        from app.services.call_library_service import run_call_library_report_background

        schedule_background_work(
            run_call_library_report_background, None, org_id_str, record_id_str
        )

    t = threading.Timer(delay, _kick)
    t.daemon = True
    t.start()

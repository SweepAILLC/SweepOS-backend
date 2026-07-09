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
    return int(getattr(settings, "CALL_LIBRARY_STUCK_PENDING_MINUTES", 2) or 2)


def _ready_pending_seconds() -> int:
    return int(getattr(settings, "CALL_LIBRARY_READY_PENDING_SEC", 45) or 45)


def run_call_library_reports_batch_background(
    org_id_str: str,
    record_id_strs: List[str],
) -> None:
    """Process a batch of library reports in one RQ job (staggered sleep inside worker)."""
    from app.services.call_library_service import run_call_library_report_background

    stagger = _library_stagger_sec()
    for i, rid in enumerate(record_id_strs):
        if i > 0:
            time.sleep(stagger)
        try:
            run_call_library_report_background(org_id_str, rid)
        except Exception:
            logger.exception(
                "call_library batch item failed org=%s record=%s", org_id_str, rid
            )


def _max_batch_size() -> int:
    return int(getattr(settings, "CALL_LIBRARY_MAX_BATCH_SIZE", 12) or 12)


def schedule_call_library_reports(
    org_id: uuid.UUID,
    record_ids: List[uuid.UUID],
    background_tasks: Any | None,
    *,
    start_index: int = 0,
) -> int:
    """
    Queue LLM report jobs with stagger so we stay under per-org LLM budget (~45/min).

    In production (REDIS_URL + USE_RQ_LONG_JOBS), jobs are enqueued to the RQ worker
    instead of the web dyno — BackgroundTasks cannot run hour-long LLM batches reliably.
    """
    if not record_ids:
        return 0
    from app.db.session import SessionLocal
    from app.long_jobs import long_jobs_enabled, schedule_background_work
    from app.services.call_library_service import (
        filter_fathom_records_needing_library_analysis,
    )

    db = SessionLocal()
    try:
        record_ids = filter_fathom_records_needing_library_analysis(db, org_id, record_ids)
    finally:
        db.close()
    if not record_ids:
        return 0

    max_batch = _max_batch_size()
    if len(record_ids) > max_batch:
        logger.info(
            "call_library capping batch org=%s requested=%s max=%s",
            org_id,
            len(record_ids),
            max_batch,
        )
        record_ids = record_ids[:max_batch]

    oid = str(org_id)
    stagger = _library_stagger_sec()
    id_strs = [str(r) for r in record_ids]

    if start_index > 0:
        time.sleep(start_index * stagger)

    # Web dynos must not run long LLM batches; the worker process handles these in prod.
    tasks = None if long_jobs_enabled() else background_tasks

    llm_timeout = float(getattr(settings, "CALL_LIBRARY_LLM_TIMEOUT_SEC", 90) or 90)
    per_item = llm_timeout + stagger + 15.0
    job_timeout = int(len(record_ids) * per_item) + 180
    job_timeout = max(job_timeout, 600)
    job_timeout = min(job_timeout, 7200)

    schedule_background_work(
        run_call_library_reports_batch_background,
        tasks,
        oid,
        id_strs,
        prefer_rq=True,
        job_timeout=job_timeout,
    )

    logger.info(
        "call_library queued org=%s count=%s stagger_s=%s rq=%s batch=true",
        org_id,
        len(record_ids),
        stagger,
        long_jobs_enabled(),
    )
    return len(record_ids)


def find_pending_report_ids_with_content(
    db: Session,
    org_id: uuid.UUID,
    *,
    min_age_seconds: int = 0,
    limit: Optional[int] = None,
) -> List[uuid.UUID]:
    """Pending rows whose Fathom source call has summary/transcript to analyze."""
    from app.models.fathom_call_record import FathomCallRecord
    from sqlalchemy import or_

    q = (
        db.query(CallLibraryReport.fathom_call_record_id)
        .join(
            FathomCallRecord,
            FathomCallRecord.id == CallLibraryReport.fathom_call_record_id,
        )
        .filter(
            CallLibraryReport.org_id == org_id,
            CallLibraryReport.status == "pending",
            FathomCallRecord.org_id == org_id,
            or_(
                FathomCallRecord.summary_text.isnot(None),
                FathomCallRecord.transcript_snippet.isnot(None),
            ),
            or_(
                FathomCallRecord.summary_text != "",
                FathomCallRecord.transcript_snippet != "",
            ),
        )
    )
    if min_age_seconds > 0:
        cutoff = datetime.now(timezone.utc) - timedelta(seconds=min_age_seconds)
        q = q.filter(CallLibraryReport.updated_at < cutoff)
    q = q.order_by(CallLibraryReport.updated_at.asc())
    batch = limit or int(getattr(settings, "CALL_LIBRARY_STUCK_REQUEUE_BATCH", 25) or 25)
    rows = q.limit(batch).all()
    return [r[0] for r in rows if r[0] is not None]


def find_stuck_pending_report_ids(db: Session, org_id: uuid.UUID) -> List[uuid.UUID]:
    """Pending rows older than threshold — job likely never ran or was starved."""
    return find_pending_report_ids_with_content(
        db,
        org_id,
        min_age_seconds=_stuck_pending_minutes() * 60,
    )


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


def requeue_pending_reports(
    db: Session,
    org_id: uuid.UUID,
    background_tasks: Any | None,
    *,
    min_age_seconds: int = 0,
) -> int:
    mark_orphan_pending_reports_failed(db, org_id)
    ids = find_pending_report_ids_with_content(
        db,
        org_id,
        min_age_seconds=min_age_seconds,
    )
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
    db.commit()
    schedule_call_library_reports(org_id, ids, background_tasks)
    logger.info(
        "call_library requeued pending org=%s count=%s min_age_s=%s",
        org_id,
        len(ids),
        min_age_seconds,
    )
    return len(ids)


def requeue_stuck_pending_reports(
    db: Session,
    org_id: uuid.UUID,
    background_tasks: Any | None,
) -> int:
    return requeue_pending_reports(
        db,
        org_id,
        background_tasks,
        min_age_seconds=_stuck_pending_minutes() * 60,
    )


def maybe_drain_stuck_pending_on_read(
    db: Session,
    org_id: uuid.UUID,
    background_tasks: Any | None,
) -> None:
    """Optional self-heal when the Call Library tab is open (disabled by default)."""
    if not getattr(settings, "CALL_LIBRARY_AUTO_DRAIN_ON_READ", False):
        return
    key = str(org_id)
    now = time.time()
    with _org_drain_lock:
        if now - _org_drain_last.get(key, 0.0) < _DRAIN_COOLDOWN_SEC:
            return
        _org_drain_last[key] = now
    try:
        n = requeue_pending_reports(
            db,
            org_id,
            background_tasks,
            min_age_seconds=max(_stuck_pending_minutes() * 60, _ready_pending_seconds()),
        )
        if n:
            logger.info("call_library auto-drain on read org=%s requeued=%s", org_id, n)
    except Exception:
        logger.exception("call_library auto-drain failed org=%s", org_id)


def schedule_budget_retry(org_id_str: str, record_id_str: str) -> None:
    """Re-run a single report after LLM budget window rolls (≈65s)."""
    from app.long_jobs import schedule_delayed_background_work
    from app.services.call_library_service import run_call_library_report_background

    delay = float(getattr(settings, "CALL_LIBRARY_BUDGET_RETRY_SEC", 65) or 65)
    schedule_delayed_background_work(
        run_call_library_report_background,
        None,
        delay,
        org_id_str,
        record_id_str,
        prefer_rq=True,
    )


def drain_stuck_pending_all_orgs() -> int:
    """Worker-safe: re-queue pending library rows stuck past threshold (all orgs)."""
    from app.db.session import SessionLocal
    from app.models.call_library_report import CallLibraryReport

    cutoff = datetime.now(timezone.utc) - timedelta(minutes=_stuck_pending_minutes())
    db = SessionLocal()
    try:
        org_ids = {
            r[0]
            for r in db.query(CallLibraryReport.org_id)
            .filter(
                CallLibraryReport.status == "pending",
                CallLibraryReport.updated_at < cutoff,
            )
            .distinct()
            .all()
            if r[0] is not None
        }
        total = 0
        for org_id in org_ids:
            total += requeue_pending_reports(
                db,
                org_id,
                None,
                min_age_seconds=_stuck_pending_minutes() * 60,
            )
        return total
    finally:
        db.close()

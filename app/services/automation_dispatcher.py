"""Automation dispatcher: claim due jobs, render drafts, send via Brevo, persist outcome.

Designed to run inside ``app.worker`` (a separate process from the API). The API
never calls ``tick`` directly so a slow LLM or Brevo round-trip can't block a
request, and crashes in either system don't drop email — the next worker tick
re-claims jobs that were left ``sending``.

Concurrency model:
- ``SELECT ... FOR UPDATE SKIP LOCKED`` claims a small batch of due jobs in a
  single transaction.
- Each job's render+send is performed in its own transaction. State transitions
  are: ``scheduled``/``ready``/``awaiting_approval`` -> ``sending`` -> ``sent``
  / ``failed`` / ``skipped``.
- Recovery: on worker boot ``recover_all_sending_on_boot`` resets every
  ``sending`` row; during ticks ``recover_in_flight`` resets only stale ones
  so multiple replicas don't double-claim.
"""
from __future__ import annotations

import logging
import os
import socket
import time
import uuid
from datetime import datetime, timedelta
from typing import List, Optional

from sqlalchemy import and_, or_, text
from sqlalchemy.orm import Session

from app.core.config import settings
from app.models.automation import (
    AutomationEmailJob,
    AutomationRule,
    AutomationWorkerHeartbeat,
    JobState,
)
from app.models.client import Client
from app.services.automation_drafts import (
    build_automation_email_draft,
    resolve_sender_for_org,
)
from app.services.brevo_client import (
    BrevoNotConnectedError,
    BrevoSendError,
    get_brevo_auth_headers,
    send_email,
)

LOG = logging.getLogger(__name__)


# Tunables
MAX_ATTEMPTS = 5
BATCH_SIZE = 8
STALE_SENDING_AFTER_SECONDS = 10 * 60  # if "sending" longer than this, recover it
APPROVAL_DEFAULT_TTL_HOURS = 72  # used when rule.approval_ttl_hours is None
HEARTBEAT_HEALTHY_AFTER_SECONDS = 90


# ---------------------------------------------------------------------------
# Heartbeat
# ---------------------------------------------------------------------------

def write_heartbeat(db: Session) -> None:
    row = (
        db.query(AutomationWorkerHeartbeat)
        .filter(AutomationWorkerHeartbeat.id == 1)
        .with_for_update()
        .first()
    )
    if row is None:
        row = AutomationWorkerHeartbeat(id=1)
        db.add(row)

    queue_depth = (
        db.query(AutomationEmailJob)
        .filter(AutomationEmailJob.state.in_((JobState.SCHEDULED.value, JobState.READY.value)))
        .count()
    )
    in_flight = (
        db.query(AutomationEmailJob)
        .filter(AutomationEmailJob.state == JobState.SENDING.value)
        .count()
    )
    awaiting = (
        db.query(AutomationEmailJob)
        .filter(AutomationEmailJob.state == JobState.AWAITING_APPROVAL.value)
        .count()
    )

    row.last_tick_at = datetime.utcnow()
    row.worker_pid = os.getpid()
    try:
        row.worker_host = socket.gethostname()[:255]
    except Exception:
        row.worker_host = None
    row.queue_depth = queue_depth
    row.in_flight = in_flight
    row.awaiting_approval = awaiting
    db.commit()


def read_dispatcher_health(db: Session) -> dict:
    row = (
        db.query(AutomationWorkerHeartbeat)
        .filter(AutomationWorkerHeartbeat.id == 1)
        .first()
    )
    if not row:
        return {
            "healthy": False,
            "last_tick_at": None,
            "seconds_since_tick": None,
            "worker_pid": None,
            "worker_host": None,
            "queue_depth": 0,
            "in_flight": 0,
            "awaiting_approval": 0,
            "rq_enabled": bool(settings.REDIS_URL and settings.USE_RQ_LONG_JOBS),
            "notes": "no heartbeat row yet — worker has never run",
        }
    elapsed = int((datetime.utcnow() - row.last_tick_at).total_seconds())
    return {
        "healthy": elapsed < HEARTBEAT_HEALTHY_AFTER_SECONDS,
        "last_tick_at": row.last_tick_at,
        "seconds_since_tick": elapsed,
        "worker_pid": row.worker_pid,
        "worker_host": row.worker_host,
        "queue_depth": int(row.queue_depth or 0),
        "in_flight": int(row.in_flight or 0),
        "awaiting_approval": int(row.awaiting_approval or 0),
        "rq_enabled": bool(settings.REDIS_URL and settings.USE_RQ_LONG_JOBS),
        "notes": None if elapsed < HEARTBEAT_HEALTHY_AFTER_SECONDS else "Worker heartbeat is stale.",
    }


# ---------------------------------------------------------------------------
# Recovery
# ---------------------------------------------------------------------------

def _reset_sending_rows(db: Session, *, only_if_last_attempt_before: Optional[datetime] = None) -> int:
    """Move ``sending`` rows back to ``scheduled`` so the dispatcher can reclaim them."""
    q = db.query(AutomationEmailJob).filter(
        AutomationEmailJob.state == JobState.SENDING.value,
    )
    if only_if_last_attempt_before is not None:
        q = q.filter(
            AutomationEmailJob.last_attempt_at.isnot(None),
            AutomationEmailJob.last_attempt_at < only_if_last_attempt_before,
        )
    result = q.update(
        {
            "state": JobState.SCHEDULED.value,
            "updated_at": datetime.utcnow(),
        },
        synchronize_session=False,
    )
    if result:
        db.commit()
    return int(result or 0)


def recover_all_sending_on_boot(db: Session) -> int:
    """On worker start, reclaim every in-flight row.

    Safe for the typical single-worker Render deployment. A restarted process
    cannot still be sending mail from the previous PID.
    """
    return _reset_sending_rows(db)


def recover_in_flight(db: Session) -> int:
    """Reset stale ``sending`` rows during normal ticks.

    Conservative: only resets rows older than STALE_SENDING_AFTER_SECONDS so two
    worker replicas running in parallel don't double-claim. Returns count reset.
    """
    cutoff = datetime.utcnow() - timedelta(seconds=STALE_SENDING_AFTER_SECONDS)
    return _reset_sending_rows(db, only_if_last_attempt_before=cutoff)


def expire_awaiting_approval(db: Session) -> int:
    """Move ``awaiting_approval`` rows past their TTL to ``skipped``."""
    now = datetime.utcnow()
    rows = (
        db.query(AutomationEmailJob, AutomationRule)
        .outerjoin(AutomationRule, AutomationRule.id == AutomationEmailJob.rule_id)
        .filter(AutomationEmailJob.state == JobState.AWAITING_APPROVAL.value)
        .all()
    )
    expired = 0
    for job, rule in rows:
        ttl = APPROVAL_DEFAULT_TTL_HOURS
        if rule and rule.approval_ttl_hours:
            ttl = int(rule.approval_ttl_hours)
        if (now - job.created_at) > timedelta(hours=ttl):
            job.state = JobState.SKIPPED.value
            job.error_text = f"Approval TTL ({ttl}h) elapsed without action"
            job.updated_at = now
            expired += 1
    if expired:
        db.commit()
    return expired


# ---------------------------------------------------------------------------
# Claim + send loop
# ---------------------------------------------------------------------------

def _claim_due(db: Session, batch_size: int = BATCH_SIZE) -> List[AutomationEmailJob]:
    """Atomically claim up to ``batch_size`` due jobs (state=scheduled|ready, scheduled_at<=now).

    Uses Postgres ``FOR UPDATE SKIP LOCKED`` so multiple worker processes can
    coexist safely.
    """
    sql = text(
        """
        WITH due AS (
            SELECT id FROM automation_email_jobs
            WHERE state IN ('scheduled', 'ready')
              AND scheduled_at <= now()
            ORDER BY scheduled_at ASC, created_at ASC
            FOR UPDATE SKIP LOCKED
            LIMIT :batch_size
        )
        UPDATE automation_email_jobs aej
        SET state = 'sending',
            attempts = aej.attempts + 1,
            last_attempt_at = now(),
            updated_at = now()
        FROM due
        WHERE aej.id = due.id
        RETURNING aej.id
        """
    )
    rows = db.execute(sql, {"batch_size": int(batch_size)}).fetchall()
    db.commit()
    if not rows:
        return []

    ids = [r[0] for r in rows]
    return (
        db.query(AutomationEmailJob)
        .filter(AutomationEmailJob.id.in_(ids))
        .all()
    )


def _backoff_seconds(attempts: int) -> int:
    # 30s, 2m, 8m, 30m, 2h capped
    base = 30
    return min(int(base * (4 ** max(0, attempts - 1))), 2 * 60 * 60)


def _process_one(db: Session, job: AutomationEmailJob) -> None:
    rule = (
        db.query(AutomationRule).filter(AutomationRule.id == job.rule_id).first()
        if job.rule_id
        else None
    )
    if rule is None or not rule.enabled:
        job.state = JobState.SKIPPED.value
        job.error_text = "Rule missing or disabled at send time"
        job.updated_at = datetime.utcnow()
        db.commit()
        return

    client = (
        db.query(Client)
        .filter(Client.id == job.client_id, Client.org_id == job.org_id)
        .first()
    )
    if not client:
        job.state = JobState.SKIPPED.value
        job.error_text = "Client no longer exists"
        job.updated_at = datetime.utcnow()
        db.commit()
        return

    if not (client.email and "@" in client.email):
        job.state = JobState.SKIPPED.value
        job.error_text = "Client has no usable email"
        job.updated_at = datetime.utcnow()
        db.commit()
        return

    insight_id = None
    if isinstance(job.payload_json, dict):
        raw_iid = job.payload_json.get("insight_id")
        if raw_iid:
            try:
                insight_id = uuid.UUID(str(raw_iid))
            except (ValueError, TypeError):
                insight_id = None

    try:
        draft = build_automation_email_draft(
            db,
            rule=rule,
            client=client,
            insight_id=insight_id,
        )
    except Exception as e:  # noqa: BLE001 - any draft failure should retry/backoff
        LOG.exception("draft build failed for job %s", job.id)
        _record_failure(db, job, f"draft error: {e}")
        return

    sender = resolve_sender_for_org(db, job.org_id)

    try:
        headers = get_brevo_auth_headers(db, job.org_id)
    except BrevoNotConnectedError as e:
        LOG.warning("Brevo not connected for org %s; skipping job %s", job.org_id, job.id)
        job.state = JobState.SKIPPED.value
        job.error_text = f"brevo_not_connected: {e}"
        job.updated_at = datetime.utcnow()
        db.commit()
        return

    try:
        resp = send_email(
            headers=headers,
            sender=sender,
            to=[{"email": client.email, "name": ((client.first_name or "") + " " + (client.last_name or "")).strip() or None}],
            subject=draft.subject,
            html_content=draft.html,
            text_content=draft.body_plain,
            tags=[f"automation:{rule.playbook}"],
            idempotency_key=f"{job.org_id}:{job.idempotency_key}",
        )
    except BrevoSendError as e:
        if e.retryable and job.attempts < MAX_ATTEMPTS:
            _schedule_retry(db, job, str(e))
        else:
            _record_failure(db, job, str(e))
        return

    job.state = JobState.SENT.value
    job.dispatched_at = datetime.utcnow()
    job.updated_at = datetime.utcnow()
    job.error_text = None
    if isinstance(resp, dict):
        msg_id = resp.get("messageId") or resp.get("messageIds")
        if isinstance(msg_id, list) and msg_id:
            msg_id = msg_id[0]
        if msg_id:
            job.brevo_message_id = str(msg_id)[:200]
    db.commit()


def _schedule_retry(db: Session, job: AutomationEmailJob, error_text: str) -> None:
    delay = _backoff_seconds(job.attempts)
    job.state = JobState.SCHEDULED.value
    job.scheduled_at = datetime.utcnow() + timedelta(seconds=delay)
    job.error_text = error_text[:2000]
    job.updated_at = datetime.utcnow()
    db.commit()


def _record_failure(db: Session, job: AutomationEmailJob, error_text: str) -> None:
    job.state = JobState.FAILED.value
    job.error_text = error_text[:2000]
    job.updated_at = datetime.utcnow()
    db.commit()


# ---------------------------------------------------------------------------
# Public tick
# ---------------------------------------------------------------------------

def tick(db: Session, *, batch_size: int = BATCH_SIZE) -> int:
    """Process up to ``batch_size`` due jobs. Returns count attempted."""
    expire_awaiting_approval(db)
    recover_in_flight(db)
    jobs = _claim_due(db, batch_size=batch_size)
    for job in jobs:
        try:
            _process_one(db, job)
        except Exception as e:  # noqa: BLE001 - keep the loop alive
            LOG.exception("dispatcher unhandled error on job %s", job.id)
            try:
                if job.attempts < MAX_ATTEMPTS:
                    _schedule_retry(db, job, f"unhandled: {e}")
                else:
                    _record_failure(db, job, f"unhandled: {e}")
            except Exception:
                db.rollback()
    return len(jobs)

"""Persist inbound webhooks, then process with bounded retries.

Used by calendar + payment webhook handlers and the worker dispatcher.
Idempotent on (org_id, provider, event_id).
"""
from __future__ import annotations

import logging
import uuid
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, Optional, Tuple

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.models.inbound_webhook_event import InboundWebhookEvent

LOG = logging.getLogger("app.inbound_webhook_inbox")

STATUS_PENDING = "pending"
STATUS_PROCESSING = "processing"
STATUS_DONE = "done"
STATUS_FAILED = "failed"
MAX_ATTEMPTS = 8
PROCESS_LEASE_SEC = 300


def _backoff_seconds(attempts: int) -> int:
    return min(int(30 * (3 ** max(0, attempts - 1))), 2 * 60 * 60)


def record_inbound_event(
    db: Session,
    *,
    org_id: uuid.UUID,
    provider: str,
    event_id: str,
    event_type: Optional[str],
    payload: Dict[str, Any],
) -> Tuple[InboundWebhookEvent, bool]:
    """Insert or return existing row. Returns (row, is_new)."""
    event_id = (event_id or "").strip()[:255]
    if not event_id:
        event_id = f"anon-{uuid.uuid4()}"
    existing = (
        db.query(InboundWebhookEvent)
        .filter(
            InboundWebhookEvent.org_id == org_id,
            InboundWebhookEvent.provider == provider,
            InboundWebhookEvent.event_id == event_id,
        )
        .first()
    )
    if existing:
        return existing, False
    row = InboundWebhookEvent(
        org_id=org_id,
        provider=provider,
        event_id=event_id,
        event_type=(event_type or "")[:128] or None,
        payload=payload,
        status=STATUS_PENDING,
        attempts=0,
        received_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
    )
    db.add(row)
    try:
        db.commit()
        db.refresh(row)
        return row, True
    except IntegrityError:
        db.rollback()
        existing = (
            db.query(InboundWebhookEvent)
            .filter(
                InboundWebhookEvent.org_id == org_id,
                InboundWebhookEvent.provider == provider,
                InboundWebhookEvent.event_id == event_id,
            )
            .first()
        )
        if existing:
            return existing, False
        raise


def mark_inbound_done(db: Session, row: InboundWebhookEvent) -> None:
    row.status = STATUS_DONE
    row.processed_at = datetime.utcnow()
    row.next_attempt_at = None
    row.error_text = None
    row.updated_at = datetime.utcnow()
    db.commit()


def mark_inbound_retry(db: Session, row: InboundWebhookEvent, error: str) -> None:
    row.attempts = int(row.attempts or 0) + 1
    row.error_text = (error or "unknown")[:2000]
    row.updated_at = datetime.utcnow()
    if row.attempts >= MAX_ATTEMPTS:
        row.status = STATUS_FAILED
        row.next_attempt_at = None
    else:
        row.status = STATUS_PENDING
        row.next_attempt_at = datetime.utcnow() + timedelta(seconds=_backoff_seconds(row.attempts))
    db.commit()


def process_recorded_event(
    db: Session,
    row: InboundWebhookEvent,
    processor: Callable[[Session, uuid.UUID, Dict[str, Any]], Any],
) -> bool:
    """Run processor; mark done or schedule retry. Returns True on success."""
    if row.status == STATUS_DONE:
        return True
    payload = row.payload if isinstance(row.payload, dict) else {}
    row.status = STATUS_PROCESSING
    row.updated_at = datetime.utcnow()
    # Lease: claim_due_inbound_events re-selects "processing" rows once due, so a
    # second worker instance must not grab this row while it is being processed.
    row.next_attempt_at = datetime.utcnow() + timedelta(seconds=PROCESS_LEASE_SEC)
    db.commit()
    try:
        processor(db, row.org_id, payload)
        mark_inbound_done(db, row)
        return True
    except Exception as exc:
        LOG.exception(
            "inbound webhook process failed provider=%s event=%s org=%s",
            row.provider,
            row.event_id,
            row.org_id,
        )
        try:
            db.rollback()
        except Exception:
            pass
        fresh = db.query(InboundWebhookEvent).filter(InboundWebhookEvent.id == row.id).first()
        if fresh:
            mark_inbound_retry(db, fresh, str(exc))
        return False


def claim_due_inbound_events(db: Session, *, limit: int = 20, exclude_providers: Tuple[str, ...] = ()) -> list:
    now = datetime.utcnow()
    q = db.query(InboundWebhookEvent).filter(
        InboundWebhookEvent.status.in_((STATUS_PENDING, STATUS_PROCESSING)),
        (InboundWebhookEvent.next_attempt_at.is_(None))
        | (InboundWebhookEvent.next_attempt_at <= now),
    )
    if exclude_providers:
        q = q.filter(~InboundWebhookEvent.provider.in_(exclude_providers))
    return (
        q.order_by(InboundWebhookEvent.received_at.asc())
        .limit(limit)
        .with_for_update(skip_locked=True)
        .all()
    )


def flush_due_inbound_webhooks(db: Session, *, limit: Optional[int] = None) -> int:
    """Worker entry: retry due calendar/payment inbox rows."""
    from app.core.config import settings
    from app.api.calendar_webhooks import (
        process_calcom_webhook_payload,
        process_calendly_webhook_payload,
    )
    from app.api.ghl_webhooks import process_ghl_webhook_payload
    from app.api.webhooks import process_whop_webhook_payload
    from app.services.funnel_webhooks import PROVIDER as FUNNEL_WEBHOOK_PROVIDER
    from app.services.ghl_lead_sync import PROVIDER as GHL_SYNC_PROVIDER, process_submission_payload

    if limit is None:
        limit = int(getattr(settings, "INBOUND_WEBHOOK_FLUSH_LIMIT", 50) or 50)

    processors = {
        "calcom": process_calcom_webhook_payload,
        "calendly": process_calendly_webhook_payload,
        "whop": process_whop_webhook_payload,
        "stripe": _process_stripe_inbox,
        "ghl": process_ghl_webhook_payload,
        GHL_SYNC_PROVIDER: process_submission_payload,
    }
    attempted = 0
    try:
        # Funnel webhooks have their own drainer threads (funnel_webhooks.drain_due)
        # so a lead burst never stalls this dispatcher tick.
        rows = claim_due_inbound_events(db, limit=limit, exclude_providers=(FUNNEL_WEBHOOK_PROVIDER,))
    except Exception:
        LOG.exception("claim due inbound webhooks failed")
        db.rollback()
        return 0
    for row in rows:
        fn = processors.get(row.provider)
        if fn is None:
            mark_inbound_retry(db, row, f"unknown provider {row.provider}")
            continue
        process_recorded_event(db, row, fn)
        attempted += 1
    return attempted


def _process_stripe_inbox(db: Session, org_id: uuid.UUID, payload: Dict[str, Any]) -> None:
    from app.services.stripe_processor import process_stripe_event

    process_stripe_event(db, payload, org_id)


def retry_unprocessed_stripe_events(db: Session, *, limit: int = 10) -> int:
    """Replay StripeEvent rows that landed but never finished processing."""
    from app.models.stripe_event import StripeEvent
    from app.services.stripe_processor import process_stripe_event

    rows = (
        db.query(StripeEvent)
        .filter(StripeEvent.processed.is_(False))
        .order_by(StripeEvent.received_at.asc())
        .limit(limit)
        .all()
    )
    n = 0
    for row in rows:
        payload = row.payload if isinstance(row.payload, dict) else {}
        try:
            process_stripe_event(db, payload, row.org_id)
            row.processed = True
            row.processed_at = datetime.utcnow()
            db.commit()
            n += 1
        except Exception:
            LOG.exception("stripe event retry failed id=%s", row.stripe_event_id)
            try:
                db.rollback()
            except Exception:
                pass
    return n

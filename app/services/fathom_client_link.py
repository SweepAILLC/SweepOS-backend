"""Link Fathom call records to pipeline clients by attendee email (two-way sync)."""
from __future__ import annotations

import logging
import re
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

from sqlalchemy.orm import Session

from app.models.client import Client, find_client_by_email
from app.models.fathom_call_record import FathomCallRecord
from app.services.fathom_ingest import apply_sentiment_to_record
from app.services.health_score_cache_service import invalidate_health_score_cache

logger = logging.getLogger(__name__)


def _norm_email(e: Optional[str]) -> Optional[str]:
    if not e or not isinstance(e, str):
        return None
    return re.sub(r"\s+", "", e.lower().strip()) or None


def external_attendee_emails(attendees_json: Any) -> Set[str]:
    """External (non-team) attendee emails stored on a Fathom call record."""
    out: Set[str] = set()
    if not isinstance(attendees_json, list):
        return out
    for item in attendees_json:
        if not isinstance(item, dict) or item.get("is_team_member"):
            continue
        ne = _norm_email(item.get("email"))
        if ne:
            out.add(ne)
    return out


def find_client_id_for_attendee_emails(
    db: Session,
    org_id: uuid.UUID,
    emails: Set[str],
) -> Optional[uuid.UUID]:
    for email in emails:
        client = find_client_by_email(db, org_id, email)
        if client:
            return client.id
    return None


def find_client_id_for_fathom_record(
    db: Session,
    org_id: uuid.UUID,
    record: FathomCallRecord,
) -> Optional[uuid.UUID]:
    if record.client_id:
        return record.client_id
    emails = external_attendee_emails(record.attendees_json)
    return find_client_id_for_attendee_emails(db, org_id, emails) if emails else None


def _apply_client_link_to_record(
    db: Session,
    org_id: uuid.UUID,
    record: FathomCallRecord,
    client_id: uuid.UUID,
) -> None:
    record.client_id = client_id
    if record.sentiment_status != "complete":
        apply_sentiment_to_record(db, record)
    invalidate_health_score_cache(db, client_id, org_id, do_commit=False)
    record.updated_at = datetime.now(timezone.utc)


def relink_fathom_records_for_client(
    db: Session,
    org_id: uuid.UUID,
    client: Client,
) -> List[uuid.UUID]:
    """
    When a client is created or their email(s) change, attach any orphan Fathom calls
    whose external attendee emails match.
    """
    client_emails = client.get_all_emails_normalized()
    if not client_emails:
        return []

    orphans = (
        db.query(FathomCallRecord)
        .filter(
            FathomCallRecord.org_id == org_id,
            FathomCallRecord.client_id.is_(None),
        )
        .all()
    )
    linked: List[uuid.UUID] = []
    for rec in orphans:
        if not external_attendee_emails(rec.attendees_json) & client_emails:
            continue
        _apply_client_link_to_record(db, org_id, rec, client.id)
        linked.append(rec.id)

    if linked:
        logger.info(
            "fathom relink client=%s org=%s linked_records=%s",
            client.id,
            org_id,
            len(linked),
        )
    return linked


def relink_orphan_fathom_records_for_org(
    db: Session,
    org_id: uuid.UUID,
) -> List[Tuple[uuid.UUID, uuid.UUID]]:
    """
    Scan all unlinked Fathom calls in the org and attach them when a pipeline client
    now matches an attendee email (e.g. client added after initial Fathom sync).
    """
    orphans = (
        db.query(FathomCallRecord)
        .filter(
            FathomCallRecord.org_id == org_id,
            FathomCallRecord.client_id.is_(None),
        )
        .all()
    )
    linked: List[Tuple[uuid.UUID, uuid.UUID]] = []
    for rec in orphans:
        client_id = find_client_id_for_fathom_record(db, org_id, rec)
        if not client_id:
            continue
        _apply_client_link_to_record(db, org_id, rec, client_id)
        linked.append((rec.id, client_id))

    if linked:
        logger.info("fathom relink org=%s linked_records=%s", org_id, len(linked))
    return linked


def queue_fathom_relink_followups(
    background_tasks: Any,
    org_id: uuid.UUID,
    fathom_record_ids: List[uuid.UUID],
) -> None:
    """Run client-scoped call insight + refresh library report after a late link."""
    if not fathom_record_ids:
        return
    from app.long_jobs import schedule_background_work
    from app.services.call_insight_service import run_call_insight_background
    from app.services.call_library_queue import schedule_call_library_reports

    oid = str(org_id)
    for rid in fathom_record_ids:
        schedule_background_work(run_call_insight_background, background_tasks, oid, str(rid))
    schedule_call_library_reports(org_id, fathom_record_ids, background_tasks)


def relink_fathom_for_client_and_queue(
    db: Session,
    org_id: uuid.UUID,
    client: Client,
    background_tasks: Any = None,
) -> int:
    """Relink orphan calls for one client, commit, and queue insight jobs."""
    linked = relink_fathom_records_for_client(db, org_id, client)
    if not linked:
        return 0
    db.commit()
    queue_fathom_relink_followups(background_tasks, org_id, [rec_id for rec_id, _ in linked])
    return len(linked)

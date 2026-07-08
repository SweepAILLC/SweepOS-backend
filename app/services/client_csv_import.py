"""Bulk CSV/JSON import of clients into the pipeline board."""
from __future__ import annotations

import re
import uuid
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

from sqlalchemy.orm import Session

from app.models.client import Client, LifecycleState


def _normalize_email(email: str) -> str:
    return re.sub(r"\s+", "", email.lower().strip())


def _build_email_index(
    db: Session, org_id: uuid.UUID
) -> Dict[str, Client]:
    """Pre-load all org clients into a normalized-email → Client map."""
    from sqlalchemy import or_

    clients = (
        db.query(Client)
        .filter(
            Client.org_id == org_id,
            or_(Client.email.isnot(None), Client.emails.isnot(None)),
        )
        .all()
    )
    index: Dict[str, Client] = {}
    for c in clients:
        for em in c.get_all_emails_normalized():
            index[em] = c
    return index


def import_clients(
    db: Session,
    org_id: uuid.UUID,
    rows: list,
    *,
    default_pipeline_column: LifecycleState = LifecycleState.QUALIFIED,
    run_lifecycle_reconcile: bool = True,
    source_filename: Optional[str] = None,
) -> dict:
    """
    Upsert client rows by email.  New clients get the chosen column; existing
    clients keep their column and only empty fields are filled.

    Returns a summary dict matching ClientImportResponse.
    """
    from app.services.client_automation import apply_automatic_lifecycle_for_client

    batch_id = str(uuid.uuid4())
    now_iso = datetime.now(timezone.utc).isoformat()

    email_index = _build_email_index(db, org_id)

    created_count = 0
    updated_count = 0
    skipped_count = 0
    failed_rows: list = []
    touched_clients: List[Client] = []

    seen_emails: set = set()

    for idx, row in enumerate(rows):
        raw_email = (getattr(row, "email", None) or "").strip()
        if not raw_email or "@" not in raw_email:
            skipped_count += 1
            continue

        norm = _normalize_email(raw_email)
        if norm in seen_emails:
            skipped_count += 1
            continue
        seen_emails.add(norm)

        target_column = getattr(row, "pipeline_column", None) or default_pipeline_column

        try:
            existing = email_index.get(norm)
            if existing:
                changed = _merge_into_existing(existing, row)
                if changed:
                    existing.updated_at = datetime.utcnow()
                    db.flush()
                    updated_count += 1
                else:
                    updated_count += 1
                touched_clients.append(existing)
            else:
                client = _create_new_client(
                    db,
                    org_id,
                    row,
                    target_column,
                    batch_id=batch_id,
                    now_iso=now_iso,
                    source_filename=source_filename,
                )
                db.flush()
                for em in client.get_all_emails_normalized():
                    email_index[em] = client
                created_count += 1
                touched_clients.append(client)
        except Exception as exc:
            failed_rows.append(
                {"row_index": idx, "email": raw_email, "error": str(exc)}
            )
            try:
                db.rollback()
            except Exception:
                pass
            email_index = _build_email_index(db, org_id)

    try:
        db.commit()
    except Exception as commit_err:
        db.rollback()
        return {
            "success": False,
            "created_count": 0,
            "updated_count": 0,
            "skipped_count": skipped_count,
            "failed_count": len(rows),
            "failed_rows": [{"row_index": 0, "email": "", "error": str(commit_err)}],
            "imported_client_ids": [],
            "lifecycle_adjusted_count": 0,
        }

    lifecycle_adjusted = 0
    if run_lifecycle_reconcile and touched_clients:
        for client in touched_clients:
            try:
                if apply_automatic_lifecycle_for_client(db, client):
                    lifecycle_adjusted += 1
            except Exception:
                pass
        try:
            db.commit()
        except Exception:
            db.rollback()

    _relink_fathom_batch(db, org_id, [c for c in touched_clients if c.id])

    return {
        "success": True,
        "created_count": created_count,
        "updated_count": updated_count,
        "skipped_count": skipped_count,
        "failed_count": len(failed_rows),
        "failed_rows": failed_rows,
        "imported_client_ids": [str(c.id) for c in touched_clients],
        "lifecycle_adjusted_count": lifecycle_adjusted,
    }


def _create_new_client(
    db: Session,
    org_id: uuid.UUID,
    row,
    target_column: LifecycleState,
    *,
    batch_id: str,
    now_iso: str,
    source_filename: Optional[str],
) -> Client:
    first_name = (getattr(row, "first_name", None) or "").strip() or None
    last_name = (getattr(row, "last_name", None) or "").strip() or None
    phone = (getattr(row, "phone", None) or "").strip() or None
    instagram = (getattr(row, "instagram", None) or "").strip() or None
    notes = (getattr(row, "notes", None) or "").strip() or None

    import_note = f"Imported from CSV on {now_iso[:10]}"
    if notes:
        notes = f"{notes}\n{import_note}"
    else:
        notes = import_note

    meta = {
        "csv_import": {
            "batch_id": batch_id,
            "imported_at": now_iso,
            "default_column": target_column.value if isinstance(target_column, LifecycleState) else str(target_column),
            "source_filename": source_filename,
        }
    }

    client = Client(
        org_id=org_id,
        email=row.email.strip(),
        first_name=first_name,
        last_name=last_name,
        phone=phone,
        instagram=instagram,
        lifecycle_state=target_column,
        notes=notes,
        meta=meta,
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
    )

    program_start = getattr(row, "program_start_date", None)
    program_days = getattr(row, "program_duration_days", None)
    if program_start:
        client.program_start_date = program_start
        if program_days and int(program_days) > 0:
            client.program_duration_days = int(program_days)
        client.update_program_dates()
        client.program_progress_percent = client.calculate_progress()

    db.add(client)
    return client


def _merge_into_existing(client: Client, row) -> bool:
    """Fill empty fields on an existing client. Returns True if anything changed."""
    changed = False

    if not client.first_name and getattr(row, "first_name", None):
        client.first_name = row.first_name.strip()
        changed = True

    if not client.last_name and getattr(row, "last_name", None):
        client.last_name = row.last_name.strip()
        changed = True

    if not client.phone and getattr(row, "phone", None):
        client.phone = row.phone.strip()
        changed = True

    if not client.instagram and getattr(row, "instagram", None):
        client.instagram = row.instagram.strip()
        changed = True

    csv_notes = (getattr(row, "notes", None) or "").strip()
    if csv_notes:
        existing_notes = (client.notes or "").strip()
        if csv_notes not in existing_notes:
            client.notes = f"{existing_notes}\n{csv_notes}".strip() if existing_notes else csv_notes
            changed = True

    if not client.program_start_date and getattr(row, "program_start_date", None):
        client.program_start_date = row.program_start_date
        days = getattr(row, "program_duration_days", None)
        if days and int(days) > 0:
            client.program_duration_days = int(days)
        client.update_program_dates()
        client.program_progress_percent = client.calculate_progress()
        changed = True

    return changed


def _relink_fathom_batch(
    db: Session, org_id: uuid.UUID, clients: List[Client]
) -> None:
    """Best-effort Fathom relink for newly created clients."""
    try:
        from app.services.fathom_client_link import relink_fathom_for_client_and_queue

        for client in clients:
            try:
                relink_fathom_for_client_and_queue(db, org_id, client)
            except Exception:
                pass
    except ImportError:
        pass

"""
Client lifecycle automation service.

Handles pipeline stage transitions (funnel → qualified → booked → nurturing → cold),
payment → active, and program progress → offboarding → dead.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Optional
import uuid

from sqlalchemy import and_, or_
from sqlalchemy.orm import Session
from sqlalchemy.orm.attributes import flag_modified

from app.models.client import (
    Client,
    LifecycleState,
    LEAD_PIPELINE_LIFECYCLE_STATES,
    PRE_PAYMENT_LIFECYCLE_STATES,
)

DEFAULT_FOLLOW_UP_DAYS = 14
META_FOLLOW_UP_DUE_AT = "follow_up_due_at"
META_LIFECYCLE_MANUAL_AT = "lifecycle_manual_at"
META_LIFECYCLE_MANUAL_STAGE = "lifecycle_manual_stage"

LEGACY_LIFECYCLE_ALIASES = {
    "warm_lead": LifecycleState.BOOKED,
}


def resolve_lifecycle_state(raw) -> LifecycleState:
    """Accept enum, column id, or legacy DB values (e.g. warm_lead → booked)."""
    if isinstance(raw, LifecycleState):
        return raw
    key = str(raw).strip().lower()
    if key in LEGACY_LIFECYCLE_ALIASES:
        return LEGACY_LIFECYCLE_ALIASES[key]
    return LifecycleState(key)


def is_manual_lifecycle_protected(client: Client, *, now: Optional[datetime] = None) -> bool:
    """True when an operator recently moved this card — skip automated stage overrides."""
    meta = client.meta if isinstance(client.meta, dict) else {}
    raw = meta.get(META_LIFECYCLE_MANUAL_AT)
    if not raw or not isinstance(raw, str):
        return False
    try:
        s = raw.replace("Z", "+00:00") if raw.endswith("Z") else raw
        manual_at = _as_naive_utc(datetime.fromisoformat(s))
    except (ValueError, TypeError):
        return False
    if manual_at is None:
        return False
    now_naive = _as_naive_utc(now or datetime.utcnow())
    return (now_naive - manual_at) < timedelta(days=DEFAULT_FOLLOW_UP_DAYS)


def _lifecycle_str(state) -> str:
    if state is None:
        return ""
    if hasattr(state, "value"):
        return str(state.value)
    return str(state)


def _as_naive_utc(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt
    return dt.astimezone(timezone.utc).replace(tzinfo=None)


def apply_manual_lifecycle_change(client: Client, new_state: LifecycleState) -> None:
    """
    Persist an operator-driven column move and reset follow-up so pipeline automation
    does not immediately revert the card on the next calendar sync.
    """
    client.lifecycle_state = new_state
    now = datetime.utcnow()
    client.last_activity_at = now
    due = now + timedelta(days=DEFAULT_FOLLOW_UP_DAYS)
    meta = dict(client.meta) if isinstance(client.meta, dict) else {}
    meta[META_FOLLOW_UP_DUE_AT] = due.isoformat() + "Z"
    meta[META_LIFECYCLE_MANUAL_AT] = now.isoformat() + "Z"
    meta[META_LIFECYCLE_MANUAL_STAGE] = _lifecycle_str(new_state)
    client.meta = meta
    flag_modified(client, "meta")


def get_follow_up_due_at(client: Client) -> Optional[datetime]:
    """Effective follow-up due instant (naive UTC), mirroring frontend leadFollowUp.ts."""
    meta = client.meta if isinstance(client.meta, dict) else {}
    raw = meta.get("follow_up_due_at")
    if raw and isinstance(raw, str):
        try:
            s = raw.replace("Z", "+00:00") if raw.endswith("Z") else raw
            parsed = datetime.fromisoformat(s)
            return _as_naive_utc(parsed)
        except (ValueError, TypeError):
            pass

    anchor_str = client.last_activity_at or client.created_at or client.updated_at
    if not anchor_str:
        return None
    anchor = _as_naive_utc(anchor_str)
    if anchor is None:
        return None
    return anchor + timedelta(days=DEFAULT_FOLLOW_UP_DAYS)


def is_follow_up_expired(client: Client, *, now: Optional[datetime] = None) -> bool:
    due = get_follow_up_due_at(client)
    if due is None:
        return False
    now_naive = _as_naive_utc(now or datetime.utcnow())
    return now_naive >= due


def client_has_recorded_sale(db: Session, org_id: uuid.UUID, client_id: uuid.UUID) -> bool:
    from app.services.automation_engine import _has_no_recorded_sale

    return not _has_no_recorded_sale(db, org_id, client_id)


def _has_unclosed_past_sales_call(
    db: Session,
    org_id: uuid.UUID,
    client_id: uuid.UUID,
    *,
    now: Optional[datetime] = None,
) -> bool:
    from app.models.client_checkin import ClientCheckIn

    now_naive = _as_naive_utc(now or datetime.utcnow())
    if now_naive is None:
        return False

    rows = (
        db.query(ClientCheckIn)
        .filter(
            ClientCheckIn.org_id == org_id,
            ClientCheckIn.client_id == client_id,
            ClientCheckIn.is_sales_call.is_(True),
            or_(ClientCheckIn.sale_closed.is_(False), ClientCheckIn.sale_closed.is_(None)),
        )
        .order_by(ClientCheckIn.start_time.desc())
        .all()
    )
    for row in rows:
        start = _as_naive_utc(row.start_time)
        if start is None or start > now_naive:
            continue
        end = _as_naive_utc(row.end_time)
        call_end = end if end is not None else start
        if call_end <= now_naive:
            return True
    return False


def update_client_progress(db: Session, client: Client) -> bool:
    """Calculate and update client's program progress."""
    if not client.program_start_date or not client.program_duration_days:
        if client.program_progress_percent is not None:
            client.program_progress_percent = None
            return True
        return False

    new_progress = client.calculate_progress()
    if client.program_progress_percent != new_progress:
        client.program_progress_percent = new_progress
        return True
    return False


def update_booked_to_nurturing(db: Session, client: Client) -> bool:
    """
    Booked leads who had a sales call that did not close and have not paid → nurturing.
    """
    if _lifecycle_str(client.lifecycle_state) != LifecycleState.BOOKED.value:
        return False
    if client_has_recorded_sale(db, client.org_id, client.id):
        return False
    if not _has_unclosed_past_sales_call(db, client.org_id, client.id):
        return False
    print(
        f"[CLIENT_AUTOMATION] Client {client.id} ({client.email}): "
        "booked + unclosed past sales call → NURTURING"
    )
    client.lifecycle_state = LifecycleState.NURTURING
    db.flush()
    return True


def update_expired_follow_ups_to_cold_lead(client: Client) -> bool:
    """
    Qualified / nurturing / booked leads whose follow-up timer has elapsed → cold_lead.
    """
    state = _lifecycle_str(client.lifecycle_state)
    if state not in {s.value for s in (LifecycleState.QUALIFIED, LifecycleState.NURTURING, LifecycleState.BOOKED)}:
        return False
    if not is_follow_up_expired(client):
        return False
    print(
        f"[CLIENT_AUTOMATION] Client {client.id} ({client.email}): "
        f"follow-up expired in {state} → COLD_LEAD"
    )
    client.lifecycle_state = LifecycleState.COLD_LEAD
    return True


def update_client_lifecycle_state(db: Session, client: Client, force: bool = False) -> bool:
    """
    Program-based transitions for paying clients only:
    - active at 75% → offboarding
    - offboarding at 100% → dead
    """
    if not force and is_manual_lifecycle_protected(client):
        return False
    state = _lifecycle_str(client.lifecycle_state)
    if state not in (LifecycleState.ACTIVE.value, LifecycleState.OFFBOARDING.value):
        return False

    if not client.program_start_date or not client.program_duration_days:
        return False

    progress = client.calculate_progress()
    if progress is None:
        return False

    target_state = None
    print(
        f"[CLIENT_AUTOMATION] Client {client.id} ({client.email}): "
        f"progress={progress:.2f}%, current_state={state}"
    )

    if progress >= 100.0:
        if state != LifecycleState.DEAD.value:
            target_state = LifecycleState.DEAD
    elif progress >= 75.0:
        if state == LifecycleState.ACTIVE.value:
            target_state = LifecycleState.OFFBOARDING

    if not target_state:
        return False

    target_str = target_state.value
    if state == target_str:
        return False

    print(
        f"[CLIENT_AUTOMATION] ✅ Updating client {client.id} from {state} to {target_str} "
        f"(progress: {progress:.1f}%)"
    )
    client.lifecycle_state = target_state
    db.flush()
    db.refresh(client)

    if target_str == LifecycleState.OFFBOARDING.value:
        try:
            from app.services.automation_engine import on_lifecycle_entered_offboarding

            on_lifecycle_entered_offboarding(
                db,
                org_id=client.org_id,
                client_id=client.id,
            )
        except Exception as automation_error:
            print(f"[AUTOMATION_ENGINE] ⚠️  Error enqueueing offboarding job: {automation_error}")
    return True


def process_pipeline_lifecycle_for_client(db: Session, client: Client) -> bool:
    """Run booked→nurturing and follow-up expiry rules for one client."""
    if is_manual_lifecycle_protected(client):
        return False
    changed = False
    if update_booked_to_nurturing(db, client):
        changed = True
    if update_expired_follow_ups_to_cold_lead(client):
        changed = True
    return changed


def run_pipeline_lifecycle_for_org(db: Session, org_id: uuid.UUID) -> int:
    """Apply pipeline rules to all pre-payment clients in an org. Returns change count."""
    candidates = (
        db.query(Client)
        .filter(
            Client.org_id == org_id,
            Client.lifecycle_state.in_(
                [
                    LifecycleState.COLD_LEAD,
                    LifecycleState.NURTURING,
                    LifecycleState.QUALIFIED,
                    LifecycleState.BOOKED,
                ]
            ),
        )
        .all()
    )
    changed = 0
    for client in candidates:
        if process_pipeline_lifecycle_for_client(db, client):
            changed += 1
    if changed:
        db.commit()
    return changed


def process_client_automation(db: Session, org_id: uuid.UUID = None):
    """
    Process automation for clients: program progress, program lifecycle, and pipeline rules.
    """
    query = db.query(Client)
    if org_id:
        query = query.filter(Client.org_id == org_id)

    clients = query.all()
    progress_updates = 0
    program_state_changes = 0
    pipeline_changes = 0

    for client in clients:
        if client.program_start_date and client.program_duration_days:
            if update_client_progress(db, client):
                progress_updates += 1
            if update_client_lifecycle_state(db, client):
                program_state_changes += 1
        if process_pipeline_lifecycle_for_client(db, client):
            pipeline_changes += 1

    db.commit()

    print(
        f"[CLIENT_AUTOMATION] Processed {len(clients)} clients: "
        f"{progress_updates} progress updates, {program_state_changes} program state changes, "
        f"{pipeline_changes} pipeline changes"
    )

    return {
        "clients_processed": len(clients),
        "progress_updates": progress_updates,
        "state_changes": program_state_changes + pipeline_changes,
        "pipeline_changes": pipeline_changes,
    }


def move_client_to_active_on_payment(db: Session, client: Client) -> bool:
    """
    Move client to active when they receive a payment.
    Applies to any pre-payment pipeline stage plus offboarding/dead win-backs.
    """
    state = client.lifecycle_state
    if state in PRE_PAYMENT_LIFECYCLE_STATES or state in (
        LifecycleState.OFFBOARDING,
        LifecycleState.DEAD,
    ):
        print(
            f"[CLIENT_AUTOMATION] Moving client {client.id} to ACTIVE due to payment "
            f"(was {_lifecycle_str(state)})"
        )
        client.lifecycle_state = LifecycleState.ACTIVE
        return True
    return False

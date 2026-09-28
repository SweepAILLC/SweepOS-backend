"""
Client lifecycle automation service.

Handles pipeline stage transitions (funnel → qualified → booked → nurturing → cold),
payment → active, and program progress → offboarding → dead.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
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
META_FOLLOW_UP_ANCHOR_AT = "follow_up_anchor_at"
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

    # Without a program timeline, stale progress must not re-trigger auto-dead on sync/get.
    if _lifecycle_str(new_state) != LifecycleState.DEAD.value:
        if not client.program_start_date or not client.program_duration_days:
            client.program_progress_percent = None
            if not client.program_start_date:
                client.program_end_date = None
                client.program_duration_days = None


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
    """True when client has succeeded Stripe or paid-like Whop rows."""
    from app.services.automation_engine import _has_no_recorded_sale

    return not _has_no_recorded_sale(db, org_id, client_id)


def client_has_recorded_payment(
    db: Session,
    org_id: uuid.UUID,
    client_id: uuid.UUID,
) -> bool:
    """True when client has any recorded sale (Stripe, Whop, or manual payment)."""
    if client_has_recorded_sale(db, org_id, client_id):
        return True
    from app.models.manual_payment import ManualPayment

    manual = (
        db.query(ManualPayment.id)
        .filter(
            ManualPayment.org_id == org_id,
            ManualPayment.client_id == client_id,
        )
        .limit(1)
        .first()
    )
    return manual is not None


def _amount_and_date_match(
    a_cents: Optional[int],
    a_date: Optional[datetime],
    b_cents: int,
    b_date: Optional[datetime],
    *,
    window_days: int,
    tolerance_pct: float,
) -> bool:
    if a_cents is None or a_cents <= 0 or b_cents <= 0:
        return False
    diff_pct = abs(a_cents - b_cents) / max(a_cents, b_cents) * 100.0
    if diff_pct > tolerance_pct:
        return False
    if a_date is None or b_date is None:
        return True
    a_naive = _as_naive_utc(a_date)
    b_naive = _as_naive_utc(b_date)
    if a_naive is None or b_naive is None:
        return True
    return abs((a_naive - b_naive).days) <= window_days


def find_matching_real_payment(
    db: Session,
    org_id: uuid.UUID,
    client_id: uuid.UUID,
    *,
    amount: Optional[float],
    near_date: Optional[datetime],
    window_days: int = 7,
    tolerance_pct: float = 15.0,
) -> bool:
    """
    True when a real (non-auto-detected) payment already covers this deal —
    same client, amount within tolerance, dated within window_days of near_date.
    Used before creating a call_library_auto ManualPayment row, so Fathom's read
    never duplicates a payment that already landed through Stripe/Whop/manual entry.
    """
    if amount is None:
        return False
    target_cents = int(round(amount * 100))
    from app.models.manual_payment import ManualPayment
    from app.models.stripe_payment import StripePayment
    from app.models.whop_payment import WhopPayment

    for p in (
        db.query(StripePayment)
        .filter(
            StripePayment.org_id == org_id,
            StripePayment.client_id == client_id,
            StripePayment.status == "succeeded",
        )
        .all()
    ):
        if _amount_and_date_match(
            target_cents, near_date, int(p.amount_cents or 0), p.created_at,
            window_days=window_days, tolerance_pct=tolerance_pct,
        ):
            return True

    for p in (
        db.query(WhopPayment)
        .filter(WhopPayment.org_id == org_id, WhopPayment.client_id == client_id)
        .all()
    ):
        if (p.status or "").lower() not in ("paid", "succeeded", "completed", "successful"):
            continue
        if _amount_and_date_match(
            target_cents, near_date, int(p.amount_cents or 0), p.created_at,
            window_days=window_days, tolerance_pct=tolerance_pct,
        ):
            return True

    for p in (
        db.query(ManualPayment)
        .filter(
            ManualPayment.org_id == org_id,
            ManualPayment.client_id == client_id,
            ManualPayment.source != "call_library_auto",
        )
        .all()
    ):
        if _amount_and_date_match(
            target_cents, near_date, int(p.amount_cents or 0), p.payment_date,
            window_days=window_days, tolerance_pct=tolerance_pct,
        ):
            return True

    return False


def find_auto_payment_to_supersede(
    db: Session,
    org_id: uuid.UUID,
    client_id: uuid.UUID,
    *,
    amount_cents: int,
    near_date: Optional[datetime],
    window_days: int = 7,
    tolerance_pct: float = 15.0,
):
    """
    The reverse direction: a real payment just landed — find an earlier
    call_library_auto row (not yet superseded) for this client that it covers,
    so the auto-detected row can be marked superseded_at and stop double-counting
    alongside the real payment. Priority: the payment processor always wins.
    """
    from app.models.manual_payment import ManualPayment

    candidates = (
        db.query(ManualPayment)
        .filter(
            ManualPayment.org_id == org_id,
            ManualPayment.client_id == client_id,
            ManualPayment.source == "call_library_auto",
            ManualPayment.superseded_at.is_(None),
        )
        .all()
    )
    for p in candidates:
        if _amount_and_date_match(
            amount_cents, near_date, int(p.amount_cents or 0), p.payment_date,
            window_days=window_days, tolerance_pct=tolerance_pct,
        ):
            return p
    return None


def _succeeded_payment_count(db: Session, org_id: uuid.UUID, client_id: uuid.UUID) -> int:
    """Succeeded Stripe + paid-like Whop + manual rows for this client."""
    from app.models.manual_payment import ManualPayment
    from app.models.stripe_payment import StripePayment
    from app.models.whop_payment import WhopPayment

    stripe_n = (
        db.query(StripePayment.id)
        .filter(
            StripePayment.org_id == org_id,
            StripePayment.client_id == client_id,
            StripePayment.status == "succeeded",
            StripePayment.amount_cents > 0,
        )
        .count()
    )
    whop_n = (
        db.query(WhopPayment.id)
        .filter(
            WhopPayment.org_id == org_id,
            WhopPayment.client_id == client_id,
            WhopPayment.status.in_(("paid", "succeeded", "completed", "successful")),
        )
        .count()
    )
    manual_n = (
        db.query(ManualPayment.id)
        .filter(ManualPayment.org_id == org_id, ManualPayment.client_id == client_id)
        .count()
    )
    return int(stripe_n or 0) + int(whop_n or 0) + int(manual_n or 0)


def _has_upcoming_sales_call(
    db: Session,
    org_id: uuid.UUID,
    client_id: uuid.UUID,
    *,
    now: Optional[datetime] = None,
) -> bool:
    """Upcoming check-in marked (or event-typed) as a sales call."""
    from app.models.client_checkin import ClientCheckIn
    from app.services.calendar_booking_time import effective_end_sql_expression, ensure_utc

    now_utc = ensure_utc(now or datetime.now(timezone.utc))
    effective_end = effective_end_sql_expression()
    row = (
        db.query(ClientCheckIn.id)
        .filter(
            ClientCheckIn.org_id == org_id,
            ClientCheckIn.client_id == client_id,
            ClientCheckIn.is_sales_call.is_(True),
            ClientCheckIn.cancelled.is_(False),
            ClientCheckIn.no_show.is_(False),
            effective_end >= now_utc,
        )
        .limit(1)
        .first()
    )
    return row is not None


def _has_unclosed_past_sales_call(
    db: Session,
    org_id: uuid.UUID,
    client_id: uuid.UUID,
    *,
    now: Optional[datetime] = None,
) -> bool:
    """Past sales call that has not been marked sale-closed."""
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
            ClientCheckIn.cancelled.is_(False),
            ClientCheckIn.no_show.is_(False),
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


def find_active_sale_closed_mismatches(
    db: Session,
    org_id: uuid.UUID,
) -> list[uuid.UUID]:
    """
    Data-integrity flag: clients whose lifecycle already advanced to ACTIVE
    (a real payment landed) but whose most recent past sales call still shows
    `sale_closed != True`. This drift happens when the booking's event type was
    never tagged `is_sales_call` (missing CalendarBookingSales/EventTypeSalesCall
    default) — the payment correctly promotes lifecycle_state, but nothing
    stamps sale_closed, so anything reading sale_closed specifically still
    shows this client as open. One query for active clients, one for their
    sales check-ins — no N+1.
    """
    from app.models.client_checkin import ClientCheckIn

    active_client_ids = {
        row[0]
        for row in db.query(Client.id)
        .filter(Client.org_id == org_id, Client.lifecycle_state == LifecycleState.ACTIVE)
        .all()
    }
    if not active_client_ids:
        return []

    now_naive = _as_naive_utc(datetime.utcnow())
    latest_call_by_client: dict[uuid.UUID, ClientCheckIn] = {}
    rows = (
        db.query(ClientCheckIn)
        .filter(
            ClientCheckIn.org_id == org_id,
            ClientCheckIn.client_id.in_(active_client_ids),
            ClientCheckIn.is_sales_call.is_(True),
            ClientCheckIn.cancelled.is_(False),
        )
        .all()
    )
    for row in rows:
        start = _as_naive_utc(row.start_time)
        if start is None or (now_naive is not None and start > now_naive):
            continue
        current = latest_call_by_client.get(row.client_id)
        current_start = _as_naive_utc(current.start_time) if current else None
        if current is None or (current_start is not None and start > current_start):
            latest_call_by_client[row.client_id] = row

    return [
        client_id
        for client_id, call in latest_call_by_client.items()
        if call.sale_closed is not True
    ]


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


def update_to_booked_on_upcoming_sales_call(db: Session, client: Client) -> bool:
    """
    Pre-payment leads with an upcoming sales call on the calendar → booked.
    """
    state = _lifecycle_str(client.lifecycle_state)
    if state not in {s.value for s in PRE_PAYMENT_LIFECYCLE_STATES}:
        return False
    if state == LifecycleState.BOOKED.value:
        return False
    if client_has_recorded_payment(db, client.org_id, client.id):
        return False
    if not _has_upcoming_sales_call(db, client.org_id, client.id):
        return False
    print(
        f"[CLIENT_AUTOMATION] Client {client.id} ({client.email}): "
        f"{state} + upcoming sales call → BOOKED"
    )
    client.lifecycle_state = LifecycleState.BOOKED
    client.last_activity_at = datetime.utcnow()
    db.flush()
    return True


def update_booked_to_nurturing(db: Session, client: Client) -> bool:
    """
    Booked leads who had a sales call that did not close and have not paid → nurturing.
    """
    if _lifecycle_str(client.lifecycle_state) != LifecycleState.BOOKED.value:
        return False
    if client_has_recorded_payment(db, client.org_id, client.id):
        return False
    if _has_upcoming_sales_call(db, client.org_id, client.id):
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


def revert_booked_without_sales_call(db: Session, client: Client) -> bool:
    """
    Booked column requires an upcoming or past unclosed sales call.
    Corrects profiles that were moved on generic calendar check-ins.
    """
    if _lifecycle_str(client.lifecycle_state) != LifecycleState.BOOKED.value:
        return False
    if client_has_recorded_payment(db, client.org_id, client.id):
        return False
    if _has_upcoming_sales_call(db, client.org_id, client.id):
        return False
    if _has_unclosed_past_sales_call(db, client.org_id, client.id):
        return False
    print(
        f"[CLIENT_AUTOMATION] Client {client.id} ({client.email}): "
        "booked without sales call basis → QUALIFIED"
    )
    client.lifecycle_state = LifecycleState.QUALIFIED
    db.flush()
    return True


def restart_follow_up_timer(client: Client, *, now: Optional[datetime] = None) -> None:
    """
    Reset the follow-up bar to 0% without touching last_activity_at (no real
    activity happened). The frontend bar anchors on max(last_activity_at,
    meta.follow_up_anchor_at), so both sides agree the new window starts now.
    """
    now_naive = _as_naive_utc(now or datetime.utcnow())
    meta = dict(client.meta) if isinstance(client.meta, dict) else {}
    meta[META_FOLLOW_UP_ANCHOR_AT] = now_naive.isoformat() + "Z"
    meta[META_FOLLOW_UP_DUE_AT] = (now_naive + timedelta(days=DEFAULT_FOLLOW_UP_DAYS)).isoformat() + "Z"
    client.meta = meta
    try:
        flag_modified(client, "meta")
    except Exception:
        # Plain objects in unit tests have no SQLAlchemy instance state.
        pass


def update_expired_follow_ups(db: Session, client: Client) -> bool:
    """
    Follow-up bar hit 100% on an unpaid lead with no upcoming sales call:
    - qualified -> nurturing, with a fresh follow-up window
    - nurturing / booked -> cold_lead
    One step per run, so a qualified lead gets a full nurturing window before
    it can reach cold_lead.
    """
    state = _lifecycle_str(client.lifecycle_state)
    if state not in {s.value for s in (LifecycleState.QUALIFIED, LifecycleState.NURTURING, LifecycleState.BOOKED)}:
        return False
    if client_has_recorded_payment(db, client.org_id, client.id):
        return False
    if _has_upcoming_sales_call(db, client.org_id, client.id):
        return False
    if not is_follow_up_expired(client):
        return False
    if state == LifecycleState.QUALIFIED.value:
        print(
            f"[CLIENT_AUTOMATION] Client {client.id} ({client.email}): "
            "follow-up expired in qualified → NURTURING"
        )
        client.lifecycle_state = LifecycleState.NURTURING
        restart_follow_up_timer(client)
        return True
    print(
        f"[CLIENT_AUTOMATION] Client {client.id} ({client.email}): "
        f"follow-up expired in {state} → COLD_LEAD"
    )
    client.lifecycle_state = LifecycleState.COLD_LEAD
    return True


def sweep_expired_follow_ups_all_orgs(db: Session, *, now: Optional[datetime] = None) -> int:
    """
    Worker sweep: apply the follow-up expiry rule to every lead card already
    at 100%, across all orgs. Runs on worker boot (so a deploy applies it to
    existing cards immediately) and on an interval after that — the rule
    otherwise only fires during a calendar sync.

    Qualified and nurturing only: booked cards keep their existing sync-time
    handling (booked->nurturing on an unclosed past call comes first there), so
    the sweep never jumps a booked card straight to cold_lead on its own.

    Only the follow-up rule runs here, not the full lifecycle pass. Honors the
    manual-lock like apply_automatic_lifecycle_for_client does. The expiry
    check is pure Python and runs first, so the per-client payment/upcoming-call
    queries only run for cards that are actually due.
    """
    candidates = (
        db.query(Client)
        .filter(
            Client.lifecycle_state.in_([LifecycleState.QUALIFIED, LifecycleState.NURTURING])
        )
        .all()
    )
    changed = 0
    for client in candidates:
        if not is_follow_up_expired(client, now=now):
            continue
        if is_manual_lifecycle_protected(client, now=now):
            continue
        sp = db.begin_nested()
        try:
            if update_expired_follow_ups(db, client):
                changed += 1
            sp.commit()
        except Exception as client_err:
            sp.rollback()
            print(f"[CLIENT_AUTOMATION] follow-up sweep skip for {client.id}: {client_err}")
    if changed:
        db.commit()
    return changed


def apply_funnel_lead_lifecycle(client: Client) -> bool:
    """Funnel capture moves early-stage leads to qualified."""
    state = _lifecycle_str(client.lifecycle_state)
    if state in (LifecycleState.COLD_LEAD.value, LifecycleState.NURTURING.value):
        client.lifecycle_state = LifecycleState.QUALIFIED
        client.last_activity_at = datetime.utcnow()
        return True
    return False


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
    if target_str == LifecycleState.DEAD.value:
        hook_sp = db.begin_nested()
        try:
            from app.long_jobs import schedule_background_work
            from app.services.call_insight_service import (
                on_client_became_dead,
                refresh_latest_call_insight_background,
            )

            has_fathom = on_client_became_dead(db, client.org_id, client)
            if has_fathom:
                schedule_background_work(
                    refresh_latest_call_insight_background,
                    None,
                    str(client.org_id),
                    str(client.id),
                )
            hook_sp.commit()
        except Exception as dead_hook_err:
            hook_sp.rollback()
            print(f"[CLIENT_AUTOMATION] ⚠️  Dead lifecycle insight hook failed: {dead_hook_err}")
    return True


def apply_automatic_lifecycle_for_client(
    db: Session,
    client: Client,
    *,
    force: bool = False,
) -> bool:
    """
    Apply all automatic lifecycle rules for one client (priority order):
    1. Payment → active
    2. Upcoming sales call → booked (pre-payment)
    3. Program progress → offboarding @ 75%, dead @ 100%
    4. Past unclosed sales call without payment → nurturing
    5. Booked without sales call basis → qualified (backfill)
    6. Follow-up expired → qualified steps to nurturing; nurturing/booked to cold_lead

    The manual-lock (`is_manual_lifecycle_protected`) only gates 3-6 — automation
    second-guessing or downgrading a stage an operator just set by hand. It never
    blocks 1 or 2: a real payment or a brand-new booking is a new external signal,
    not automation re-litigating stale state, so neither should sit blocked for
    the 14-day window just because the card was dragged for an unrelated reason
    earlier. This is what lets Manual Payment and Whop payments (which route
    through this function) behave the same as Stripe's payment webhook (which
    bypasses this function and calls move_client_to_active_on_payment directly)
    — one rule, applied consistently everywhere a payment or booking lands.
    """
    if client_has_recorded_payment(db, client.org_id, client.id):
        if move_client_to_active_on_payment(db, client):
            db.flush()
            return True

    state = _lifecycle_str(client.lifecycle_state)
    if state in {s.value for s in PRE_PAYMENT_LIFECYCLE_STATES}:
        if update_to_booked_on_upcoming_sales_call(db, client):
            return True

    if not force and is_manual_lifecycle_protected(client):
        return False

    changed = False

    if client.program_start_date and client.program_duration_days:
        if update_client_progress(db, client):
            changed = True
        if update_client_lifecycle_state(db, client, force=force):
            changed = True

    state = _lifecycle_str(client.lifecycle_state)
    if state not in {s.value for s in PRE_PAYMENT_LIFECYCLE_STATES}:
        return changed

    if update_booked_to_nurturing(db, client):
        changed = True
    elif revert_booked_without_sales_call(db, client):
        changed = True
    elif update_expired_follow_ups(db, client):
        changed = True
    return changed


def process_pipeline_lifecycle_for_client(db: Session, client: Client) -> bool:
    """Run upcoming-call→booked, booked→nurturing, and follow-up expiry rules for one client."""
    return apply_automatic_lifecycle_for_client(db, client)


def run_pipeline_lifecycle_for_org(
    db: Session,
    org_id: uuid.UUID,
    *,
    force: bool = False,
) -> int:
    """Apply lifecycle rules to all clients in an org. Returns change count."""
    clients = db.query(Client).filter(Client.org_id == org_id).all()
    changed = 0
    for client in clients:
        sp = db.begin_nested()
        try:
            if apply_automatic_lifecycle_for_client(db, client, force=force):
                changed += 1
            sp.commit()
        except Exception as client_err:
            sp.rollback()
            print(f"[CLIENT_AUTOMATION] pipeline rule skip for {client.id}: {client_err}")
    if changed:
        try:
            db.commit()
        except Exception as commit_err:
            print(f"[CLIENT_AUTOMATION] pipeline lifecycle commit failed: {commit_err}")
            try:
                db.rollback()
            except Exception:
                pass
            return 0
    return changed


def reconcile_org_client_lifecycles(
    db: Session,
    org_id: uuid.UUID,
    *,
    force: bool = True,
) -> int:
    """
    Re-evaluate lifecycle for every client in the org (backfill / board refresh).
    ``force=True`` bypasses the 14-day manual column-move shield so misclassified
    existing profiles can be corrected.
    """
    return run_pipeline_lifecycle_for_org(db, org_id, force=force)


def process_client_automation(
    db: Session,
    org_id: uuid.UUID = None,
    *,
    force: bool = False,
):
    """
    Process automation for clients: program progress, program lifecycle, and pipeline rules.
    """
    query = db.query(Client)
    if org_id:
        query = query.filter(Client.org_id == org_id)

    clients = query.all()
    progress_updates = 0
    pipeline_changes = 0

    for client in clients:
        if apply_automatic_lifecycle_for_client(db, client, force=force):
            pipeline_changes += 1

    db.commit()

    print(
        f"[CLIENT_AUTOMATION] Processed {len(clients)} clients: "
        f"{pipeline_changes} lifecycle changes"
    )

    return {
        "clients_processed": len(clients),
        "progress_updates": progress_updates,
        "state_changes": pipeline_changes,
        "pipeline_changes": pipeline_changes,
    }




def mark_latest_sales_call_closed(db: Session, org_id: uuid.UUID, client: Client) -> Optional[datetime]:
    """Mark the most recent open sales call closed (payment or close form).

    Returns the call start_time when a row was updated, else None.
    Does not commit — caller owns the transaction.
    """
    from app.models.client_checkin import ClientCheckIn
    from app.models.calendar_booking_sales import CalendarBookingSales

    check_in = (
        db.query(ClientCheckIn)
        .filter(
            ClientCheckIn.org_id == org_id,
            ClientCheckIn.client_id == client.id,
            ClientCheckIn.is_sales_call == True,
            (ClientCheckIn.sale_closed == False) | (ClientCheckIn.sale_closed.is_(None)),
        )
        .order_by(ClientCheckIn.start_time.desc())
        .limit(1)
        .first()
    )
    if not check_in:
        return None

    check_in.sale_closed = True
    check_in.no_show = False
    check_in.updated_at = datetime.utcnow()

    if getattr(check_in, "provider", None) in ("calcom", "calendly") and check_in.event_id:
        sales_row = (
            db.query(CalendarBookingSales)
            .filter(
                CalendarBookingSales.org_id == org_id,
                CalendarBookingSales.provider == check_in.provider,
                CalendarBookingSales.event_id == check_in.event_id,
            )
            .first()
        )
        if sales_row:
            sales_row.is_sales_call = True
            sales_row.sale_closed = True
            sales_row.updated_at = datetime.utcnow()
        else:
            db.add(
                CalendarBookingSales(
                    org_id=org_id,
                    provider=check_in.provider,
                    event_id=check_in.event_id,
                    event_uri=check_in.event_uri,
                    is_sales_call=True,
                    sale_closed=True,
                )
            )
    print(
        f"[SALES_CLOSE] Marked sales call {check_in.event_id} closed for client {client.id}"
    )
    return check_in.start_time


def apply_payment_pipeline_effects(db: Session, client: Client) -> Optional[datetime]:
    """After a payment is recorded: Active (if needed) + close latest sales call.

    Only the client's first succeeded payment stamps sale_closed. Repeat charges
    stay Active without adding another KPI close. Does not commit.
    """
    move_client_to_active_on_payment(db, client)
    if _succeeded_payment_count(db, client.org_id, client.id) > 1:
        return None
    return mark_latest_sales_call_closed(db, client.org_id, client)


def run_payment_pipeline_effects_job(org_id: str, client_id: str, when_iso: str | None = None) -> None:
    """RQ/thread-safe: close last sales call, Active, KPI sync. Works without a logged-in user."""
    from app.db.session import SessionLocal
    from app.services.kpi_integration_sync import sync_kpi_for_datetime
    from app.services.terminal_metrics_service import invalidate_terminal_monthly_trends_cache

    db = SessionLocal()
    try:
        oid = uuid.UUID(str(org_id))
        cid = uuid.UUID(str(client_id))
        client = db.query(Client).filter(Client.id == cid, Client.org_id == oid).first()
        if not client:
            return
        call_when = apply_payment_pipeline_effects(db, client)
        db.commit()
        sync_when = call_when
        if sync_when is None and when_iso:
            try:
                sync_when = datetime.fromisoformat(when_iso.replace("Z", "+00:00"))
            except Exception:
                sync_when = datetime.utcnow()
        try:
            sync_kpi_for_datetime(db, oid, sync_when or datetime.utcnow(), commit=True)
        except Exception as kpi_err:
            print(f"[PAYMENT_PIPELINE] KPI sync failed: {kpi_err}")
        try:
            invalidate_terminal_monthly_trends_cache(oid)
        except Exception:
            pass
    except Exception as e:
        db.rollback()
        print(f"[PAYMENT_PIPELINE] job failed org={org_id} client={client_id}: {e}")
    finally:
        db.close()


def enqueue_payment_pipeline_effects(
    org_id: uuid.UUID,
    client_id: uuid.UUID,
    *,
    when: Optional[datetime] = None,
) -> None:
    """Fire-and-forget payment → close + Active + KPI (RQ when enabled)."""
    from app.long_jobs import schedule_background_work

    when_iso = None
    if when is not None:
        when_iso = when.isoformat()
    schedule_background_work(
        run_payment_pipeline_effects_job,
        None,
        str(org_id),
        str(client_id),
        when_iso,
        prefer_rq=True,
        job_timeout=120,
    )


def run_close_survey_kpi_sync_job(
    org_id: str,
    when_iso: str | None,
    entry_day: str,
    revenue_delta_usd: float = 0.0,
) -> None:
    """Async KPI + terminal cache refresh after public close-survey submit."""
    from app.db.session import SessionLocal
    from app.services.kpi_integration_sync import sync_kpi_for_datetime
    from app.services.terminal_metrics_service import invalidate_terminal_monthly_trends_cache

    db = SessionLocal()
    try:
        oid = uuid.UUID(str(org_id))
        when = None
        if when_iso:
            try:
                when = datetime.fromisoformat(when_iso.replace("Z", "+00:00"))
            except Exception:
                when = None
        if when is None:
            try:
                d = date.fromisoformat(entry_day)
                when = datetime(d.year, d.month, d.day, 12, 0, 0, tzinfo=timezone.utc)
            except Exception:
                when = datetime.now(timezone.utc)
        sync_kpi_for_datetime(db, oid, when, commit=True)
        if revenue_delta_usd:
            try:
                from app.api.kpi import _upsert_kpi_entry_for_org
                from datetime import date as date_cls

                _upsert_kpi_entry_for_org(
                    db,
                    oid,
                    date_cls.fromisoformat(entry_day),
                    {"revenue": float(revenue_delta_usd)},
                    additive=True,
                )
            except Exception as rev_err:
                print(f"[CLOSE_SURVEY_KPI] revenue add failed: {rev_err}")
        try:
            invalidate_terminal_monthly_trends_cache(oid)
        except Exception:
            pass
    except Exception as e:
        db.rollback()
        print(f"[CLOSE_SURVEY_KPI] job failed: {e}")
    finally:
        db.close()


def enqueue_close_survey_kpi_sync(
    org_id: uuid.UUID,
    *,
    when: Optional[datetime],
    entry_day,
    revenue_delta_usd: float = 0.0,
) -> None:
    from app.long_jobs import schedule_background_work

    when_iso = when.isoformat() if when is not None else None
    day_str = entry_day.isoformat() if hasattr(entry_day, "isoformat") else str(entry_day)
    schedule_background_work(
        run_close_survey_kpi_sync_job,
        None,
        str(org_id),
        when_iso,
        day_str,
        float(revenue_delta_usd or 0),
        prefer_rq=True,
        job_timeout=120,
    )


def move_client_to_active_on_payment(db: Session, client: Client) -> bool:
    """
    Move client to active when they have recorded payment.
    Applies to any pre-payment pipeline stage plus offboarding/dead win-backs.
    """
    if not client_has_recorded_payment(db, client.org_id, client.id):
        return False
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
        client.last_activity_at = datetime.utcnow()
        db.flush()
        return True
    return False

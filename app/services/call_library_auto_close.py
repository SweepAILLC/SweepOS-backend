"""Auto-close a sales call from its Call Library deal_outcome.

The payment processor always wins: this only ever creates the FIRST record of
a close (a real Stripe/Whop/manual payment later supersedes it via
supersede_auto_payment_if_matched). See
docs/features/SALES_PIPELINE_ATTRIBUTION_PRD.md, "New design: fully automated
close" and "Priority: the payment processor always wins, exactly deduped."
"""
from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from sqlalchemy.orm import Session

from app.models.client import Client
from app.models.client_checkin import ClientCheckIn
from app.models.fathom_call_record import FathomCallRecord
from app.models.manual_payment import ManualPayment
from app.models.sales_activity_event import SalesActivityEvent

logger = logging.getLogger(__name__)

_MIN_AUTO_CLOSE_CONFIDENCE = "high"
_ALLOWED_BILLING = ("one_time", "recurring_monthly", "recurring_annual")


def _resolve_billing(raw: Any) -> Optional[str]:
    return raw if raw in _ALLOWED_BILLING else None


def attempt_auto_close_from_call_library_report(
    db: Session,
    org_id: uuid.UUID,
    fathom_call_record_id: uuid.UUID,
    call_library_report_id: uuid.UUID,
    report_json: Dict[str, Any],
) -> Optional[str]:
    """
    Called right after a sales Call Library report is persisted complete.
    Returns an outcome string for logging, or None on a malformed report_json
    (caller must never let this fail report generation).
    """
    deal = report_json.get("deal_outcome") if isinstance(report_json, dict) else None
    if not isinstance(deal, dict):
        return None

    if not bool(deal.get("cash_collected_on_call")):
        # verbally_agreed_not_paid or nothing — never auto-closes. Only a real
        # payment landing later closes this (handled by the payment-side hook).
        return "skipped_not_cash_collected"

    confidence = str(deal.get("confidence") or "low").lower()
    if confidence != _MIN_AUTO_CLOSE_CONFIDENCE:
        # Medium/low confidence needs a human — never auto-close on a shaky read.
        return "skipped_low_confidence"

    rec = (
        db.query(FathomCallRecord)
        .filter(FathomCallRecord.id == fathom_call_record_id, FathomCallRecord.org_id == org_id)
        .first()
    )
    if not rec or not rec.client_id:
        return "skipped_no_client"

    client = db.query(Client).filter(Client.id == rec.client_id, Client.org_id == org_id).first()
    if not client:
        return "skipped_no_client"

    call_date = rec.meeting_at or datetime.now(timezone.utc)
    amount = deal.get("amount")
    try:
        amount_f = float(amount) if amount is not None else None
    except (TypeError, ValueError):
        amount_f = None

    from app.services.client_automation import find_matching_real_payment

    if find_matching_real_payment(db, org_id, client.id, amount=amount_f, near_date=call_date):
        # A real payment already covers this — never create a duplicate.
        return "skipped_duplicate_real_payment"

    amount_cents = int(round(amount_f * 100)) if amount_f and amount_f > 0 else 0
    currency_raw = str(deal.get("currency") or "USD").upper().strip()
    currency = currency_raw.lower()[:3] if 2 <= len(currency_raw) <= 8 and currency_raw.isalpha() else "usd"
    billing = _resolve_billing(deal.get("billing"))

    payment = ManualPayment(
        id=uuid.uuid4(),
        org_id=org_id,
        client_id=client.id,
        amount_cents=amount_cents,
        currency=currency,
        payment_date=call_date,
        description="Auto-detected from Call Library analysis",
        payment_method=billing,
        source="call_library_auto",
        call_library_report_id=call_library_report_id,
    )
    db.add(payment)
    db.flush()

    from app.services.client_automation import (
        mark_latest_sales_call_closed,
        move_client_to_active_on_payment,
    )

    call_start = mark_latest_sales_call_closed(db, org_id, client)
    move_client_to_active_on_payment(db, client)

    # Closer: the round-robin host on the check-in we just stamped sale_closed.
    closer_id: Optional[uuid.UUID] = None
    check_in = (
        db.query(ClientCheckIn)
        .filter(
            ClientCheckIn.org_id == org_id,
            ClientCheckIn.client_id == client.id,
            ClientCheckIn.is_sales_call.is_(True),
            ClientCheckIn.sale_closed.is_(True),
        )
        .order_by(ClientCheckIn.start_time.desc())
        .first()
    )
    if check_in is not None:
        closer_id = check_in.host_user_id

    from app.services.kpi_integration_sync import find_setter_claim_for_client

    setter_id = find_setter_claim_for_client(db, org_id, client.id)

    ref_date = call_start or call_date
    entry_day = ref_date.date() if ref_date else datetime.now(timezone.utc).date()

    db.add(
        SalesActivityEvent(
            org_id=org_id,
            entry_date=entry_day,
            rep_user_id=closer_id,
            rep_role="closer",
            client_id=client.id,
            cash_collected_cents=amount_cents or None,
            is_closed=True,
            source="call_library_auto",
            call_library_report_id=call_library_report_id,
        )
    )
    if setter_id is not None:
        db.add(
            SalesActivityEvent(
                org_id=org_id,
                entry_date=entry_day,
                rep_user_id=setter_id,
                rep_role="setter",
                client_id=client.id,
                cash_collected_cents=amount_cents or None,
                is_closed=True,
                source="call_library_auto",
                call_library_report_id=call_library_report_id,
            )
        )

    db.commit()
    logger.info(
        "call_library auto-close org=%s client=%s payment=%s closer=%s setter=%s",
        org_id, client.id, payment.id, closer_id, setter_id,
    )
    return "closed"


def supersede_auto_payment_if_matched(
    db: Session,
    org_id: uuid.UUID,
    client_id: uuid.UUID,
    *,
    amount_cents: int,
    near_date: Optional[datetime],
) -> bool:
    """
    Call right after a REAL payment (Stripe/Whop) is persisted. If an earlier
    call_library_auto row matches, mark it superseded — the real payment always
    wins. Returns True if a row was superseded.
    """
    from app.services.client_automation import find_auto_payment_to_supersede

    match = find_auto_payment_to_supersede(
        db, org_id, client_id, amount_cents=amount_cents, near_date=near_date,
    )
    if match is None:
        return False
    match.superseded_at = datetime.now(timezone.utc)
    db.commit()
    logger.info(
        "call_library auto-payment superseded org=%s client=%s payment=%s",
        org_id, client_id, match.id,
    )
    return True

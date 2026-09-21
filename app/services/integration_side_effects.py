"""Idempotent Discord + automation side effects for payments and bookings.

Webhook and pull-sync both call these. First claim wins.
"""
from __future__ import annotations

import logging
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.models.integration_event_dispatch import IntegrationEventDispatch

LOG = logging.getLogger("app.integration_side_effects")

ACTION_DISCORD_BOOKING = "discord_new_booking"
ACTION_DISCORD_PAYMENT = "discord_new_transaction"
RECENT_BOOKING_GRACE = timedelta(hours=2)


def claim_dispatch(
    db: Session,
    *,
    org_id: uuid.UUID,
    source: str,
    source_id: str,
    action: str,
) -> bool:
    """Return True if this process owns the side effect (inserted the claim)."""
    source_id = (source_id or "").strip()[:255]
    if not source_id:
        return False
    row = IntegrationEventDispatch(
        org_id=org_id,
        source=source,
        source_id=source_id,
        action=action,
    )
    db.add(row)
    try:
        db.commit()
        return True
    except IntegrityError:
        db.rollback()
        return False
    except Exception:
        db.rollback()
        LOG.exception("claim_dispatch failed action=%s source=%s", action, source)
        return False


def _client_display_name(client: Any, fallback: Optional[str] = None) -> str:
    if client is not None:
        name = " ".join(
            p for p in (getattr(client, "first_name", None), getattr(client, "last_name", None)) if p
        ).strip()
        if name:
            return name
    return (fallback or "").strip() or "Unknown"


def emit_new_booking_discord(
    db: Session,
    *,
    org_id: uuid.UUID,
    provider: str,
    event_id: str,
    client: Any = None,
    attendee_name: Optional[str] = None,
    attendee_email: Optional[str] = None,
    event_type_label: Optional[str] = None,
    start_time: Optional[datetime] = None,
    require_recent: bool = False,
) -> bool:
    """Fire Discord new_booking once. Returns True if sent (or queued)."""
    if require_recent and start_time is not None:
        st = start_time
        if st.tzinfo is None:
            st = st.replace(tzinfo=timezone.utc)
        if st < datetime.now(timezone.utc) - RECENT_BOOKING_GRACE:
            return False
    if not claim_dispatch(
        db, org_id=org_id, source=provider, source_id=event_id, action=ACTION_DISCORD_BOOKING
    ):
        return False
    try:
        from app.services import discord_notify

        fields = [("Provider", "Cal.com" if provider == "calcom" else "Calendly")]
        if start_time is not None:
            fields.append(("When", discord_notify.format_org_local_datetime(db, org_id, start_time)))
        if attendee_email:
            fields.append(("Email", attendee_email))
        discord_notify.send_discord_event_background(
            org_id,
            "new_booking",
            title=f"New booking: {_client_display_name(client, attendee_name)}",
            description=event_type_label or f"{provider} booking",
            fields=fields,
        )
        return True
    except Exception:
        LOG.warning("emit_new_booking_discord failed org=%s event=%s", org_id, event_id, exc_info=True)
        return False


def emit_new_payment_discord(
    db: Session,
    *,
    org_id: uuid.UUID,
    source: str,
    payment_id: str,
    amount_cents: int,
    currency: str = "usd",
    client: Any = None,
    event_type: Optional[str] = None,
) -> bool:
    if not claim_dispatch(
        db, org_id=org_id, source=source, source_id=payment_id, action=ACTION_DISCORD_PAYMENT
    ):
        return False
    try:
        from app.services import discord_notify

        fields = [("Payment ID", payment_id)]
        if event_type:
            fields.append(("Event", event_type))
        else:
            fields.append(("Source", source.title()))
        discord_notify.send_discord_event_background(
            org_id,
            "new_transaction",
            title=f"New transaction: ${int(amount_cents or 0) / 100:.2f} {(currency or 'usd').upper()}",
            description=_client_display_name(client),
            fields=fields,
        )
        return True
    except Exception:
        LOG.warning("emit_new_payment_discord failed org=%s payment=%s", org_id, payment_id, exc_info=True)
        return False

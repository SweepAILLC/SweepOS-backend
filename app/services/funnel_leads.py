"""Funnel lead upsert shared by POST /funnels/leads and GHL lead intake.

One code path tags a captured lead to a funnel, so a Sweep-built page, the GHL
opt-in webhook and the GHL reconcile pull can never disagree on attribution,
lifecycle, or notifications.

Default arguments reproduce POST /funnels/leads exactly. GHL callers pass
`opted_in_at` (the submission time), `ghl_contact_id`, and `reattribute=True`:

- Match order: GHL contact id, then email, then phone (all org-scoped).
- Re-attribution: a client with no funnel and no recorded opt-in (typically made
  by the manual GHL contact sync or by a booking webhook before the opt-in
  arrived) is tagged to this funnel. Anyone already tagged keeps first touch.
- Idempotent per funnel: when the client already opted in to this funnel, a
  second arrival (webhook and pull both delivering one submission) only merges
  answers/UTM; it never moves the opt-in date or sends another notification.
"""
from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from fastapi import HTTPException, status
from sqlalchemy.orm import Session
from sqlalchemy.orm.attributes import flag_modified

from app.models.client import (
    Client,
    LifecycleState,
    find_client_by_email,
    find_client_by_phone,
)
from app.models.funnel import Funnel
from app.schemas.funnel import FunnelLeadIn
from app.services.health_score_cache_service import invalidate_health_score_cache

LOG = logging.getLogger(__name__)


@dataclass
class FunnelLeadResult:
    client: Client
    created: bool
    # True when this call set source_funnel_id (new client or re-attribution).
    tagged_now: bool = False
    # True when the client had already opted in to this funnel (no notification sent).
    duplicate: bool = False


def find_client_by_ghl_contact_id(db: Session, org_id: uuid.UUID, ghl_contact_id: str) -> Optional[Client]:
    """Org-scoped lookup on meta->>'ghl_contact_id' (expression index from migration 097)."""
    if not ghl_contact_id:
        return None
    return (
        db.query(Client)
        .filter(Client.org_id == org_id, Client.meta["ghl_contact_id"].as_string() == ghl_contact_id)
        .order_by(Client.created_at.asc())
        .first()
    )


def _split_name(lead: FunnelLeadIn) -> tuple[Optional[str], Optional[str]]:
    first_name, last_name = lead.first_name, lead.last_name
    if lead.name and isinstance(lead.name, str) and lead.name.strip():
        parts = lead.name.strip().split(None, 1)
        first_name = first_name or parts[0]
        last_name = last_name if last_name is not None else (parts[1] if len(parts) > 1 else None)
    return first_name, last_name


def _to_naive_utc(dt: datetime) -> datetime:
    """clients.created_at is a naive UTC column."""
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def _apply_prospect_meta(
    client: Client,
    funnel: Funnel,
    lead: FunnelLeadIn,
    utm: Optional[Dict[str, Any]],
    captured_at: datetime,
    ghl_contact_id: Optional[str],
) -> None:
    # Always stamp funnel_id + captured_at so the Leads tab can list every capture.
    meta = client.meta if isinstance(client.meta, dict) else {}
    prev = meta.get("prospect") if isinstance(meta.get("prospect"), dict) else {}
    prospect = {
        **prev,
        "funnel_id": str(funnel.id),
        "captured_at": captured_at.isoformat(),
    }
    if lead.source is not None:
        prospect["source"] = lead.source
    if lead.quiz_answers is not None:
        prospect["quiz_answers"] = lead.quiz_answers or {}
    if lead.opt_in_data is not None:
        prospect["opt_in_data"] = lead.opt_in_data or {}
    if lead.funnel_step_reached is not None:
        prospect["funnel_step_reached"] = lead.funnel_step_reached
    if utm:
        prospect["utm"] = utm
    meta = {**meta, "prospect": prospect}
    if ghl_contact_id and not meta.get("ghl_contact_id"):
        meta["ghl_contact_id"] = ghl_contact_id
    client.meta = meta
    flag_modified(client, "meta")


def _already_opted_in_here(client: Client, funnel: Funnel) -> bool:
    return client.source_funnel_id == funnel.id and client.opted_in_at is not None


def _can_reattribute(client: Client) -> bool:
    return (
        client.source_funnel_id is None
        and client.opted_in_at is None
        and (client.source_channel or "organic") != "paid"
    )


def _notify(db: Session, *, client: Client, funnel: Funnel, lead: FunnelLeadIn, is_new_client: bool) -> None:
    try:
        from app.services.funnel_lead_notifications import enqueue_funnel_lead_notification

        enqueue_funnel_lead_notification(
            db,
            org_id=funnel.org_id,
            client=client,
            funnel=funnel,
            lead=lead,
            is_new_client=is_new_client,
        )
    except Exception as e:
        LOG.warning("funnel lead notification enqueue skipped: %s", e)
        try:
            db.rollback()
        except Exception:
            pass


def upsert_funnel_lead(
    db: Session,
    funnel: Funnel,
    lead: FunnelLeadIn,
    *,
    utm: Optional[Dict[str, Any]] = None,
    opted_in_at: Optional[datetime] = None,
    ghl_contact_id: Optional[str] = None,
    reattribute: bool = False,
    notify: bool = True,
) -> FunnelLeadResult:
    """Create or update the client for one captured lead and tag it to `funnel`.

    Commits. Raises HTTPException(400) when the lead has nothing to identify a
    new client by (same contract as POST /funnels/leads).
    """
    org_id = funnel.org_id
    first_name, last_name = _split_name(lead)
    email = (lead.email or "").strip() or None
    phone = (lead.phone or "").strip() or None
    instagram = (lead.instagram or "").strip() or None
    notes = (lead.notes or "").strip() or None
    captured_at = opted_in_at or datetime.now(timezone.utc)

    client = find_client_by_ghl_contact_id(db, org_id, ghl_contact_id) if ghl_contact_id else None
    if client is None and email:
        client = find_client_by_email(db, org_id, email)
    if client is None and phone:
        client = find_client_by_phone(db, org_id, phone)

    if client is not None:
        duplicate = opted_in_at is not None and _already_opted_in_here(client, funnel)

        if first_name is not None and first_name:
            client.first_name = first_name
        if last_name is not None:
            client.last_name = last_name
        if email and (not client.email or not str(client.email).strip()):
            client.email = email
        if phone is not None and phone:
            client.phone = phone
        if instagram is not None and instagram:
            client.instagram = instagram
        if notes is not None and notes:
            client.notes = (client.notes or "").strip() + ("\n\n" + notes if (client.notes or "").strip() else notes)
        # A duplicate keeps its original capture time; answers and UTM still merge.
        prev_prospect = client.meta.get("prospect") if isinstance(client.meta, dict) else None
        prev_captured = prev_prospect.get("captured_at") if isinstance(prev_prospect, dict) else None
        _apply_prospect_meta(
            client,
            funnel,
            lead,
            utm,
            captured_at,
            ghl_contact_id,
        )
        if duplicate and prev_captured:
            client.meta["prospect"]["captured_at"] = prev_captured

        tagged_now = False
        if reattribute and _can_reattribute(client):
            client.source_channel = "paid"
            client.source_funnel_id = funnel.id
            client.opted_in_at = captured_at
            tagged_now = True
        elif client.source_channel is None:
            # First-touch attribution: never overwrite a known channel, but a legacy
            # row with no channel on record (pre-091) takes this funnel as its source.
            client.source_channel = "paid"
            client.source_funnel_id = funnel.id
            tagged_now = True

        from app.services.client_automation import (
            apply_automatic_lifecycle_for_client,
            apply_funnel_lead_lifecycle,
        )

        apply_funnel_lead_lifecycle(client)
        client.updated_at = datetime.utcnow()
        db.commit()
        db.refresh(client)
        try:
            apply_automatic_lifecycle_for_client(db, client)
            db.commit()
            db.refresh(client)
        except Exception as lc_err:
            LOG.warning("funnel lead lifecycle reconcile skipped for %s: %s", client.id, lc_err)
            from app.services.integration_side_effects import emit_automation_failure_discord

            emit_automation_failure_discord(
                org_id=org_id,
                where="funnels.apply_automatic_lifecycle_for_client",
                error=lc_err,
                client_id=client.id,
            )
        invalidate_health_score_cache(db, client.id, org_id)
        if notify and not duplicate:
            _notify(db, client=client, funnel=funnel, lead=lead, is_new_client=False)
        return FunnelLeadResult(client=client, created=False, tagged_now=tagged_now, duplicate=duplicate)

    # Create new client (require at least one identifier for a useful record)
    if not email and not first_name and not last_name and not phone and not instagram:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="At least one of email, name, phone, or instagram is required",
        )
    client = Client(
        org_id=org_id,
        email=email or None,
        first_name=first_name or None,
        last_name=last_name or None,
        phone=phone or None,
        instagram=instagram or None,
        notes=notes or None,
        lifecycle_state=LifecycleState.QUALIFIED,
        source_channel="paid",
        source_funnel_id=funnel.id,
    )
    if opted_in_at is not None:
        # Backfilled leads keep their real dates in every count that reads created_at.
        client.opted_in_at = opted_in_at
        client.created_at = _to_naive_utc(opted_in_at)
    _apply_prospect_meta(client, funnel, lead, utm, captured_at, ghl_contact_id)
    db.add(client)
    db.commit()
    db.refresh(client)
    invalidate_health_score_cache(db, client.id, org_id)
    if notify:
        _notify(db, client=client, funnel=funnel, lead=lead, is_new_client=True)
    return FunnelLeadResult(client=client, created=True, tagged_now=True)

"""Orchestration for the public post-sales close survey."""
from __future__ import annotations

import logging
import uuid
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from fastapi import HTTPException, status
from sqlalchemy.orm import Session
from sqlalchemy.orm.attributes import flag_modified

from app.models.client import Client, LifecycleState
from app.models.manual_payment import ManualPayment
from app.models.organization import Organization
from app.schemas.client import ClientOfferEnrollmentPatch
from app.schemas.close_survey import (
    CloseSurveyClientOption,
    CloseSurveyMetaResponse,
    CloseSurveyOfferOption,
    CloseSurveySubmitRequest,
    CloseSurveySubmitResponse,
)
from app.services.offer_ladder import resolve_org_offer_ladder
from app.services.stripe_processor import _mark_latest_sales_call_closed
from app.services.terminal_metrics_service import invalidate_terminal_monthly_trends_cache

LOG = logging.getLogger("app.close_survey")

_CLIENT_CAP = 2000
_IDEMPOTENCY_WINDOW = timedelta(minutes=5)


def resolve_org_by_close_token(db: Session, token: str) -> Organization:
    try:
        token_uuid = uuid.UUID(str(token).strip())
    except (ValueError, TypeError, AttributeError):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Invalid survey link")
    org = (
        db.query(Organization)
        .filter(Organization.close_form_token == token_uuid)
        .first()
    )
    if not org:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Survey link not found")
    return org


def _client_display_name(client: Client) -> str:
    parts = [p for p in [(client.first_name or "").strip(), (client.last_name or "").strip()] if p]
    if parts:
        return " ".join(parts)
    if client.email:
        return str(client.email).strip()
    return "Unnamed client"


def _lifecycle_str(state: Any) -> str:
    if state is None:
        return "qualified"
    if hasattr(state, "value"):
        return str(state.value)
    return str(state)


def build_close_survey_meta(db: Session, org: Organization) -> CloseSurveyMetaResponse:
    clients = (
        db.query(Client)
        .filter(Client.org_id == org.id)
        .order_by(Client.first_name.asc().nullslast(), Client.last_name.asc().nullslast())
        .limit(_CLIENT_CAP)
        .all()
    )
    client_opts = [
        CloseSurveyClientOption(
            id=str(c.id),
            name=_client_display_name(c),
            email=(c.email or None),
            lifecycle_state=_lifecycle_str(c.lifecycle_state),
        )
        for c in clients
    ]
    client_opts.sort(key=lambda c: (c.name or "").lower())

    offers: List[CloseSurveyOfferOption] = []
    ladder = resolve_org_offer_ladder(db, org.id)
    if ladder:
        core = ladder.get("core_offer") or {}
        if isinstance(core, dict) and (core.get("name") or "").strip():
            offers.append(
                CloseSurveyOfferOption(
                    slot="core",
                    label=str(core.get("name")).strip(),
                    suggested_total_cents=None,
                )
            )
        for idx, item in enumerate(ladder.get("upsells") or []):
            if not isinstance(item, dict):
                continue
            name = (item.get("name") or "").strip()
            if not name:
                continue
            offers.append(
                CloseSurveyOfferOption(
                    slot=f"upsell:{idx}",
                    label=name,
                    suggested_total_cents=None,
                )
            )
        for idx, item in enumerate(ladder.get("downsells") or []):
            if not isinstance(item, dict):
                continue
            name = (item.get("name") or "").strip()
            if not name:
                continue
            offers.append(
                CloseSurveyOfferOption(
                    slot=f"downsell:{idx}",
                    label=name,
                    suggested_total_cents=None,
                )
            )
        referral = ladder.get("referral_offer")
        if isinstance(referral, dict) and (
            (referral.get("incentive") or "").strip()
            or (referral.get("ask_script_hints") or "").strip()
        ):
            offers.append(
                CloseSurveyOfferOption(
                    slot="referral",
                    label="Referral offer",
                    suggested_total_cents=None,
                )
            )
    offers.append(CloseSurveyOfferOption(slot="custom", label="Custom", suggested_total_cents=None))

    return CloseSurveyMetaResponse(
        org_name=org.name or "Organization",
        clients=client_opts,
        offers=offers,
    )


def _parse_dollars_to_cents(amount: Optional[float]) -> Optional[int]:
    if amount is None:
        return None
    if amount < 0:
        raise HTTPException(status_code=400, detail="Amount cannot be negative")
    return int(round(float(amount) * 100))


def _entry_day(body: CloseSurveySubmitRequest) -> date:
    return body.entry_date or datetime.now(timezone.utc).date()


def _entry_datetime(entry_day: date) -> datetime:
    # Noon UTC keeps the calendar day stable across most US timezones.
    return datetime(entry_day.year, entry_day.month, entry_day.day, 12, 0, 0, tzinfo=timezone.utc)


def _idempotency_conflict(
    db: Session,
    *,
    org_id: uuid.UUID,
    client: Client,
    entry_day: date,
    cash_cents: Optional[int],
) -> bool:
    header = f"### Post-sales {entry_day.isoformat()}"
    notes = client.notes or ""
    if header in notes:
        # Allow re-submit after the window if notes are old enough / no recent payment.
        updated = client.updated_at
        if updated is not None:
            if updated.tzinfo is None:
                updated = updated.replace(tzinfo=timezone.utc)
            if datetime.now(timezone.utc) - updated < _IDEMPOTENCY_WINDOW:
                return True

    if cash_cents and cash_cents > 0:
        since = datetime.now(timezone.utc) - _IDEMPOTENCY_WINDOW
        dup = (
            db.query(ManualPayment)
            .filter(
                ManualPayment.org_id == org_id,
                ManualPayment.client_id == client.id,
                ManualPayment.amount_cents == cash_cents,
                ManualPayment.description.ilike("%Post-sales survey%"),
                ManualPayment.created_at >= since,
            )
            .first()
        )
        if dup:
            return True
    return False


def _append_notes(
    client: Client,
    *,
    entry_day: date,
    closed: bool,
    payment_source: str,
    cash_cents: Optional[int],
    recording_url: Optional[str],
    call_notes: Optional[str],
) -> None:
    if payment_source == "manual" and cash_cents and cash_cents > 0:
        pay_line = f"manual|${cash_cents / 100:.2f}"
    else:
        pay_line = payment_source

    lines = [
        f"### Post-sales {entry_day.isoformat()}",
        f"Closed: {'yes' if closed else 'no'} · Payment: {pay_line}",
    ]
    if recording_url:
        lines.append(f"Recording: {recording_url.strip()}")
    notes_body = (call_notes or "").strip()
    if notes_body:
        lines.append(notes_body)

    block = "\n".join(lines).rstrip() + "\n"
    existing = (client.notes or "").rstrip()
    client.notes = f"{existing}\n\n{block}".strip() + "\n" if existing else block + "\n"
    client.updated_at = datetime.utcnow()


def _apply_offer_enrollment(
    client: Client,
    *,
    offer_slot: Optional[str],
    offer_name: Optional[str],
    contract_cents: Optional[int],
    offer_labels: Dict[str, str],
) -> None:
    if not offer_slot and contract_cents is None:
        return
    slot = (offer_slot or "custom").strip()
    label = (offer_name or "").strip() or offer_labels.get(slot) or slot
    total = int(contract_cents or 0)
    existing = client.offer_enrollment if isinstance(client.offer_enrollment, dict) else {}
    paid = int(existing.get("paid_cents") or 0) if existing.get("slot") == slot else 0
    patch = ClientOfferEnrollmentPatch(
        slot=slot,
        name_snapshot=label[:220] if label else None,
        total_cents=total,
        paid_cents=paid,
        currency="usd",
        notes=existing.get("notes") if isinstance(existing.get("notes"), str) else None,
    )
    client.offer_enrollment = patch.model_dump()
    flag_modified(client, "offer_enrollment")


def _force_active(client: Client) -> None:
    """Survey asserts close — move to Active even without a payment row yet."""
    if client.lifecycle_state == LifecycleState.ACTIVE:
        return
    client.lifecycle_state = LifecycleState.ACTIVE
    client.last_activity_at = datetime.utcnow()


def submit_close_survey(
    db: Session,
    org: Organization,
    body: CloseSurveySubmitRequest,
) -> CloseSurveySubmitResponse:
    entry_day = _entry_day(body)
    when = _entry_datetime(entry_day)
    cash_cents = _parse_dollars_to_cents(body.cash_collected)
    contract_cents = _parse_dollars_to_cents(body.contract_amount)

    client = (
        db.query(Client)
        .filter(Client.id == body.client_id, Client.org_id == org.id)
        .first()
    )
    if not client:
        raise HTTPException(status_code=404, detail="Client not found")

    if _idempotency_conflict(
        db,
        org_id=org.id,
        client=client,
        entry_day=entry_day,
        cash_cents=cash_cents if body.payment_source == "manual" else None,
    ):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Duplicate submit for this client and date — wait a few minutes or change cash amount.",
        )

    meta = build_close_survey_meta(db, org)
    offer_labels = {o.slot: o.label for o in meta.offers}

    manual_payment_id: Optional[str] = None

    # 1) Manual payment
    if body.payment_source == "manual" and cash_cents and cash_cents > 0:
        mp = ManualPayment(
            org_id=org.id,
            client_id=client.id,
            amount_cents=cash_cents,
            currency="usd",
            payment_date=when,
            description=f"Post-sales survey · {entry_day.isoformat()}",
            payment_method="manual",
            created_by=None,
        )
        db.add(mp)
        db.flush()
        manual_payment_id = str(mp.id)
        try:
            from app.services.client_automation import apply_automatic_lifecycle_for_client

            apply_automatic_lifecycle_for_client(db, client)
        except Exception as lc_err:
            LOG.warning("lifecycle after manual pay skipped for %s: %s", client.id, lc_err)

    # 2) Closed → sale_closed + Active
    if body.closed:
        try:
            _mark_latest_sales_call_closed(db, org.id, client)
        except Exception as e:
            LOG.warning("mark sale closed failed for %s: %s", client.id, e)
        _force_active(client)

    # 3) Offer enrollment — capture prior contract so KPI revenue uses a delta
    # (avoids double-counting when drawer already synced the same amount).
    prev_contract_cents = 0
    if isinstance(getattr(client, "offer_enrollment", None), dict):
        try:
            prev_contract_cents = int(client.offer_enrollment.get("total_cents") or 0) or 0
        except (TypeError, ValueError):
            prev_contract_cents = 0
    try:
        _apply_offer_enrollment(
            client,
            offer_slot=body.offer_slot,
            offer_name=body.offer_name,
            contract_cents=contract_cents,
            offer_labels=offer_labels,
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid offer enrollment: {e}") from e

    # 4) Notes
    _append_notes(
        client,
        entry_day=entry_day,
        closed=body.closed,
        payment_source=body.payment_source,
        cash_cents=cash_cents,
        recording_url=body.recording_url,
        call_notes=body.call_notes,
    )

    db.commit()
    db.refresh(client)

    # 5) KPI live sync (cash / closes from check-ins + payments)
    try:
        from app.services.kpi_integration_sync import sync_kpi_for_datetime

        sync_kpi_for_datetime(db, org.id, when, commit=True)
    except Exception as e:
        LOG.warning("KPI sync after close survey failed: %s", e)

    # 6) Optional KPI revenue add (contract / AOV) — delta vs previous enrollment.
    # Only when the survey supplied a contract amount (or cleared via offer write).
    if contract_cents is not None or body.offer_slot:
        new_contract = int(contract_cents or 0)
        revenue_delta_usd = (new_contract - prev_contract_cents) / 100.0
        if revenue_delta_usd != 0:
            try:
                from app.api.kpi import _upsert_kpi_entry_for_org

                _upsert_kpi_entry_for_org(
                    db,
                    org.id,
                    entry_day,
                    {"revenue": revenue_delta_usd},
                    additive=True,
                )
            except Exception as e:
                LOG.warning("KPI revenue add after close survey failed: %s", e)

    try:
        invalidate_terminal_monthly_trends_cache(org.id)
    except Exception:
        pass

    return CloseSurveySubmitResponse(
        ok=True,
        client_id=str(client.id),
        closed=body.closed,
        payment_source=body.payment_source,
        manual_payment_id=manual_payment_id,
        lifecycle_state=_lifecycle_str(client.lifecycle_state),
        submitted_at=datetime.now(timezone.utc),
    )

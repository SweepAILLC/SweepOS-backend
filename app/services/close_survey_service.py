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
from app.models.client_checkin import ClientCheckIn
from app.models.calendar_booking_sales import CalendarBookingSales
from app.models.funnel import Funnel
from app.models.manual_payment import ManualPayment
from app.models.organization import Organization
from app.models.sales_activity_event import SalesActivityEvent
from app.models.user import User, role_to_api
from app.models.user_organization import UserOrganization
from app.schemas.client import ClientOfferEnrollmentPatch
from app.schemas.close_survey import (
    CloseSurveyClientOption,
    CloseSurveyCloserOption,
    CloseSurveyLeadSourceOption,
    DealOutcome,
    CloseSurveyMetaResponse,
    CloseSurveyOfferOption,
    CloseSurveySubmitRequest,
    CloseSurveySubmitResponse,
)
from app.services.offer_ladder import resolve_org_offer_ladder
from app.services.terminal_metrics_service import invalidate_terminal_monthly_trends_cache

_ORGANIC_LEAD_KEY = "organic"

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


def _user_display_name(user: User) -> str:
    email = (getattr(user, "email", None) or "").strip()
    if email and "@" in email:
        return email.split("@")[0]
    return email or "Member"


def _list_org_closers(db: Session, org_id: uuid.UUID) -> List[CloseSurveyCloserOption]:
    """Union of users.home-org + UserOrganization members (owners/admins/system owners)."""
    by_id: Dict[str, User] = {}
    for u in db.query(User).filter(User.org_id == org_id).all():
        by_id[str(u.id)] = u
    for u in (
        db.query(User)
        .join(UserOrganization, UserOrganization.user_id == User.id)
        .filter(UserOrganization.org_id == org_id)
        .all()
    ):
        by_id[str(u.id)] = u

    opts: List[CloseSurveyCloserOption] = []
    for u in by_id.values():
        try:
            role = role_to_api(u.role) if u.role is not None else "member"
        except Exception:
            role = "member"
        opts.append(
            CloseSurveyCloserOption(
                id=str(u.id),
                name=_user_display_name(u),
                email=(u.email or None),
                role=role,
            )
        )
    opts.sort(key=lambda c: (c.name or "").lower())
    return opts


def _list_lead_sources(db: Session, org_id: uuid.UUID) -> List[CloseSurveyLeadSourceOption]:
    sources: List[CloseSurveyLeadSourceOption] = [
        CloseSurveyLeadSourceOption(key=_ORGANIC_LEAD_KEY, label="Organic", funnel_id=None)
    ]
    funnels = (
        db.query(Funnel)
        .filter(Funnel.org_id == org_id)
        .order_by(Funnel.name.asc())
        .all()
    )
    for f in funnels:
        name = (f.name or "").strip() or "Untitled funnel"
        sources.append(
            CloseSurveyLeadSourceOption(
                key=str(f.id),
                label=name,
                funnel_id=str(f.id),
            )
        )
    return sources


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
        closers=_list_org_closers(db, org.id),
        lead_sources=_list_lead_sources(db, org.id),
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
    deal_outcome: DealOutcome,
    payment_source: str,
    cash_cents: Optional[int],
    recording_url: Optional[str],
    call_notes: Optional[str],
    closer_name: Optional[str] = None,
    lead_source_label: Optional[str] = None,
) -> None:
    if payment_source == "manual" and cash_cents and cash_cents > 0:
        pay_line = f"manual|${cash_cents / 100:.2f}"
    else:
        pay_line = payment_source

    lines = [
        f"### Post-sales {entry_day.isoformat()}",
        f"Outcome: {deal_outcome.replace('_', '-')} · Payment: {pay_line}",
    ]
    if closer_name:
        lines.append(f"Closer: {closer_name}")
    if lead_source_label:
        lines.append(f"Lead source: {lead_source_label}")
    if recording_url:
        lines.append(f"Recording: {recording_url.strip()}")
    notes_body = (call_notes or "").strip()
    if notes_body:
        lines.append(notes_body)

    block = "\n".join(lines).rstrip() + "\n"
    existing = (client.notes or "").rstrip()
    client.notes = f"{existing}\n\n{block}".strip() + "\n" if existing else block + "\n"
    client.updated_at = datetime.utcnow()


def _stamp_closer_and_lead_source(
    client: Client,
    *,
    closer_user_id: Optional[str],
    closer_name: Optional[str],
    lead_source_key: str,
    lead_source_label: str,
    funnel_id: Optional[str],
) -> None:
    meta = client.meta if isinstance(client.meta, dict) else {}
    prev_ps = meta.get("post_sales") if isinstance(meta.get("post_sales"), dict) else {}
    post_sales = {
        **prev_ps,
        "closer_user_id": closer_user_id,
        "closer_name": closer_name,
        "lead_source_key": lead_source_key,
        "lead_source": lead_source_label,
        "funnel_id": funnel_id,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    meta = {**meta, "post_sales": post_sales}

    prev_prospect = meta.get("prospect") if isinstance(meta.get("prospect"), dict) else {}
    prospect = {**prev_prospect, "source": lead_source_label}
    if funnel_id:
        prospect["funnel_id"] = funnel_id
    meta["prospect"] = prospect

    client.meta = meta
    flag_modified(client, "meta")


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


def _resolve_deal_outcome(body: CloseSurveySubmitRequest) -> DealOutcome:
    if body.deal_outcome is not None:
        return body.deal_outcome
    return "yes" if body.closed else "no"


def _apply_latest_sales_call_outcome(
    db: Session,
    *,
    org_id: uuid.UUID,
    client: Client,
    outcome: DealOutcome,
) -> Optional[datetime]:
    """Apply Close / No show to latest sales call. No Close leaves flags empty."""
    check_in = (
        db.query(ClientCheckIn)
        .filter(
            ClientCheckIn.org_id == org_id,
            ClientCheckIn.client_id == client.id,
            ClientCheckIn.is_sales_call == True,
        )
        .order_by(ClientCheckIn.start_time.desc())
        .limit(1)
        .first()
    )
    if not check_in:
        return None

    # "no" (No Close): leave sale_closed / no_show empty — do not stamp the call.
    if outcome == "no":
        return check_in.start_time

    if outcome == "yes":
        check_in.sale_closed = True
        check_in.no_show = False
    elif outcome == "no_show":
        check_in.sale_closed = False
        check_in.no_show = True
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
            sales_row.sale_closed = check_in.sale_closed
            sales_row.updated_at = datetime.utcnow()
        else:
            db.add(
                CalendarBookingSales(
                    org_id=org_id,
                    provider=check_in.provider,
                    event_id=check_in.event_id,
                    event_uri=check_in.event_uri,
                    is_sales_call=True,
                    sale_closed=check_in.sale_closed,
                )
            )
    return check_in.start_time


def submit_close_survey(
    db: Session,
    org: Organization,
    body: CloseSurveySubmitRequest,
) -> CloseSurveySubmitResponse:
    outcome = _resolve_deal_outcome(body)
    is_closed = outcome == "yes"
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

    closer_user_id_str: Optional[str] = None
    closer_name: Optional[str] = None
    if body.closer_user_id is not None:
        closer_key = str(body.closer_user_id)
        closer_opt = next((c for c in meta.closers if c.id == closer_key), None)
        if closer_opt is None:
            raise HTTPException(status_code=400, detail="Invalid closer")
        closer_user_id_str = closer_opt.id
        closer_name = closer_opt.name or closer_opt.email or closer_opt.id

    lead_key = (body.lead_source_key or _ORGANIC_LEAD_KEY).strip() or _ORGANIC_LEAD_KEY
    lead_opt = next((ls for ls in meta.lead_sources if ls.key == lead_key), None)
    if lead_opt is None:
        raise HTTPException(status_code=400, detail="Invalid lead source")
    lead_source_label = lead_opt.label
    lead_funnel_id = lead_opt.funnel_id

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

    # 2) Outcome → latest sales check-in state + lifecycle
    sales_event_when: Optional[datetime] = None
    try:
        sales_event_when = _apply_latest_sales_call_outcome(
            db,
            org_id=org.id,
            client=client,
            outcome=outcome,
        )
    except Exception as e:
        LOG.warning("apply sales outcome failed for %s: %s", client.id, e)

    if outcome == "yes":
        _force_active(client)
    elif outcome == "no_show":
        client.lifecycle_state = LifecycleState.QUALIFIED
        client.last_activity_at = datetime.utcnow()
    else:
        # No Close → nurturing; call flags left empty above.
        client.lifecycle_state = LifecycleState.NURTURING
        client.last_activity_at = datetime.utcnow()

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

    # 4) Closer + lead source on client meta
    _stamp_closer_and_lead_source(
        client,
        closer_user_id=closer_user_id_str,
        closer_name=closer_name,
        lead_source_key=lead_key,
        lead_source_label=lead_source_label,
        funnel_id=lead_funnel_id,
    )

    # 4b) Immutable per-rep log entry (independent of the client-meta stamp
    # above, which gets overwritten on the client's next survey) — this is
    # what the by-rep KPI performance dashboard reads from.
    db.add(
        SalesActivityEvent(
            org_id=org.id,
            entry_date=entry_day,
            rep_user_id=uuid.UUID(closer_user_id_str) if closer_user_id_str else None,
            rep_role="closer",
            client_id=client.id,
            cash_collected_cents=cash_cents,
            is_closed=is_closed,
            source="close_survey",
        )
    )

    # 5) Notes
    _append_notes(
        client,
        entry_day=entry_day,
        deal_outcome=outcome,
        payment_source=body.payment_source,
        cash_cents=cash_cents,
        recording_url=body.recording_url,
        call_notes=body.call_notes,
        closer_name=closer_name,
        lead_source_label=lead_source_label,
    )

    db.commit()
    db.refresh(client)

    # 6) KPI + terminal refresh async (public form stays fast; works offline for users)
    revenue_delta_usd = 0.0
    if contract_cents is not None or body.offer_slot:
        new_contract = int(contract_cents or 0)
        revenue_delta_usd = (new_contract - prev_contract_cents) / 100.0

    try:
        from app.services.client_automation import enqueue_close_survey_kpi_sync

        enqueue_close_survey_kpi_sync(
            org.id,
            when=sales_event_when or when,
            entry_day=entry_day,
            revenue_delta_usd=revenue_delta_usd if revenue_delta_usd != 0 else 0.0,
        )
    except Exception as e:
        LOG.warning("enqueue close-survey KPI sync failed: %s", e)

    return CloseSurveySubmitResponse(
        ok=True,
        client_id=str(client.id),
        closed=is_closed,
        deal_outcome=outcome,
        payment_source=body.payment_source,
        closer_user_id=closer_user_id_str,
        lead_source=lead_source_label,
        manual_payment_id=manual_payment_id,
        lifecycle_state=_lifecycle_str(client.lifecycle_state),
        submitted_at=datetime.now(timezone.utc),
    )

"""Live sync of KPI auto fields from calendar and payment sources.

Force-refreshes: calls_booked, calls_taken, closes, no_shows, cash_collected.
Never writes revenue (manual-only).
"""
from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

from sqlalchemy.orm import Session

from app.models.client import Client
from app.models.client_checkin import ClientCheckIn
from app.models.manual_payment import ManualPayment
from app.models.oauth_token import OAuthProvider, OAuthToken
from app.models.org_kpi_daily_entry import OrgKpiDailyEntry
from app.models.sales_activity_event import SalesActivityEvent
from app.models.stripe_payment import StripePayment
from app.models.whop_payment import WhopPayment
from app.schemas.kpi import KpiRevenueContributor

LIVE_CALENDAR_FIELDS = ("calls_booked", "calls_booked_activity", "calls_taken", "closes", "no_shows")
LIVE_PAYMENT_FIELDS = ("cash_collected",)
_WHOP_PAID = ("paid", "succeeded", "completed", "successful")

# Distinguishes "caller did not supply a DM activation date" from the meaningful
# value None, which means this org must never have its DM fields auto-filled.
_RESOLVE = object()

def _sales_call_unique_key(ci: ClientCheckIn) -> str:
    """Stable key so duplicate check-in rows for the same calendar event count once."""
    event_id = (getattr(ci, "event_id", None) or "").strip()
    if event_id:
        provider = (getattr(ci, "provider", None) or "").strip()
        return f"{provider}:{event_id}"
    return f"id:{ci.id}"


_BOOKING_TS_KEYS = ("createdAt", "created_at", "dateAdded", "date_added")


def sales_call_booked_at(ci: ClientCheckIn) -> Optional[datetime]:
    """
    When the call was *booked* (not when it happens). Cal.com stores `createdAt`,
    Calendly `created_at`, GHL `dateAdded` in raw_event_data. The row's own
    created_at is a sync timestamp — backfills land days after the call — so it is
    only trusted when it precedes the call; otherwise fall back to the call time.
    """
    raw = getattr(ci, "raw_event_data", None)
    if raw:
        try:
            data = json.loads(raw) if isinstance(raw, str) else raw
        except (TypeError, ValueError):
            data = None
        if isinstance(data, dict):
            for key in _BOOKING_TS_KEYS:
                val = data.get(key)
                if isinstance(val, str) and val:
                    try:
                        return _ensure_utc(datetime.fromisoformat(val.replace("Z", "+00:00")))
                    except ValueError:
                        continue
    start = _ensure_utc(getattr(ci, "start_time", None))
    created = _ensure_utc(getattr(ci, "created_at", None))
    if created is not None and start is not None and created <= start:
        return created
    return start


def count_sales_calls(
    checkins: Iterable[ClientCheckIn],
    start: datetime,
    end: datetime,
) -> Dict[str, int]:
    """
    The one definition of the calendar-derived sales-call counts, for any window
    ([start, end] inclusive, tz-aware). Used by Sales KPIs (per day, per rep) and
    the Funnels scorecard (per week) so the two tabs cannot disagree.

    - calls_booked: calls *scheduled* in the window, not cancelled ("Calls on Calendar")
    - calls_booked_activity: calls *booked* in the window (provider booking timestamp),
      not cancelled ("Booked Calls")
    - calls_taken: scheduled in the window, attended ("Live Calls")
    - no_shows: scheduled in the window, marked no-show
    Duplicate rows for the same calendar event count once.
    """
    scheduled: List[ClientCheckIn] = []
    booked: List[ClientCheckIn] = []
    for ci in checkins:
        st = _ensure_utc(ci.start_time)
        if st and start <= st <= end:
            scheduled.append(ci)
        booked_at = sales_call_booked_at(ci)
        if booked_at and start <= booked_at <= end:
            booked.append(ci)
    return {
        "calls_booked": _count_unique_sales_calls(scheduled, lambda ci: not ci.cancelled),
        "calls_booked_activity": _count_unique_sales_calls(booked, lambda ci: not ci.cancelled),
        "calls_taken": _count_unique_sales_calls(
            scheduled, lambda ci: bool(ci.completed) and not ci.cancelled and not ci.no_show
        ),
        "no_shows": _count_unique_sales_calls(scheduled, lambda ci: bool(ci.no_show)),
    }


def _count_unique_sales_calls(rows: Iterable[ClientCheckIn], predicate) -> int:
    seen: Set[str] = set()
    n = 0
    for ci in rows:
        if not getattr(ci, "is_sales_call", False):
            continue
        if not predicate(ci):
            continue
        key = _sales_call_unique_key(ci)
        if key in seen:
            continue
        seen.add(key)
        n += 1
    return n


def _min_date_map(*maps: Dict[uuid.UUID, date]) -> Dict[uuid.UUID, date]:
    """Earliest date per client across sources (payment, sale_closed, form)."""
    out: Dict[uuid.UUID, date] = {}
    for mapping in maps:
        for cid, day in mapping.items():
            if cid is None or day is None:
                continue
            prev = out.get(cid)
            if prev is None or day < prev:
                out[cid] = day
    return out


def count_conversions_on_day(conversion_dates: Dict[uuid.UUID, date], entry_day: date) -> int:
    return sum(1 for day in conversion_dates.values() if day == entry_day)


def _first_payment_dates_by_client(db: Session, org_id: uuid.UUID) -> Dict[uuid.UUID, date]:
    out: Dict[uuid.UUID, date] = {}

    def add(cid: Optional[uuid.UUID], ts: Optional[datetime]) -> None:
        if cid is None:
            return
        when = _ensure_utc(ts)
        if when is None:
            return
        day = when.date()
        prev = out.get(cid)
        if prev is None or day < prev:
            out[cid] = day

    for p in (
        db.query(StripePayment.client_id, StripePayment.created_at)
        .filter(
            StripePayment.org_id == org_id,
            StripePayment.status == "succeeded",
            StripePayment.client_id.isnot(None),
            StripePayment.amount_cents > 0,
        )
        .all()
    ):
        add(p.client_id, p.created_at)
    for p in (
        db.query(WhopPayment.client_id, WhopPayment.created_at, WhopPayment.status)
        .filter(WhopPayment.org_id == org_id, WhopPayment.client_id.isnot(None))
        .all()
    ):
        if (p.status or "").lower() not in _WHOP_PAID:
            continue
        add(p.client_id, p.created_at)
    for p in (
        db.query(ManualPayment.client_id, ManualPayment.payment_date, ManualPayment.created_at)
        .filter(ManualPayment.org_id == org_id, ManualPayment.superseded_at.is_(None))
        .all()
    ):
        add(p.client_id, p.payment_date or p.created_at)
    return out


def _first_sale_closed_dates(checkins: Iterable[ClientCheckIn]) -> Dict[uuid.UUID, date]:
    out: Dict[uuid.UUID, date] = {}
    for ci in checkins:
        if not getattr(ci, "is_sales_call", False) or ci.sale_closed is not True:
            continue
        cid = getattr(ci, "client_id", None)
        when = _ensure_utc(getattr(ci, "start_time", None))
        if cid is None or when is None:
            continue
        day = when.date()
        prev = out.get(cid)
        if prev is None or day < prev:
            out[cid] = day
    return out


def _first_form_close_dates(db: Session, org_id: uuid.UUID) -> Dict[uuid.UUID, date]:
    out: Dict[uuid.UUID, date] = {}
    rows = (
        db.query(SalesActivityEvent.client_id, SalesActivityEvent.entry_date)
        .filter(
            SalesActivityEvent.org_id == org_id,
            SalesActivityEvent.is_closed.is_(True),
            SalesActivityEvent.client_id.isnot(None),
        )
        .all()
    )
    for cid, entry_day in rows:
        if cid is None or entry_day is None:
            continue
        prev = out.get(cid)
        if prev is None or entry_day < prev:
            out[cid] = entry_day
    return out


def conversion_dates_by_client(
    db: Session,
    org_id: uuid.UUID,
    checkins: Iterable[ClientCheckIn],
) -> Dict[uuid.UUID, date]:
    """One conversion date per client: first payment, first sale_closed, or first form yes."""
    return _min_date_map(
        _first_payment_dates_by_client(db, org_id),
        _first_sale_closed_dates(checkins),
        _first_form_close_dates(db, org_id),
    )


def compute_sales_call_rates_for_window(
    db: Session,
    org_id: uuid.UUID,
    window_start: datetime,
    window_end: datetime,
    *,
    now_utc: Optional[datetime] = None,
) -> Dict[str, Any]:
    """
    Shared close-rate / show-up-rate engine — the single source of truth for
    Sales KPIs, the Terminal general dashboard, and the Terminal calendar/
    booking widget. Replaces three previously-independent implementations
    (admin.py::_org_close_rate_pct/_org_show_up_rate_pct, calendar_trend_summary.py's
    inline math) that disagreed on provider scope, payment sources, and
    denominator — see the PRD audit "three different close-rate formulas."

    window_start/window_end are a half-open [start, end) datetime range.
    Pass `now_utc` to clip the booked/taken portion to calls that have
    already happened (needed for "this month so far" / rolling-30d windows
    where window_end is in the future) — conversion dates are never clipped,
    since a real payment/close timestamp is already bounded by reality.

    Denominator for close rate: calls_taken (completed, not cancelled, not
    no-show) — a no-show is a show-up-rate failure, not a close-rate failure.
    Providers: all (Cal.com, Calendly, GHL) — none excluded.
    Payment sources: Stripe + Whop + ManualPayment (superseded rows already
    excluded upstream) + sale_closed + funnel form-close, via the existing
    conversion_dates_by_client — the most complete signal set already used by
    Sales KPIs' `closes` field.
    """
    # Callers pass a mix of naive-UTC and aware datetimes; normalize so the
    # comparisons below never raise on naive-vs-aware.
    def _as_utc(dt: datetime) -> datetime:
        return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)

    window_start = _as_utc(window_start)
    window_end = _as_utc(window_end)
    if now_utc is not None:
        now_utc = _as_utc(now_utc)
    effective_end = min(window_end, now_utc) if now_utc is not None else window_end

    booked_total = 0
    taken_total = 0
    if effective_end > window_start:
        windowed_checkins = (
            db.query(ClientCheckIn)
            .filter(
                ClientCheckIn.org_id == org_id,
                ClientCheckIn.is_sales_call.is_(True),
                ClientCheckIn.cancelled.is_(False),
                ClientCheckIn.start_time >= window_start,
                ClientCheckIn.start_time < effective_end,
            )
            .all()
        )
        booked_total = len(windowed_checkins)
        taken_total = sum(1 for c in windowed_checkins if bool(c.completed) and not c.no_show)

    # conversion_dates_by_client needs ALL org checkins, unfiltered (matching
    # refresh_kpi_live_fields_for_range's own precedent exactly), to correctly
    # resolve each client's sale_closed date — then the resulting dates are
    # filtered to this window.
    all_checkins = db.query(ClientCheckIn).filter(ClientCheckIn.org_id == org_id).all()
    conversion_dates = conversion_dates_by_client(db, org_id, all_checkins)
    closed_count = sum(
        1
        for d in conversion_dates.values()
        if window_start <= datetime.combine(d, time.min, tzinfo=timezone.utc) < window_end
    )

    show_up_rate_pct = round((taken_total / booked_total) * 100.0, 1) if booked_total else None
    close_rate_pct = round((closed_count / taken_total) * 100.0, 1) if taken_total else None

    return {
        "window_start": window_start,
        "window_end": window_end,
        "sales_calls_booked": booked_total,
        "sales_calls_taken": taken_total,
        "closed_count": closed_count,
        "show_up_rate_pct": show_up_rate_pct,
        "close_rate_pct": close_rate_pct,
    }


@dataclass
class FunnelFacts:
    """
    Raw per-client facts for one window + scope, shared by the Sales KPIs funnel
    strip (compute_funnel_summary) and the Funnels dashboard (weekly buckets,
    UTM-source rows) so both read one data pass and can never disagree.
    """

    window_start: date
    window_end: date
    # client_id -> opt-in time, coalesce(opted_in_at, created_at) (paid / funnel-scoped, in window)
    opt_ins: Dict[uuid.UUID, datetime]
    # client_id -> first sales-call start in window (booked) / first attended call (showed)
    booked: Dict[uuid.UUID, datetime]
    showed: Dict[uuid.UUID, datetime]
    # client_id -> conversion date in window
    closed: Dict[uuid.UUID, date]
    # (client_id, paid_at, amount_cents)
    cash: List[Tuple[Optional[uuid.UUID], datetime, int]]
    # (entry_date, outreach_sent, respondents) from org-aggregate daily KPI rows;
    # empty when scoped to a single funnel (organic outreach has no funnel)
    outreach_rows: List[Tuple[date, int, int]]
    include_organic_outreach: bool
    # Client ids in the channel/funnel scope; None = whole org (no filter).
    scope_ids: Optional[Set[uuid.UUID]] = None


def collect_funnel_facts(
    db: Session,
    org_id: uuid.UUID,
    window_start: date,
    window_end: date,
    channel: Optional[str] = None,
    funnel_id: Optional[uuid.UUID] = None,
) -> FunnelFacts:
    """
    Scope: `channel` None/"all", "organic", or "paid"; `funnel_id` narrows to
    clients whose source_funnel_id is that funnel (implicitly paid). A handful
    of single filtered scans, no per-row N+1.
    """
    start, _ = day_bounds_utc(window_start)
    _, end = day_bounds_utc(window_end)

    # One narrow scan of the org's clients drives both the scope set and opt-ins.
    client_rows = (
        db.query(
            Client.id,
            Client.source_channel,
            Client.source_funnel_id,
            Client.created_at,
            Client.opted_in_at,
        )
        .filter(Client.org_id == org_id)
        .all()
    )
    scope_ids: Optional[Set[uuid.UUID]] = None
    if channel in ("organic", "paid") or funnel_id is not None:
        scope_ids = set()
        for cid, ch, fid, _created, _opted in client_rows:
            if funnel_id is not None and fid != funnel_id:
                continue
            if channel == "paid" and ch != "paid":
                continue
            if channel == "organic" and ch == "paid":
                continue
            scope_ids.add(cid)

    def _in_scope(cid: Optional[uuid.UUID]) -> bool:
        return scope_ids is None or cid in scope_ids

    opt_ins: Dict[uuid.UUID, datetime] = {}
    if channel != "organic":
        for cid, ch, _fid, created, opted in client_rows:
            # A lead tagged after its row existed (GHL sync, re-attribution) opted in at
            # opted_in_at, not when the row was created.
            opted_utc = _ensure_utc(opted or created)
            if ch != "paid" or opted_utc is None or not (start <= opted_utc <= end):
                continue
            if _in_scope(cid):
                opt_ins[cid] = opted_utc

    include_organic_outreach = funnel_id is None and channel != "paid"
    outreach_rows: List[Tuple[date, int, int]] = []
    if include_organic_outreach:
        # One daily ledger: organic respondents include setters' EODs.
        from app.services.kpi_org_totals import fold_org_daily_totals

        for r in fold_org_daily_totals(
            db.query(OrgKpiDailyEntry)
            .filter(
                OrgKpiDailyEntry.org_id == org_id,
                OrgKpiDailyEntry.entry_date >= window_start,
                OrgKpiDailyEntry.entry_date <= window_end,
            )
            .all()
        ):
            outreach_rows.append((r.entry_date, r.outreach_sent or 0, r.respondents or 0))

    booked: Dict[uuid.UUID, datetime] = {}
    showed: Dict[uuid.UUID, datetime] = {}
    for c in (
        db.query(ClientCheckIn)
        .filter(
            ClientCheckIn.org_id == org_id,
            ClientCheckIn.is_sales_call.is_(True),
            ClientCheckIn.cancelled.is_(False),
            ClientCheckIn.start_time >= start,
            ClientCheckIn.start_time <= end,
        )
        .order_by(ClientCheckIn.start_time.asc())
        .all()
    ):
        if not c.client_id or not _in_scope(c.client_id):
            continue
        booked.setdefault(c.client_id, _ensure_utc(c.start_time))
        if bool(c.completed) and not c.no_show:
            showed.setdefault(c.client_id, _ensure_utc(c.start_time))

    all_checkins = db.query(ClientCheckIn).filter(ClientCheckIn.org_id == org_id).all()
    closed = {
        cid: d
        for cid, d in conversion_dates_by_client(db, org_id, all_checkins).items()
        if window_start <= d <= window_end and _in_scope(cid)
    }

    cash: List[Tuple[Optional[uuid.UUID], datetime, int]] = []
    for p in (
        db.query(StripePayment)
        .filter(
            StripePayment.org_id == org_id,
            StripePayment.status == "succeeded",
            StripePayment.created_at >= start,
            StripePayment.created_at <= end,
        )
        .all()
    ):
        if _in_scope(p.client_id):
            cash.append((p.client_id, _ensure_utc(p.created_at), p.amount_cents or 0))
    for p in (
        db.query(WhopPayment)
        .filter(
            WhopPayment.org_id == org_id,
            WhopPayment.created_at >= start,
            WhopPayment.created_at <= end,
        )
        .all()
    ):
        if (p.status or "").lower() not in _WHOP_PAID:
            continue
        if _in_scope(p.client_id):
            cash.append((p.client_id, _ensure_utc(p.created_at), p.amount_cents or 0))
    for p in (
        db.query(ManualPayment)
        .filter(ManualPayment.org_id == org_id, ManualPayment.superseded_at.is_(None))
        .all()
    ):
        ts = _ensure_utc(p.payment_date or p.created_at)
        if ts is None or ts < start or ts > end:
            continue
        if _in_scope(p.client_id):
            cash.append((p.client_id, ts, p.amount_cents or 0))

    return FunnelFacts(
        window_start=window_start,
        window_end=window_end,
        opt_ins=opt_ins,
        booked=booked,
        showed=showed,
        closed=closed,
        cash=cash,
        outreach_rows=outreach_rows,
        include_organic_outreach=include_organic_outreach,
        scope_ids=scope_ids,
    )


def _pct(n: int, d: int) -> Optional[float]:
    return round((n / d) * 100.0, 1) if d else None


def paid_leads_by_day(db: Session, org_id: uuid.UUID, window_start: date, window_end: date) -> Dict[date, int]:
    """
    Paid funnel opt-ins per org-local day (same definition as the Funnels dashboard's
    paid opt-ins). Feeds the KPI snapshot's Total Leads alongside logged conversations.
    """
    from app.services.date_window import org_tz

    tz = org_tz(db, org_id)
    facts = collect_funnel_facts(db, org_id, window_start, window_end, "paid", None)
    out: Dict[date, int] = {}
    for created in facts.opt_ins.values():
        created_utc = _ensure_utc(created)
        if created_utc is None:
            continue
        day = created_utc.astimezone(tz).date()
        if window_start <= day <= window_end:
            out[day] = out.get(day, 0) + 1
    return out


def summarize_funnel_facts(facts: FunnelFacts, channel: Optional[str] = None) -> Dict[str, Any]:
    """Stage counts + ratios for the funnel strip (KpiFunnelSummaryResponse shape)."""
    outreach_sent = sum(o for _d, o, _r in facts.outreach_rows)
    respondents = sum(r for _d, _o, r in facts.outreach_rows)
    # Organic has no landing-page opt-in step — respondents are its top-of-funnel proxy.
    opt_ins = len(facts.opt_ins) + (respondents if facts.include_organic_outreach else 0)
    booked, showed, closed = len(facts.booked), len(facts.showed), len(facts.closed)
    cash_cents = sum(c for _cid, _ts, c in facts.cash)
    return {
        "window_start": facts.window_start,
        "window_end": facts.window_end,
        "channel": channel or "all",
        "outreach_sent": outreach_sent,
        "respondents": respondents,
        "opt_ins": opt_ins,
        "booked": booked,
        "showed": showed,
        "closed": closed,
        "cash_usd": round(cash_cents / 100.0, 2),
        "reply_rate_pct": _pct(respondents, outreach_sent),
        "lead_to_book_rate_pct": _pct(booked, opt_ins),
        "show_rate_pct": _pct(showed, booked),
        "close_rate_pct": _pct(closed, showed),
        "cash_per_close_usd": round(cash_cents / 100.0 / closed, 2) if closed else None,
    }


def compute_funnel_summary(
    db: Session,
    org_id: uuid.UUID,
    window_start: date,
    window_end: date,
    channel: Optional[str] = None,
    funnel_id: Optional[uuid.UUID] = None,
) -> Dict[str, Any]:
    """
    Opt-ins -> Booked -> Showed -> Closed -> Cash stage strip (PRD phases 7-8)
    — replaces the Google Sheet's hand-typed `Call Funnel` tab with a live query.
    Shared by Sales KPIs and the Funnels dashboard via collect_funnel_facts.

    `channel`: None/"all", "organic", or "paid". Organic has no landing-page
    opt-in step — its top-of-funnel proxy is the outreach_sent/respondents
    aggregate from daily KPI entries. Paid opt-ins are Client rows created via
    a tracked funnel in the window. `funnel_id` scopes everything to one funnel.
    """
    facts = collect_funnel_facts(db, org_id, window_start, window_end, channel, funnel_id)
    return summarize_funnel_facts(facts, channel)


def _count_host_conversions(
    rows: Iterable[ClientCheckIn],
    conversion_dates: Dict[uuid.UUID, date],
    entry_day: date,
) -> int:
    seen: Set[uuid.UUID] = set()
    n = 0
    for ci in rows:
        if not getattr(ci, "is_sales_call", False) or ci.sale_closed is not True:
            continue
        cid = getattr(ci, "client_id", None)
        if cid is None or cid in seen:
            continue
        if conversion_dates.get(cid) != entry_day:
            continue
        seen.add(cid)
        n += 1
    return n



def _ensure_utc(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def day_bounds_utc(entry_day: date) -> Tuple[datetime, datetime]:
    start = datetime.combine(entry_day, time.min, tzinfo=timezone.utc)
    end = datetime.combine(entry_day, time.max, tzinfo=timezone.utc)
    return start, end


def find_clients_booked_on_day(
    db: Session,
    org_id: uuid.UUID,
    entry_day: date,
) -> List[Client]:
    """
    Clients whose booking was *made* on this day (ClientCheckIn.created_at),
    matching the same window calls_booked_activity counts — the EOD picker's
    search list, so the count a setter logs and the clients they can tag stay
    the same set.
    """
    start, end = day_bounds_utc(entry_day)
    client_ids = {
        row[0]
        for row in db.query(ClientCheckIn.client_id)
        .filter(
            ClientCheckIn.org_id == org_id,
            ClientCheckIn.created_at >= start,
            ClientCheckIn.created_at <= end,
            ClientCheckIn.cancelled.is_(False),
        )
        .all()
    }
    if not client_ids:
        return []
    return db.query(Client).filter(Client.id.in_(client_ids)).all()


def find_setter_claim_for_client(
    db: Session,
    org_id: uuid.UUID,
    client_id: uuid.UUID,
) -> Optional[uuid.UUID]:
    """
    Which setter (rep_user_id) claimed this client via the EOD picker, if any —
    the most recent per-rep daily-entry row whose setter_booked_client_ids
    contains this client. Used to prefill setter_user_id on the client's close
    event (Close Survey / auto-close); not wired into either yet.
    """
    rows = (
        db.query(OrgKpiDailyEntry)
        .filter(
            OrgKpiDailyEntry.org_id == org_id,
            OrgKpiDailyEntry.rep_user_id.isnot(None),
            OrgKpiDailyEntry.setter_booked_client_ids.isnot(None),
        )
        .order_by(OrgKpiDailyEntry.entry_date.desc())
        .all()
    )
    cid_str = str(client_id)
    for row in rows:
        ids = row.setter_booked_client_ids or []
        if cid_str in ids:
            return row.rep_user_id
    return None


def has_calendar_source(db: Session, org_id: uuid.UUID) -> bool:
    token = (
        db.query(OAuthToken)
        .filter(
            OAuthToken.org_id == org_id,
            OAuthToken.provider.in_([OAuthProvider.CALENDLY, OAuthProvider.CALCOM]),
        )
        .first()
    )
    if token is not None:
        return True
    return (
        db.query(ClientCheckIn.id)
        .filter(ClientCheckIn.org_id == org_id)
        .first()
        is not None
    )


def has_payment_source(db: Session, org_id: uuid.UUID) -> bool:
    return (
        db.query(StripePayment.id).filter(StripePayment.org_id == org_id).first() is not None
        or db.query(WhopPayment.id).filter(WhopPayment.org_id == org_id).first() is not None
        or db.query(ManualPayment.id).filter(ManualPayment.org_id == org_id).first() is not None
    )


def _payment_cash_by_day(
    db: Session,
    org_id: uuid.UUID,
    start: date,
    end: date,
) -> Dict[date, int]:
    """Prefetch cash (cents) per day for a date window — one scan per payment source."""
    range_start, _ = day_bounds_utc(start)
    _, range_end = day_bounds_utc(end)
    by_day: Dict[date, int] = {}

    def add(ts: Optional[datetime], cents: int) -> None:
        if ts is None or cents == 0:
            return
        if ts < range_start or ts > range_end:
            return
        d = ts.date()
        by_day[d] = by_day.get(d, 0) + cents

    for p in (
        db.query(StripePayment)
        .filter(StripePayment.org_id == org_id, StripePayment.status == "succeeded")
        .all()
    ):
        add(_ensure_utc(p.created_at), int(p.amount_cents or 0))
    for p in db.query(WhopPayment).filter(WhopPayment.org_id == org_id).all():
        if (p.status or "").lower() not in ("paid", "succeeded", "completed", "successful"):
            continue
        add(_ensure_utc(p.created_at), int(p.amount_cents or 0))
    for p in (
        db.query(ManualPayment)
        .filter(ManualPayment.org_id == org_id, ManualPayment.superseded_at.is_(None))
        .all()
    ):
        add(_ensure_utc(p.payment_date or p.created_at), int(p.amount_cents or 0))
    return by_day


def _client_display_name(client: Optional[Client]) -> str:
    if client is None:
        return "Unknown client"
    name = " ".join(part for part in (client.first_name, client.last_name) if part).strip()
    return name or (client.email or "Unknown client")


def get_revenue_contributors_for_day(
    db: Session,
    org_id: uuid.UUID,
    entry_day: date,
) -> List[KpiRevenueContributor]:
    """Which clients' payments made up this day's cash_collected — same three
    sources _payment_cash_by_day sums, but itemized instead of totaled."""
    range_start, range_end = day_bounds_utc(entry_day)
    out: List[KpiRevenueContributor] = []

    def add(payment_id: str, client_id: Optional[uuid.UUID], amount_cents: int, source: str) -> None:
        if amount_cents == 0:
            return
        client = db.query(Client).filter(Client.id == client_id).first() if client_id else None
        out.append(
            KpiRevenueContributor(
                client_id=client_id,
                client_name=_client_display_name(client),
                amount_cents=amount_cents,
                source=source,
                payment_id=payment_id,
            )
        )

    for p in (
        db.query(StripePayment)
        .filter(StripePayment.org_id == org_id, StripePayment.status == "succeeded")
        .all()
    ):
        ts = _ensure_utc(p.created_at)
        if ts and range_start <= ts <= range_end:
            add(str(p.id), p.client_id, int(p.amount_cents or 0), "stripe")

    for p in db.query(WhopPayment).filter(WhopPayment.org_id == org_id).all():
        if (p.status or "").lower() not in ("paid", "succeeded", "completed", "successful"):
            continue
        ts = _ensure_utc(p.created_at)
        if ts and range_start <= ts <= range_end:
            add(str(p.id), p.client_id, int(p.amount_cents or 0), "whop")

    for p in (
        db.query(ManualPayment)
        .filter(ManualPayment.org_id == org_id, ManualPayment.superseded_at.is_(None))
        .all()
    ):
        ts = _ensure_utc(p.payment_date or p.created_at)
        if ts and range_start <= ts <= range_end:
            add(str(p.id), p.client_id, int(p.amount_cents or 0), "manual")

    out.sort(key=lambda c: c.amount_cents, reverse=True)
    return out


def compute_live_fields_for_day(
    db: Session,
    org_id: uuid.UUID,
    entry_day: date,
    *,
    calendar_available: Optional[bool] = None,
    payments_available: Optional[bool] = None,
    checkins: Optional[Iterable[ClientCheckIn]] = None,
    cash_by_day: Optional[Dict[date, int]] = None,
    conversion_dates: Optional[Dict[uuid.UUID, date]] = None,
) -> Dict[str, Any]:
    """Compute live auto field values for a single day. Does not include revenue."""
    out: Dict[str, Any] = {}
    start, end = day_bounds_utc(entry_day)
    cal = has_calendar_source(db, org_id) if calendar_available is None else calendar_available
    pay = has_payment_source(db, org_id) if payments_available is None else payments_available

    if checkins is None:
        checkins = db.query(ClientCheckIn).filter(ClientCheckIn.org_id == org_id).all()
    if conversion_dates is None:
        conversion_dates = conversion_dates_by_client(db, org_id, checkins)
    # First payment / form / sale_closed — one close per client, shared with snapshot + grid.
    out["closes"] = count_conversions_on_day(conversion_dates, entry_day)

    if cal:
        # Shared with the Funnels scorecard — see count_sales_calls.
        out.update(count_sales_calls(checkins, start, end))

    if pay:
        if cash_by_day is None:
            cash_by_day = _payment_cash_by_day(db, org_id, entry_day, entry_day)
        out["cash_collected"] = round(cash_by_day.get(entry_day, 0) / 100.0, 2)

    return out


def _compute_host_breakdown_for_day(
    entry_day: date,
    checkins: Iterable[ClientCheckIn],
    conversion_dates: Optional[Dict[uuid.UUID, date]] = None,
) -> Dict[uuid.UUID, Dict[str, int]]:
    """Same calls_booked/calls_taken/closes/no_shows logic as compute_live_fields_for_day,
    scoped per host_user_id — feeds the per-rep org_kpi_daily_entries rows the By Rep
    view reads. Only meaningful for orgs whose calendar setup assigns different hosts;
    checkins with no resolved host_user_id contribute nothing here (they still count
    toward the org-aggregate row as before)."""
    start, end = day_bounds_utc(entry_day)
    by_host: Dict[uuid.UUID, List[ClientCheckIn]] = {}
    for ci in checkins:
        host_id = getattr(ci, "host_user_id", None)
        if host_id:
            by_host.setdefault(host_id, []).append(ci)

    out: Dict[uuid.UUID, Dict[str, int]] = {}
    for host_id, host_rows in by_host.items():
        scheduled = [ci for ci in host_rows if (st := _ensure_utc(ci.start_time)) and start <= st <= end]
        # Shared with the org row and the Funnels scorecard — see count_sales_calls.
        counts = count_sales_calls(host_rows, start, end)
        if not scheduled and not any(counts.values()):
            continue
        out[host_id] = {
            **counts,
            "closes": _count_host_conversions(scheduled, conversion_dates or {}, entry_day),
        }
    return out


def _sync_host_kpi_rows_for_day(
    db: Session,
    org_id: uuid.UUID,
    entry_day: date,
    checkins: Iterable[ClientCheckIn],
    conversion_dates: Optional[Dict[uuid.UUID, date]] = None,
) -> None:
    """Upsert one org_kpi_daily_entries row per host_user_id with calendar-derived
    metrics for entry_day. Never touches cash_collected/revenue/manual fields —
    those come from close-survey attribution (sales_activity_events), not calendars."""
    breakdown = _compute_host_breakdown_for_day(
        entry_day, checkins, conversion_dates=conversion_dates
    )
    for host_id, values in breakdown.items():
        row = (
            db.query(OrgKpiDailyEntry)
            .filter(
                OrgKpiDailyEntry.org_id == org_id,
                OrgKpiDailyEntry.entry_date == entry_day,
                OrgKpiDailyEntry.rep_user_id == host_id,
            )
            .first()
        )
        if row is None:
            row = OrgKpiDailyEntry(org_id=org_id, entry_date=entry_day, rep_user_id=host_id)
            db.add(row)
        for field, value in values.items():
            setattr(row, field, value)
        row.updated_at = datetime.utcnow()


def sync_kpi_day_from_integrations(
    db: Session,
    org_id: uuid.UUID,
    entry_day: date,
    *,
    commit: bool = False,
    force_create: bool = True,
    checkins: Optional[Iterable[ClientCheckIn]] = None,
    calendar_available: Optional[bool] = None,
    payments_available: Optional[bool] = None,
    cash_by_day: Optional[Dict[date, int]] = None,
    conversion_dates: Optional[Dict[uuid.UUID, date]] = None,
) -> Optional[OrgKpiDailyEntry]:
    """
    Force-refresh live auto KPI fields for a day from integrations.
    Creates a row when force_create or when there is non-zero live activity.
    Never modifies revenue or manual fields.
    """
    cal = has_calendar_source(db, org_id) if calendar_available is None else calendar_available
    pay = has_payment_source(db, org_id) if payments_available is None else payments_available

    if checkins is None:
        checkins = db.query(ClientCheckIn).filter(ClientCheckIn.org_id == org_id).all()
    if conversion_dates is None:
        conversion_dates = conversion_dates_by_client(db, org_id, checkins)

    if not cal and not pay and not conversion_dates:
        return None

    values = compute_live_fields_for_day(
        db,
        org_id,
        entry_day,
        calendar_available=cal,
        payments_available=pay,
        checkins=checkins,
        cash_by_day=cash_by_day,
        conversion_dates=conversion_dates,
    )

    if cal and checkins is not None:
        _sync_host_kpi_rows_for_day(
            db, org_id, entry_day, checkins, conversion_dates=conversion_dates
        )

    if not values:
        return None

    has_activity = any(
        (isinstance(v, (int, float)) and v != 0) for v in values.values()
    )
    row = (
        db.query(OrgKpiDailyEntry)
        .filter(
            OrgKpiDailyEntry.org_id == org_id,
            OrgKpiDailyEntry.entry_date == entry_day,
            OrgKpiDailyEntry.rep_user_id.is_(None),
        )
        .first()
    )
    if row is None:
        if not force_create and not has_activity:
            return None
        row = OrgKpiDailyEntry(org_id=org_id, entry_date=entry_day)
        db.add(row)
        if entry_day <= date.today() and row.new_followers is None:
            row.new_followers = 0

    for field, value in values.items():
        setattr(row, field, value)
    row.updated_at = datetime.utcnow()

    if commit:
        db.commit()
        db.refresh(row)
    else:
        db.flush()
    return row


def apply_manual_payment_kpi(
    db: Session,
    org_id: uuid.UUID,
    when: Optional[datetime],
    *,
    revenue_delta_usd: float = 0.0,
) -> None:
    """Refresh live cash/closes for `when`, then add `revenue` onto that KPI day.

    Cash is recomputed from all payments. Revenue stays additive so close-survey
    contract amounts and grid edits are not overwritten.
    """
    sync_kpi_for_datetime(db, org_id, when, commit=True)
    if not revenue_delta_usd:
        return
    ts = _ensure_utc(when)
    if ts is None:
        return
    try:
        from app.api.kpi import _upsert_kpi_entry_for_org

        _upsert_kpi_entry_for_org(
            db,
            org_id,
            ts.date(),
            {"revenue": float(revenue_delta_usd)},
            additive=True,
        )
    except Exception:
        try:
            db.rollback()
        except Exception:
            pass


def sync_kpi_for_datetime(
    db: Session,
    org_id: uuid.UUID,
    when: Optional[datetime],
    *,
    commit: bool = True,
) -> None:
    """Sync the KPI day corresponding to a check-in/payment timestamp."""
    if when is None:
        return
    ts = _ensure_utc(when)
    if ts is None:
        return
    try:
        sync_kpi_day_from_integrations(
            db, org_id, ts.date(), commit=commit, force_create=True
        )
    except Exception:
        # Never break calendar/payment pipelines on KPI sync failure.
        if commit:
            try:
                db.rollback()
            except Exception:
                pass


def refresh_kpi_live_fields_for_range(
    db: Session,
    org_id: uuid.UUID,
    start: date,
    end: date,
) -> None:
    """Recompute live auto fields for each day in [start, end] (inclusive).

    Prefetches check-ins and payments once so month switches / 2-month compare
    do not re-scan payment tables per day.
    """
    if end < start:
        return
    cal = has_calendar_source(db, org_id)
    pay = has_payment_source(db, org_id)

    checkins = db.query(ClientCheckIn).filter(ClientCheckIn.org_id == org_id).all()
    conversion_dates = conversion_dates_by_client(db, org_id, checkins)
    if not cal and not pay and not conversion_dates:
        return

    cash_by_day: Optional[Dict[date, int]] = None
    if pay:
        cash_by_day = _payment_cash_by_day(db, org_id, start, end)

    # Also refresh any existing rows in range even with zero activity
    existing_dates: Set[date] = {
        r.entry_date
        for r in db.query(OrgKpiDailyEntry.entry_date)
        .filter(
            OrgKpiDailyEntry.org_id == org_id,
            OrgKpiDailyEntry.entry_date >= start,
            OrgKpiDailyEntry.entry_date <= end,
        )
        .all()
    }

    cur = start
    dirty = False
    while cur <= end:
        force = cur in existing_dates
        row = sync_kpi_day_from_integrations(
            db,
            org_id,
            cur,
            commit=False,
            force_create=force or (conversion_dates and any(d == cur for d in conversion_dates.values())),
            checkins=checkins,
            calendar_available=cal,
            payments_available=pay,
            cash_by_day=cash_by_day,
            conversion_dates=conversion_dates,
        )
        if row is not None:
            dirty = True
        cur += timedelta(days=1)
    if dirty:
        db.commit()

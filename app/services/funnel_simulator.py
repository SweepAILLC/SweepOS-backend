"""Funnel Simulator historic baselines + org-saved scenarios."""
from __future__ import annotations

import uuid
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.models.client import Client, LifecycleState
from app.models.client_checkin import ClientCheckIn
from app.models.event import Event
from app.models.funnel import Funnel, FunnelStep
from app.models.funnel_lead_notification import FunnelLeadNotification
from app.models.funnel_simulator_scenario import FunnelSimulatorScenario
from app.models.org_kpi_daily_entry import OrgKpiDailyEntry
from app.schemas.kpi import safe_avg, safe_pct
from app.services.kpi_integration_sync import has_calendar_source


def _as_utc(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _lifecycle_value(raw: Any) -> str:
    if raw is None:
        return ""
    if isinstance(raw, LifecycleState):
        return raw.value
    return str(raw).strip().lower()


def _field(
    value: Optional[float] = None,
    *,
    source: Optional[str] = None,
    sample_n: Optional[int] = None,
    sample_d: Optional[int] = None,
    missing_reason: Optional[str] = None,
) -> Dict[str, Any]:
    return {
        "value": round(float(value), 2) if value is not None else None,
        "source": source,
        "sample_n": sample_n,
        "sample_d": sample_d,
        "missing_reason": missing_reason,
    }


def lookback_window(
    *,
    days: int = 90,
    mtd: bool = False,
    today: Optional[date] = None,
) -> Tuple[datetime, datetime, int]:
    end_d = today or date.today()
    end = datetime.combine(end_d, time.max, tzinfo=timezone.utc)
    if mtd:
        start_d = end_d.replace(day=1)
    else:
        n = max(1, min(int(days or 90), 365))
        start_d = end_d - timedelta(days=n - 1)
    start = datetime.combine(start_d, time.min, tzinfo=timezone.utc)
    span = (end_d - start_d).days + 1
    return start, end, span


def _sum_int(rows: Sequence[OrgKpiDailyEntry], attr: str) -> int:
    total = 0
    for row in rows:
        v = getattr(row, attr, None)
        if v is None:
            continue
        try:
            total += int(v)
        except (TypeError, ValueError):
            continue
    return total


def _sum_money(rows: Sequence[OrgKpiDailyEntry], attr: str) -> float:
    total = 0.0
    any_val = False
    for row in rows:
        v = getattr(row, attr, None)
        if v is None:
            continue
        try:
            total += float(v)
            any_val = True
        except (TypeError, ValueError):
            continue
    return total if any_val else 0.0


def _kpi_fields(rows: Sequence[OrgKpiDailyEntry]) -> Dict[str, Any]:
    if not rows:
        missing = "No KPI daily entries in this window. Log KPIs in Command Center."
        empty = _field(missing_reason=missing)
        return {
            "show_pct": dict(empty),
            "close_pct": dict(empty),
            "aov": dict(empty),
            "convo_to_book_pct": dict(empty),
            "pitch_to_book_pct": dict(empty),
            "aov_basis": None,
        }

    booked = _sum_int(rows, "calls_booked")
    taken = _sum_int(rows, "calls_taken")
    closes = _sum_int(rows, "closes")
    respondents = _sum_int(rows, "respondents")
    pitched = _sum_int(rows, "calls_pitched")
    cash = _sum_money(rows, "cash_collected")
    revenue = _sum_money(rows, "revenue")

    show = safe_pct(taken, booked)
    close = safe_pct(closes, taken)
    convo = safe_pct(booked, respondents)
    pitch = safe_pct(booked, pitched) if pitched > 0 else None

    aov_basis: Optional[str] = None
    aov = None
    if closes > 0 and cash > 0:
        aov = safe_avg(cash, closes)
        aov_basis = "cash_collected"
    elif closes > 0 and revenue > 0:
        aov = safe_avg(revenue, closes)
        aov_basis = "revenue"

    return {
        "show_pct": _field(
            show,
            source="kpi_rollup" if show is not None else None,
            sample_n=taken,
            sample_d=booked,
            missing_reason=None if show is not None else "Need calls booked and calls taken in KPI.",
        ),
        "close_pct": _field(
            close,
            source="kpi_rollup" if close is not None else None,
            sample_n=closes,
            sample_d=taken,
            missing_reason=None if close is not None else "Need closes and calls taken in KPI.",
        ),
        "aov": _field(
            aov,
            source="kpi_rollup" if aov is not None else None,
            sample_n=closes,
            sample_d=closes,
            missing_reason=None
            if aov is not None
            else "Need closes plus cash collected (or deal revenue) in KPI.",
        ),
        "convo_to_book_pct": _field(
            convo,
            source="kpi_rollup" if convo is not None else None,
            sample_n=booked,
            sample_d=respondents,
            missing_reason=None if convo is not None else "Need respondents and calls booked in KPI.",
        ),
        "pitch_to_book_pct": _field(
            pitch,
            source="kpi_rollup" if pitch is not None else None,
            sample_n=booked,
            sample_d=pitched if pitched > 0 else None,
            missing_reason=None if pitch is not None else "Need calls pitched in KPI.",
        ),
        "aov_basis": aov_basis,
    }


def _org_funnels(db: Session, org_id: uuid.UUID) -> List[Funnel]:
    return (
        db.query(Funnel)
        .filter(Funnel.org_id == org_id)
        .order_by(Funnel.created_at.desc())
        .all()
    )


def _parse_captured_at(raw: Any) -> Optional[datetime]:
    if isinstance(raw, datetime):
        return _as_utc(raw)
    if isinstance(raw, str) and raw.strip():
        try:
            return _as_utc(datetime.fromisoformat(raw.replace("Z", "+00:00")))
        except ValueError:
            return None
    return None


def _new_lead_universe(
    db: Session,
    org_id: uuid.UUID,
    funnel_ids: Sequence[uuid.UUID],
    start: datetime,
    end: datetime,
) -> List[Tuple[uuid.UUID, datetime]]:
    """Unique first-touch funnel leads in window: (client_id, captured_at)."""
    if not funnel_ids:
        return []

    start_naive = start.replace(tzinfo=None)
    end_naive = end.replace(tzinfo=None)

    notifs = (
        db.query(FunnelLeadNotification)
        .filter(
            FunnelLeadNotification.org_id == org_id,
            FunnelLeadNotification.funnel_id.in_(list(funnel_ids)),
            FunnelLeadNotification.is_new_client.is_(True),
            FunnelLeadNotification.client_id.isnot(None),
            FunnelLeadNotification.created_at >= start_naive,
            FunnelLeadNotification.created_at <= end_naive,
        )
        .all()
    )

    by_client: Dict[uuid.UUID, datetime] = {}
    for n in notifs:
        if n.client_id is None:
            continue
        captured = _as_utc(n.created_at) or start
        prev = by_client.get(n.client_id)
        if prev is None or captured < prev:
            by_client[n.client_id] = captured

    if by_client:
        return list(by_client.items())

    funnel_id_strs = {str(fid) for fid in funnel_ids}
    clients = db.query(Client).filter(Client.org_id == org_id, Client.meta.isnot(None)).all()
    for c in clients:
        meta = c.meta if isinstance(c.meta, dict) else {}
        prospect = meta.get("prospect") if isinstance(meta.get("prospect"), dict) else {}
        fid = str(prospect.get("funnel_id") or "")
        if fid not in funnel_id_strs:
            continue
        captured = _parse_captured_at(prospect.get("captured_at")) or _as_utc(c.created_at)
        if captured is None or captured < start or captured > end:
            continue
        created = _as_utc(c.created_at)
        if created is not None and abs((created - captured).total_seconds()) > 86400:
            continue
        prev = by_client.get(c.id)
        if prev is None or captured < prev:
            by_client[c.id] = captured
    return list(by_client.items())


def _booked_client_ids(
    db: Session,
    org_id: uuid.UUID,
    leads: Sequence[Tuple[uuid.UUID, datetime]],
    calendar_available: bool,
) -> Set[uuid.UUID]:
    if not leads:
        return set()
    client_ids = [cid for cid, _ in leads]
    captured_by = {cid: cap for cid, cap in leads}
    booked: Set[uuid.UUID] = set()

    if calendar_available:
        checkins = (
            db.query(ClientCheckIn)
            .filter(
                ClientCheckIn.org_id == org_id,
                ClientCheckIn.client_id.in_(client_ids),
                ClientCheckIn.is_sales_call.is_(True),
                ClientCheckIn.cancelled.is_(False),
            )
            .all()
        )
        for ci in checkins:
            cap = captured_by.get(ci.client_id)
            st = _as_utc(ci.start_time)
            if cap is None or st is None:
                continue
            if st >= cap:
                booked.add(ci.client_id)
        return booked

    rows = db.query(Client).filter(Client.org_id == org_id, Client.id.in_(client_ids)).all()
    for c in rows:
        if _lifecycle_value(c.lifecycle_state) in ("booked", "active"):
            booked.add(c.id)
    return booked


def _lp_conversion(
    db: Session,
    org_id: uuid.UUID,
    funnels: Sequence[Funnel],
    new_lead_count: int,
    start: datetime,
    end: datetime,
) -> Dict[str, Any]:
    if not funnels:
        return _field(missing_reason="No funnels in this organization.")

    start_naive = start.replace(tzinfo=None)
    end_naive = end.replace(tzinfo=None)
    visitor_ids: Set[str] = set()
    any_steps = False

    for funnel in funnels:
        first = (
            db.query(FunnelStep)
            .filter(FunnelStep.funnel_id == funnel.id, FunnelStep.org_id == org_id)
            .order_by(FunnelStep.step_order.asc())
            .first()
        )
        if not first:
            continue
        any_steps = True
        rows = (
            db.query(Event.visitor_id)
            .filter(
                Event.org_id == org_id,
                Event.funnel_id == funnel.id,
                Event.event_name == first.event_name,
                Event.occurred_at >= start_naive,
                Event.occurred_at <= end_naive,
                Event.visitor_id.isnot(None),
            )
            .distinct()
            .all()
        )
        for (vid,) in rows:
            if vid:
                visitor_ids.add(str(vid))

    if not any_steps:
        return _field(missing_reason="Selected funnel has no steps to count visitors.")

    visitors = len(visitor_ids)
    if visitors <= 0:
        return _field(
            missing_reason="No unique landing-page visitors in this window.",
            sample_n=new_lead_count,
            sample_d=0,
        )
    rate = safe_pct(new_lead_count, visitors)
    return _field(
        rate,
        source="funnel_events",
        sample_n=new_lead_count,
        sample_d=visitors,
        missing_reason=None if rate is not None else "Need unique visitors and new leads.",
    )


def build_funnel_simulator_baselines(
    db: Session,
    org_id: uuid.UUID,
    *,
    days: int = 90,
    mtd: bool = False,
    funnel_id: Optional[uuid.UUID] = None,
) -> Dict[str, Any]:
    start, end, span = lookback_window(days=days, mtd=mtd)
    start_d, end_d = start.date(), end.date()

    funnels = _org_funnels(db, org_id)
    selected: List[Funnel]
    if funnel_id is not None:
        match = next((f for f in funnels if f.id == funnel_id), None)
        if match is None:
            return {"error": "Funnel not found"}
        selected = [match]
    else:
        selected = funnels

    kpi_rows = (
        db.query(OrgKpiDailyEntry)
        .filter(
            OrgKpiDailyEntry.org_id == org_id,
            OrgKpiDailyEntry.entry_date >= start_d,
            OrgKpiDailyEntry.entry_date <= end_d,
        )
        .all()
    )
    kpi = _kpi_fields(kpi_rows)

    calendar_available = has_calendar_source(db, org_id)
    funnel_ids = [f.id for f in selected]
    leads = _new_lead_universe(db, org_id, funnel_ids, start, end)
    booked_ids = _booked_client_ids(db, org_id, leads, calendar_available)
    new_n = len(leads)
    booked_n = len(booked_ids)
    book_pct = safe_pct(booked_n, new_n)
    book_field = _field(
        book_pct,
        source="funnel_leads" if book_pct is not None else None,
        sample_n=booked_n,
        sample_d=new_n,
        missing_reason=None
        if book_pct is not None
        else (
            "No first-touch funnel leads in this window."
            if new_n == 0
            else "Need new funnel leads who later booked a sales call."
        ),
    )

    lp_field = _lp_conversion(db, org_id, selected, new_n, start, end)

    return {
        "lookback_start": start_d.isoformat(),
        "lookback_end": end_d.isoformat(),
        "days": span,
        "funnel_id": str(funnel_id) if funnel_id else None,
        "calendar_available": calendar_available,
        "aov_basis": kpi.get("aov_basis"),
        "fields": {
            "show_pct": kpi["show_pct"],
            "close_pct": kpi["close_pct"],
            "aov": kpi["aov"],
            "book_call_pct": book_field,
            "convo_to_book_pct": kpi["convo_to_book_pct"],
            "pitch_to_book_pct": kpi["pitch_to_book_pct"],
            "lp_conv_pct": lp_field,
        },
        "funnels": [{"id": str(f.id), "name": f.name} for f in funnels],
    }


def ensure_funnel_simulator_scenarios_table(db: Session) -> None:
    FunnelSimulatorScenario.__table__.create(db.bind, checkfirst=True)
    db.execute(
        text(
            "CREATE INDEX IF NOT EXISTS ix_funnel_simulator_scenarios_org_id "
            "ON funnel_simulator_scenarios (org_id)"
        )
    )

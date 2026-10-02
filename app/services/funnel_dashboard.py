"""Funnels dashboard (PRD phase 8): one screen, two graphs, ten numbers, one table.

Everything is built on kpi_integration_sync.collect_funnel_facts — the same data
pass behind the Sales KPIs funnel strip — so the stage counts on both surfaces
can never disagree. This module only adds what the strip doesn't have: visitors,
weekly ad spend and the paid money row, weekly buckets, UTM-source rows, and a
tracking-status dot.
"""
from __future__ import annotations

import uuid
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

from sqlalchemy import func
from sqlalchemy.orm import Session

from app.models.client import Client
from app.models.event import Event
from app.models.event_error import EventError
from app.models.funnel import Funnel
from app.models.funnel_ad_spend import FunnelAdSpend
from app.services.funnel_scorecard import compute_scorecard
from app.services.kpi_integration_sync import (
    FunnelFacts,
    collect_funnel_facts,
    summarize_funnel_facts,
)

NO_UTM_SOURCE = "(no UTM)"
TRACKING_SILENT_AFTER = timedelta(hours=24)


def week_start_for(d: date) -> date:
    """Monday of d's ISO week — the one spend bucket key used everywhere."""
    return d - timedelta(days=d.weekday())


def upsert_ad_spend(
    db: Session,
    org_id: uuid.UUID,
    funnel_id: Optional[uuid.UUID],
    week_start: date,
    amount_cents: int,
    user_id: Optional[uuid.UUID],
    ads_deployed: Optional[int] = None,
    angles_deployed: Optional[int] = None,
) -> Optional[FunnelAdSpend]:
    """
    Set one (funnel, week) row: spend plus the sheet's "New Ads/Angles Deployed"
    counts. `week_start` is normalized to its Monday. A row with no spend and no
    counts is cleared (returns None) so "nothing logged" and "$0 logged" never
    diverge in the money row.
    """
    week = week_start_for(week_start)
    q = db.query(FunnelAdSpend).filter(
        FunnelAdSpend.org_id == org_id,
        FunnelAdSpend.week_start == week,
        FunnelAdSpend.funnel_id == funnel_id if funnel_id is not None else FunnelAdSpend.funnel_id.is_(None),
    )
    row = q.first()
    if amount_cents <= 0 and not ads_deployed and not angles_deployed:
        if row is not None:
            db.delete(row)
            db.commit()
        return None
    if row is None:
        row = FunnelAdSpend(org_id=org_id, funnel_id=funnel_id, week_start=week)
        db.add(row)
    row.amount_cents = max(amount_cents, 0)
    row.ads_deployed = ads_deployed
    row.angles_deployed = angles_deployed
    row.entered_by_user_id = user_id
    row.updated_at = datetime.now(timezone.utc)
    db.commit()
    db.refresh(row)
    return row


def list_ad_spend(
    db: Session,
    org_id: uuid.UUID,
    window_start: date,
    window_end: date,
    funnel_id: Optional[uuid.UUID] = None,
) -> List[FunnelAdSpend]:
    """Spend rows whose week_start falls in the window. No funnel = every row, incl. unassigned."""
    q = db.query(FunnelAdSpend).filter(
        FunnelAdSpend.org_id == org_id,
        FunnelAdSpend.week_start >= window_start,
        FunnelAdSpend.week_start <= window_end,
    )
    if funnel_id is not None:
        q = q.filter(FunnelAdSpend.funnel_id == funnel_id)
    return q.order_by(FunnelAdSpend.week_start.asc()).all()


def _ratio(n: float, d: float, digits: int = 2) -> Optional[float]:
    """Zero denominators render as "—" on the UI, never 0 or infinity."""
    return round(n / d, digits) if d else None


def _ghl_activity_times(cfg: Any) -> List[datetime]:
    """Last webhook lead and last pulled lead recorded in a funnel's ghl_config."""
    if not isinstance(cfg, dict):
        return []
    raw = [
        (cfg.get("webhook") or {}).get("last_received_at") if isinstance(cfg.get("webhook"), dict) else None,
        (cfg.get("sync") or {}).get("last_lead_at") if isinstance(cfg.get("sync"), dict) else None,
    ]
    out: List[datetime] = []
    for value in raw:
        if not value:
            continue
        try:
            ts = datetime.fromisoformat(str(value))
        except ValueError:
            continue
        out.append(ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc))
    return out


def _tracking_status(db: Session, funnel_ids: Set[uuid.UUID], now: datetime) -> Dict[str, Any]:
    if not funnel_ids:
        return {"status": "no_funnels", "last_event_at": None, "errors_24h": 0}
    last = (
        db.query(func.max(func.coalesce(Event.received_at, Event.occurred_at)))
        .filter(Event.funnel_id.in_(list(funnel_ids)))
        .scalar()
    )
    if last is not None and last.tzinfo is None:
        last = last.replace(tzinfo=timezone.utc)
    # GHL-paired funnels are also live when leads arrive by webhook or pull, even
    # before (or without) the visitor snippet.
    for (cfg,) in db.query(Funnel.ghl_config).filter(Funnel.id.in_(list(funnel_ids)), Funnel.source == "ghl").all():
        for ts in _ghl_activity_times(cfg):
            if last is None or ts > last:
                last = ts
    # EventError has no org/funnel column; attribute by payload funnel_id, 24h only.
    wanted = {str(f) for f in funnel_ids}
    errors = 0
    for (payload,) in (
        db.query(EventError.payload)
        .filter(EventError.created_at >= (now - timedelta(days=1)).replace(tzinfo=None))
        .all()
    ):
        if isinstance(payload, dict) and str(payload.get("funnel_id") or "") in wanted:
            errors += 1
    if errors:
        status = "errors"
    elif last is not None and now - last <= TRACKING_SILENT_AFTER:
        status = "live"
    else:
        status = "silent"
    return {"status": status, "last_event_at": last, "errors_24h": errors}


def _visitors(
    db: Session,
    funnel_ids: Set[uuid.UUID],
    start: datetime,
    end: datetime,
) -> int:
    if not funnel_ids:
        return 0
    return (
        db.query(func.count(func.distinct(Event.visitor_id)))
        .filter(
            Event.funnel_id.in_(list(funnel_ids)),
            Event.visitor_id.isnot(None),
            Event.occurred_at >= start.replace(tzinfo=None),
            Event.occurred_at <= end.replace(tzinfo=None),
        )
        .scalar()
        or 0
    )


def _utm_sources(db: Session, org_id: uuid.UUID, facts: FunnelFacts) -> List[Dict[str, Any]]:
    """One row per utm_source over the clients in this window's facts, ranked by cash then opt-ins."""
    client_ids = set(facts.opt_ins) | set(facts.booked) | set(facts.closed)
    client_ids |= {cid for cid, _ts, _c in facts.cash if cid is not None}
    if not client_ids:
        return []
    source_by_client: Dict[uuid.UUID, str] = {}
    for cid, meta in db.query(Client.id, Client.meta).filter(
        Client.org_id == org_id, Client.id.in_(list(client_ids))
    ):
        prospect = meta.get("prospect") if isinstance(meta, dict) else None
        utm = prospect.get("utm") if isinstance(prospect, dict) else None
        src = str(utm.get("source") or "").strip() if isinstance(utm, dict) else ""
        source_by_client[cid] = src or NO_UTM_SOURCE

    rows: Dict[str, Dict[str, Any]] = defaultdict(
        lambda: {"opt_ins": 0, "booked": 0, "closed": 0, "cash_cents": 0}
    )
    for cid in facts.opt_ins:
        rows[source_by_client.get(cid, NO_UTM_SOURCE)]["opt_ins"] += 1
    for cid in facts.booked:
        rows[source_by_client.get(cid, NO_UTM_SOURCE)]["booked"] += 1
    for cid in facts.closed:
        rows[source_by_client.get(cid, NO_UTM_SOURCE)]["closed"] += 1
    for cid, _ts, cents in facts.cash:
        rows[source_by_client.get(cid, NO_UTM_SOURCE) if cid else NO_UTM_SOURCE]["cash_cents"] += cents

    out = [
        {
            "source": src,
            "opt_ins": r["opt_ins"],
            "booked": r["booked"],
            "closed": r["closed"],
            "cash_usd": round(r["cash_cents"] / 100.0, 2),
        }
        for src, r in rows.items()
    ]
    out.sort(key=lambda r: (r["source"] == NO_UTM_SOURCE, -r["cash_usd"], -r["opt_ins"]))
    return out


def _weekly(
    window_start: date,
    window_end: date,
    facts: FunnelFacts,
    money_facts: Optional[FunnelFacts],
    spend_rows: List[FunnelAdSpend],
) -> List[Dict[str, Any]]:
    weeks: Dict[date, Dict[str, Any]] = {}
    cursor = week_start_for(window_start)
    while cursor <= window_end:
        weeks[cursor] = {"week_start": cursor, "spend_cents": 0, "cash_cents": 0, "opt_ins": 0, "closed": 0}
        cursor += timedelta(days=7)

    def _bucket(d: date) -> Optional[Dict[str, Any]]:
        return weeks.get(week_start_for(d))

    for created in facts.opt_ins.values():
        b = _bucket(created.date())
        if b:
            b["opt_ins"] += 1
    for d, _o, respondents in facts.outreach_rows:
        b = _bucket(d)
        if b:
            b["opt_ins"] += respondents
    for d in facts.closed.values():
        b = _bucket(d)
        if b:
            b["closed"] += 1
    # Cash on the spend graph is paid cash when the money row is shown, so the
    # bars and the line compare like with like.
    for _cid, ts, cents in (money_facts or facts).cash:
        b = _bucket(ts.date())
        if b:
            b["cash_cents"] += cents
    for row in spend_rows:
        b = weeks.get(row.week_start)
        if b:
            b["spend_cents"] += row.amount_cents or 0

    return [
        {
            "week_start": w["week_start"],
            "spend_usd": round(w["spend_cents"] / 100.0, 2),
            "cash_usd": round(w["cash_cents"] / 100.0, 2),
            "opt_ins": w["opt_ins"],
            "closed": w["closed"],
            "cac_usd": _ratio(w["spend_cents"] / 100.0, w["closed"]),
        }
        for w in weeks.values()
    ]


def compute_funnel_dashboard(
    db: Session,
    org_id: uuid.UUID,
    window_start: date,
    window_end: date,
    channel: Optional[str] = None,
    funnel_id: Optional[uuid.UUID] = None,
    *,
    now: Optional[datetime] = None,
    compare: Optional[Tuple[date, date]] = None,
) -> Dict[str, Any]:
    """
    Whole Funnels dashboard in one call. `channel` None/"all" | "organic" | "paid";
    `funnel_id` narrows to one funnel (implicitly paid). The money row (spend,
    CPL, CAC, ROAS, profit) is paid-only: hidden for organic, and computed over
    the paid subset when channel is "all".
    """
    now = now or datetime.now(timezone.utc)
    start = datetime.combine(window_start, datetime.min.time(), tzinfo=timezone.utc)
    end = datetime.combine(window_end, datetime.max.time(), tzinfo=timezone.utc)

    org_funnel_ids = {fid for (fid,) in db.query(Funnel.id).filter(Funnel.org_id == org_id).all()}
    scope_funnel_ids = {funnel_id} & org_funnel_ids if funnel_id is not None else org_funnel_ids

    facts = collect_funnel_facts(db, org_id, window_start, window_end, channel, funnel_id)
    summary = summarize_funnel_facts(facts, channel)

    money: Optional[Dict[str, Any]] = None
    money_facts: Optional[FunnelFacts] = None
    spend_rows: List[FunnelAdSpend] = []
    if channel != "organic":
        money_facts = (
            facts
            if channel == "paid" or funnel_id is not None
            else collect_funnel_facts(db, org_id, window_start, window_end, "paid", None)
        )
        spend_rows = list_ad_spend(db, org_id, window_start, window_end, funnel_id)
        spend = sum(r.amount_cents or 0 for r in spend_rows) / 100.0
        paid_cash = sum(c for _cid, _ts, c in money_facts.cash) / 100.0
        paid_opt_ins = len(money_facts.opt_ins)
        paid_closed = len(money_facts.closed)
        has_spend = spend > 0
        # No spend logged = no cost metrics at all; a $0 CPL/CAC would read as "free leads".
        money = {
            "has_spend": has_spend,
            "spend_usd": round(spend, 2),
            "paid_cash_usd": round(paid_cash, 2),
            "cpl_usd": _ratio(spend, paid_opt_ins) if has_spend else None,
            "cac_usd": _ratio(spend, paid_closed) if has_spend else None,
            "roas": _ratio(paid_cash, spend),
            "profit_usd": round(paid_cash - spend, 2) if has_spend else None,
        }

    return {
        "window_start": window_start,
        "window_end": window_end,
        "channel": channel or "all",
        "funnel_id": funnel_id,
        "tracking": _tracking_status(db, scope_funnel_ids, now),
        # Organic has no landing page, so visitors only exist for paid/funnel scope.
        "visitors": None if channel == "organic" else _visitors(db, scope_funnel_ids, start, end),
        "summary": summary,
        "money": money,
        "weekly": _weekly(window_start, window_end, facts, money_facts if money else None, spend_rows),
        "sources": _utm_sources(db, org_id, facts),
        # Sheet-style scorecard: last full week vs the average week in range.
        "scorecard": compute_scorecard(
            db, org_id, window_start, window_end, channel, funnel_id, scope_funnel_ids, today=now.date(), compare=compare
        ),
    }

"""Attribute leads, calls, closes and cash back to individual Meta ads.

A lead is tied to an ad through the UTM captured at opt-in (`prospect.utm`):
`utm_content` (or `utm_term`) holding the Meta ad id, or `utm_content` holding the
ad's name. Id matches win; a name only matches when exactly one ad carries it, so a
reused name never credits the wrong creative.

Counts come from the same `collect_funnel_facts` pass the Funnels scorecard uses, so
the per-ad numbers add up to (and never disagree with) the funnel totals: leads by
opt-in date in the window, booked/showed by call date, closes by close date, cash by
payment date. Leads whose UTM names no known ad are reported as unattributed.
"""
from __future__ import annotations

import logging
import uuid
from collections import defaultdict
from datetime import date
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set

from sqlalchemy.orm import Session

from app.models.client import Client
from app.models.meta_ad import MetaAd
from app.services.kpi_integration_sync import collect_funnel_facts

logger = logging.getLogger(__name__)

_EPOCH = date(2000, 1, 1)
_COUNT_KEYS = ("leads", "booked_calls", "live_calls", "deals_closed", "cash_cents")


def _norm(value: Any) -> str:
    return " ".join(str(value or "").split()).casefold()


class AdResolver:
    """Maps a lead's UTM dict to a `meta_ad_id`, or None."""

    def __init__(self, ads: Iterable[MetaAd]):
        self._ids: Set[str] = set()
        names: Dict[str, Set[str]] = defaultdict(set)
        for ad in ads:
            self._ids.add(ad.meta_ad_id)
            if ad.name and _norm(ad.name):
                names[_norm(ad.name)].add(ad.meta_ad_id)
        self._unique_names = {n: next(iter(ids)) for n, ids in names.items() if len(ids) == 1}

    def resolve(self, utm: Optional[Dict[str, Any]]) -> Optional[str]:
        if not isinstance(utm, dict):
            return None
        for key in ("content", "term"):
            raw = str(utm.get(key) or "").strip()
            if raw in self._ids:
                return raw
        name = _norm(utm.get("content"))
        return self._unique_names.get(name) if name else None


def _blank() -> Dict[str, int]:
    return {k: 0 for k in _COUNT_KEYS}


def attribute_ads(
    db: Session,
    org_id: uuid.UUID,
    ads: Sequence[MetaAd],
    start: Optional[date],
    end: date,
    *,
    funnel_id: Optional[uuid.UUID] = None,
) -> Dict[str, Any]:
    """Per-ad attributed counts for [start, end] plus coverage of the paid leads.

    Returns {"by_ad": {meta_ad_id: counts}, "coverage": {...}}.
    """
    facts = collect_funnel_facts(db, org_id, start or _EPOCH, end, "paid", funnel_id)

    client_ids: Set[uuid.UUID] = set(facts.opt_ins) | set(facts.booked) | set(facts.showed) | set(facts.closed)
    client_ids |= {cid for cid, _ts, _c in facts.cash if cid is not None}

    resolver = AdResolver(ads)
    ad_by_client: Dict[uuid.UUID, str] = {}
    if client_ids:
        for cid, meta in db.query(Client.id, Client.meta).filter(
            Client.org_id == org_id, Client.id.in_(list(client_ids))
        ):
            prospect = meta.get("prospect") if isinstance(meta, dict) else None
            utm = prospect.get("utm") if isinstance(prospect, dict) else None
            ad_id = resolver.resolve(utm)
            if ad_id:
                ad_by_client[cid] = ad_id

    by_ad: Dict[str, Dict[str, int]] = defaultdict(_blank)
    unattributed = _blank()

    def _bucket(cid: Optional[uuid.UUID]) -> Dict[str, int]:
        ad_id = ad_by_client.get(cid) if cid is not None else None
        return by_ad[ad_id] if ad_id else unattributed

    for cid in facts.opt_ins:
        _bucket(cid)["leads"] += 1
    for cid in facts.booked:
        _bucket(cid)["booked_calls"] += 1
    for cid in facts.showed:
        _bucket(cid)["live_calls"] += 1
    for cid in facts.closed:
        _bucket(cid)["deals_closed"] += 1
    for cid, _ts, cents in facts.cash:
        _bucket(cid)["cash_cents"] += cents or 0

    attributed_leads = sum(v["leads"] for v in by_ad.values())
    return {
        "by_ad": dict(by_ad),
        "coverage": {
            "attributed_leads": attributed_leads,
            "total_paid_leads": attributed_leads + unattributed["leads"],
            "attributed_cash_cents": sum(v["cash_cents"] for v in by_ad.values()),
            "unattributed_cash_cents": unattributed["cash_cents"],
        },
    }


def _pct(n: int, d: int) -> Optional[float]:
    return round(n / d * 100, 1) if d else None


def ad_metrics(counts: Optional[Dict[str, int]], spend_cents: int) -> Dict[str, Any]:
    """Funnel metrics for one ad. Cost metrics need spend > 0 (never a misleading $0);
    rates are percent-scale like `ctr`."""
    c = counts or _blank()
    spend = spend_cents or 0

    def _cost(denom: int) -> Optional[int]:
        return round(spend / denom) if spend > 0 and denom else None

    cash = c["cash_cents"]
    return {
        "leads": c["leads"],
        "booked_calls": c["booked_calls"],
        "live_calls": c["live_calls"],
        "deals_closed": c["deals_closed"],
        "cash_cents": cash,
        "lead_to_book_pct": _pct(c["booked_calls"], c["leads"]),
        "show_rate_pct": _pct(c["live_calls"], c["booked_calls"]),
        "cash_per_close_cents": round(cash / c["deals_closed"]) if c["deals_closed"] else None,
        "cost_per_lead_cents": _cost(c["leads"]),
        "cost_per_booking_cents": _cost(c["booked_calls"]),
        "cac_cents": _cost(c["deals_closed"]),
        "roas": round(cash / spend, 2) if spend > 0 else None,
        "profit_cents": (cash - spend) if spend > 0 else None,
    }


def attach_attribution(
    db: Session,
    org_id: uuid.UUID,
    ads: Sequence[MetaAd],
    ad_rows: List[Dict[str, Any]],
    start: Optional[date],
    end: date,
    *,
    funnel_id: Optional[uuid.UUID] = None,
) -> Optional[Dict[str, Any]]:
    """Add an `attribution` dict to every row in `ad_rows` (keyed by meta_ad_id) and
    return the coverage summary. Attribution is additive: if it fails, the ad table
    still loads with `attribution: None` and the failure is logged."""
    try:
        result = attribute_ads(db, org_id, ads, start, end, funnel_id=funnel_id)
    except Exception:
        logger.exception("ad attribution failed org=%s", org_id)
        for row in ad_rows:
            row["attribution"] = None
        return None
    for row in ad_rows:
        row["attribution"] = ad_metrics(result["by_ad"].get(row["meta_ad_id"]), row.get("spend_cents") or 0)
    return result["coverage"]

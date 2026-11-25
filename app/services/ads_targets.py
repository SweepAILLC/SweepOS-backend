"""Ads tab targets: derived from the Scale Planner by default, manual when switched off.

`effective_targets` is what the ad rules (kill rule, scale-ready, ROAS floor) and the Ads
tab use. Following the planner, target cost per result = what a booked call is worth /
break-even ROAS, and minimum ROAS = business break-even ROAS. A planner value that can't
be computed yet (no business numbers, no bookings) falls back to the manual value, then to
the planner's typed target cost per booked call.
"""
from __future__ import annotations

import logging
import uuid
from typing import Any, Dict, Optional

from sqlalchemy.orm import Session

from app.models.meta_ad_account import MetaAdAccount
from app.models.org_business_economics import OrgBusinessEconomics

logger = logging.getLogger(__name__)


def planner_suggestions(db: Session, org_id: uuid.UUID) -> Optional[Dict[str, Any]]:
    """The planner's suggested targets, or None when it has no business numbers or fails."""
    from app.api.business_economics import compute_readiness  # avoid import cycle at load

    try:
        return compute_readiness(db, org_id).get("suggested_ad_targets")
    except Exception:
        logger.exception("ads targets: planner suggestions failed org=%s", org_id)
        return None


def _cents(dollars: Optional[float]) -> Optional[int]:
    return None if dollars is None else int(round(dollars * 100))


def resolve_targets(
    account: Optional[MetaAdAccount],
    suggested: Optional[Dict[str, Any]],
    typed_cost_per_call_cents: Optional[int],
) -> Dict[str, Any]:
    """Pure: pick each target from the planner (when following) or the manual value."""
    follow = True if account is None else bool(account.targets_follow_planner)
    manual_cpr = account.target_cpr_cents if account else None
    manual_roas = account.min_roas if account else None
    s = suggested or {}
    planner_cpr = _cents(s.get("target_cost_per_booking"))
    planner_roas = s.get("min_roas")

    if follow and planner_cpr is not None:
        cpr, cpr_source = planner_cpr, "planner"
    elif manual_cpr is not None:
        cpr, cpr_source = int(manual_cpr), "manual"
    elif typed_cost_per_call_cents:
        cpr, cpr_source = int(typed_cost_per_call_cents), "business_numbers"
    else:
        cpr, cpr_source = None, None

    if follow and planner_roas is not None:
        roas, roas_source = float(planner_roas), "planner"
    elif manual_roas is not None:
        roas, roas_source = float(manual_roas), "manual"
    else:
        roas, roas_source = None, None

    return {
        "follow_planner": follow,
        "target_cpr_cents": cpr,
        "target_cpr_source": cpr_source,
        "min_roas": roas,
        "min_roas_source": roas_source,
        "marginal_breakeven_roas": s.get("marginal_breakeven_roas"),
        "suggested": suggested,
    }


def effective_targets(
    db: Session,
    org_id: uuid.UUID,
    account: Optional[MetaAdAccount],
    *,
    suggested: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    if suggested is None:
        suggested = planner_suggestions(db, org_id)
    econ = db.query(OrgBusinessEconomics).filter(OrgBusinessEconomics.org_id == org_id).first()
    typed = int(econ.target_cost_per_call_cents) if econ and econ.target_cost_per_call_cents else None
    return resolve_targets(account, suggested, typed)

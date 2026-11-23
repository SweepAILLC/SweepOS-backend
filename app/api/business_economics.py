"""
Business economics + scale readiness (Scale Planner).

GET/PUT /business-economics          — the org's fixed costs, variable %, reserves, risk tolerance.
GET     /business-economics/readiness — bulk/hold/cut verdict from the profile + last 8 weeks of paid data.

Org owners/admins only: reserves and living costs are personal finances. Sweep system
owners may pass ?as_org_id= to read or edit another org's profile (the owner "org view").
"""
from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.api.deps import get_current_user, user_is_system_owner
from app.core.encryption import decrypt_token, encrypt_token
from app.db.session import get_db
from app.models.funnel import Funnel
from app.models.org_business_economics import OrgBusinessEconomics
from app.models.organization import Organization
from app.services import scale_planner as sp
from app.services.funnel_scorecard import weekly_metric_values
from app.services.org_user_context import user_can_manage_org_integrations

router = APIRouter()
logger = logging.getLogger(__name__)


class EconomicsIn(BaseModel):
    fixed_monthly_costs: Optional[float] = Field(default=None, ge=0, le=100_000_000)
    variable_cost_pct: Optional[float] = Field(default=None, ge=0, le=95)
    include_living_in_breakeven: Optional[bool] = None
    cash_reserves: Optional[float] = Field(default=None, ge=0, le=1_000_000_000)
    tax_reserve: Optional[float] = Field(default=None, ge=0, le=1_000_000_000)
    living_expenses: Optional[float] = Field(default=None, ge=0, le=100_000_000)
    risk_tolerance: Optional[str] = None
    target_cost_per_call: Optional[float] = Field(default=None, ge=0, le=1_000_000)
    roas_decay: Optional[float] = Field(default=None, ge=0, le=2)
    monthly_recurring_cash: Optional[float] = Field(default=None, ge=0, le=1_000_000_000)


def resolve_org(
    db: Session, current_user: Any, as_org_id: Optional[uuid.UUID]
) -> uuid.UUID:
    """Selected org for owners/admins; any org for system owners via as_org_id."""
    if isinstance(as_org_id, uuid.UUID):
        if not user_is_system_owner(current_user, db):
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="System owner access required.")
        if not db.query(Organization.id).filter(Organization.id == as_org_id).first():
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Organization not found.")
        return as_org_id
    if not user_can_manage_org_integrations(current_user, db):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only org owners and admins can see business economics.",
        )
    raw = getattr(current_user, "selected_org_id", None) or current_user.org_id
    return raw if isinstance(raw, uuid.UUID) else uuid.UUID(str(raw))


def _enc(value: Optional[float]) -> Optional[str]:
    return None if value is None else encrypt_token(repr(float(value)))


def _dec(value: Optional[str], org_id: uuid.UUID, field: str) -> Optional[float]:
    if not value:
        return None
    try:
        return float(decrypt_token(value, audit_context={"org_id": str(org_id), "field": field}))
    except Exception:
        logger.warning("business economics decrypt failed org=%s field=%s", org_id, field)
        return None


def _cents(value: Optional[float]) -> Optional[int]:
    return None if value is None else int(round(value * 100))


def _dollars(cents: Optional[int]) -> Optional[float]:
    return None if cents is None else cents / 100


def _row(db: Session, org_id: uuid.UUID) -> Optional[OrgBusinessEconomics]:
    return db.query(OrgBusinessEconomics).filter(OrgBusinessEconomics.org_id == org_id).first()


def serialize(row: Optional[OrgBusinessEconomics], org_id: uuid.UUID) -> Dict[str, Any]:
    if row is None:
        return {
            "configured": False,
            "fixed_monthly_costs": None,
            "variable_cost_pct": None,
            "include_living_in_breakeven": False,
            "cash_reserves": None,
            "tax_reserve": None,
            "living_expenses": None,
            "risk_tolerance": "balanced",
            "target_cost_per_call": None,
            "roas_decay": None,
            "monthly_recurring_cash": None,
            "updated_at": None,
        }
    return {
        "configured": True,
        "fixed_monthly_costs": _dollars(row.fixed_monthly_costs_cents),
        "variable_cost_pct": row.variable_cost_pct,
        "include_living_in_breakeven": bool(row.include_living_in_breakeven),
        "cash_reserves": _dec(row.cash_reserves_enc, org_id, "cash_reserves"),
        "tax_reserve": _dec(row.tax_reserve_enc, org_id, "tax_reserve"),
        "living_expenses": _dec(row.living_expenses_enc, org_id, "living_expenses"),
        "risk_tolerance": row.risk_tolerance or "balanced",
        "target_cost_per_call": _dollars(row.target_cost_per_call_cents),
        "roas_decay": row.roas_decay,
        "monthly_recurring_cash": _dollars(row.monthly_recurring_cash_cents),
        "updated_at": row.updated_at.isoformat() if row.updated_at else None,
    }


def economics_from(profile: Dict[str, Any]) -> sp.Economics:
    return sp.Economics(
        fixed_monthly=profile.get("fixed_monthly_costs") or 0.0,
        variable_pct=profile.get("variable_cost_pct"),
        include_living_in_breakeven=bool(profile.get("include_living_in_breakeven")),
        cash_reserves=profile.get("cash_reserves"),
        tax_reserve=profile.get("tax_reserve"),
        living_expenses=profile.get("living_expenses"),
        risk_tolerance=profile.get("risk_tolerance") or "balanced",
        roas_decay=profile.get("roas_decay"),
        recurring_cash=profile.get("monthly_recurring_cash") or 0.0,
    )


def compute_readiness(db: Session, org_id: uuid.UUID, *, today=None) -> Dict[str, Any]:
    """Profile + trailing paid data -> verdict. Shared by the REST route and MCP."""
    today = today or datetime.now(timezone.utc).date()
    profile = serialize(_row(db, org_id), org_id)
    weeks = sp.complete_weeks(today, 8)
    funnel_ids = {fid for (fid,) in db.query(Funnel.id).filter(Funnel.org_id == org_id).all()}
    weekly, _ = weekly_metric_values(db, org_id, weeks, "paid", None, funnel_ids, include_ads=True)
    trailing = sp.trailing_summary(weekly)
    verdict = sp.readiness(
        economics_from(profile),
        monthly_spend=trailing["monthly_spend"],
        current_roas=trailing["roas"],
        worst_week_roas=trailing["worst_week_roas"],
    )
    return {
        "configured": profile["configured"],
        "trailing": {**trailing, "weeks": [w.isoformat() for w in weeks]},
        "readiness": verdict,
        "target_cost_per_call": profile["target_cost_per_call"],
    }


@router.get("")
def get_business_economics(
    as_org_id: Optional[uuid.UUID] = Query(default=None),
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user),
):
    org_id = resolve_org(db, current_user, as_org_id)
    return serialize(_row(db, org_id), org_id)


@router.put("")
def put_business_economics(
    body: EconomicsIn,
    as_org_id: Optional[uuid.UUID] = Query(default=None),
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user),
):
    org_id = resolve_org(db, current_user, as_org_id)
    if body.risk_tolerance is not None and body.risk_tolerance not in sp.RISK_LEVELS:
        raise HTTPException(status_code=400, detail="risk_tolerance must be conservative, balanced or aggressive.")
    row = _row(db, org_id)
    if row is None:
        row = OrgBusinessEconomics(org_id=org_id)
        db.add(row)
    fields = body.model_dump(exclude_unset=True)
    if "fixed_monthly_costs" in fields:
        row.fixed_monthly_costs_cents = _cents(body.fixed_monthly_costs)
    if "variable_cost_pct" in fields:
        row.variable_cost_pct = body.variable_cost_pct
    if body.include_living_in_breakeven is not None:
        row.include_living_in_breakeven = body.include_living_in_breakeven
    if "cash_reserves" in fields:
        row.cash_reserves_enc = _enc(body.cash_reserves)
    if "tax_reserve" in fields:
        row.tax_reserve_enc = _enc(body.tax_reserve)
    if "living_expenses" in fields:
        row.living_expenses_enc = _enc(body.living_expenses)
    if body.risk_tolerance is not None:
        row.risk_tolerance = body.risk_tolerance
    if "target_cost_per_call" in fields:
        row.target_cost_per_call_cents = _cents(body.target_cost_per_call)
    if "roas_decay" in fields:
        row.roas_decay = body.roas_decay
    if "monthly_recurring_cash" in fields:
        row.monthly_recurring_cash_cents = _cents(body.monthly_recurring_cash)
    row.updated_by_user_id = getattr(current_user, "id", None)
    row.updated_at = datetime.now(timezone.utc)
    db.commit()
    db.refresh(row)
    return serialize(row, org_id)


@router.get("/readiness")
def get_scale_readiness(
    as_org_id: Optional[uuid.UUID] = Query(default=None),
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user),
):
    org_id = resolve_org(db, current_user, as_org_id)
    return compute_readiness(db, org_id)

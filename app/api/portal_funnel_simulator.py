"""Funnel Simulator portal endpoints — baselines + named scenarios."""
from __future__ import annotations

from datetime import datetime
from typing import List, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from app.api.deps import get_current_user
from app.api.portal import require_consulting_org_id
from app.db.session import get_db
from app.models.funnel_simulator_scenario import (
    MAX_FUNNEL_SIMULATOR_SCENARIOS_PER_ORG,
    FunnelSimulatorScenario,
)
from app.models.user import User
from app.schemas.portal import (
    FunnelSimulatorScenarioCreate,
    FunnelSimulatorScenarioResponse,
    FunnelSimulatorScenarioUpdate,
)
from app.services.funnel_simulator import (
    build_funnel_simulator_baselines,
    ensure_funnel_simulator_scenarios_table,
)

router = APIRouter()


def _ensure(db: Session) -> None:
    try:
        ensure_funnel_simulator_scenarios_table(db)
        db.commit()
    except Exception:
        db.rollback()


@router.get("/funnel-simulator/baselines")
def get_funnel_simulator_baselines(
    days: int = Query(90, ge=1, le=365),
    mtd: bool = Query(False),
    funnel_id: Optional[UUID] = Query(None),
    org_id: UUID = Depends(require_consulting_org_id),
    db: Session = Depends(get_db),
):
    """Historic rates for the simulator: KPI rollups + unique new-lead book rate + LP conv."""
    payload = build_funnel_simulator_baselines(
        db, org_id, days=days, mtd=mtd, funnel_id=funnel_id
    )
    if payload.get("error"):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=payload["error"])
    return payload


@router.get(
    "/funnel-simulator/scenarios",
    response_model=List[FunnelSimulatorScenarioResponse],
)
def list_funnel_simulator_scenarios(
    org_id: UUID = Depends(require_consulting_org_id),
    db: Session = Depends(get_db),
):
    _ensure(db)
    rows = (
        db.query(FunnelSimulatorScenario)
        .filter(FunnelSimulatorScenario.org_id == org_id)
        .order_by(FunnelSimulatorScenario.updated_at.desc())
        .all()
    )
    return rows


@router.post(
    "/funnel-simulator/scenarios",
    response_model=FunnelSimulatorScenarioResponse,
    status_code=status.HTTP_201_CREATED,
)
def create_funnel_simulator_scenario(
    body: FunnelSimulatorScenarioCreate,
    org_id: UUID = Depends(require_consulting_org_id),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    _ensure(db)
    count = (
        db.query(FunnelSimulatorScenario)
        .filter(FunnelSimulatorScenario.org_id == org_id)
        .count()
    )
    if count >= MAX_FUNNEL_SIMULATOR_SCENARIOS_PER_ORG:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Maximum {MAX_FUNNEL_SIMULATOR_SCENARIOS_PER_ORG} scenarios per organization.",
        )
    name = (body.name or "").strip()
    if not name:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Name is required")
    now = datetime.utcnow()
    row = FunnelSimulatorScenario(
        org_id=org_id,
        name=name[:120],
        mode=body.mode,
        funnel_id=body.funnel_id,
        lookback_days=str(body.lookback_days or "90")[:16],
        inputs=body.inputs if isinstance(body.inputs, dict) else {},
        created_by=current_user.id,
        created_at=now,
        updated_at=now,
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return row


@router.patch(
    "/funnel-simulator/scenarios/{scenario_id}",
    response_model=FunnelSimulatorScenarioResponse,
)
def update_funnel_simulator_scenario(
    scenario_id: UUID,
    body: FunnelSimulatorScenarioUpdate,
    org_id: UUID = Depends(require_consulting_org_id),
    db: Session = Depends(get_db),
):
    _ensure(db)
    row = (
        db.query(FunnelSimulatorScenario)
        .filter(
            FunnelSimulatorScenario.id == scenario_id,
            FunnelSimulatorScenario.org_id == org_id,
        )
        .first()
    )
    if not row:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Scenario not found")
    if body.name is not None:
        name = body.name.strip()
        if not name:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Name is required")
        row.name = name[:120]
    if body.mode is not None:
        row.mode = body.mode
    if body.funnel_id is not None or (body.model_fields_set and "funnel_id" in body.model_fields_set):
        row.funnel_id = body.funnel_id
    if body.lookback_days is not None:
        row.lookback_days = str(body.lookback_days)[:16]
    if body.inputs is not None:
        row.inputs = body.inputs if isinstance(body.inputs, dict) else {}
    row.updated_at = datetime.utcnow()
    db.commit()
    db.refresh(row)
    return row


@router.delete(
    "/funnel-simulator/scenarios/{scenario_id}",
    status_code=status.HTTP_204_NO_CONTENT,
)
def delete_funnel_simulator_scenario(
    scenario_id: UUID,
    org_id: UUID = Depends(require_consulting_org_id),
    db: Session = Depends(get_db),
):
    _ensure(db)
    row = (
        db.query(FunnelSimulatorScenario)
        .filter(
            FunnelSimulatorScenario.id == scenario_id,
            FunnelSimulatorScenario.org_id == org_id,
        )
        .first()
    )
    if not row:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Scenario not found")
    db.delete(row)
    db.commit()
    return None

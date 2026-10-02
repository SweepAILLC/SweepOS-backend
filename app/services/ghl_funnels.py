"""Pair a Sweep funnel with a GoHighLevel funnel.

Pairing stores the GHL funnel's id, name and step paths in `funnels.ghl_config`
and, for a funnel with no steps yet, creates one FunnelStep per GHL step with event
name `view:<path>` (what the visitor snippet sends), so per-step drop-off works in
the existing steps tab. Lead intake (webhook + reconcile pull) reads this config.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from sqlalchemy.orm.attributes import flag_modified

from app.models.funnel import Funnel, FunnelStep
from app.services import ghl_client as gc

GHL_SOURCE = "ghl"
STEP_EVENT_PREFIX = "view:"


class GhlFunnelNotFoundError(Exception):
    pass


class GhlFunnelAlreadyPairedError(Exception):
    def __init__(self, funnel: Funnel):
        super().__init__(f"GHL funnel already paired to '{funnel.name}'")
        self.funnel = funnel


def step_event_name(path: str) -> str:
    # Event names are capped at 100 chars (EventIn); the visitor snippet cuts the path
    # at the same 95 so long paths still match their step.
    return f"{STEP_EVENT_PREFIX}{path[:95]}"


def set_extra_form_ids(db: Session, funnel: Funnel, form_ids: list[str]) -> Funnel:
    """Forms outside the funnel's pages (popups, standalone links) whose submissions
    count as this funnel's opt-ins in the reconcile pull."""
    cfg = dict(funnel.ghl_config or {})
    cfg["extra_form_ids"] = sorted({f.strip() for f in form_ids if f and f.strip()})
    funnel.ghl_config = cfg
    flag_modified(funnel, "ghl_config")
    funnel.updated_at = datetime.utcnow()
    db.commit()
    db.refresh(funnel)
    return funnel


def find_paired_funnel(db: Session, org_id: uuid.UUID, ghl_funnel_id: str) -> Optional[Funnel]:
    return (
        db.query(Funnel)
        .filter(
            Funnel.org_id == org_id,
            Funnel.source == GHL_SOURCE,
            Funnel.ghl_config["ghl_funnel_id"].astext == ghl_funnel_id,
        )
        .first()
    )


def paired_funnels_by_ghl_id(db: Session, org_id: uuid.UUID) -> Dict[str, Funnel]:
    rows = db.query(Funnel).filter(Funnel.org_id == org_id, Funnel.source == GHL_SOURCE).all()
    return {
        str(f.ghl_config.get("ghl_funnel_id")): f
        for f in rows
        if isinstance(f.ghl_config, dict) and f.ghl_config.get("ghl_funnel_id")
    }


def build_ghl_config(ghl_funnel: Dict[str, Any], previous: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Pairing fields from a normalized GHL funnel; keeps sync/webhook state on re-pair of the same funnel."""
    prev = previous if isinstance(previous, dict) else {}
    same = prev.get("ghl_funnel_id") == ghl_funnel["ghl_funnel_id"]
    config: Dict[str, Any] = {
        "ghl_funnel_id": ghl_funnel["ghl_funnel_id"],
        "name": ghl_funnel["name"],
        "path": ghl_funnel.get("path"),
        "steps": ghl_funnel["steps"],
        "extra_form_ids": list(prev.get("extra_form_ids") or []) if same else [],
        "paired_at": prev.get("paired_at") if same and prev.get("paired_at") else datetime.now(timezone.utc).isoformat(),
    }
    if same:
        for key in ("sync", "webhook"):
            if key in prev:
                config[key] = prev[key]
    return config


def _create_steps(db: Session, funnel: Funnel, ghl_funnel: Dict[str, Any]) -> int:
    if db.query(FunnelStep.id).filter(FunnelStep.funnel_id == funnel.id).first() is not None:
        return 0  # never duplicate or reorder steps the coach already set up
    for order, step in enumerate(ghl_funnel["steps"], start=1):
        db.add(
            FunnelStep(
                org_id=funnel.org_id,
                funnel_id=funnel.id,
                step_order=order,
                event_name=step_event_name(step["path"]),
                label=step.get("name"),
            )
        )
    return len(ghl_funnel["steps"])


def fetch_ghl_funnel(db: Session, org_id: uuid.UUID, ghl_funnel_id: str, user_id: Optional[uuid.UUID] = None) -> Dict[str, Any]:
    """Raises GhlNotConnectedError, GhlApiError, GhlFunnelNotFoundError."""
    headers, location_id = gc.get_ghl_connection(db, org_id, user_id=user_id)
    for item in gc.list_ghl_funnels(headers, location_id):
        if item["ghl_funnel_id"] == ghl_funnel_id:
            return item
    raise GhlFunnelNotFoundError("GHL funnel not found in the connected location")


def pair_funnel_with_ghl(
    db: Session,
    funnel: Funnel,
    ghl_funnel: Dict[str, Any],
) -> Funnel:
    """Pair `funnel` (already added to the session) with a normalized GHL funnel and commit.

    Raises GhlFunnelAlreadyPairedError when another funnel in the org holds it,
    including a concurrent pairing caught by the unique index from migration 097.
    """
    other = find_paired_funnel(db, funnel.org_id, ghl_funnel["ghl_funnel_id"])
    if other is not None and other.id != funnel.id:
        raise GhlFunnelAlreadyPairedError(other)

    funnel.source = GHL_SOURCE
    funnel.ghl_config = build_ghl_config(ghl_funnel, funnel.ghl_config)
    funnel.updated_at = datetime.utcnow()
    try:
        # The unique index (migration 097) can fire at flush or commit.
        db.flush()
        _create_steps(db, funnel, ghl_funnel)
        db.commit()
    except IntegrityError:
        db.rollback()
        winner = find_paired_funnel(db, funnel.org_id, ghl_funnel["ghl_funnel_id"])
        if winner is not None:
            raise GhlFunnelAlreadyPairedError(winner)
        raise
    db.refresh(funnel)
    return funnel


def unpair_funnel(db: Session, funnel: Funnel) -> Funnel:
    """Back to a Sweep-tracked funnel. Leads already tagged keep their attribution."""
    funnel.source = "sweep"
    funnel.ghl_config = None
    funnel.updated_at = datetime.utcnow()
    db.commit()
    db.refresh(funnel)
    return funnel

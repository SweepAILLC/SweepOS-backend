"""Messaging Matrix routes (Content Engine, consulting-tier orgs).

Reads: any member of a consulting-tier org. Refresh, archive and Recreate: org admin or
owner. Org always comes from the session (selected_org_id), never from the request.
See docs/features/MESSAGING_MATRIX.md.
"""
from __future__ import annotations

import logging
import uuid
from typing import Literal, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.api.deps import require_admin_or_owner
from app.api.portal import require_consulting_org_id
from app.core.rate_limit import check_sliding_window
from app.db.session import get_db
from app.long_jobs import schedule_background_work
from app.models.user import User
from app.services import messaging_matrix_service as mm

logger = logging.getLogger(__name__)
router = APIRouter()


class AngleStatusBody(BaseModel):
    status: Literal["active", "archived"]


class RecreateBody(BaseModel):
    channel: Literal["organic", "paid"] = "organic"


@router.get("")
def get_messaging_matrix(
    channel: Optional[Literal["organic", "paid"]] = None,
    awareness_stage: Optional[str] = None,
    min_samples: int = Query(1, ge=0, le=50),
    include_archived: bool = False,
    limit: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_db),
    org_id: uuid.UUID = Depends(require_consulting_org_id),
):
    return mm.list_matrix(
        db,
        org_id,
        channel=channel,
        awareness_stage=awareness_stage,
        min_samples=min_samples,
        include_archived=include_archived,
        limit=limit,
    )


@router.get("/status")
def get_refresh_status(org_id: uuid.UUID = Depends(require_consulting_org_id)):
    return mm.get_refresh_status(org_id)


@router.post("/refresh", status_code=status.HTTP_202_ACCEPTED)
def refresh_messaging_matrix(
    org_id: uuid.UUID = Depends(require_consulting_org_id),
    current_user: User = Depends(require_admin_or_owner),
):
    if mm.is_refresh_active(org_id):
        return {"queued": False, "status": mm.get_refresh_status(org_id)}
    check_sliding_window(
        f"mm_refresh_{org_id}",
        max_requests=6,
        window_seconds=3600,
        endpoint_name="messaging_matrix_refresh",
    )
    mm.mark_queued(org_id)
    schedule_background_work(mm.refresh_matrix_job, None, str(org_id), job_timeout=1800)
    return {"queued": True, "status": mm.get_refresh_status(org_id)}


@router.get("/angles/{angle_id}")
def get_messaging_angle(
    angle_id: uuid.UUID,
    db: Session = Depends(get_db),
    org_id: uuid.UUID = Depends(require_consulting_org_id),
):
    try:
        return mm.angle_detail(db, org_id, angle_id)
    except LookupError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc


@router.patch("/angles/{angle_id}")
def patch_messaging_angle(
    angle_id: uuid.UUID,
    body: AngleStatusBody,
    db: Session = Depends(get_db),
    org_id: uuid.UUID = Depends(require_consulting_org_id),
    current_user: User = Depends(require_admin_or_owner),
):
    try:
        return mm.set_angle_status(db, org_id, angle_id, body.status)
    except LookupError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc


@router.post("/angles/{angle_id}/recreate")
def recreate_messaging_angle(
    angle_id: uuid.UUID,
    body: RecreateBody,
    db: Session = Depends(get_db),
    org_id: uuid.UUID = Depends(require_consulting_org_id),
    current_user: User = Depends(require_admin_or_owner),
):
    check_sliding_window(
        f"mm_recreate_{org_id}_{current_user.id}",
        max_requests=20,
        window_seconds=3600,
        endpoint_name="messaging_matrix_recreate",
    )
    try:
        return mm.recreate_angle(db, org_id, angle_id, channel=body.channel, user_id=current_user.id)
    except LookupError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except RuntimeError as exc:
        msg = str(exc) or "Draft failed"
        code = status.HTTP_503_SERVICE_UNAVAILABLE if "not configured" in msg.lower() else status.HTTP_502_BAD_GATEWAY
        raise HTTPException(status_code=code, detail=msg) from exc

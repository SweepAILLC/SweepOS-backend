"""Portal Content Angle Map routes (consulting-tier org)."""
from __future__ import annotations

import logging
import uuid

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.api.deps import get_current_user
from app.api.portal import require_consulting_org_id
from app.core.rate_limit import check_sliding_window
from app.db.session import get_db
from app.models.user import User
from app.schemas.content_angle_map import (
    ContentAngleDeleteBody,
    ContentAngleMapOut,
    ContentAnglePatchBody,
    ContentAnglePillsPutBody,
    ContentAngleRegenerateBody,
)
from app.services import content_angle_map as cam
from app.services.llm_client import llm_available

logger = logging.getLogger(__name__)
router = APIRouter()


def _http_for_gen_error(exc: Exception) -> HTTPException:
    if isinstance(exc, ValueError):
        return HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))
    msg = str(exc) or "Generation failed"
    if "not configured" in msg.lower():
        return HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="AI generation is not configured",
        )
    return HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=msg)


@router.get("/content-angle-map", response_model=ContentAngleMapOut)
def get_content_angle_map(
    db: Session = Depends(get_db),
    org_id: uuid.UUID = Depends(require_consulting_org_id),
    current_user: User = Depends(get_current_user),
):
    payload = cam.load_map_out(db, org_id)
    if (payload.can_generate_icp and not payload.icp_angles) or (
        payload.can_generate_brand and not payload.personal_brand_angles
    ):
        cam.maybe_queue_initial_generation(org_id)
    return payload


@router.patch("/content-angle-map/angles", response_model=ContentAngleMapOut)
def patch_content_angle_map_angle(
    body: ContentAnglePatchBody,
    db: Session = Depends(get_db),
    org_id: uuid.UUID = Depends(require_consulting_org_id),
    current_user: User = Depends(get_current_user),
):
    try:
        return cam.patch_angle(db, org_id, body.card, body.id, body.text)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e)) from e


@router.delete("/content-angle-map/angles", response_model=ContentAngleMapOut)
def delete_content_angle_map_angle(
    body: ContentAngleDeleteBody,
    db: Session = Depends(get_db),
    org_id: uuid.UUID = Depends(require_consulting_org_id),
    current_user: User = Depends(get_current_user),
):
    return cam.delete_angle(db, org_id, body.card, body.id)


@router.put("/content-angle-map/pills", response_model=ContentAngleMapOut)
def put_content_angle_map_pills(
    body: ContentAnglePillsPutBody,
    db: Session = Depends(get_db),
    org_id: uuid.UUID = Depends(require_consulting_org_id),
    current_user: User = Depends(get_current_user),
):
    try:
        return cam.replace_pills(db, org_id, body.stage, body.pills)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e)) from e


@router.post("/content-angle-map/regenerate", response_model=ContentAngleMapOut)
def regenerate_content_angle_map(
    body: ContentAngleRegenerateBody,
    db: Session = Depends(get_db),
    org_id: uuid.UUID = Depends(require_consulting_org_id),
    current_user: User = Depends(get_current_user),
):
    check_sliding_window(
        f"cam_regen_{org_id}_{current_user.id}",
        max_requests=6,
        window_seconds=3600,
        endpoint_name="content_angle_map_regenerate",
    )
    if not llm_available():
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="AI generation is not configured",
        )
    try:
        return cam.generate_card(db, org_id, body.card, full=body.full)
    except Exception as e:
        logger.warning("content_angle_map regenerate org=%s card=%s: %s", org_id, body.card, e)
        raise _http_for_gen_error(e) from e

"""Post-sales close survey — private tokenized public form + auth entry-link."""
from __future__ import annotations

import uuid
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from app.api.deps import get_current_user
from app.core.config import settings
from app.db.session import get_db
from app.models.organization import Organization
from app.models.user import User
from app.schemas.close_survey import (
    CloseSurveyEntryLinkResponse,
    CloseSurveyMetaResponse,
    CloseSurveySubmitRequest,
    CloseSurveySubmitResponse,
)
from app.services.close_survey_service import (
    build_close_survey_meta,
    resolve_org_by_close_token,
    submit_close_survey,
)

router = APIRouter()


def _org_id(user: User) -> uuid.UUID:
    return getattr(user, "selected_org_id", None) or user.org_id


@router.get("/entry-link", response_model=CloseSurveyEntryLinkResponse)
def get_close_survey_entry_link(
    regenerate: bool = Query(False),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    org_id = _org_id(current_user)
    org = db.query(Organization).filter(Organization.id == org_id).first()
    if not org:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Organization not found")
    if regenerate or not org.close_form_token:
        org.close_form_token = uuid.uuid4()
        org.updated_at = datetime.utcnow()
        db.commit()
        db.refresh(org)
    base = str(getattr(settings, "FRONTEND_URL", "") or "http://localhost:3002").rstrip("/")
    token = str(org.close_form_token)
    return CloseSurveyEntryLinkResponse(
        token=token,
        url=f"{base}/close-survey/{token}",
    )


@router.get("/public/{token}/meta", response_model=CloseSurveyMetaResponse)
def get_close_survey_meta(token: str, db: Session = Depends(get_db)):
    org = resolve_org_by_close_token(db, token)
    return build_close_survey_meta(db, org)


@router.post("/public/{token}/submit", response_model=CloseSurveySubmitResponse)
def post_close_survey_submit(
    token: str,
    body: CloseSurveySubmitRequest,
    db: Session = Depends(get_db),
):
    org = resolve_org_by_close_token(db, token)
    return submit_close_survey(db, org, body)

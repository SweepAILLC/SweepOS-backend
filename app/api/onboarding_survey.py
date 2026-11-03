"""Post-sale onboarding survey — org-wide tokenized public form + auth entry-link.

Mirrors close_survey.py's close_form_token: one link per org, shown in the
Resources tab, filled in by the respondent themselves (name + business name),
not minted per client. See docs/features/ONBOARDING_SURVEY_CONTENT_ATTRIBUTION_PRD.md.
"""
from __future__ import annotations

import uuid

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from app.api.deps import get_current_user
from app.core.config import settings
from app.db.session import get_db
from app.models.organization import Organization
from app.models.user import User
from app.schemas.onboarding_survey import (
    OnboardingSurveyDashboardResponse,
    OnboardingSurveyEntryLinkResponse,
    OnboardingSurveyMetaResponse,
    OnboardingSurveyOptionsResponse,
    OnboardingSurveyOptionsUpdateRequest,
    OnboardingSurveySubmitRequest,
    OnboardingSurveySubmitResponse,
)
from app.services.onboarding_survey_service import (
    build_onboarding_survey_dashboard,
    build_onboarding_survey_meta,
    get_onboarding_survey_options,
    get_or_create_org_token,
    regenerate_org_token,
    resolve_org_by_token,
    submit_onboarding_survey,
    update_onboarding_survey_options,
)

router = APIRouter()


def _org_id(user: User) -> uuid.UUID:
    return getattr(user, "selected_org_id", None) or user.org_id


@router.get("/entry-link", response_model=OnboardingSurveyEntryLinkResponse)
def get_onboarding_survey_entry_link(
    regenerate: bool = Query(False),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    org_id = _org_id(current_user)
    org = db.query(Organization).filter(Organization.id == org_id).first()
    if not org:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Organization not found")

    org = get_or_create_org_token(db, org)
    if regenerate:
        org = regenerate_org_token(db, org)

    base = str(getattr(settings, "FRONTEND_URL", "") or "http://localhost:3003").rstrip("/")
    token = str(org.onboarding_survey_token)
    return OnboardingSurveyEntryLinkResponse(
        token=token,
        url=f"{base}/onboarding-survey/{token}",
    )


@router.get("/dashboard", response_model=OnboardingSurveyDashboardResponse)
def get_onboarding_survey_dashboard(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    org_id = _org_id(current_user)
    return build_onboarding_survey_dashboard(db, org_id)


@router.get("/options", response_model=OnboardingSurveyOptionsResponse)
def get_onboarding_survey_options_endpoint(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    org_id = _org_id(current_user)
    org = db.query(Organization).filter(Organization.id == org_id).first()
    if not org:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Organization not found")
    return get_onboarding_survey_options(db, org)


@router.put("/options", response_model=OnboardingSurveyOptionsResponse)
def put_onboarding_survey_options(
    body: OnboardingSurveyOptionsUpdateRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    org_id = _org_id(current_user)
    org = db.query(Organization).filter(Organization.id == org_id).first()
    if not org:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Organization not found")
    return update_onboarding_survey_options(db, org, body)


@router.get("/public/{token}/meta", response_model=OnboardingSurveyMetaResponse)
def get_onboarding_survey_meta(token: str, db: Session = Depends(get_db)):
    org = resolve_org_by_token(db, token)
    return build_onboarding_survey_meta(db, org)


@router.post("/public/{token}/submit", response_model=OnboardingSurveySubmitResponse)
def post_onboarding_survey_submit(
    token: str,
    body: OnboardingSurveySubmitRequest,
    db: Session = Depends(get_db),
):
    org = resolve_org_by_token(db, token)
    return submit_onboarding_survey(db, org, body)

"""Orchestration for the public post-sale onboarding survey.

One org-wide public link (organizations.onboarding_survey_token), mirroring
close_survey.py's close_form_token — unlike the earlier per-client design,
the respondent types their own name/business, so there's no entry row to
pre-create. See docs/features/ONBOARDING_SURVEY_CONTENT_ATTRIBUTION_PRD.md.
"""
from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import List

from fastapi import HTTPException, status
from sqlalchemy.orm import Session

from app.models.instagram_media import InstagramMedia
from app.models.onboarding_survey_response import OnboardingSurveyResponse
from app.models.organization import Organization
from app.schemas.onboarding_survey import (
    OnboardingSurveyDashboardResponse,
    OnboardingSurveyGalleryItem,
    OnboardingSurveyMetaResponse,
    OnboardingSurveyOptionsResponse,
    OnboardingSurveyOptionsUpdateRequest,
    OnboardingSurveyResponseItem,
    OnboardingSurveySelectedContentItem,
    OnboardingSurveySubmitRequest,
    OnboardingSurveySubmitResponse,
    OnboardingSurveyTopSelectedItem,
)

LOG = logging.getLogger("app.onboarding_survey")


def get_or_create_org_token(db: Session, org: Organization) -> Organization:
    if not org.onboarding_survey_token:
        org.onboarding_survey_token = uuid.uuid4()
        org.updated_at = datetime.utcnow()
        db.commit()
        db.refresh(org)
    return org


def regenerate_org_token(db: Session, org: Organization) -> Organization:
    org.onboarding_survey_token = uuid.uuid4()
    org.updated_at = datetime.utcnow()
    db.commit()
    db.refresh(org)
    return org


def resolve_org_by_token(db: Session, token: str) -> Organization:
    try:
        token_uuid = uuid.UUID(str(token).strip())
    except (ValueError, TypeError, AttributeError):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Invalid survey link")
    org = (
        db.query(Organization)
        .filter(Organization.onboarding_survey_token == token_uuid)
        .first()
    )
    if not org:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Survey link not found")
    return org


def get_onboarding_survey_options(db: Session, org: Organization) -> OnboardingSurveyOptionsResponse:
    options = org.onboarding_survey_options if isinstance(org.onboarding_survey_options, dict) else {}
    return OnboardingSurveyOptionsResponse(
        referral_source_options=[str(v) for v in (options.get("referral_source") or [])],
        problem_to_solve_options=[str(v) for v in (options.get("problem_to_solve") or [])],
        conversion_trigger_options=[str(v) for v in (options.get("conversion_trigger") or [])],
    )


def update_onboarding_survey_options(
    db: Session,
    org: Organization,
    body: OnboardingSurveyOptionsUpdateRequest,
) -> OnboardingSurveyOptionsResponse:
    referral_options = [v.strip() for v in body.referral_source_options if v.strip()]
    problem_options = [v.strip() for v in body.problem_to_solve_options if v.strip()]
    trigger_options = [v.strip() for v in body.conversion_trigger_options if v.strip()]
    org.onboarding_survey_options = {
        "referral_source": referral_options,
        "problem_to_solve": problem_options,
        "conversion_trigger": trigger_options,
    }
    org.updated_at = datetime.utcnow()
    db.commit()
    db.refresh(org)
    return OnboardingSurveyOptionsResponse(
        referral_source_options=referral_options,
        problem_to_solve_options=problem_options,
        conversion_trigger_options=trigger_options,
    )


def build_onboarding_survey_meta(db: Session, org: Organization) -> OnboardingSurveyMetaResponse:
    options = org.onboarding_survey_options if isinstance(org.onboarding_survey_options, dict) else {}
    referral_options: List[str] = [str(v) for v in (options.get("referral_source") or [])]
    problem_options: List[str] = [str(v) for v in (options.get("problem_to_solve") or [])]
    trigger_options: List[str] = [str(v) for v in (options.get("conversion_trigger") or [])]

    # The content-gallery step only renders for orgs with Instagram connected — reuses
    # build_instagram_performance's own connection check instead of a second OAuthToken
    # query. Orgs without Instagram get the free-text content_contribution question
    # instead (frontend branches on instagram_connected), so this form still loads the
    # same either way — it just offers a tappable gallery when there's something to show.
    from app.services.instagram_performance import build_instagram_performance

    perf = build_instagram_performance(db, org.id, days=180)
    instagram_connected = bool(perf.get("connected"))
    gallery: List[OnboardingSurveyGalleryItem] = []
    if instagram_connected:
        for p in (perf.get("top_posts") or [])[:24]:
            post_id = p.get("id")
            if not post_id:
                continue
            gallery.append(
                OnboardingSurveyGalleryItem(
                    id=str(post_id),
                    permalink=p.get("permalink"),
                    thumbnail_url=p.get("thumbnail_url"),
                    caption_excerpt=(p.get("caption") or "")[:140] or None,
                )
            )

    return OnboardingSurveyMetaResponse(
        org_name=org.name or "Organization",
        referral_source_options=referral_options,
        problem_to_solve_options=problem_options,
        conversion_trigger_options=trigger_options,
        instagram_connected=instagram_connected,
        instagram_gallery=gallery,
    )


def submit_onboarding_survey(
    db: Session,
    org: Organization,
    body: OnboardingSurveySubmitRequest,
) -> OnboardingSurveySubmitResponse:
    entry = OnboardingSurveyResponse(
        org_id=org.id,
        respondent_name=body.respondent_name.strip(),
        respondent_business_name=(body.respondent_business_name or "").strip() or None,
        referral_source=body.referral_source,
        started_following_date=body.started_following_date,
        interest_spark=body.interest_spark,
        content_contribution=(body.content_contribution or "").strip() or None,
        content_selected_ids=[str(i) for i in body.content_selected_ids] or None,
        problem_to_solve=body.problem_to_solve,
        offer_fit_reason=body.offer_fit_reason,
        investment_reason=body.investment_reason,
        decision_speed_blocker=body.decision_speed_blocker,
        commitment_score=body.commitment_score,
        conversion_trigger=body.conversion_trigger,
        referral_name=(body.referral_name or "").strip() or None,
        referral_contact=(body.referral_contact or "").strip() or None,
        submitted_at=datetime.now(timezone.utc),
    )
    db.add(entry)
    db.commit()
    db.refresh(entry)

    return OnboardingSurveySubmitResponse(
        ok=True,
        submitted_at=entry.submitted_at,
    )


def build_onboarding_survey_dashboard(db: Session, org_id: uuid.UUID) -> OnboardingSurveyDashboardResponse:
    """All submitted onboarding-survey responses for an org, plus the content-selection tally —
    the data backing the coach-facing Content Engine dashboard (not sent anywhere automatically)."""
    rows = (
        db.query(OnboardingSurveyResponse)
        .filter(
            OnboardingSurveyResponse.org_id == org_id,
            OnboardingSurveyResponse.submitted_at.isnot(None),
        )
        .order_by(OnboardingSurveyResponse.submitted_at.desc())
        .all()
    )

    all_media_ids: set = set()
    for response in rows:
        for media_id in response.content_selected_ids or []:
            all_media_ids.add(media_id)

    media_by_id = {}
    if all_media_ids:
        media_rows = (
            db.query(InstagramMedia)
            .filter(InstagramMedia.org_id == org_id, InstagramMedia.id.in_(all_media_ids))
            .all()
        )
        media_by_id = {m.id: m for m in media_rows}

    response_items = []
    for response in rows:
        selected = []
        for media_id in response.content_selected_ids or []:
            media = media_by_id.get(media_id)
            selected.append(
                OnboardingSurveySelectedContentItem(
                    instagram_media_id=str(media_id),
                    permalink=media.permalink if media else None,
                    thumbnail_url=media.thumbnail_url if media else None,
                )
            )
        response_items.append(
            OnboardingSurveyResponseItem(
                respondent_name=response.respondent_name or "Unnamed respondent",
                respondent_business_name=response.respondent_business_name,
                submitted_at=response.submitted_at,
                referral_source=response.referral_source,
                started_following_date=response.started_following_date,
                interest_spark=response.interest_spark,
                content_contribution=response.content_contribution,
                content_selected=selected,
                problem_to_solve=response.problem_to_solve,
                offer_fit_reason=response.offer_fit_reason,
                investment_reason=response.investment_reason,
                decision_speed_blocker=response.decision_speed_blocker,
                commitment_score=response.commitment_score,
                conversion_trigger=response.conversion_trigger,
                referral_name=response.referral_name,
                referral_contact=response.referral_contact,
            )
        )

    from app.services.content_conversion_service import get_top_selected_content

    top = get_top_selected_content(db, org_id, limit=5)
    top_items = [
        OnboardingSurveyTopSelectedItem(
            instagram_media_id=t["instagram_media_id"],
            selection_count=t["selection_count"],
            permalink=t.get("permalink"),
            thumbnail_url=t.get("thumbnail_url"),
            caption=t.get("caption"),
        )
        for t in top
    ]

    return OnboardingSurveyDashboardResponse(
        total_submitted=len(response_items),
        responses=response_items,
        top_selected_content=top_items,
    )

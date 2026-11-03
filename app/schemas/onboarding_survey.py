"""Schemas for the public post-sale client onboarding survey.

See docs/features/ONBOARDING_SURVEY_CONTENT_ATTRIBUTION_PRD.md.
"""
from __future__ import annotations

from datetime import date, datetime
from typing import List, Optional
from uuid import UUID

from pydantic import BaseModel, Field


class OnboardingSurveyEntryLinkResponse(BaseModel):
    token: str
    url: str


class OnboardingSurveyGalleryItem(BaseModel):
    """One Instagram post offered in the content-gallery step — only sent when the org has
    Instagram connected; otherwise the frontend falls back to a free-text question."""

    id: str
    permalink: Optional[str] = None
    thumbnail_url: Optional[str] = None
    caption_excerpt: Optional[str] = None


class OnboardingSurveyMetaResponse(BaseModel):
    org_name: str
    referral_source_options: List[str] = []
    problem_to_solve_options: List[str] = []
    conversion_trigger_options: List[str] = []
    instagram_connected: bool = False
    instagram_gallery: List[OnboardingSurveyGalleryItem] = []


class OnboardingSurveyOptionsResponse(BaseModel):
    """The org-configurable choice lists behind the three multi-choice survey steps."""

    referral_source_options: List[str] = []
    problem_to_solve_options: List[str] = []
    conversion_trigger_options: List[str] = []


class OnboardingSurveyOptionsUpdateRequest(BaseModel):
    referral_source_options: List[str] = Field(default_factory=list, max_length=20)
    problem_to_solve_options: List[str] = Field(default_factory=list, max_length=20)
    conversion_trigger_options: List[str] = Field(default_factory=list, max_length=20)


class OnboardingSurveySubmitRequest(BaseModel):
    respondent_name: str = Field(..., min_length=1, max_length=255)
    respondent_business_name: Optional[str] = Field(None, max_length=255)
    referral_source: Optional[str] = Field(None, max_length=255)
    started_following_date: Optional[date] = None
    interest_spark: Optional[str] = Field(None, max_length=4000)
    content_contribution: Optional[str] = Field(None, max_length=4000)
    # Legacy — no longer sent by the current form (replaced by content_contribution,
    # which doesn't require a live Instagram connection), kept for compatibility.
    content_selected_ids: List[UUID] = []
    problem_to_solve: Optional[str] = Field(None, max_length=255)
    offer_fit_reason: Optional[str] = Field(None, max_length=4000)
    investment_reason: Optional[str] = Field(None, max_length=4000)
    decision_speed_blocker: Optional[str] = Field(None, max_length=4000)
    commitment_score: Optional[int] = Field(None, ge=1, le=10)
    conversion_trigger: Optional[str] = Field(None, max_length=255)
    referral_name: Optional[str] = Field(None, max_length=255)
    referral_contact: Optional[str] = Field(None, max_length=255)


class OnboardingSurveySubmitResponse(BaseModel):
    ok: bool = True
    submitted_at: datetime
    message: str = "Thanks — your answers were recorded."


class OnboardingSurveySelectedContentItem(BaseModel):
    """One picked post, resolved for display — raw content_selected_ids are InstagramMedia.id,
    not useful on their own in a dashboard."""

    instagram_media_id: str
    permalink: Optional[str] = None
    thumbnail_url: Optional[str] = None


class OnboardingSurveyResponseItem(BaseModel):
    """One respondent's full submitted answers, for the coach-facing Content Engine dashboard."""

    respondent_name: str
    respondent_business_name: Optional[str] = None
    submitted_at: datetime
    referral_source: Optional[str] = None
    started_following_date: Optional[date] = None
    interest_spark: Optional[str] = None
    content_contribution: Optional[str] = None
    content_selected: List[OnboardingSurveySelectedContentItem] = []
    problem_to_solve: Optional[str] = None
    offer_fit_reason: Optional[str] = None
    investment_reason: Optional[str] = None
    decision_speed_blocker: Optional[str] = None
    commitment_score: Optional[int] = None
    conversion_trigger: Optional[str] = None
    referral_name: Optional[str] = None
    referral_contact: Optional[str] = None


class OnboardingSurveyTopSelectedItem(BaseModel):
    instagram_media_id: str
    selection_count: int
    permalink: Optional[str] = None
    thumbnail_url: Optional[str] = None
    caption: Optional[str] = None


class OnboardingSurveyDashboardResponse(BaseModel):
    total_submitted: int
    responses: List[OnboardingSurveyResponseItem] = []
    top_selected_content: List[OnboardingSurveyTopSelectedItem] = []

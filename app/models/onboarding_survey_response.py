"""Post-sale onboarding survey — one row per submission, filled in by the
respondent through the org-wide public link (organizations.onboarding_survey_token).
Not tied to an existing Client row; the respondent types their own name and
business name. See docs/features/ONBOARDING_SURVEY_CONTENT_ATTRIBUTION_PRD.md.
"""
from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import Column, Date, DateTime, ForeignKey, Integer, String, Text
from sqlalchemy.dialects.postgresql import ARRAY, UUID

from app.db.session import Base


class OnboardingSurveyResponse(Base):
    __tablename__ = "onboarding_survey_responses"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    org_id = Column(
        UUID(as_uuid=True),
        ForeignKey("organizations.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    # Respondent-entered, not looked up — the public form has no login and
    # isn't bound to an existing Client row.
    respondent_name = Column(String, nullable=True)
    respondent_business_name = Column(String, nullable=True)

    # Q1-Q3
    referral_source = Column(String, nullable=True)
    started_following_date = Column(Date, nullable=True)
    interest_spark = Column(Text, nullable=True)

    # Q4 (legacy) — Instagram post IDs picked in the content gallery. Replaced by
    # content_contribution (free text) since the gallery required a live Instagram
    # connection; kept for the historical tally (content_conversion_service) and
    # any pre-existing rows, but new submissions leave this null.
    content_selected_ids = Column(ARRAY(UUID(as_uuid=True)), nullable=True)
    # Q4 — free-text replacement, no Instagram dependency.
    content_contribution = Column(Text, nullable=True)

    # Q5-Q8
    problem_to_solve = Column(String, nullable=True)
    offer_fit_reason = Column(Text, nullable=True)
    investment_reason = Column(Text, nullable=True)
    decision_speed_blocker = Column(Text, nullable=True)

    # Q9-Q10
    commitment_score = Column(Integer, nullable=True)
    conversion_trigger = Column(String, nullable=True)

    # Q11 — referral ask: who they know that might want the same program/results.
    referral_name = Column(String, nullable=True)
    referral_contact = Column(String, nullable=True)

    submitted_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

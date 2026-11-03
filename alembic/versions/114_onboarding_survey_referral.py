"""Onboarding survey: add a final referral question — who the respondent
knows that might want the same program/results, with that person's name and
contact info (email/phone/IG, free text).

Revision ID: 114
Revises: 113
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "114"
down_revision = "113"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "onboarding_survey_responses", sa.Column("referral_name", sa.String(), nullable=True)
    )
    op.add_column(
        "onboarding_survey_responses", sa.Column("referral_contact", sa.String(), nullable=True)
    )


def downgrade() -> None:
    op.drop_column("onboarding_survey_responses", "referral_contact")
    op.drop_column("onboarding_survey_responses", "referral_name")

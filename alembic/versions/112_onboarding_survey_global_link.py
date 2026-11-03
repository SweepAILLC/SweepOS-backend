"""Onboarding survey: switch from a per-client tokenized link to one org-wide
link (mirrors organizations.close_form_token), filled in by the respondent
themselves with their own name + business name instead of being bound to an
existing Client row. Also turns the free-text "time elapsed" question into an
actual date (started_following_date).

No real survey submissions exist yet (feature shipped earlier this session,
still unused in production), so this is a clean redesign rather than a
backward-compatible migration: drops client_id/survey_token, adds
respondent_name/respondent_business_name/started_following_date.

Revision ID: 112
Revises: 111
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import UUID

revision = "112"
down_revision = "111"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "organizations", sa.Column("onboarding_survey_token", UUID(as_uuid=True), nullable=True)
    )
    op.create_index(
        "ix_organizations_onboarding_survey_token",
        "organizations",
        ["onboarding_survey_token"],
        unique=True,
    )

    op.drop_index(
        "ix_onboarding_survey_responses_client_id", table_name="onboarding_survey_responses"
    )
    op.drop_index(
        "ix_onboarding_survey_responses_survey_token", table_name="onboarding_survey_responses"
    )
    op.drop_constraint(
        "onboarding_survey_responses_client_id_fkey",
        "onboarding_survey_responses",
        type_="foreignkey",
    )
    op.drop_column("onboarding_survey_responses", "client_id")
    op.drop_column("onboarding_survey_responses", "survey_token")

    op.add_column(
        "onboarding_survey_responses", sa.Column("respondent_name", sa.String(), nullable=True)
    )
    op.add_column(
        "onboarding_survey_responses",
        sa.Column("respondent_business_name", sa.String(), nullable=True),
    )

    op.add_column(
        "onboarding_survey_responses", sa.Column("started_following_date", sa.Date(), nullable=True)
    )
    op.drop_column("onboarding_survey_responses", "time_elapsed")


def downgrade() -> None:
    op.add_column(
        "onboarding_survey_responses", sa.Column("time_elapsed", sa.String(), nullable=True)
    )
    op.drop_column("onboarding_survey_responses", "started_following_date")

    op.drop_column("onboarding_survey_responses", "respondent_business_name")
    op.drop_column("onboarding_survey_responses", "respondent_name")

    op.add_column(
        "onboarding_survey_responses",
        sa.Column("survey_token", UUID(as_uuid=True), nullable=True),
    )
    op.add_column(
        "onboarding_survey_responses", sa.Column("client_id", UUID(as_uuid=True), nullable=True)
    )
    op.create_foreign_key(
        "onboarding_survey_responses_client_id_fkey",
        "onboarding_survey_responses",
        "clients",
        ["client_id"],
        ["id"],
        ondelete="CASCADE",
    )
    op.create_index(
        "ix_onboarding_survey_responses_survey_token",
        "onboarding_survey_responses",
        ["survey_token"],
        unique=True,
    )
    op.create_index(
        "ix_onboarding_survey_responses_client_id", "onboarding_survey_responses", ["client_id"]
    )

    op.drop_index(
        "ix_organizations_onboarding_survey_token", table_name="organizations"
    )
    op.drop_column("organizations", "onboarding_survey_token")

"""Post-sale client onboarding survey.

Adds `onboarding_survey_responses` (one row per client, created when the
survey link is generated, filled in on submit) and `organizations.onboarding_survey_options`
(customizable multiple-choice option lists for Q5/Q10). Additive only — new
table, one new nullable column, no existing table altered.

See docs/features/ONBOARDING_SURVEY_CONTENT_ATTRIBUTION_PRD.md.

Revision ID: 111
Revises: 110
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import ARRAY, JSONB, UUID

revision = "111"
down_revision = "110"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("organizations", sa.Column("onboarding_survey_options", JSONB(), nullable=True))

    op.create_table(
        "onboarding_survey_responses",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "org_id",
            UUID(as_uuid=True),
            sa.ForeignKey("organizations.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "client_id",
            UUID(as_uuid=True),
            sa.ForeignKey("clients.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("survey_token", UUID(as_uuid=True), nullable=False),
        sa.Column("referral_source", sa.String(), nullable=True),
        sa.Column("time_elapsed", sa.String(), nullable=True),
        sa.Column("interest_spark", sa.Text(), nullable=True),
        sa.Column("content_selected_ids", ARRAY(UUID(as_uuid=True)), nullable=True),
        sa.Column("problem_to_solve", sa.String(), nullable=True),
        sa.Column("offer_fit_reason", sa.Text(), nullable=True),
        sa.Column("investment_reason", sa.Text(), nullable=True),
        sa.Column("decision_speed_blocker", sa.Text(), nullable=True),
        sa.Column("commitment_score", sa.Integer(), nullable=True),
        sa.Column("conversion_trigger", sa.String(), nullable=True),
        sa.Column("submitted_at", sa.DateTime(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
    )
    op.create_index(
        "ix_onboarding_survey_responses_org_id", "onboarding_survey_responses", ["org_id"]
    )
    op.create_index(
        "ix_onboarding_survey_responses_client_id", "onboarding_survey_responses", ["client_id"]
    )
    op.create_index(
        "ix_onboarding_survey_responses_survey_token",
        "onboarding_survey_responses",
        ["survey_token"],
        unique=True,
    )
    # GIN index: the tally service (get_top_selected_content) groups by each
    # unnested post id in content_selected_ids across every org's responses.
    op.create_index(
        "ix_onboarding_survey_responses_content_selected_ids",
        "onboarding_survey_responses",
        ["content_selected_ids"],
        postgresql_using="gin",
    )


def downgrade() -> None:
    op.drop_index(
        "ix_onboarding_survey_responses_content_selected_ids",
        table_name="onboarding_survey_responses",
    )
    op.drop_index(
        "ix_onboarding_survey_responses_survey_token", table_name="onboarding_survey_responses"
    )
    op.drop_index(
        "ix_onboarding_survey_responses_client_id", table_name="onboarding_survey_responses"
    )
    op.drop_index(
        "ix_onboarding_survey_responses_org_id", table_name="onboarding_survey_responses"
    )
    op.drop_table("onboarding_survey_responses")
    op.drop_column("organizations", "onboarding_survey_options")

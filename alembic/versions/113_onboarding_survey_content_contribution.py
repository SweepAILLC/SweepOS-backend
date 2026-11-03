"""Onboarding survey: add a free-text "what content contributed to you
starting with us" question, replacing the Instagram-gallery multi-select
step (which broke/showed nothing for orgs with no Instagram connection).
The old content_selected_ids column and its tally pipeline
(content_conversion_service.get_top_selected_content) are left in place —
historical rows keep their data, new submissions will just leave it null.

Revision ID: 113
Revises: 112
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "113"
down_revision = "112"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "onboarding_survey_responses", sa.Column("content_contribution", sa.Text(), nullable=True)
    )


def downgrade() -> None:
    op.drop_column("onboarding_survey_responses", "content_contribution")

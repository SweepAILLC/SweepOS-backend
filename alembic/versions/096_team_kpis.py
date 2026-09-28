"""Team KPIs: member rep types (sales / marketing, also on invitations), EOD submission stamp, settings, notification log.

See docs/features/TEAM_KPIS_PRD.md. All additive and nullable, no backfill.
The tracker's range scan (org_id, entry_date, rep_user_id) is already served by
the existing partial unique index uq_org_kpi_daily_entries_org_date_rep.

Revision ID: 096
Revises: 095
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "096"
down_revision = "095"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Rep type for member-access users: "sales" | "marketing"; NULL = plain member / admin / owner.
    op.add_column("user_organizations", sa.Column("team_role", sa.String(length=16), nullable=True))
    # Rep type chosen at invite time, copied onto the membership when the invite is accepted.
    op.add_column("organization_invitations", sa.Column("team_role", sa.String(length=16), nullable=True))
    # Set only when a person submits their EOD — calendar sync / CSV never set it.
    op.add_column("org_kpi_daily_entries", sa.Column("submitted_at", sa.DateTime(timezone=True), nullable=True))

    op.create_table(
        "team_kpi_settings",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "org_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("organizations.id", ondelete="CASCADE"),
            nullable=False,
            unique=True,
        ),
        sa.Column("settings", postgresql.JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    )

    op.create_table(
        "team_kpi_notifications",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("org_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False),
        sa.Column("kind", sa.String(length=16), nullable=False),  # "reminder" | "digest"
        sa.Column("user_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=True),
        # Reminder: the local date; digest: the Monday of the week summarized.
        sa.Column("period_key", sa.Date(), nullable=False),
        sa.Column("channel", sa.String(length=16), nullable=False),  # "discord" | "email"
        sa.Column("sent_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    )
    # Idempotency: one send per (org, kind, channel, member-or-team, period).
    op.create_index(
        "uq_team_kpi_notifications_member",
        "team_kpi_notifications",
        ["org_id", "kind", "channel", "user_id", "period_key"],
        unique=True,
        postgresql_where=sa.text("user_id IS NOT NULL"),
    )
    op.create_index(
        "uq_team_kpi_notifications_team",
        "team_kpi_notifications",
        ["org_id", "kind", "channel", "period_key"],
        unique=True,
        postgresql_where=sa.text("user_id IS NULL"),
    )


def downgrade() -> None:
    op.drop_index("uq_team_kpi_notifications_team", table_name="team_kpi_notifications")
    op.drop_index("uq_team_kpi_notifications_member", table_name="team_kpi_notifications")
    op.drop_table("team_kpi_notifications")
    op.drop_table("team_kpi_settings")
    op.drop_column("org_kpi_daily_entries", "submitted_at")
    op.drop_column("organization_invitations", "team_role")
    op.drop_column("user_organizations", "team_role")

"""Add automation_rule + automation_email_job + automation_worker_heartbeat tables.

Powers the playbook-based outbound email engine:
- automation_rule: per-org config for each playbook (first_payment_onboarding,
  first_payment_referral, win_combined_ask, offboarding_recap_ask).
- automation_email_job: durable queue + audit log. Triggers (Stripe webhook,
  Whop sync, Fathom call insight, lifecycle transition) only insert rows here;
  a separately deployed worker process materializes drafts and sends via Brevo.
  UNIQUE(org_id, idempotency_key) prevents duplicate sends from redelivered
  webhooks or repeated polls.
- automation_worker_heartbeat: singleton row updated by the worker every tick
  so the API can expose dispatcher health (used as a fallback when REDIS_URL
  is not configured).
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "042"
down_revision = "041"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "automation_rules",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("org_id", postgresql.UUID(as_uuid=True), nullable=False, index=True),
        sa.Column("playbook", sa.String(64), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("delay_seconds", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("content_mode", sa.String(32), nullable=False, server_default="ai_generated"),
        sa.Column("subject_template", sa.Text(), nullable=True),
        sa.Column(
            "html_template_ref",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column(
            "audience_filter",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column(
            "opportunity_priority",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column("combine_top_n", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("require_approval", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("approval_ttl_hours", sa.Integer(), nullable=True),
        sa.Column("last_modified_by", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=False), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=False), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["org_id"], ["organizations.id"]),
        sa.UniqueConstraint("org_id", "playbook", name="uq_automation_rules_org_playbook"),
    )

    op.create_table(
        "automation_email_jobs",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("org_id", postgresql.UUID(as_uuid=True), nullable=False, index=True),
        sa.Column("rule_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("client_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("playbook", sa.String(64), nullable=False),
        sa.Column("trigger_event", sa.Text(), nullable=True),
        sa.Column("idempotency_key", sa.Text(), nullable=False),
        sa.Column("scheduled_at", sa.DateTime(timezone=False), nullable=False, server_default=sa.func.now()),
        sa.Column("state", sa.String(32), nullable=False, server_default="scheduled"),
        sa.Column(
            "payload_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("last_attempt_at", sa.DateTime(timezone=False), nullable=True),
        sa.Column("dispatched_at", sa.DateTime(timezone=False), nullable=True),
        sa.Column("brevo_message_id", sa.Text(), nullable=True),
        sa.Column("error_text", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=False), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=False), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["org_id"], ["organizations.id"]),
        sa.ForeignKeyConstraint(["rule_id"], ["automation_rules.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["client_id"], ["clients.id"], ondelete="CASCADE"),
        sa.UniqueConstraint("org_id", "idempotency_key", name="uq_automation_email_jobs_org_idemp"),
    )
    op.create_index(
        "ix_automation_email_jobs_state_scheduled",
        "automation_email_jobs",
        ["org_id", "state", "scheduled_at"],
    )
    op.create_index(
        "ix_automation_email_jobs_client_created",
        "automation_email_jobs",
        ["client_id", "created_at"],
    )

    op.create_table(
        "automation_worker_heartbeat",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("last_tick_at", sa.DateTime(timezone=False), nullable=False, server_default=sa.func.now()),
        sa.Column("worker_pid", sa.Integer(), nullable=True),
        sa.Column("worker_host", sa.String(255), nullable=True),
        sa.Column("queue_depth", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("in_flight", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("awaiting_approval", sa.Integer(), nullable=False, server_default="0"),
    )
    op.execute(
        "INSERT INTO automation_worker_heartbeat (id, last_tick_at) "
        "VALUES (1, now() - interval '1 day') ON CONFLICT (id) DO NOTHING"
    )


def downgrade() -> None:
    op.drop_table("automation_worker_heartbeat")
    op.drop_index("ix_automation_email_jobs_client_created", table_name="automation_email_jobs")
    op.drop_index("ix_automation_email_jobs_state_scheduled", table_name="automation_email_jobs")
    op.drop_table("automation_email_jobs")
    op.drop_table("automation_rules")

"""Restore automation tables if dropped + add flow step metadata.

Revision ID: 057
Revises: 056

Handles two cases:
1. Fresh path: automation_rules exists → add flow / trigger_kind / schedule_mode / step_index
2. Partial n8n gut left alembic ahead of schema → recreate automation_* tables, then add flow cols

Also drops orphan ``n8n_event_deliveries`` if present from the reverted n8n work.
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "057"
down_revision = "056"
branch_labels = None
depends_on = None

_BACKFILL = [
    ("pre_sale_post_booking", "post_booking", "booking", "after_booking", 0),
    ("pre_sale_pre_meeting", "post_booking", "booking", "before_meeting", 100),
    ("first_payment_onboarding", "onboarding", "payment", "after_trigger", 0),
    ("first_payment_referral", "onboarding", "payment", "after_previous", 10),
    ("win_combined_ask", "wins_ascension", "win", "after_trigger", 0),
    ("offboarding_recap_ask", "wins_ascension", "offboarding", "after_trigger", 0),
]


def _ensure_automation_tables(conn) -> None:
    insp = sa.inspect(conn)
    tables = set(insp.get_table_names())

    if "automation_rules" not in tables:
        op.create_table(
            "automation_rules",
            sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
            sa.Column("org_id", postgresql.UUID(as_uuid=True), nullable=False, index=True),
            sa.Column("playbook", sa.String(64), nullable=False),
            sa.Column("enabled", sa.Boolean(), nullable=False, server_default=sa.text("false")),
            sa.Column("delay_seconds", sa.Integer(), nullable=False, server_default="0"),
            sa.Column("content_mode", sa.String(32), nullable=False, server_default="ai_generated"),
            sa.Column("subject_template", sa.Text(), nullable=True),
            sa.Column("html_template_ref", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
            sa.Column("ai_content_system_prompt", sa.Text(), nullable=True),
            sa.Column("audience_filter", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
            sa.Column("trigger_config", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
            sa.Column("opportunity_priority", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
            sa.Column("combine_top_n", sa.Integer(), nullable=False, server_default="1"),
            sa.Column("require_approval", sa.Boolean(), nullable=False, server_default=sa.text("false")),
            sa.Column("approval_ttl_hours", sa.Integer(), nullable=True),
            sa.Column("last_modified_by", postgresql.UUID(as_uuid=True), nullable=True),
            sa.Column("created_at", sa.DateTime(timezone=False), nullable=False, server_default=sa.func.now()),
            sa.Column("updated_at", sa.DateTime(timezone=False), nullable=False, server_default=sa.func.now()),
            sa.ForeignKeyConstraint(["org_id"], ["organizations.id"]),
            sa.UniqueConstraint("org_id", "playbook", name="uq_automation_rules_org_playbook"),
        )
    else:
        cols = {c["name"] for c in insp.get_columns("automation_rules")}
        if "trigger_config" not in cols:
            op.add_column(
                "automation_rules",
                sa.Column("trigger_config", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
            )
        if "ai_content_system_prompt" not in cols:
            op.add_column(
                "automation_rules",
                sa.Column("ai_content_system_prompt", sa.Text(), nullable=True),
            )

    insp = sa.inspect(conn)
    tables = set(insp.get_table_names())
    if "automation_email_jobs" not in tables:
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
            sa.Column("payload_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
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

    insp = sa.inspect(conn)
    tables = set(insp.get_table_names())
    if "automation_worker_heartbeat" not in tables:
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


def upgrade() -> None:
    conn = op.get_bind()
    insp = sa.inspect(conn)
    tables = set(insp.get_table_names())

    if "n8n_event_deliveries" in tables:
        op.drop_table("n8n_event_deliveries")

    _ensure_automation_tables(conn)

    insp = sa.inspect(conn)
    cols = {c["name"] for c in insp.get_columns("automation_rules")}
    if "flow" not in cols:
        op.add_column("automation_rules", sa.Column("flow", sa.String(length=32), nullable=True))
    if "trigger_kind" not in cols:
        op.add_column("automation_rules", sa.Column("trigger_kind", sa.String(length=32), nullable=True))
    if "schedule_mode" not in cols:
        op.add_column("automation_rules", sa.Column("schedule_mode", sa.String(length=32), nullable=True))
    if "step_index" not in cols:
        op.add_column(
            "automation_rules",
            sa.Column("step_index", sa.Integer(), nullable=False, server_default="0"),
        )

    insp = sa.inspect(conn)
    indexes = {ix["name"] for ix in insp.get_indexes("automation_rules")}
    if "ix_automation_rules_flow" not in indexes:
        op.create_index("ix_automation_rules_flow", "automation_rules", ["flow"])
    if "ix_automation_rules_trigger_kind" not in indexes:
        op.create_index("ix_automation_rules_trigger_kind", "automation_rules", ["trigger_kind"])

    for playbook, flow, trigger_kind, schedule_mode, step_index in _BACKFILL:
        conn.execute(
            sa.text(
                """
                UPDATE automation_rules
                SET flow = :flow,
                    trigger_kind = :trigger_kind,
                    schedule_mode = :schedule_mode,
                    step_index = :step_index
                WHERE playbook = :playbook
                  AND (flow IS NULL OR trigger_kind IS NULL OR schedule_mode IS NULL)
                """
            ),
            {
                "playbook": playbook,
                "flow": flow,
                "trigger_kind": trigger_kind,
                "schedule_mode": schedule_mode,
                "step_index": step_index,
            },
        )


def downgrade() -> None:
    conn = op.get_bind()
    insp = sa.inspect(conn)
    if "automation_rules" not in insp.get_table_names():
        return
    indexes = {ix["name"] for ix in insp.get_indexes("automation_rules")}
    if "ix_automation_rules_trigger_kind" in indexes:
        op.drop_index("ix_automation_rules_trigger_kind", table_name="automation_rules")
    if "ix_automation_rules_flow" in indexes:
        op.drop_index("ix_automation_rules_flow", table_name="automation_rules")
    cols = {c["name"] for c in insp.get_columns("automation_rules")}
    for col in ("step_index", "schedule_mode", "trigger_kind", "flow"):
        if col in cols:
            op.drop_column("automation_rules", col)

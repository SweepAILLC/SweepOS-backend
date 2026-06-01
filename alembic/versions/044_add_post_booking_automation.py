"""Pre-sale post-booking automation: trigger_config on rules + event_type_id/label on check-ins.

Adds the schema needed by the new ``pre_sale_post_booking`` playbook:

- ``automation_rules.trigger_config`` JSONB: per-rule trigger options that don't fit the
  generic ``audience_filter`` (e.g. ``{"provider": "calendly", "event_type_ids": ["abc",
  "def"], "match_all_events": false}``). Stored as JSONB so we can extend without
  another migration when more trigger types arrive.
- ``client_check_ins.event_type_id`` (string) and ``event_type_label`` (string): captured at
  sync time from the provider payload (Cal.com ``eventType.id`` / ``eventType.title``,
  Calendly ``event_type`` URI / event ``name``). Letting us match a rule to a specific
  event type without re-parsing ``raw_event_data`` JSON on every trigger.

These columns are nullable so existing rows remain valid; the engine treats missing
``event_type_id`` as "match anything" and the rule's ``match_all_events`` flag is the
safety net that prevents accidentally firing on every booking when the user hasn't
picked an event yet.
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "044"
down_revision = "043"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "automation_rules",
        sa.Column(
            "trigger_config",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
    )

    op.add_column(
        "client_check_ins",
        sa.Column("event_type_id", sa.String(), nullable=True),
    )
    op.add_column(
        "client_check_ins",
        sa.Column("event_type_label", sa.String(), nullable=True),
    )
    op.create_index(
        "ix_client_check_ins_event_type_id",
        "client_check_ins",
        ["org_id", "provider", "event_type_id"],
    )


def downgrade() -> None:
    op.drop_index("ix_client_check_ins_event_type_id", table_name="client_check_ins")
    op.drop_column("client_check_ins", "event_type_label")
    op.drop_column("client_check_ins", "event_type_id")
    op.drop_column("automation_rules", "trigger_config")

"""Normalize lifecycle_state rows to lowercase enum values (matches Python LifecycleState.value)."""

from alembic import op
import sqlalchemy as sa

revision = "047"
down_revision = "046"
branch_labels = None
depends_on = None

_LOWERCASE_LABELS = (
    "cold_lead",
    "nurturing",
    "qualified",
    "booked",
    "active",
    "offboarding",
    "dead",
)


def _add_enum_value(label: str) -> None:
    op.execute(
        sa.text(
            "DO $$ BEGIN "
            f"ALTER TYPE lifecyclestate ADD VALUE IF NOT EXISTS '{label}'; "
            "EXCEPTION WHEN duplicate_object THEN NULL; "
            "END $$;"
        )
    )


def upgrade() -> None:
    conn = op.get_bind()
    insp = sa.inspect(conn)
    if "clients" not in insp.get_table_names():
        return

    with op.get_context().autocommit_block():
        for label in _LOWERCASE_LABELS:
            _add_enum_value(label)

    conn.execute(
        sa.text(
            """
            UPDATE clients
            SET lifecycle_state = CASE lower(lifecycle_state::text)
                WHEN 'cold_lead' THEN 'cold_lead'
                WHEN 'nurturing' THEN 'nurturing'
                WHEN 'qualified' THEN 'qualified'
                WHEN 'booked' THEN 'booked'
                WHEN 'warm_lead' THEN 'booked'
                WHEN 'active' THEN 'active'
                WHEN 'offboarding' THEN 'offboarding'
                WHEN 'dead' THEN 'dead'
                ELSE lower(lifecycle_state::text)
            END::lifecyclestate
            WHERE lifecycle_state::text <> lower(lifecycle_state::text)
               OR lifecycle_state::text = 'warm_lead'
            """
        )
    )


def downgrade() -> None:
    pass

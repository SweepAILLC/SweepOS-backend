"""Expand client lifecycle pipeline: nurturing, qualified, booked; migrate warm_lead."""

from alembic import op
import sqlalchemy as sa

revision = "046"
down_revision = "045"
branch_labels = None
depends_on = None

_NEW_LIFECYCLE_LABELS = (
    "nurturing",
    "qualified",
    "booked",
    "NURTURING",
    "QUALIFIED",
    "BOOKED",
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


def _pick_enum_label(conn, canonical: str) -> str:
    """Return the lifecyclestate label matching canonical case-insensitively."""
    row = conn.execute(
        sa.text(
            "SELECT e.enumlabel "
            "FROM pg_enum e "
            "JOIN pg_type t ON e.enumtypid = t.oid "
            "WHERE t.typname = 'lifecyclestate' "
            "AND lower(e.enumlabel) = lower(:name) "
            "ORDER BY CASE WHEN e.enumlabel = lower(:name) THEN 0 ELSE 1 END "
            "LIMIT 1"
        ),
        {"name": canonical},
    ).fetchone()
    if row is None:
        raise RuntimeError(f"lifecyclestate enum missing label for {canonical!r}")
    return str(row[0])


def upgrade() -> None:
    conn = op.get_bind()
    insp = sa.inspect(conn)
    if "clients" not in insp.get_table_names():
        return

    # PG requires new enum labels to be committed before use in the same session.
    with op.get_context().autocommit_block():
        for label in _NEW_LIFECYCLE_LABELS:
            _add_enum_value(label)

    booked_label = _pick_enum_label(conn, "booked")

    conn.execute(
        sa.text(
            "UPDATE clients "
            "SET lifecycle_state = CAST(:booked AS lifecyclestate) "
            "WHERE lifecycle_state::text ILIKE 'warm_lead'"
        ),
        {"booked": booked_label},
    )


def downgrade() -> None:
    conn = op.get_bind()
    cold_label = _pick_enum_label(conn, "cold_lead")

    nurturing = _pick_enum_label(conn, "nurturing")
    qualified = _pick_enum_label(conn, "qualified")
    booked = _pick_enum_label(conn, "booked")

    conn.execute(
        sa.text(
            "UPDATE clients "
            "SET lifecycle_state = CAST(:cold AS lifecyclestate) "
            "WHERE lifecycle_state::text IN (:n, :q, :b)"
        ),
        {"cold": cold_label, "n": nurturing, "q": qualified, "b": booked},
    )

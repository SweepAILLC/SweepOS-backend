"""Allow same google_id across multi-org user rows (same person, multiple orgs).

Revision ID: 051
Revises: 050
"""

from alembic import op
import sqlalchemy as sa

revision = "051"
down_revision = "050"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    insp = sa.inspect(conn)
    if "users" not in insp.get_table_names():
        return
    # Drop unique constraint/index on google_id — one Google identity may map to
    # multiple users rows (one per org). App logic still rejects linking a Google
    # account that already belongs to a different email.
    for ix in insp.get_indexes("users"):
        if ix.get("name") == "ix_users_google_id":
            op.drop_index("ix_users_google_id", table_name="users")
            break
    # Also drop unique constraints that might have been created as constraints
    for uc in insp.get_unique_constraints("users"):
        cols = uc.get("column_names") or []
        if cols == ["google_id"] or set(cols) == {"google_id"}:
            op.drop_constraint(uc["name"], "users", type_="unique")
    op.execute("DROP INDEX IF EXISTS ix_users_google_id")
    op.create_index("ix_users_google_id", "users", ["google_id"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_users_google_id", table_name="users")
    op.create_index("ix_users_google_id", "users", ["google_id"], unique=True)

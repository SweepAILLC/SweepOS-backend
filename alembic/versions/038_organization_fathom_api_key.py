"""Per-organization Fathom API key."""

from alembic import op
import sqlalchemy as sa

revision = "038"
down_revision = "037"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    insp = sa.inspect(conn)
    cols = {c["name"] for c in insp.get_columns("organizations")} if "organizations" in insp.get_table_names() else set()
    if "fathom_api_key" not in cols:
        op.add_column("organizations", sa.Column("fathom_api_key", sa.Text(), nullable=True))

    # One-time: copy legacy per-user keys into the org row when org has no key yet.
    op.execute(
        """
        UPDATE organizations o
        SET fathom_api_key = s.key
        FROM (
            SELECT DISTINCT ON (org_id) org_id, fathom_api_key AS key
            FROM users
            WHERE fathom_api_key IS NOT NULL AND TRIM(fathom_api_key) <> ''
            ORDER BY org_id, id
        ) s
        WHERE o.id = s.org_id
          AND (o.fathom_api_key IS NULL OR TRIM(o.fathom_api_key) = '')
        """
    )


def downgrade() -> None:
    conn = op.get_bind()
    insp = sa.inspect(conn)
    if "organizations" in insp.get_table_names():
        cols = {c["name"] for c in insp.get_columns("organizations")}
        if "fathom_api_key" in cols:
            op.drop_column("organizations", "fathom_api_key")

"""Add per-org Intelligence bank (ai_profile) on user_organizations."""

from alembic import op
import sqlalchemy as sa

revision = "048"
down_revision = "047"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    insp = sa.inspect(conn)
    tables = insp.get_table_names()
    if "user_organizations" not in tables:
        return
    cols = {c["name"] for c in insp.get_columns("user_organizations")}
    if "ai_profile" not in cols:
        op.add_column(
            "user_organizations",
            sa.Column("ai_profile", sa.JSON(), nullable=True),
        )


def downgrade() -> None:
    pass

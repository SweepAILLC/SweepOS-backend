"""Add call_title_override to call_library_reports for user-renamed titles."""

from alembic import op
import sqlalchemy as sa

revision = "039"
down_revision = "038"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    insp = sa.inspect(conn)
    tables = insp.get_table_names()
    if "call_library_reports" not in tables:
        return
    clr_cols = {c["name"] for c in insp.get_columns("call_library_reports")}
    if "call_title_override" not in clr_cols:
        op.add_column(
            "call_library_reports",
            sa.Column("call_title_override", sa.Text(), nullable=True),
        )


def downgrade() -> None:
    pass

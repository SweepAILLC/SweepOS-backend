"""Add analysis_kind to call_library_reports (sales vs glance).

Revision ID: 056
Revises: 055

Full sales-call audits vs lightweight non-sales glance summaries.
"""

from alembic import op
import sqlalchemy as sa

revision = "056"
down_revision = "055"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    insp = sa.inspect(conn)
    tables = insp.get_table_names()
    if "call_library_reports" not in tables:
        return
    cols = {c["name"] for c in insp.get_columns("call_library_reports")}
    if "analysis_kind" not in cols:
        op.add_column(
            "call_library_reports",
            sa.Column("analysis_kind", sa.String(length=16), nullable=True),
        )


def downgrade() -> None:
    conn = op.get_bind()
    insp = sa.inspect(conn)
    tables = insp.get_table_names()
    if "call_library_reports" not in tables:
        return
    cols = {c["name"] for c in insp.get_columns("call_library_reports")}
    if "analysis_kind" in cols:
        op.drop_column("call_library_reports", "analysis_kind")

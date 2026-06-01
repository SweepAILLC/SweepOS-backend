"""Add deal outcome columns to call_library_reports.

Surfaces a separate "deal closed for $X" metric next to the existing
call_score on each Call Library report, when the LLM is confident the
sale was closed on the call.
"""

from alembic import op
import sqlalchemy as sa

revision = "043"
down_revision = "042"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    insp = sa.inspect(conn)
    tables = insp.get_table_names()
    if "call_library_reports" not in tables:
        return
    cols = {c["name"] for c in insp.get_columns("call_library_reports")}

    if "deal_closed" not in cols:
        op.add_column(
            "call_library_reports",
            sa.Column(
                "deal_closed",
                sa.Boolean(),
                nullable=False,
                server_default=sa.text("false"),
            ),
        )
    if "deal_value_cents" not in cols:
        op.add_column(
            "call_library_reports",
            sa.Column("deal_value_cents", sa.BigInteger(), nullable=True),
        )
    if "deal_currency" not in cols:
        op.add_column(
            "call_library_reports",
            sa.Column("deal_currency", sa.String(length=8), nullable=True),
        )
    if "deal_billing" not in cols:
        op.add_column(
            "call_library_reports",
            sa.Column("deal_billing", sa.String(length=32), nullable=True),
        )


def downgrade() -> None:
    pass

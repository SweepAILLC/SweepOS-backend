"""Ensure Fathom / Call Library columns exist (idempotent repair if 036 partially applied)."""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "037"
down_revision = "036"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    insp = sa.inspect(conn)
    tables = insp.get_table_names()

    if "fathom_call_records" in tables:
        fathom_cols = {c["name"] for c in insp.get_columns("fathom_call_records")}
        if "recording_url" not in fathom_cols:
            op.add_column(
                "fathom_call_records",
                sa.Column("recording_url", sa.Text(), nullable=True),
            )
        if "attendees_json" not in fathom_cols:
            op.add_column(
                "fathom_call_records",
                sa.Column("attendees_json", postgresql.JSON(astext_type=sa.Text()), nullable=True),
            )
        if "related_client_ids" not in fathom_cols:
            op.add_column(
                "fathom_call_records",
                sa.Column("related_client_ids", postgresql.JSON(astext_type=sa.Text()), nullable=True),
            )

    if "call_library_reports" in tables:
        clr_cols = {c["name"] for c in insp.get_columns("call_library_reports")}
        if "call_score" not in clr_cols:
            op.add_column(
                "call_library_reports",
                sa.Column("call_score", sa.Float(), nullable=True),
            )
        if "recording_url" not in clr_cols:
            op.add_column(
                "call_library_reports",
                sa.Column("recording_url", sa.Text(), nullable=True),
            )
        if "attendees_json" not in clr_cols:
            op.add_column(
                "call_library_reports",
                sa.Column("attendees_json", postgresql.JSON(astext_type=sa.Text()), nullable=True),
            )


def downgrade() -> None:
    pass

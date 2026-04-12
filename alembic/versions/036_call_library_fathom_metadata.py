"""Call library + Fathom: recording URL, attendees, related clients, call score."""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "036"
down_revision = "035"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    insp = sa.inspect(conn)
    tables = insp.get_table_names()

    # Idempotent: partial runs or manual DDL must not fail with "column already exists".
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

    if "call_library_reports" not in tables:
        op.create_table(
            "call_library_reports",
            sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
            sa.Column("org_id", postgresql.UUID(as_uuid=True), nullable=False),
            sa.Column("fathom_call_record_id", postgresql.UUID(as_uuid=True), nullable=False),
            sa.Column("status", sa.String(length=32), nullable=False, server_default="pending"),
            sa.Column("report_json", postgresql.JSON(astext_type=sa.Text()), nullable=True),
            sa.Column("failure_reason", sa.Text(), nullable=True),
            sa.Column("call_title", sa.Text(), nullable=True),
            sa.Column("call_score", sa.Float(), nullable=True),
            sa.Column("recording_url", sa.Text(), nullable=True),
            sa.Column("attendees_json", postgresql.JSON(astext_type=sa.Text()), nullable=True),
            sa.Column("computed_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column(
                "created_at",
                sa.DateTime(timezone=True),
                nullable=False,
                server_default=sa.text("now()"),
            ),
            sa.Column(
                "updated_at",
                sa.DateTime(timezone=True),
                nullable=False,
                server_default=sa.text("now()"),
            ),
            sa.ForeignKeyConstraint(["org_id"], ["organizations.id"]),
            sa.ForeignKeyConstraint(
                ["fathom_call_record_id"],
                ["fathom_call_records.id"],
                ondelete="CASCADE",
            ),
            sa.PrimaryKeyConstraint("id"),
            sa.UniqueConstraint("fathom_call_record_id"),
        )
        op.create_index(
            "ix_call_library_reports_org_created",
            "call_library_reports",
            ["org_id", "created_at"],
        )
    else:
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
    conn = op.get_bind()
    insp = sa.inspect(conn)
    if "call_library_reports" in insp.get_table_names():
        cols = {c["name"] for c in insp.get_columns("call_library_reports")}
        if "attendees_json" in cols:
            op.drop_column("call_library_reports", "attendees_json")
        if "recording_url" in cols:
            op.drop_column("call_library_reports", "recording_url")
        if "call_score" in cols:
            op.drop_column("call_library_reports", "call_score")

    if "fathom_call_records" in insp.get_table_names():
        fc = {c["name"] for c in insp.get_columns("fathom_call_records")}
        if "related_client_ids" in fc:
            op.drop_column("fathom_call_records", "related_client_ids")
        if "attendees_json" in fc:
            op.drop_column("fathom_call_records", "attendees_json")
        if "recording_url" in fc:
            op.drop_column("fathom_call_records", "recording_url")

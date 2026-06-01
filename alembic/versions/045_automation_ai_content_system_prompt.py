"""Add ai_content_system_prompt to automation_rules for AI content mode."""

from alembic import op
import sqlalchemy as sa

revision = "045"
down_revision = "044"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    insp = sa.inspect(conn)
    tables = insp.get_table_names()
    if "automation_rules" not in tables:
        return
    cols = {c["name"] for c in insp.get_columns("automation_rules")}
    if "ai_content_system_prompt" not in cols:
        op.add_column(
            "automation_rules",
            sa.Column("ai_content_system_prompt", sa.Text(), nullable=True),
        )


def downgrade() -> None:
    pass

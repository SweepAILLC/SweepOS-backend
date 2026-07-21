"""Allow multiple named shared pads per org (tabs, max 10).

Revision ID: 054
Revises: 053
"""

from alembic import op
import sqlalchemy as sa

revision = "054"
down_revision = "053"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    insp = sa.inspect(conn)
    if "portal_shared_pads" not in insp.get_table_names():
        return

    cols = {c["name"] for c in insp.get_columns("portal_shared_pads")}
    if "title" not in cols:
        op.add_column(
            "portal_shared_pads",
            sa.Column("title", sa.String(length=120), nullable=False, server_default="Shared space"),
        )
    if "sort_order" not in cols:
        op.add_column(
            "portal_shared_pads",
            sa.Column("sort_order", sa.Integer(), nullable=False, server_default="0"),
        )

    # Drop single-pad-per-org uniqueness so orgs can have multiple tabs.
    uqs = {uq["name"] for uq in insp.get_unique_constraints("portal_shared_pads")}
    if "uq_portal_shared_pads_org_id" in uqs:
        op.drop_constraint("uq_portal_shared_pads_org_id", "portal_shared_pads", type_="unique")

    # Seed titles for existing rows that still have the server default.
    op.execute(
        sa.text(
            """
            UPDATE portal_shared_pads
            SET title = 'Onboarding'
            WHERE title = 'Shared space' OR title IS NULL OR BTRIM(title) = ''
            """
        )
    )


def downgrade() -> None:
    conn = op.get_bind()
    insp = sa.inspect(conn)
    if "portal_shared_pads" not in insp.get_table_names():
        return

    # Keep only the earliest pad per org so unique(org_id) can be restored.
    op.execute(
        sa.text(
            """
            DELETE FROM portal_shared_pads p
            WHERE p.id NOT IN (
                SELECT DISTINCT ON (org_id) id
                FROM portal_shared_pads
                ORDER BY org_id, sort_order ASC, created_at ASC
            )
            """
        )
    )

    cols = {c["name"] for c in insp.get_columns("portal_shared_pads")}
    if "sort_order" in cols:
        op.drop_column("portal_shared_pads", "sort_order")
    if "title" in cols:
        op.drop_column("portal_shared_pads", "title")

    uqs = {uq["name"] for uq in insp.get_unique_constraints("portal_shared_pads")}
    if "uq_portal_shared_pads_org_id" not in uqs:
        op.create_unique_constraint("uq_portal_shared_pads_org_id", "portal_shared_pads", ["org_id"])

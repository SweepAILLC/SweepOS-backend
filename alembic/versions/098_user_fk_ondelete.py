"""User FKs: add ON DELETE rules so removing a team member never hits a FK violation.

Seven nullable "who did this" columns referenced users.id with no ON DELETE rule,
so deleting a member who had touched any of those rows raised a 500. Attribution
columns become SET NULL; MCP OAuth grants CASCADE (a removed member loses
connector access).

Constraints are looked up by column rather than name, so environments whose
default FK names differ still get exactly one FK per column.

Revision ID: 098
Revises: 097
"""
from __future__ import annotations

from alembic import op

revision = "098"
down_revision = "097"
branch_labels = None
depends_on = None


# (table, column, ON DELETE action)
_FKS = (
    ("portal_todos", "created_by", "SET NULL"),
    ("portal_shared_pads", "updated_by", "SET NULL"),
    ("portal_shared_pad_defaults", "updated_by", "SET NULL"),
    ("funnel_simulator_scenarios", "created_by", "SET NULL"),
    ("funnel_ad_spend", "entered_by_user_id", "SET NULL"),
    ("owner_org_notices", "created_by", "SET NULL"),
    ("mcp_oauth_grants", "user_id", "CASCADE"),
)


def _drop_user_fks(table: str, column: str) -> None:
    op.execute(
        f"""
        DO $$
        DECLARE r record;
        BEGIN
            FOR r IN
                SELECT c.conname
                FROM pg_constraint c
                JOIN pg_attribute a
                  ON a.attrelid = c.conrelid AND a.attnum = ANY (c.conkey)
                WHERE c.contype = 'f'
                  AND c.conrelid = '{table}'::regclass
                  AND c.confrelid = 'users'::regclass
                  AND a.attname = '{column}'
            LOOP
                EXECUTE format('ALTER TABLE {table} DROP CONSTRAINT %I', r.conname);
            END LOOP;
        END $$;
        """
    )


def _set_fks(with_action: bool) -> None:
    for table, column, action in _FKS:
        _drop_user_fks(table, column)
        on_delete = f" ON DELETE {action}" if with_action else ""
        op.execute(
            f"ALTER TABLE {table} ADD CONSTRAINT {table}_{column}_fkey "
            f"FOREIGN KEY ({column}) REFERENCES users (id){on_delete}"
        )


def upgrade() -> None:
    _set_fks(with_action=True)


def downgrade() -> None:
    _set_fks(with_action=False)

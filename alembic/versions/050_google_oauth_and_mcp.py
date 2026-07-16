"""Google OAuth columns on users + MCP OAuth client/grant tables.

Revision ID: 050
Revises: 049
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "050"
down_revision = "049"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    insp = sa.inspect(conn)
    tables = insp.get_table_names()

    if "users" in tables:
        cols = {c["name"] for c in insp.get_columns("users")}
        if "google_id" not in cols:
            op.add_column("users", sa.Column("google_id", sa.String(), nullable=True))
        if "google_email" not in cols:
            op.add_column("users", sa.Column("google_email", sa.String(), nullable=True))
        # Allow Google-only accounts (no password)
        op.execute("ALTER TABLE users ALTER COLUMN hashed_password DROP NOT NULL")
        # Non-unique: same Google account can be linked on multiple org user rows
        existing_indexes = {ix["name"] for ix in insp.get_indexes("users")}
        if "ix_users_google_id" in existing_indexes:
            op.drop_index("ix_users_google_id", table_name="users")
        op.create_index("ix_users_google_id", "users", ["google_id"], unique=False)

    if "mcp_oauth_clients" not in tables:
        op.create_table(
            "mcp_oauth_clients",
            sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
            sa.Column("client_id", sa.String(), nullable=False, unique=True),
            sa.Column("client_secret_encrypted", sa.Text(), nullable=True),
            sa.Column("client_name", sa.String(), nullable=True),
            sa.Column("redirect_uris", postgresql.JSONB(), nullable=False, server_default="[]"),
            sa.Column("grant_types", postgresql.JSONB(), nullable=False, server_default='["authorization_code","refresh_token"]'),
            sa.Column("token_endpoint_auth_method", sa.String(), nullable=False, server_default="none"),
            sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.text("NOW()")),
        )
        op.create_index("ix_mcp_oauth_clients_client_id", "mcp_oauth_clients", ["client_id"], unique=True)

    if "mcp_oauth_grants" not in tables:
        op.create_table(
            "mcp_oauth_grants",
            sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
            sa.Column("client_id", sa.String(), nullable=False, index=True),
            sa.Column("user_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("users.id"), nullable=True),
            sa.Column("org_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("organizations.id"), nullable=True),
            sa.Column("redirect_uri", sa.Text(), nullable=False),
            sa.Column("scope", sa.String(), nullable=True),
            sa.Column("state", sa.String(), nullable=True),
            sa.Column("code_challenge", sa.String(), nullable=True),
            sa.Column("code_challenge_method", sa.String(), nullable=True),
            sa.Column("authorization_code", sa.String(), nullable=True, unique=True),
            sa.Column("code_expires_at", sa.DateTime(), nullable=True),
            sa.Column("code_used_at", sa.DateTime(), nullable=True),
            sa.Column("refresh_token_hash", sa.String(), nullable=True, unique=True),
            sa.Column("refresh_expires_at", sa.DateTime(), nullable=True),
            sa.Column("revoked_at", sa.DateTime(), nullable=True),
            sa.Column("pending_nonce", sa.String(), nullable=True, index=True),
            sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.text("NOW()")),
        )


def downgrade() -> None:
    op.drop_table("mcp_oauth_grants")
    op.drop_table("mcp_oauth_clients")
    op.drop_index("ix_users_google_id", table_name="users")
    op.drop_column("users", "google_email")
    op.drop_column("users", "google_id")

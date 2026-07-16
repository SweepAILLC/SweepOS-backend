"""MCP OAuth Dynamic Client Registration + authorization grants."""
from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import Column, DateTime, ForeignKey, String, Text
from sqlalchemy.dialects.postgresql import JSONB, UUID

from app.db.session import Base


class McpOAuthClient(Base):
    __tablename__ = "mcp_oauth_clients"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    client_id = Column(String, nullable=False, unique=True, index=True)
    client_secret_encrypted = Column(Text, nullable=True)
    client_name = Column(String, nullable=True)
    redirect_uris = Column(JSONB, nullable=False, default=list)
    grant_types = Column(JSONB, nullable=False, default=lambda: ["authorization_code", "refresh_token"])
    token_endpoint_auth_method = Column(String, nullable=False, default="none")
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)


class McpOAuthGrant(Base):
    """Pending authorize request, one-time auth code, and/or refresh token."""

    __tablename__ = "mcp_oauth_grants"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    client_id = Column(String, nullable=False, index=True)
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=True)
    org_id = Column(UUID(as_uuid=True), ForeignKey("organizations.id"), nullable=True)
    redirect_uri = Column(Text, nullable=False)
    scope = Column(String, nullable=True)
    state = Column(String, nullable=True)
    code_challenge = Column(String, nullable=True)
    code_challenge_method = Column(String, nullable=True)
    authorization_code = Column(String, nullable=True, unique=True)
    code_expires_at = Column(DateTime, nullable=True)
    code_used_at = Column(DateTime, nullable=True)
    refresh_token_hash = Column(String, nullable=True, unique=True)
    refresh_expires_at = Column(DateTime, nullable=True)
    revoked_at = Column(DateTime, nullable=True)
    # Short-lived nonce bridging Google callback back to this grant
    pending_nonce = Column(String, nullable=True, index=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

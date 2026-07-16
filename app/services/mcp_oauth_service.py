"""
MCP OAuth authorization-server helpers (DCR grants, PKCE, token minting).
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import logging
import secrets
import uuid
from datetime import datetime, timedelta
from typing import Optional
from urllib.parse import urlencode

from fastapi import HTTPException
from sqlalchemy.orm import Session

from app.core.config import settings
from app.core.security import create_access_token
from app.models.mcp_oauth import McpOAuthClient, McpOAuthGrant
from app.models.user import User, role_to_api
from app.models.user_organization import UserOrganization
from sqlalchemy import func

_logger = logging.getLogger(__name__)


def mcp_issuer() -> str:
    base = (settings.MCP_ISSUER_URL or settings.BACKEND_PUBLIC_URL or "http://localhost:8000").rstrip("/")
    return base


def mcp_resource() -> str:
    if settings.MCP_RESOURCE_URL:
        return settings.MCP_RESOURCE_URL.rstrip("/")
    return f"{mcp_issuer()}/mcp"


def mcp_scopes() -> list[str]:
    return [s.strip() for s in (settings.MCP_SCOPES or "clients:read").split() if s.strip()]


def hash_refresh_token(raw: str) -> str:
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def verify_pkce(code_verifier: str, code_challenge: str, method: str = "S256") -> bool:
    if not code_verifier or not code_challenge:
        return False
    method = (method or "S256").upper()
    if method == "S256":
        digest = hashlib.sha256(code_verifier.encode("ascii")).digest()
        computed = base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")
        return hmac.compare_digest(computed, code_challenge)
    if method == "PLAIN":
        return hmac.compare_digest(code_verifier, code_challenge)
    return False


def get_or_reject_client(db: Session, client_id: str) -> McpOAuthClient:
    client = db.query(McpOAuthClient).filter(McpOAuthClient.client_id == client_id).first()
    if not client:
        raise HTTPException(status_code=400, detail="Unknown client_id")
    return client


def redirect_uri_allowed(client: McpOAuthClient, redirect_uri: str) -> bool:
    uris = client.redirect_uris or []
    if redirect_uri in uris:
        return True
    # Claude Code loopback: port-agnostic localhost / 127.0.0.1
    for allowed in uris:
        if _loopback_match(allowed, redirect_uri):
            return True
    return False


def _loopback_match(allowed: str, actual: str) -> bool:
    from urllib.parse import urlparse

    a = urlparse(allowed)
    b = urlparse(actual)
    if a.scheme != b.scheme or a.path != b.path:
        return False
    hosts = {"localhost", "127.0.0.1"}
    return (a.hostname or "") in hosts and (b.hostname or "") in hosts


def register_client(
    db: Session,
    *,
    redirect_uris: list[str],
    client_name: Optional[str] = None,
    token_endpoint_auth_method: str = "none",
    grant_types: Optional[list[str]] = None,
) -> McpOAuthClient:
    if not redirect_uris:
        raise HTTPException(status_code=400, detail="redirect_uris required")
    client_id = f"mcp_{secrets.token_urlsafe(24)}"
    row = McpOAuthClient(
        id=uuid.uuid4(),
        client_id=client_id,
        client_name=client_name,
        redirect_uris=list(redirect_uris),
        grant_types=grant_types or ["authorization_code", "refresh_token"],
        token_endpoint_auth_method=token_endpoint_auth_method or "none",
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return row


def start_authorize(
    db: Session,
    *,
    client_id: str,
    redirect_uri: str,
    state: Optional[str],
    scope: Optional[str],
    code_challenge: str,
    code_challenge_method: str = "S256",
) -> McpOAuthGrant:
    client = get_or_reject_client(db, client_id)
    if not redirect_uri_allowed(client, redirect_uri):
        raise HTTPException(status_code=400, detail="redirect_uri not registered for client")
    if not code_challenge:
        raise HTTPException(status_code=400, detail="code_challenge required (PKCE S256)")
    method = (code_challenge_method or "S256").upper()
    if method != "S256":
        raise HTTPException(status_code=400, detail="Only S256 PKCE is supported")

    nonce = secrets.token_urlsafe(32)
    grant = McpOAuthGrant(
        id=uuid.uuid4(),
        client_id=client_id,
        redirect_uri=redirect_uri,
        scope=scope or " ".join(mcp_scopes()),
        state=state,
        code_challenge=code_challenge,
        code_challenge_method=method,
        pending_nonce=nonce,
    )
    db.add(grant)
    db.commit()
    db.refresh(grant)
    return grant


def google_start_url_for_mcp(mcp_nonce: str) -> str:
    """Relative path Claude authorize redirects into for Google identity."""
    return f"/auth/google/start?mode=mcp&mcp_nonce={mcp_nonce}&redirect=1"


def complete_mcp_grant_after_google(
    db: Session,
    *,
    mcp_nonce: Optional[str],
    google_id: str,
    email: str,
) -> str:
    """
    After Google identity succeeds, bind user+org to the pending grant,
    issue an authorization code, and return the Claude redirect URL.
    """
    if not mcp_nonce:
        raise HTTPException(status_code=400, detail="Missing MCP nonce")
    grant = (
        db.query(McpOAuthGrant)
        .filter(
            McpOAuthGrant.pending_nonce == mcp_nonce,
            McpOAuthGrant.authorization_code.is_(None),
            McpOAuthGrant.revoked_at.is_(None),
        )
        .first()
    )
    if not grant:
        raise HTTPException(status_code=400, detail="MCP authorization request expired or invalid")

    user = db.query(User).filter(User.google_id == google_id).first()
    if not user:
        matches = (
            db.query(User)
            .filter(func.lower(User.email) == email.lower().strip())
            .order_by(User.created_at.asc())
            .all()
        )
        if not matches:
            raise HTTPException(
                status_code=400,
                detail="No SweepOS account for this Google email. Sign up via invite first.",
            )
        user = matches[0]
        for u in matches:
            if not u.google_id:
                u.google_id = google_id
                u.google_email = email
        db.commit()

    # Prefer user's primary org
    org_id = user.org_id
    uo = (
        db.query(UserOrganization)
        .filter(UserOrganization.user_id == user.id, UserOrganization.is_primary.is_(True))
        .first()
    )
    if uo:
        org_id = uo.org_id

    auth_code = secrets.token_urlsafe(32)
    grant.user_id = user.id
    grant.org_id = org_id
    grant.authorization_code = auth_code
    grant.code_expires_at = datetime.utcnow() + timedelta(minutes=10)
    grant.pending_nonce = None
    db.commit()

    params = {"code": auth_code}
    if grant.state:
        params["state"] = grant.state
    return f"{grant.redirect_uri}?{urlencode(params)}"


def exchange_authorization_code(
    db: Session,
    *,
    code: str,
    client_id: str,
    redirect_uri: str,
    code_verifier: str,
) -> dict:
    grant = (
        db.query(McpOAuthGrant)
        .filter(
            McpOAuthGrant.authorization_code == code,
            McpOAuthGrant.client_id == client_id,
            McpOAuthGrant.revoked_at.is_(None),
        )
        .first()
    )
    if not grant:
        raise HTTPException(status_code=400, detail={"error": "invalid_grant"})
    if grant.code_used_at is not None:
        raise HTTPException(status_code=400, detail={"error": "invalid_grant"})
    if not grant.code_expires_at or grant.code_expires_at < datetime.utcnow():
        raise HTTPException(status_code=400, detail={"error": "invalid_grant"})
    if grant.redirect_uri != redirect_uri:
        raise HTTPException(status_code=400, detail={"error": "invalid_grant"})
    if not verify_pkce(code_verifier, grant.code_challenge or "", grant.code_challenge_method or "S256"):
        raise HTTPException(status_code=400, detail={"error": "invalid_grant"})
    if not grant.user_id or not grant.org_id:
        raise HTTPException(status_code=400, detail={"error": "invalid_grant"})

    user = db.query(User).filter(User.id == grant.user_id).first()
    if not user:
        raise HTTPException(status_code=400, detail={"error": "invalid_grant"})

    grant.code_used_at = datetime.utcnow()
    access, refresh, expires_in = _mint_tokens(db, grant, user)
    db.commit()
    return {
        "access_token": access,
        "token_type": "Bearer",
        "expires_in": expires_in,
        "refresh_token": refresh,
        "scope": grant.scope or " ".join(mcp_scopes()),
    }


def refresh_access_token(db: Session, *, refresh_token: str, client_id: str) -> dict:
    token_hash = hash_refresh_token(refresh_token)
    grant = (
        db.query(McpOAuthGrant)
        .filter(
            McpOAuthGrant.refresh_token_hash == token_hash,
            McpOAuthGrant.client_id == client_id,
            McpOAuthGrant.revoked_at.is_(None),
        )
        .first()
    )
    if not grant:
        raise HTTPException(status_code=400, detail={"error": "invalid_grant"})
    if not grant.refresh_expires_at or grant.refresh_expires_at < datetime.utcnow():
        raise HTTPException(status_code=400, detail={"error": "invalid_grant"})
    if not grant.user_id or not grant.org_id:
        raise HTTPException(status_code=400, detail={"error": "invalid_grant"})

    user = db.query(User).filter(User.id == grant.user_id).first()
    if not user:
        raise HTTPException(status_code=400, detail={"error": "invalid_grant"})

    # Rotate refresh token
    access, refresh, expires_in = _mint_tokens(db, grant, user)
    db.commit()
    return {
        "access_token": access,
        "token_type": "Bearer",
        "expires_in": expires_in,
        "refresh_token": refresh,
        "scope": grant.scope or " ".join(mcp_scopes()),
    }


def _mint_tokens(db: Session, grant: McpOAuthGrant, user: User) -> tuple[str, str, int]:
    expires_minutes = settings.MCP_ACCESS_TOKEN_EXPIRE_MINUTES or 60
    access = create_access_token(
        data={
            "sub": user.email,
            "user_id": str(user.id),
            "org_id": str(grant.org_id),
            "role": role_to_api(user.role) if user.role else "member",
            "scope": grant.scope or " ".join(mcp_scopes()),
            "aud": mcp_resource(),
            "token_use": "mcp",
        },
        expires_delta=timedelta(minutes=expires_minutes),
    )
    raw_refresh = secrets.token_urlsafe(48)
    grant.refresh_token_hash = hash_refresh_token(raw_refresh)
    grant.refresh_expires_at = datetime.utcnow() + timedelta(days=settings.MCP_REFRESH_TOKEN_EXPIRE_DAYS or 30)
    return access, raw_refresh, expires_minutes * 60


def verify_mcp_access_token(token: str) -> Optional[dict]:
    from app.core.security import decode_access_token

    payload = decode_access_token(token)
    if not payload:
        return None
    if payload.get("token_use") != "mcp":
        # Also accept standard app JWTs for local testing of tools
        if payload.get("aud") and payload.get("aud") != mcp_resource():
            return None
    aud = payload.get("aud")
    if aud and aud != mcp_resource():
        return None
    if not payload.get("user_id") or not payload.get("org_id"):
        return None
    return payload

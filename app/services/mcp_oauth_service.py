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
    return canonicalize_resource(base)


def mcp_resource() -> str:
    if settings.MCP_RESOURCE_URL:
        return canonicalize_resource(settings.MCP_RESOURCE_URL)
    return canonicalize_resource(f"{mcp_issuer()}/mcp")


def canonicalize_resource(url: str) -> str:
    """
    RFC 8707 / MCP canonical resource form:
    lowercase scheme+host, no trailing slash, no fragment, omit default ports.
    """
    from urllib.parse import urlsplit, urlunsplit

    raw = (url or "").strip()
    if not raw:
        return ""
    parts = urlsplit(raw)
    scheme = (parts.scheme or "https").lower()
    host = (parts.hostname or "").lower()
    if not host:
        return raw.rstrip("/")
    port = parts.port
    if port and not (
        (scheme == "https" and port == 443) or (scheme == "http" and port == 80)
    ):
        netloc = f"{host}:{port}"
    else:
        netloc = host
    path = (parts.path or "").rstrip("/")
    return urlunsplit((scheme, netloc, path, "", ""))


def resources_match(a: Optional[str], b: Optional[str]) -> bool:
    if not a or not b:
        return False
    return canonicalize_resource(a) == canonicalize_resource(b)


def mcp_scopes() -> list[str]:
    return [s.strip() for s in (settings.MCP_SCOPES or "clients:read").split() if s.strip()]


def assert_resource_allowed(resource: Optional[str]) -> str:
    """Validate Claude's RFC 8707 resource param (when present) against our MCP URL."""
    expected = mcp_resource()
    if not resource:
        return expected
    if not resources_match(resource, expected):
        _logger.warning(
            "mcp_oauth resource mismatch got=%r expected=%r",
            resource,
            expected,
        )
        raise HTTPException(
            status_code=400,
            detail={
                "error": "invalid_target",
                "error_description": (
                    f"resource must be {expected} (got {canonicalize_resource(resource)}). "
                    "Paste that exact URL into Claude, and set MCP_RESOURCE_URL to match."
                ),
            },
        )
    return expected


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
    resource: Optional[str] = None,
) -> McpOAuthGrant:
    client = get_or_reject_client(db, client_id)
    if not redirect_uri_allowed(client, redirect_uri):
        raise HTTPException(status_code=400, detail="redirect_uri not registered for client")
    if not code_challenge:
        raise HTTPException(status_code=400, detail="code_challenge required (PKCE S256)")
    method = (code_challenge_method or "S256").upper()
    if method != "S256":
        raise HTTPException(status_code=400, detail="Only S256 PKCE is supported")
    assert_resource_allowed(resource)

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
    """Absolute URL Claude authorize redirects into for Google identity."""
    return f"{mcp_issuer()}/auth/google/start?mode=mcp&mcp_nonce={mcp_nonce}&redirect=1"


def _pending_mcp_grant(db: Session, mcp_nonce: str) -> McpOAuthGrant:
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
    return grant


def resolve_org_choices_for_google(
    db: Session,
    *,
    google_id: str,
    email: str,
) -> list[dict]:
    """
    All orgs this Google identity can access.

    SweepOS multi-org is often multiple User rows sharing an email (one per org),
    plus optional UserOrganization memberships.
    """
    from app.models.organization import Organization

    email_norm = (email or "").lower().strip()
    by_gid = db.query(User).filter(User.google_id == google_id).all() if google_id else []
    by_email = (
        db.query(User)
        .filter(func.lower(User.email) == email_norm)
        .order_by(User.created_at.asc())
        .all()
        if email_norm
        else []
    )
    users = {u.id: u for u in (by_gid + by_email)}
    if not users:
        raise HTTPException(
            status_code=400,
            detail="No SweepOS account for this Google email. Sign up via invite first.",
        )

    for u in users.values():
        if google_id and not u.google_id:
            u.google_id = google_id
            u.google_email = email
    db.commit()

    choices: dict = {}
    for u in users.values():
        org = db.query(Organization).filter(Organization.id == u.org_id).first()
        if org:
            choices[str(org.id)] = {
                "org_id": str(org.id),
                "org_name": org.name,
                "user_id": str(u.id),
                "role": role_to_api(u.role) if u.role else "member",
                "email": u.email,
            }
        for uo in (
            db.query(UserOrganization)
            .filter(UserOrganization.user_id == u.id)
            .all()
        ):
            org = db.query(Organization).filter(Organization.id == uo.org_id).first()
            if not org:
                continue
            key = str(org.id)
            if key in choices:
                continue
            # Prefer a User row that already belongs to this org
            org_user = next((x for x in users.values() if x.org_id == org.id), u)
            choices[key] = {
                "org_id": key,
                "org_name": org.name,
                "user_id": str(org_user.id),
                "role": role_to_api(org_user.role) if org_user.role else "member",
                "email": org_user.email,
            }

    return sorted(choices.values(), key=lambda c: (c.get("org_name") or "").lower())


def bind_mcp_grant_to_org(
    db: Session,
    *,
    mcp_nonce: str,
    org_id: str,
    user_id: str,
) -> str:
    """Bind pending grant to a chosen org/user and return Claude redirect URL."""
    grant = _pending_mcp_grant(db, mcp_nonce)
    try:
        org_uuid = uuid.UUID(str(org_id))
        user_uuid = uuid.UUID(str(user_id))
    except ValueError as e:
        raise HTTPException(status_code=400, detail="Invalid org_id or user_id") from e

    user = db.query(User).filter(User.id == user_uuid).first()
    if not user:
        raise HTTPException(status_code=400, detail="User not found for organization")

    # Ensure the chosen user can access this org
    if user.org_id != org_uuid:
        membership = (
            db.query(UserOrganization)
            .filter(
                UserOrganization.user_id == user.id,
                UserOrganization.org_id == org_uuid,
            )
            .first()
        )
        if not membership:
            raise HTTPException(status_code=400, detail="User is not a member of that organization")

    auth_code = secrets.token_urlsafe(32)
    grant.user_id = user.id
    grant.org_id = org_uuid
    grant.authorization_code = auth_code
    grant.code_expires_at = datetime.utcnow() + timedelta(minutes=10)
    grant.pending_nonce = None
    db.commit()

    params = {"code": auth_code}
    if grant.state:
        params["state"] = grant.state
    return f"{grant.redirect_uri}?{urlencode(params)}"


def complete_mcp_grant_after_google(
    db: Session,
    *,
    mcp_nonce: Optional[str],
    google_id: str,
    email: str,
    org_id: Optional[str] = None,
) -> dict:
    """
    After Google identity succeeds, bind user+org to the pending grant.

    Returns either:
      {"status": "redirect", "redirect_url": "..."} when org is unambiguous / selected
      {"status": "select_org", "organizations": [...]} when the user must pick an org
    """
    grant = _pending_mcp_grant(db, mcp_nonce or "")
    choices = resolve_org_choices_for_google(db, google_id=google_id, email=email)
    if not choices:
        raise HTTPException(status_code=400, detail="No organizations for this account")

    selected = None
    if org_id:
        selected = next((c for c in choices if c["org_id"] == str(org_id)), None)
        if not selected:
            raise HTTPException(status_code=400, detail="Selected organization is not available for this account")
    elif len(choices) == 1:
        selected = choices[0]
    else:
        # Keep pending_nonce so the picker can finish the grant
        _ = grant
        return {"status": "select_org", "organizations": choices, "mcp_nonce": mcp_nonce}

    redirect_url = bind_mcp_grant_to_org(
        db,
        mcp_nonce=mcp_nonce or "",
        org_id=selected["org_id"],
        user_id=selected["user_id"],
    )
    return {
        "status": "redirect",
        "redirect_url": redirect_url,
        "org_id": selected["org_id"],
        "org_name": selected["org_name"],
    }

def exchange_authorization_code(
    db: Session,
    *,
    code: str,
    client_id: str,
    redirect_uri: str,
    code_verifier: str,
    resource: Optional[str] = None,
) -> dict:
    assert_resource_allowed(resource)
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
        "resource": mcp_resource(),
    }


def refresh_access_token(
    db: Session,
    *,
    refresh_token: str,
    client_id: str,
    resource: Optional[str] = None,
) -> dict:
    assert_resource_allowed(resource)
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
        "resource": mcp_resource(),
    }


def _mint_tokens(db: Session, grant: McpOAuthGrant, user: User) -> tuple[str, str, int]:
    from app.models.organization import Organization

    org_name = None
    if grant.org_id:
        org = db.query(Organization).filter(Organization.id == grant.org_id).first()
        org_name = org.name if org else None

    expires_minutes = settings.MCP_ACCESS_TOKEN_EXPIRE_MINUTES or 60
    access = create_access_token(
        data={
            "sub": user.email,
            "user_id": str(user.id),
            "org_id": str(grant.org_id),
            "org_name": org_name,
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
    expected = mcp_resource()
    aud = payload.get("aud")
    if payload.get("token_use") != "mcp":
        # Also accept standard app JWTs for local testing of tools
        if aud and not resources_match(str(aud), expected):
            return None
    elif aud and not resources_match(str(aud), expected):
        return None
    if not payload.get("user_id") or not payload.get("org_id"):
        return None
    return payload

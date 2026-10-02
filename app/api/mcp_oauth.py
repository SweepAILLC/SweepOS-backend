"""
OAuth 2.0 Authorization Server endpoints for the MCP connector (Claude, ChatGPT, any
spec-compliant MCP client).

Discovery:
  GET /.well-known/oauth-protected-resource
  GET /.well-known/oauth-protected-resource/{path}
  GET /.well-known/oauth-authorization-server
  GET /.well-known/oauth-authorization-server/{path}

DCR + authorize + token:
  POST /mcp/oauth/register
  GET|POST /mcp/oauth/authorize
  POST /mcp/oauth/token

Multi-org selection (after Google identity):
  GET  /mcp/oauth/org-choices
  POST /mcp/oauth/select-org
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, Form, HTTPException, Query, Request
from fastapi.responses import JSONResponse, RedirectResponse
from jose import JWTError, jwt
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.core.config import settings
from app.db.session import get_db
from app.services import mcp_oauth_service as svc

router = APIRouter()
_logger = logging.getLogger(__name__)


class DCRRequest(BaseModel):
    redirect_uris: List[str] = Field(..., min_length=1)
    client_name: Optional[str] = None
    token_endpoint_auth_method: Optional[str] = "none"
    grant_types: Optional[List[str]] = None
    response_types: Optional[List[str]] = None
    scope: Optional[str] = None


class SelectOrgRequest(BaseModel):
    select_token: str
    org_id: str


def _decode_mcp_select_token(select_token: str) -> Dict[str, Any]:
    try:
        data = jwt.decode(
            select_token,
            settings.SECRET_KEY,
            algorithms=["HS256"],
            options={"verify_aud": False},
        )
    except JWTError as e:
        raise HTTPException(status_code=400, detail=f"Invalid or expired select token: {e}") from e
    if data.get("purpose") != "mcp_org_select":
        raise HTTPException(status_code=400, detail="Invalid select token purpose")
    if not data.get("mcp_nonce") or not data.get("email"):
        raise HTTPException(status_code=400, detail="Incomplete select token")
    return data


def _as_metadata() -> dict:
    issuer = svc.mcp_issuer()
    return {
        "issuer": issuer,
        "authorization_endpoint": f"{issuer}/mcp/oauth/authorize",
        "token_endpoint": f"{issuer}/mcp/oauth/token",
        "registration_endpoint": f"{issuer}/mcp/oauth/register",
        "response_types_supported": ["code"],
        "grant_types_supported": ["authorization_code", "refresh_token"],
        "code_challenge_methods_supported": ["S256"],
        "token_endpoint_auth_methods_supported": list(svc.SUPPORTED_AUTH_METHODS),
        "scopes_supported": svc.mcp_scopes() + ["offline_access"],
        "revocation_endpoint_auth_methods_supported": ["none"],
        "resource_indicators_supported": True,
    }


@router.get("/.well-known/oauth-protected-resource")
@router.get("/.well-known/oauth-protected-resource/{path:path}")
def oauth_protected_resource(path: str = ""):
    resource = svc.mcp_resource()
    return {
        "resource": resource,
        "authorization_servers": [svc.mcp_issuer()],
        "scopes_supported": svc.mcp_scopes(),
        "bearer_methods_supported": ["header"],
    }


@router.get("/.well-known/oauth-authorization-server")
@router.get("/.well-known/oauth-authorization-server/{path:path}")
def oauth_authorization_server(path: str = ""):
    return _as_metadata()


@router.get("/.well-known/openid-configuration")
@router.get("/.well-known/openid-configuration/{path:path}")
def openid_configuration(path: str = ""):
    """OIDC discovery fallback used by some MCP clients when AS metadata 404s."""
    return _as_metadata()


@router.get("/mcp/oauth/org-choices")
def mcp_org_choices(select_token: str = Query(...), db: Session = Depends(get_db)):
    """List Sweep orgs available for the in-progress MCP Google OAuth."""
    data = _decode_mcp_select_token(select_token)
    # Ensure pending grant still exists
    grant = svc._pending_mcp_grant(db, data["mcp_nonce"])
    choices = svc.resolve_org_choices_for_google(
        db,
        google_id=data.get("google_id") or "",
        email=data["email"],
    )
    return {
        "email": data["email"],
        "client_name": svc.client_display_name(db, grant.client_id),
        "organizations": [
            {"id": c["org_id"], "name": c["org_name"], "role": c.get("role")} for c in choices
        ],
    }


@router.post("/mcp/oauth/select-org")
def mcp_select_org(body: SelectOrgRequest, db: Session = Depends(get_db)):
    """Finish Claude MCP OAuth by binding the chosen org; returns Claude redirect URL."""
    data = _decode_mcp_select_token(body.select_token)
    choices = svc.resolve_org_choices_for_google(
        db,
        google_id=data.get("google_id") or "",
        email=data["email"],
    )
    selected = next((c for c in choices if c["org_id"] == str(body.org_id)), None)
    if not selected:
        raise HTTPException(status_code=400, detail="Organization not available for this account")
    redirect_url = svc.bind_mcp_grant_to_org(
        db,
        mcp_nonce=data["mcp_nonce"],
        org_id=selected["org_id"],
        user_id=selected["user_id"],
    )
    _logger.info(
        "mcp_oauth_select_org email=%s org_id=%s org_name=%s",
        data["email"],
        selected["org_id"],
        selected["org_name"],
    )
    return {"redirect_url": redirect_url, "org_id": selected["org_id"], "org_name": selected["org_name"]}


@router.post("/mcp/oauth/register")
def dynamic_client_registration(body: DCRRequest, db: Session = Depends(get_db)):
    _logger.info(
        "mcp_oauth_register client_name=%s redirect_uris=%s",
        body.client_name,
        body.redirect_uris,
    )
    try:
        client, raw_secret = svc.register_client(
            db,
            redirect_uris=body.redirect_uris,
            client_name=body.client_name,
            token_endpoint_auth_method=body.token_endpoint_auth_method or "none",
            grant_types=body.grant_types,
        )
    except HTTPException as e:
        # RFC 7591 §3.2.2 error shape
        return JSONResponse(
            status_code=400,
            content={"error": "invalid_client_metadata", "error_description": str(e.detail)},
        )
    content = {
        "client_id": client.client_id,
        "client_id_issued_at": int(client.created_at.timestamp()) if client.created_at else None,
        "client_name": client.client_name,
        "redirect_uris": client.redirect_uris,
        "grant_types": client.grant_types,
        "token_endpoint_auth_method": client.token_endpoint_auth_method,
        "response_types": ["code"],
    }
    if raw_secret:
        content["client_secret"] = raw_secret
        content["client_secret_expires_at"] = 0  # never
    return JSONResponse(status_code=201, content=content)


def _run_authorize(
    *,
    response_type: str,
    client_id: str,
    redirect_uri: str,
    state: Optional[str],
    scope: Optional[str],
    code_challenge: str,
    code_challenge_method: str,
    resource: Optional[str],
    db: Session,
):
    if response_type != "code":
        raise HTTPException(status_code=400, detail="response_type must be code")
    grant = svc.start_authorize(
        db,
        client_id=client_id,
        redirect_uri=redirect_uri,
        state=state,
        scope=scope,
        code_challenge=code_challenge,
        code_challenge_method=code_challenge_method,
        resource=resource,
    )
    # Absolute URL + 302 so Claude's callback receives GET (not method-preserving 307)
    return RedirectResponse(url=svc.google_start_url_for_mcp(grant.pending_nonce or ""), status_code=302)


@router.get("/mcp/oauth/authorize")
def authorize_get(
    response_type: str = Query(...),
    client_id: str = Query(...),
    redirect_uri: str = Query(...),
    state: Optional[str] = Query(None),
    scope: Optional[str] = Query(None),
    code_challenge: str = Query(...),
    code_challenge_method: str = Query("S256"),
    resource: Optional[str] = Query(None),
    db: Session = Depends(get_db),
):
    return _run_authorize(
        response_type=response_type,
        client_id=client_id,
        redirect_uri=redirect_uri,
        state=state,
        scope=scope,
        code_challenge=code_challenge,
        code_challenge_method=code_challenge_method,
        resource=resource,
        db=db,
    )


@router.post("/mcp/oauth/authorize")
async def authorize_post(
    request: Request,
    db: Session = Depends(get_db),
    response_type: Optional[str] = Form(None),
    client_id: Optional[str] = Form(None),
    redirect_uri: Optional[str] = Form(None),
    state: Optional[str] = Form(None),
    scope: Optional[str] = Form(None),
    code_challenge: Optional[str] = Form(None),
    code_challenge_method: Optional[str] = Form("S256"),
    resource: Optional[str] = Form(None),
):
    """
    Claude.ai sometimes POSTs to authorize (form-urlencoded) after a consent step.
    Accept the same fields as GET so we do not return 405 Method Not Allowed.
    """
    # Fall back to query string if form empty (some clients POST with query params)
    q = request.query_params
    response_type = response_type or q.get("response_type")
    client_id = client_id or q.get("client_id")
    redirect_uri = redirect_uri or q.get("redirect_uri")
    state = state if state is not None else q.get("state")
    scope = scope if scope is not None else q.get("scope")
    code_challenge = code_challenge or q.get("code_challenge")
    code_challenge_method = code_challenge_method or q.get("code_challenge_method") or "S256"
    resource = resource if resource is not None else q.get("resource")

    if not response_type or not client_id or not redirect_uri or not code_challenge:
        raise HTTPException(
            status_code=400,
            detail="response_type, client_id, redirect_uri, and code_challenge are required",
        )
    return _run_authorize(
        response_type=response_type,
        client_id=client_id,
        redirect_uri=redirect_uri,
        state=state,
        scope=scope,
        code_challenge=code_challenge,
        code_challenge_method=code_challenge_method,
        resource=resource,
        db=db,
    )


def _basic_client_credentials(request: Request) -> tuple[Optional[str], Optional[str]]:
    """RFC 6749 §2.3.1 client_secret_basic: Authorization: Basic base64(id:secret)."""
    import base64
    from urllib.parse import unquote

    auth = request.headers.get("authorization") or ""
    if not auth.lower().startswith("basic "):
        return None, None
    try:
        decoded = base64.b64decode(auth.split(" ", 1)[1].strip()).decode("utf-8")
    except Exception:
        return None, None
    cid, sep, secret = decoded.partition(":")
    if not sep:
        return None, None
    return unquote(cid) or None, unquote(secret) or None


@router.post("/mcp/oauth/token")
async def token(
    request: Request,
    db: Session = Depends(get_db),
    grant_type: Optional[str] = Form(None),
    code: Optional[str] = Form(None),
    redirect_uri: Optional[str] = Form(None),
    client_id: Optional[str] = Form(None),
    code_verifier: Optional[str] = Form(None),
    refresh_token: Optional[str] = Form(None),
    client_secret: Optional[str] = Form(None),
    resource: Optional[str] = Form(None),
):
    """
    RFC 6749 token endpoint — expects application/x-www-form-urlencoded.
    Also accepts JSON for local debugging. Supports RFC 8707 `resource`.
    """
    # Prefer form fields; fall back to JSON body if form empty
    if not grant_type:
        try:
            body: Dict[str, Any] = await request.json()
        except Exception:
            body = {}
        grant_type = body.get("grant_type")
        code = code or body.get("code")
        redirect_uri = redirect_uri or body.get("redirect_uri")
        client_id = client_id or body.get("client_id")
        code_verifier = code_verifier or body.get("code_verifier")
        refresh_token = refresh_token or body.get("refresh_token")
        resource = resource or body.get("resource")

    basic_id, basic_secret = _basic_client_credentials(request)
    if basic_id:
        if client_id and client_id != basic_id:
            return JSONResponse(status_code=400, content={"error": "invalid_request", "error_description": "client_id mismatch"})
        client_id = basic_id
        client_secret = basic_secret
    if not client_id:
        return JSONResponse(status_code=400, content={"error": "invalid_request", "error_description": "client_id required"})
    try:
        client = svc.get_or_reject_client(db, client_id)
    except HTTPException:
        return JSONResponse(status_code=401, content={"error": "invalid_client"})
    if not svc.verify_client_secret(client, client_secret):
        _logger.warning("mcp_oauth_token client authentication failed client_id=%s", client_id)
        return JSONResponse(status_code=401, content={"error": "invalid_client"})

    _logger.info(
        "mcp_oauth_token grant_type=%s client_id=%s resource=%s has_code=%s has_refresh=%s",
        grant_type,
        client_id,
        resource,
        bool(code),
        bool(refresh_token),
    )

    try:
        if grant_type == "authorization_code":
            if not code or not redirect_uri or not code_verifier:
                return JSONResponse(
                    status_code=400,
                    content={"error": "invalid_request", "error_description": "code, redirect_uri, code_verifier required"},
                )
            result = svc.exchange_authorization_code(
                db,
                code=code,
                client_id=client_id,
                redirect_uri=redirect_uri,
                code_verifier=code_verifier,
                resource=resource,
            )
            _logger.info("mcp_oauth_token authorization_code exchange ok client_id=%s", client_id)
            return result
        if grant_type == "refresh_token":
            if not refresh_token:
                return JSONResponse(status_code=400, content={"error": "invalid_request"})
            result = svc.refresh_access_token(
                db,
                refresh_token=refresh_token,
                client_id=client_id,
                resource=resource,
            )
            _logger.info("mcp_oauth_token refresh ok client_id=%s", client_id)
            return result
        return JSONResponse(status_code=400, content={"error": "unsupported_grant_type"})
    except HTTPException as e:
        detail = e.detail
        _logger.warning("mcp_oauth_token failed grant_type=%s detail=%s", grant_type, detail)
        if isinstance(detail, dict) and "error" in detail:
            return JSONResponse(status_code=400, content=detail)
        return JSONResponse(status_code=400, content={"error": "invalid_grant", "error_description": str(detail)})

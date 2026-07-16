"""
OAuth 2.0 Authorization Server endpoints for the Claude MCP connector.

Discovery:
  GET /.well-known/oauth-protected-resource
  GET /.well-known/oauth-protected-resource/{path}
  GET /.well-known/oauth-authorization-server
  GET /.well-known/oauth-authorization-server/{path}

DCR + authorize + token:
  POST /mcp/oauth/register
  GET|POST /mcp/oauth/authorize
  POST /mcp/oauth/token
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, Form, HTTPException, Query, Request
from fastapi.responses import JSONResponse, RedirectResponse
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

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
        "token_endpoint_auth_methods_supported": ["none", "client_secret_post"],
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


@router.post("/mcp/oauth/register")
def dynamic_client_registration(body: DCRRequest, db: Session = Depends(get_db)):
    client = svc.register_client(
        db,
        redirect_uris=body.redirect_uris,
        client_name=body.client_name,
        token_endpoint_auth_method=body.token_endpoint_auth_method or "none",
        grant_types=body.grant_types,
    )
    return JSONResponse(
        status_code=201,
        content={
            "client_id": client.client_id,
            "client_id_issued_at": int(client.created_at.timestamp()) if client.created_at else None,
            "client_name": client.client_name,
            "redirect_uris": client.redirect_uris,
            "grant_types": client.grant_types,
            "token_endpoint_auth_method": client.token_endpoint_auth_method,
            "response_types": ["code"],
        },
    )


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

    if not client_id:
        return JSONResponse(status_code=400, content={"error": "invalid_request", "error_description": "client_id required"})
    try:
        svc.get_or_reject_client(db, client_id)
    except HTTPException:
        return JSONResponse(status_code=400, content={"error": "invalid_client"})

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
            return result
        if grant_type == "refresh_token":
            if not refresh_token:
                return JSONResponse(status_code=400, content={"error": "invalid_request"})
            return svc.refresh_access_token(
                db,
                refresh_token=refresh_token,
                client_id=client_id,
                resource=resource,
            )
        return JSONResponse(status_code=400, content={"error": "unsupported_grant_type"})
    except HTTPException as e:
        detail = e.detail
        if isinstance(detail, dict) and "error" in detail:
            return JSONResponse(status_code=400, content=detail)
        return JSONResponse(status_code=400, content={"error": "invalid_grant", "error_description": str(detail)})

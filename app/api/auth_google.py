"""
Google OAuth for SweepOS user identity.

Modes (via /auth/google/start?mode=...):
  - login: existing user signs in with Google
  - invite / signup: new (or existing) user accepts an invitation via Google
  - connect: logged-in user links Google to their account
  - mcp: MCP OAuth authorize continues after Google identity (pending_nonce)

State is a signed JWT carrying mode + nonce + optional invite_token / mcp_nonce / user_id.
"""
from __future__ import annotations

import logging
import secrets
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, Optional
from urllib.parse import urlencode

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from fastapi.responses import RedirectResponse
from jose import JWTError, jwt
from sqlalchemy import func, text
from sqlalchemy.orm import Session

from app.api.deps import get_current_user
from app.core.config import settings
from app.core.rate_limit import rate_limit
from app.core.security import create_access_token
from app.db.session import get_db
from app.models.organization import Organization
from app.models.organization_invitation import OrganizationInvitation
from app.models.user import (
    User,
    UserRole,
    parse_user_role_from_api,
    role_to_api,
    userrole_bind_value,
)
from app.models.user_organization import UserOrganization

router = APIRouter()
_logger = logging.getLogger(__name__)

GOOGLE_AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"
GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
GOOGLE_USERINFO_URL = "https://openidconnect.googleapis.com/v1/userinfo"
STATE_TTL_MINUTES = 15


def _google_configured() -> bool:
    return settings.google_oauth_is_configured()


def _require_google_config() -> None:
    if not _google_configured():
        raise HTTPException(
            status_code=503,
            detail="Google OAuth is not configured. Set GOOGLE_OAUTH_CLIENT_ID (or GOOGLE_CLIENT_ID), "
            "GOOGLE_OAUTH_CLIENT_SECRET (or GOOGLE_CLIENT_SECRET), and GOOGLE_OAUTH_REDIRECT_URI.",
        )


def _google_client_id() -> str:
    return settings.resolved_google_oauth_client_id() or ""


def _google_client_secret() -> str:
    return settings.resolved_google_oauth_client_secret() or ""


def _sign_state(payload: Dict[str, Any]) -> str:
    data = dict(payload)
    data["exp"] = datetime.utcnow() + timedelta(minutes=STATE_TTL_MINUTES)
    data["nonce"] = payload.get("nonce") or secrets.token_urlsafe(24)
    return jwt.encode(data, settings.SECRET_KEY, algorithm="HS256")


def _decode_state(state: str) -> Dict[str, Any]:
    try:
        return jwt.decode(state, settings.SECRET_KEY, algorithms=["HS256"])
    except JWTError as e:
        raise HTTPException(status_code=400, detail=f"Invalid OAuth state: {e}") from e


def _frontend_redirect(path: str, **params: Any) -> RedirectResponse:
    base = (settings.FRONTEND_URL or "http://localhost:3002").rstrip("/")
    qs = urlencode({k: v for k, v in params.items() if v is not None})
    url = f"{base}{path}" + (f"?{qs}" if qs else "")
    return RedirectResponse(url=url, status_code=302)


def _issue_app_token(user: User, org_id: uuid.UUID) -> str:
    return create_access_token(
        data={
            "sub": user.email,
            "org_id": str(org_id),
            "user_id": str(user.id),
            "role": role_to_api(user.role) if user.role else "member",
        },
        expires_delta=timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES),
    )


def _find_users_by_email(db: Session, email: str) -> list[User]:
    return (
        db.query(User)
        .filter(func.lower(User.email) == email.lower().strip())
        .order_by(User.created_at.asc())
        .all()
    )


def _find_user_by_google_id(db: Session, google_id: str) -> Optional[User]:
    return db.query(User).filter(User.google_id == google_id).first()


def _link_google(user: User, google_id: str, google_email: str) -> None:
    user.google_id = google_id
    user.google_email = google_email


def _create_user_from_invite(
    db: Session,
    inv: OrganizationInvitation,
    *,
    google_id: str,
    google_email: str,
    hashed_password: Optional[str] = None,
) -> User:
    """Create a users row for an invitation (Google-only or copy of existing password)."""
    from app.models.user import parse_user_role_from_api as _parse

    email_normalized = inv.invitee_email.strip().lower()
    role_normalized = (inv.role or "member").strip().lower()
    if role_normalized not in ("owner", "admin", "member"):
        role_normalized = "member"
    user_role = _parse(role_normalized)
    new_user_id = uuid.uuid4()

    db.execute(
        text(
            """
            INSERT INTO users (id, org_id, email, hashed_password, role, is_admin, created_at, google_id, google_email)
            VALUES (:id, :org_id, :email, :hashed_password, CAST(:role AS userrole), :is_admin, NOW(), :google_id, :google_email)
            """
        ),
        {
            "id": new_user_id,
            "org_id": inv.org_id,
            "email": email_normalized,
            "hashed_password": hashed_password,
            "role": userrole_bind_value(user_role),
            "is_admin": user_role in (UserRole.ADMIN, UserRole.OWNER),
            "google_id": google_id,
            "google_email": google_email,
        },
    )
    db.add(
        UserOrganization(
            user_id=new_user_id,
            org_id=inv.org_id,
            is_primary=True,
        )
    )
    inv.used_at = datetime.utcnow()
    db.commit()
    user = db.query(User).filter(User.id == new_user_id).first()
    if not user:
        raise HTTPException(status_code=500, detail="Failed to create user")
    return user


def _accept_invite_with_google(
    db: Session,
    invite_token: str,
    *,
    google_id: str,
    google_email: str,
    email: str,
) -> tuple[User, uuid.UUID]:
    inv = (
        db.query(OrganizationInvitation)
        .filter(
            OrganizationInvitation.token == invite_token.strip(),
            OrganizationInvitation.used_at.is_(None),
        )
        .first()
    )
    if not inv:
        raise HTTPException(status_code=400, detail="Invalid or already used invitation")
    if inv.expires_at <= datetime.utcnow():
        raise HTTPException(status_code=400, detail="Invitation has expired")

    invite_email = inv.invitee_email.strip().lower()
    if email.strip().lower() != invite_email:
        raise HTTPException(
            status_code=400,
            detail=f"Google account email ({email}) does not match the invitation ({invite_email}).",
        )

    org = db.query(Organization).filter(Organization.id == inv.org_id).first()
    if not org:
        raise HTTPException(status_code=400, detail="Organization not found")

    # Already linked via google_id?
    by_gid = _find_user_by_google_id(db, google_id)
    if by_gid and by_gid.org_id == inv.org_id:
        raise HTTPException(status_code=400, detail="You are already in this organization")

    existing_users = _find_users_by_email(db, invite_email)
    if existing_users:
        existing = existing_users[0]
        in_org = next((u for u in existing_users if u.org_id == inv.org_id), None)
        if in_org:
            raise HTTPException(status_code=400, detail="You are already in this organization")

        if org.max_user_seats is not None:
            current_count = db.query(func.count(User.id)).filter(User.org_id == inv.org_id).scalar() or 0
            if current_count >= org.max_user_seats:
                raise HTTPException(
                    status_code=403,
                    detail="Organization user limit has been reached.",
                )

        # Add existing person to org; link Google on all matching rows
        user = _create_user_from_invite(
            db,
            inv,
            google_id=google_id,
            google_email=google_email,
            hashed_password=existing.hashed_password,
        )
        for u in existing_users:
            if not u.google_id:
                _link_google(u, google_id, google_email)
        db.commit()
        return user, inv.org_id

    if org.max_user_seats is not None:
        current_count = db.query(func.count(User.id)).filter(User.org_id == inv.org_id).scalar() or 0
        if current_count >= org.max_user_seats:
            raise HTTPException(
                status_code=403,
                detail="Organization user limit has been reached.",
            )

    user = _create_user_from_invite(
        db,
        inv,
        google_id=google_id,
        google_email=google_email,
        hashed_password=None,
    )
    return user, inv.org_id


async def _exchange_code(code: str) -> Dict[str, Any]:
    async with httpx.AsyncClient(timeout=20.0) as client:
        token_resp = await client.post(
            GOOGLE_TOKEN_URL,
            data={
                "code": code,
                "client_id": _google_client_id(),
                "client_secret": _google_client_secret(),
                "redirect_uri": settings.GOOGLE_OAUTH_REDIRECT_URI,
                "grant_type": "authorization_code",
            },
        )
        if token_resp.status_code >= 400:
            _logger.warning("Google token exchange failed: %s", token_resp.text)
            raise HTTPException(status_code=400, detail="Failed to exchange Google authorization code")
        tokens = token_resp.json()
        access = tokens.get("access_token")
        if not access:
            raise HTTPException(status_code=400, detail="Google did not return an access token")
        info_resp = await client.get(
            GOOGLE_USERINFO_URL,
            headers={"Authorization": f"Bearer {access}"},
        )
        if info_resp.status_code >= 400:
            raise HTTPException(status_code=400, detail="Failed to fetch Google user info")
        return info_resp.json()


@router.get("/google/start")
@rate_limit(max_requests=30, window_seconds=300)
def google_oauth_start(
    request: Request,
    mode: str = Query("login", pattern="^(login|signup|invite|connect|mcp)$"),
    invite_token: Optional[str] = Query(None),
    mcp_nonce: Optional[str] = Query(None),
    db: Session = Depends(get_db),
):
    """
    Begin Google OAuth. Returns a redirect URL (JSON) or 302 when `redirect=1`.
    Frontend typically fetches this and then navigates to `authorization_url`.
    """
    _require_google_config()
    mode_norm = (mode or "login").strip().lower()
    if mode_norm in ("signup", "invite"):
        if not invite_token or not invite_token.strip():
            raise HTTPException(status_code=400, detail="invite_token is required for signup/invite mode")
        inv = (
            db.query(OrganizationInvitation)
            .filter(
                OrganizationInvitation.token == invite_token.strip(),
                OrganizationInvitation.used_at.is_(None),
            )
            .first()
        )
        if not inv or inv.expires_at <= datetime.utcnow():
            raise HTTPException(status_code=400, detail="Invalid or expired invitation")
        mode_norm = "invite"

    connect_user_id: Optional[str] = None
    if mode_norm == "connect":
        # Optional bearer for connect mode
        auth = request.headers.get("Authorization") or ""
        if not auth.lower().startswith("bearer "):
            raise HTTPException(status_code=401, detail="Authentication required to connect Google")
        from app.core.security import decode_access_token

        payload = decode_access_token(auth.split(" ", 1)[1].strip())
        if not payload or not payload.get("user_id"):
            raise HTTPException(status_code=401, detail="Invalid token")
        connect_user_id = str(payload["user_id"])

    if mode_norm == "mcp" and not mcp_nonce:
        raise HTTPException(status_code=400, detail="mcp_nonce is required for mcp mode")

    state = _sign_state(
        {
            "mode": mode_norm,
            "invite_token": invite_token.strip() if invite_token else None,
            "mcp_nonce": mcp_nonce,
            "user_id": connect_user_id,
        }
    )
    params = {
        "client_id": _google_client_id(),
        "redirect_uri": settings.GOOGLE_OAUTH_REDIRECT_URI,
        "response_type": "code",
        "scope": "openid email profile",
        "state": state,
        "access_type": "online",
        "prompt": "select_account",
    }
    authorization_url = f"{GOOGLE_AUTH_URL}?{urlencode(params)}"
    # Allow direct browser navigation: /auth/google/start?mode=login&redirect=1
    if request.query_params.get("redirect") in ("1", "true", "yes"):
        return RedirectResponse(url=authorization_url, status_code=302)
    return {"authorization_url": authorization_url, "mode": mode_norm}


@router.get("/google/callback")
@rate_limit(max_requests=40, window_seconds=300)
async def google_oauth_callback(
    request: Request,
    code: Optional[str] = None,
    state: Optional[str] = None,
    error: Optional[str] = None,
    db: Session = Depends(get_db),
):
    _require_google_config()
    if error:
        return _frontend_redirect("/login", google_error=error)
    if not code or not state:
        return _frontend_redirect("/login", google_error="missing_code")

    state_data = _decode_state(state)
    mode = (state_data.get("mode") or "login").lower()

    try:
        info = await _exchange_code(code)
    except HTTPException as e:
        return _frontend_redirect("/login", google_error=str(e.detail))

    google_id = str(info.get("sub") or "").strip()
    email = str(info.get("email") or "").strip().lower()
    email_verified = bool(info.get("email_verified", True))
    if not google_id or not email:
        return _frontend_redirect("/login", google_error="missing_google_profile")
    if not email_verified:
        return _frontend_redirect("/login", google_error="email_not_verified")

    # --- connect ---
    if mode == "connect":
        uid = state_data.get("user_id")
        if not uid:
            return _frontend_redirect("/login", google_error="connect_missing_user")
        user = db.query(User).filter(User.id == uuid.UUID(str(uid))).first()
        if not user:
            return _frontend_redirect("/login", google_error="user_not_found")
        # Require Google email to match Sweep account email (safe for existing accounts)
        if email.strip().lower() != (user.email or "").strip().lower():
            return _frontend_redirect(
                "/",
                tab="settings",
                section="profile",
                google_error="email_mismatch",
                message="Google account email must match your SweepOS email to connect.",
            )
        taken = _find_user_by_google_id(db, google_id)
        if taken and (taken.email or "").lower() != (user.email or "").lower():
            return _frontend_redirect(
                "/",
                tab="settings",
                section="profile",
                google_error="google_already_linked",
                message="That Google account is already linked to another SweepOS user.",
            )
        for u in _find_users_by_email(db, user.email):
            _link_google(u, google_id, email)
        db.commit()
        return _frontend_redirect("/", tab="settings", section="profile", google="connected")

    # --- invite / signup ---
    if mode in ("invite", "signup"):
        invite_token = state_data.get("invite_token")
        if not invite_token:
            return _frontend_redirect("/login", google_error="missing_invite")
        try:
            user, org_id = _accept_invite_with_google(
                db,
                invite_token,
                google_id=google_id,
                google_email=email,
                email=email,
            )
        except HTTPException as e:
            return _frontend_redirect(
                "/invite/accept",
                token=invite_token,
                google_error=str(e.detail),
            )
        token = _issue_app_token(user, org_id)
        return _frontend_redirect("/auth/google/complete", token=token)

    # --- mcp: resume MCP OAuth grant after Google identity ---
    if mode == "mcp":
        from app.services.mcp_oauth_service import complete_mcp_grant_after_google

        mcp_nonce = state_data.get("mcp_nonce")
        try:
            result = complete_mcp_grant_after_google(
                db,
                mcp_nonce=mcp_nonce,
                google_id=google_id,
                email=email,
            )
        except HTTPException as e:
            return _frontend_redirect("/login", google_error=str(e.detail))

        if result.get("status") == "select_org":
            # Multi-org: pause Claude OAuth and let the user pick which Sweep org to bind
            select_token = _sign_state(
                {
                    "purpose": "mcp_org_select",
                    "mcp_nonce": mcp_nonce,
                    "google_id": google_id,
                    "email": email,
                }
            )
            return _frontend_redirect(
                "/auth/mcp/select-organization",
                select_token=select_token,
            )

        redirect_url = result.get("redirect_url")
        if not redirect_url:
            return _frontend_redirect("/login", google_error="mcp_redirect_missing")
        # 303 See Other: Claude's auth_callback only accepts GET; avoid method-preserving redirects
        return RedirectResponse(url=redirect_url, status_code=303)

    # --- login (default) ---
    user = _find_user_by_google_id(db, google_id)
    if not user:
        matches = _find_users_by_email(db, email)
        if not matches:
            return _frontend_redirect(
                "/login",
                google_error="no_account",
                message="No SweepOS account for this Google email. Use your invite link to sign up.",
            )
        user = matches[0]
        for u in matches:
            if not u.google_id:
                _link_google(u, google_id, email)
        db.commit()

    # Multi-org: land on primary account; user can switch later in Settings → Accounts.
    all_email_users = _find_users_by_email(db, user.email)
    org_ids = list({u.org_id for u in all_email_users})
    if len(org_ids) > 1 and not request.query_params.get("org_id"):
        primary = None
        for u in sorted(all_email_users, key=lambda x: x.created_at or datetime.min):
            uo = (
                db.query(UserOrganization)
                .filter(
                    UserOrganization.user_id == u.id,
                    UserOrganization.org_id == u.org_id,
                    UserOrganization.is_primary == True,  # noqa: E712
                )
                .first()
            )
            if uo:
                primary = u
                break
        if primary is None:
            primary = sorted(all_email_users, key=lambda u: u.created_at or datetime.min)[0]
        token = _issue_app_token(primary, primary.org_id)
        return _frontend_redirect("/auth/google/complete", token=token)

    token = _issue_app_token(user, user.org_id)
    return _frontend_redirect("/auth/google/complete", token=token)


@router.get("/google/status")
def google_oauth_status():
    """Public: whether Google sign-in is available."""
    return {"configured": _google_configured()}


@router.post("/google/disconnect")
@rate_limit(max_requests=20, window_seconds=300)
def google_oauth_disconnect(
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Unlink Google from the current user's account(s).
    Requires a password on the account so the user can still sign in.
    """
    peers = _find_users_by_email(db, current_user.email)
    if not any(u.hashed_password for u in peers):
        raise HTTPException(
            status_code=400,
            detail="Set a password before disconnecting Google, or you will be locked out.",
        )
    for u in peers:
        u.google_id = None
        u.google_email = None
    db.commit()
    return {"ok": True, "google_connected": False}

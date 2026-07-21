from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Request, status
from sqlalchemy.orm import Session
from sqlalchemy import func, text
from datetime import datetime, timedelta
from typing import List, Optional
from uuid import UUID
import uuid
import logging
from app.db.session import get_db
from app.models.user import (
    User,
    parse_user_role_from_db,
    role_to_api,
    parse_user_role_from_api,
    userrole_bind_value,
)
from app.models.user_organization import UserOrganization
from app.models.organization import Organization
from app.schemas.user import UserLogin, Token, User as UserSchema, UserSettingsUpdate, LoginResponse
from app.schemas.organization import UserOrganizationResponse, OrganizationSwitchRequest
from app.schemas.invitation import InviteValidateResponse, InviteAcceptRequest, InviteAcceptResponse
from app.core.security import verify_password, create_access_token, get_password_hash
from app.core.config import settings
from app.core.rate_limit import rate_limit, check_sliding_window
from app.core.request_ip import get_client_ip
from app.api.deps import get_current_user, user_is_system_owner, is_sudo_admin
from app.services.fathom_client import normalize_fathom_api_key

router = APIRouter()
_logger = logging.getLogger(__name__)


@router.post("/login", response_model=LoginResponse)
def login(
    request: Request,
    user_credentials: UserLogin,
    db: Session = Depends(get_db),
):
    """
    Login endpoint. If user belongs to multiple organizations and org_id is not provided,
    returns a response indicating organization selection is needed.
    """
    check_sliding_window(
        f"auth_login:{get_client_ip(request)}",
        settings.LOGIN_RATE_LIMIT_MAX,
        settings.LOGIN_RATE_LIMIT_WINDOW_SEC,
        endpoint_name="login",
    )
    # Normalize email: lowercase and strip whitespace
    # This prevents login issues from case sensitivity or accidental spaces
    normalized_email = user_credentials.email.lower().strip()
    
    # Normalize password: strip leading/trailing whitespace (but preserve internal spaces)
    # This prevents login issues from accidental copy-paste whitespace
    normalized_password = user_credentials.password.strip()
    
    # Use case-insensitive comparison to handle emails stored with different casing
    # Get all users with this email (across all orgs) using raw SQL to avoid enum conversion
    from sqlalchemy import text
    users_result = db.execute(
        text(
            """
            SELECT id, org_id, email, hashed_password, role, is_admin, created_at
            FROM users
            WHERE LOWER(email) = LOWER(:email)
            ORDER BY created_at ASC
        """
        ),
        {"email": normalized_email},
    ).fetchall()
    
    if not users_result:
        # Don't reveal if user exists or not for security
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    # Find all users with matching password (same user across multiple orgs)
    matching_users = []
    google_only_hit = False
    for row in users_result:
        user_id, org_id, email, hashed_password, role_db_value, is_admin, created_at = row
        if not hashed_password:
            # Google-only account — password login not available
            google_only_hit = True
            continue
        if verify_password(normalized_password, hashed_password):
            # Create a minimal user-like object for matching
            user_role_enum = parse_user_role_from_db(role_db_value)

            class UserProxy:
                def __init__(self, user_id, org_id, email, hashed_password, role_enum, is_admin, created_at):
                    self.id = user_id
                    self.org_id = org_id
                    self.email = email
                    self.hashed_password = hashed_password
                    self.role = role_enum  # UserRole enum object
                    self.is_admin = is_admin
                    self.created_at = created_at
            matching_users.append(UserProxy(user_id, org_id, email, hashed_password, user_role_enum, is_admin, created_at))
    
    if not matching_users:
        if google_only_hit:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="This account uses Google sign-in. Click Sign in with Google.",
                headers={"WWW-Authenticate": "Bearer"},
            )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    # Use oldest matching user row as canonical "primary" for linking orgs.
    # Without a deterministic order, dev DBs can flip which org becomes "primary" across restarts.
    matching_users.sort(key=lambda u: (u.created_at or datetime.min))
    user = matching_users[0]
    
    # Check if user belongs to multiple organizations
    # First check UserOrganization table
    user_orgs = db.query(UserOrganization).filter(
        UserOrganization.user_id == user.id
    ).all()
    
    # If user has same password in multiple orgs, they're the same user
    # Create UserOrganization records linking them
    if len(matching_users) > 1:
        # User exists in multiple orgs with same password - link them
        all_org_ids = [u.org_id for u in matching_users]
        unique_org_ids = list(set(all_org_ids))
        primary_org_id = user.org_id
        
        # Create UserOrganization records for the canonical user to access all orgs
        # and ensure exactly one "is_primary" org (the canonical org).
        for org_id in unique_org_ids:
            org_user = next((u for u in matching_users if u.org_id == org_id), user)
            existing_uo = db.query(UserOrganization).filter(
                UserOrganization.user_id == org_user.id,
                UserOrganization.org_id == org_id
            ).first()
            if not existing_uo:
                user_org = UserOrganization(
                    user_id=org_user.id,
                    org_id=org_id,
                    is_primary=(org_id == primary_org_id),
                )
                db.add(user_org)
            else:
                # Repair drift: if we already have rows but the wrong org is marked primary, fix it.
                if org_id == primary_org_id and not existing_uo.is_primary:
                    existing_uo.is_primary = True
                if org_id != primary_org_id and existing_uo.is_primary:
                    existing_uo.is_primary = False
        
        db.commit()
        
        # Re-query user_orgs
        user_orgs = db.query(UserOrganization).filter(
            UserOrganization.user_id == user.id
        ).all()
    elif not user_orgs:
        # Single user, no UserOrganization record - create one for backward compatibility
        user_org = UserOrganization(
            user_id=user.id,
            org_id=user.org_id,
            is_primary=True
        )
        db.add(user_org)
        db.commit()
        user_orgs = [user_org]
    
    # Determine target org
    if not user_orgs:
        # Use the user's org_id from the user table (single org user)
        target_org_id = user.org_id
    elif len(user_orgs) == 1:
        # User belongs to only one org
        target_org_id = user_orgs[0].org_id
    else:
        # User belongs to multiple orgs — land on primary (or explicit org_id).
        # Account switching is done in Settings → Accounts, not at login.
        if user_credentials.org_id is None:
            primary_uo = next((uo for uo in user_orgs if uo.is_primary), None)
            target_org_id = primary_uo.org_id if primary_uo is not None else user.org_id
        else:
            # Verify user has access to the requested org
            # Convert to UUID if it's a string (from frontend)
            requested_org_id = user_credentials.org_id
            if isinstance(requested_org_id, str):
                try:
                    requested_org_id = uuid.UUID(requested_org_id)
                except (ValueError, TypeError) as e:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail=f"Invalid organization ID format: {str(e)}"
                    )

            user_org = next((uo for uo in user_orgs if uo.org_id == requested_org_id), None)
            if not user_org:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="User does not have access to this organization"
                )
            target_org_id = requested_org_id
    
    # Use the user row for the target org (same email can have different roles per org)
    user_for_token = next((u for u in matching_users if u.org_id == target_org_id), None)
    if user_for_token is not None:
        user = user_for_token
    # else: keep user as matching_users[0] for single-org or legacy
    
    # Create token with selected org_id and that org's user id/role
    access_token_expires = timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={
            "sub": user.email,
            "org_id": str(target_org_id),  # Include org_id in token for multi-tenant isolation
            "user_id": str(user.id),
            "role": role_to_api(user.role),
        },
        expires_delta=access_token_expires
    )
    return LoginResponse(
        requires_org_selection=False,
        access_token=access_token,
        token_type="bearer"
    )


@router.post("/refresh", response_model=Token)
def refresh_session(current_user: User = Depends(get_current_user)):
    """
    Issue a new access token with extended expiry (sliding window).
    Call this when the same tab/browser is active to avoid re-login.
    Does not invalidate the previous token; use for keep-alive only.
    """
    access_token_expires = timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
    org_id = getattr(current_user, "selected_org_id", current_user.org_id)
    access_token = create_access_token(
        data={
            "sub": current_user.email,
            "org_id": str(org_id),
            "user_id": str(current_user.id),
            "role": role_to_api(current_user.role),
        },
        expires_delta=access_token_expires,
    )
    return {"access_token": access_token, "token_type": "bearer"}


def _organization_name(db: Session, org_id: UUID) -> Optional[str]:
    row = db.query(Organization).filter(Organization.id == org_id).first()
    return row.name if row else None


def _organization_portal_fields(db: Session, org_id: UUID) -> tuple:
    """Return (org_name, consulting_tier, booking_url) for the given org."""
    row = db.query(Organization).filter(Organization.id == org_id).first()
    if not row:
        return None, None, None
    return (
        row.name,
        getattr(row, "consulting_tier", None),
        getattr(row, "booking_url", None),
    )


def _user_schema_response(current_user: User, db: Session) -> UserSchema:
    role_value = role_to_api(current_user.role)
    org_id = getattr(current_user, "selected_org_id", current_user.org_id)
    org_name, consulting_tier, booking_url = _organization_portal_fields(db, org_id)
    return UserSchema(
        id=current_user.id,
        org_id=org_id,
        org_name=org_name,
        email=current_user.email,
        role=role_value,
        is_admin=current_user.is_admin,
        is_system_owner=user_is_system_owner(current_user, db),
        is_sudo_admin=is_sudo_admin(current_user),
        consulting_tier=consulting_tier,
        booking_url=booking_url,
        created_at=current_user.created_at,
    )


@router.get("/me", response_model=UserSchema)
def get_current_user_info(current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    """Get current user info with proper enum serialization"""
    try:
        return _user_schema_response(current_user, db)
    except Exception as e:
        import traceback
        print(f"ERROR in get_current_user_info: {str(e)}")
        print(traceback.format_exc())
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to serialize user data: {str(e)}"
        )


@router.put("/me/settings", response_model=UserSchema)
def update_user_settings(
    settings_data: UserSettingsUpdate,
    background_tasks: BackgroundTasks,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    Update user settings including email, password, and privacy preferences.
    For password changes, current_password must be provided.
    Uses DB row by user id so updates persist for both User and UserProxy.
    """
    from app.core.security import verify_password, get_password_hash
    
    # Fetch the actual User row so updates persist (current_user may be UserProxy)
    user_row = db.query(User).filter(User.id == current_user.id).first()
    if not user_row:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found",
        )
    
    # Update email if provided
    if settings_data.email is not None and settings_data.email != user_row.email:
        # Check if email is already taken in this org
        existing_user = db.query(User).filter(
            User.email == settings_data.email,
            User.org_id == user_row.org_id,
            User.id != user_row.id
        ).first()
        if existing_user:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Email already in use in this organization"
            )
        user_row.email = settings_data.email
    
    # Update password if provided
    if settings_data.new_password is not None:
        if user_row.hashed_password:
            # Existing password account: require current password
            if not settings_data.current_password:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Current password is required to change password",
                )
            if not verify_password(settings_data.current_password, user_row.hashed_password):
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Current password is incorrect",
                )
        # Google-only accounts (null hash): allow setting a first password without current
        user_row.hashed_password = get_password_hash(settings_data.new_password)
        # Keep multi-org password rows in sync for the same email
        for peer in db.query(User).filter(func.lower(User.email) == user_row.email.lower()).all():
            if peer.id != user_row.id:
                peer.hashed_password = user_row.hashed_password
    
    if settings_data.fathom_api_key is not None:
        from app.services.org_user_context import user_can_manage_org_integrations

        org_id = getattr(current_user, "selected_org_id", user_row.org_id)
        if not user_can_manage_org_integrations(current_user, db):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Only organization admins and owners can set the Fathom API key.",
            )
        org = db.query(Organization).filter(Organization.id == org_id).first()
        if not org:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Organization not found")
        new_key = normalize_fathom_api_key(settings_data.fathom_api_key)
        org.fathom_api_key = new_key

    if settings_data.ai_profile is not None:
        from app.services.org_intelligence_profile import set_org_ai_profile

        set_org_ai_profile(db, current_user, settings_data.ai_profile)

    db.commit()
    db.refresh(user_row)

    role_value = role_to_api(user_row.role)
    org_id = getattr(current_user, "selected_org_id", user_row.org_id)
    org_name, consulting_tier, booking_url = _organization_portal_fields(db, org_id)
    return UserSchema(
        id=user_row.id,
        org_id=org_id,
        org_name=org_name,
        email=user_row.email,
        role=role_value,
        is_admin=user_row.is_admin,
        is_system_owner=user_is_system_owner(current_user, db),
        is_sudo_admin=is_sudo_admin(current_user),
        consulting_tier=consulting_tier,
        booking_url=booking_url,
        created_at=user_row.created_at,
    )


@router.get("/me/settings")
def get_user_settings(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    Get current user settings.
    Returns user info and default privacy settings.
    ai_profile is read from the database row — UserProxy from JWT auth does not include JSON columns.
    """
    from app.services.org_intelligence_profile import get_org_ai_profile

    user_row = db.query(User).filter(User.id == current_user.id).first()
    ai_profile = get_org_ai_profile(db, current_user)
    org_id_settings = getattr(current_user, "selected_org_id", current_user.org_id)
    org_row = db.query(Organization).filter(Organization.id == org_id_settings).first()
    fathom_key = None
    if org_row is not None:
        fathom_key = getattr(org_row, "fathom_api_key", None)
    if not fathom_key and user_row:
        fathom_key = getattr(user_row, "fathom_api_key", None)

    google_connected = bool(user_row and user_row.google_id)
    google_email = (user_row.google_email if user_row else None) or None
    has_password = bool(user_row and user_row.hashed_password)
    # If this email has any linked row (multi-org), treat as connected
    if user_row and not google_connected:
        peer = (
            db.query(User)
            .filter(func.lower(User.email) == user_row.email.lower(), User.google_id.isnot(None))
            .first()
        )
        if peer:
            google_connected = True
            google_email = peer.google_email or peer.email
    if user_row and not has_password:
        peer_pw = (
            db.query(User)
            .filter(func.lower(User.email) == user_row.email.lower(), User.hashed_password.isnot(None))
            .first()
        )
        has_password = peer_pw is not None

    from app.core.config import settings as app_settings

    google_oauth_available = app_settings.google_oauth_is_configured()

    return {
        "email": current_user.email,
        "role": role_to_api(current_user.role),
        "org_id": str(org_id_settings),
        "created_at": current_user.created_at.isoformat() if current_user.created_at else None,
        "data_sharing_enabled": True,
        "analytics_enabled": True,
        "fathom_api_key": fathom_key or None,
        "ai_profile": ai_profile,
        "google_connected": google_connected,
        "google_email": google_email,
        "google_oauth_available": google_oauth_available,
        "has_password": has_password,
    }


@router.get("/me/sales-content-themes")
def get_my_org_sales_content_themes(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    Recurring objection/circumstance themes validated across multiple clients in the current org.
    Used for transparency (Intelligence) and for email-draft LLM context.
    """
    from app.services.org_sales_theme_service import list_validated_themes_payload

    org_id = getattr(current_user, "selected_org_id", current_user.org_id)
    themes = list_validated_themes_payload(db, org_id)
    return {"themes": themes}


@router.get("/organizations", response_model=List[UserOrganizationResponse])
def get_user_organizations(
    email: str,
    db: Session = Depends(get_db)
):
    """
    Get all organizations a user belongs to.
    Used for organization selection after login.
    Requires email parameter for security (user must know their email).
    """
    normalized_email = email.lower().strip()

    from app.services.org_user_context import list_organizations_for_email

    org_entries = list_organizations_for_email(db, normalized_email)
    if not org_entries:
        return []

    org_ids = [entry["org_id"] for entry in org_entries]
    orgs = db.query(Organization).filter(Organization.id.in_(org_ids)).all()
    org_dict = {org.id: org for org in orgs}

    result = []
    for entry in org_entries:
        org = org_dict.get(entry["org_id"])
        if org:
            result.append(UserOrganizationResponse(
                id=org.id,
                name=org.name,
                is_primary=bool(entry["is_primary"]),
                created_at=entry["created_at"],
            ))

    result.sort(key=lambda x: (not x.is_primary, x.name))
    return result


@router.post("/switch-organization", response_model=Token)
def switch_organization(
    switch_request: OrganizationSwitchRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Switch to a different organization.
    Verifies user has access to the requested organization.
    """
    from app.services.org_user_context import (
        fetch_user_row_for_org,
        materialize_org_user_row_if_missing,
        user_has_email_org_access,
        ensure_user_organization_link,
        _role_from_invitation,
    )

    target_org_id = switch_request.org_id
    email = current_user.email

    try:
        has_row = fetch_user_row_for_org(db, email, target_org_id) is not None
        has_link = user_has_email_org_access(db, email, target_org_id)
        selected_org_id = getattr(current_user, "selected_org_id", current_user.org_id)
        if not has_row and not has_link:
            if str(selected_org_id) != str(target_org_id) and str(current_user.org_id) != str(target_org_id):
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="User does not have access to this organization",
                )

        org = db.query(Organization).filter(Organization.id == target_org_id).first()
        if not org:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Organization not found",
            )

        resolved = fetch_user_row_for_org(db, email, target_org_id)
        if not resolved:
            resolved = materialize_org_user_row_if_missing(db, email, target_org_id)

        if resolved:
            token_user_id = str(resolved[0])
            token_role = role_to_api(parse_user_role_from_db(resolved[4]))
            ensure_user_organization_link(
                db,
                email,
                target_org_id,
                resolved[0],
                is_primary=str(resolved[1]) == str(target_org_id) and str(current_user.org_id) == str(target_org_id),
            )
        else:
            invited_role = _role_from_invitation(db, email, target_org_id)
            if invited_role is None and not has_link and not has_row:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="User does not have access to this organization",
                )
            token_user_id = str(current_user.id)
            token_role = role_to_api(invited_role or current_user.role)

        access_token_expires = timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
        access_token = create_access_token(
            data={
                "sub": email,
                "org_id": str(target_org_id),
                "user_id": token_user_id,
                "role": token_role,
            },
            expires_delta=access_token_expires
        )
        return {"access_token": access_token, "token_type": "bearer"}
    except HTTPException:
        raise
    except Exception as exc:
        _logger.exception(
            "switch_organization failed for email=%s org_id=%s",
            email,
            target_org_id,
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to switch organization",
        ) from exc


@router.delete("/organizations/{org_id}", status_code=status.HTTP_204_NO_CONTENT)
def leave_organization(
    org_id: UUID,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    Leave a secondary organization.

    - Users cannot leave their primary org (backward-compat `user.org_id` or UserOrganization.is_primary).
    - Only affects the UserOrganization link; does not delete the org or user account.
    """
    # Prevent leaving primary org
    if org_id == current_user.org_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="You cannot leave your primary organization.",
        )

    user_org = db.query(UserOrganization).filter(
        UserOrganization.user_id == current_user.id,
        UserOrganization.org_id == org_id,
    ).first()

    if not user_org:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="You are not a member of this organization.",
        )

    if user_org.is_primary:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="You cannot leave your primary organization.",
        )

    db.delete(user_org)
    db.commit()
    return None


# ---------------------------------------------------------------------------
# Invitation acceptance (public; no auth required for validate/accept)
# ---------------------------------------------------------------------------

@router.get("/invite/validate", response_model=InviteValidateResponse)
@rate_limit(max_requests=30, window_seconds=300)  # 30 validations per 5 min per IP
def validate_invitation_token(
    token: str,
    request: Request,
    db: Session = Depends(get_db),
):
    """Validate an invitation token. Returns org name, type, role if valid."""
    from app.models.organization_invitation import OrganizationInvitation
    inv = db.query(OrganizationInvitation).filter(
        OrganizationInvitation.token == token.strip(),
        OrganizationInvitation.used_at.is_(None),
    ).first()
    if not inv:
        return InviteValidateResponse(valid=False, message="Invalid or expired invitation")
    if inv.expires_at <= datetime.utcnow():
        return InviteValidateResponse(valid=False, message="Invitation has expired")
    org = db.query(Organization).filter(Organization.id == inv.org_id).first()
    org_name = org.name if org else None
    return InviteValidateResponse(
        valid=True,
        org_name=org_name,
        invitation_type=inv.invitation_type,
        role=inv.role,
        expires_at=inv.expires_at,
    )


@router.post("/invite/accept", response_model=InviteAcceptResponse)
@rate_limit(max_requests=10, window_seconds=900)  # 10 accept attempts per 15 min per IP
def accept_invitation(
    body: InviteAcceptRequest,
    request: Request,
    db: Session = Depends(get_db),
):
    """
    Accept an invitation. New users must provide password; existing users are just added to the org.
    Returns access_token when a new account is created or when existing user accepts (so frontend can set cookie and switch org).
    """
    from app.models.organization_invitation import OrganizationInvitation
    from app.models.user import UserRole

    token = (body.token or "").strip()
    if not token:
        raise HTTPException(status_code=400, detail="Token is required")
    inv = db.query(OrganizationInvitation).filter(
        OrganizationInvitation.token == token,
        OrganizationInvitation.used_at.is_(None),
    ).first()
    if not inv:
        raise HTTPException(status_code=400, detail="Invalid or already used invitation")
    if inv.expires_at <= datetime.utcnow():
        raise HTTPException(status_code=400, detail="Invitation has expired")

    org = db.query(Organization).filter(Organization.id == inv.org_id).first()
    if not org:
        raise HTTPException(status_code=400, detail="Organization not found")

    email_normalized = inv.invitee_email.strip().lower()
    role_normalized = (inv.role or "member").strip().lower()
    if role_normalized not in ("owner", "admin", "member"):
        role_normalized = "member"

    # Check if user already exists (any org)
    existing_user = db.query(User).filter(func.lower(User.email) == email_normalized).first()
    if existing_user:
        in_org_user = db.query(User).filter(
            func.lower(User.email) == email_normalized,
            User.org_id == inv.org_id,
        ).first()
        if in_org_user:
            raise HTTPException(status_code=400, detail="You are already in this organization")

        already_linked = db.query(UserOrganization).filter(
            UserOrganization.org_id == inv.org_id,
            UserOrganization.user_id == existing_user.id,
        ).first()
        if already_linked:
            raise HTTPException(status_code=400, detail="You are already in this organization")

        if org.max_user_seats is not None:
            current_count = db.query(func.count(User.id)).filter(User.org_id == inv.org_id).scalar() or 0
            if current_count >= org.max_user_seats:
                raise HTTPException(
                    status_code=403,
                    detail="Organization user limit has been reached. The invitation can no longer be accepted.",
                )

        user_role = parse_user_role_from_api(role_normalized)
        org_user_id = uuid.uuid4()
        db.execute(
            text("""
                INSERT INTO users (id, org_id, email, hashed_password, role, is_admin, created_at)
                VALUES (:id, :org_id, :email, :hashed_password, CAST(:role AS userrole), :is_admin, NOW())
            """),
            {
                "id": org_user_id,
                "org_id": inv.org_id,
                "email": email_normalized,
                "hashed_password": existing_user.hashed_password,
                "role": userrole_bind_value(user_role),
                "is_admin": user_role in (UserRole.ADMIN, UserRole.OWNER),
            },
        )
        db.add(
            UserOrganization(
                user_id=org_user_id,
                org_id=inv.org_id,
                is_primary=False,
            )
        )
        inv.used_at = datetime.utcnow()
        db.commit()
        access_token_expires = timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
        access_token = create_access_token(
            data={
                "sub": email_normalized,
                "org_id": str(inv.org_id),
                "user_id": str(org_user_id),
                "role": role_to_api(user_role),
            },
            expires_delta=access_token_expires,
        )
        return InviteAcceptResponse(
            access_token=access_token,
            token_type="bearer",
            org_id=inv.org_id,
            user_id=org_user_id,
            existing_user=True,
            message="You have been added to this organization.",
        )

    # New user: require password
    if not body.password or not (body.password or "").strip():
        raise HTTPException(
            status_code=400,
            detail="Password is required to create your account",
        )
    password = (body.password or "").strip()
    if len(password) < 8:
        raise HTTPException(status_code=400, detail="Password must be at least 8 characters")

    # Check email not already in this org (shouldn't be, but safety)
    in_org = db.query(User).filter(
        func.lower(User.email) == email_normalized,
        User.org_id == inv.org_id,
    ).first()
    if in_org:
        raise HTTPException(status_code=400, detail="A user with this email already exists in this organization")

    if org.max_user_seats is not None:
        current_count = db.query(func.count(User.id)).filter(User.org_id == inv.org_id).scalar() or 0
        if current_count >= org.max_user_seats:
            raise HTTPException(
                status_code=403,
                detail="Organization user limit has been reached. The invitation can no longer be accepted.",
            )

    user_role = parse_user_role_from_api(role_normalized)

    new_user_id = uuid.uuid4()
    db.execute(
        text("""
            INSERT INTO users (id, org_id, email, hashed_password, role, is_admin, created_at)
            VALUES (:id, :org_id, :email, :hashed_password, CAST(:role AS userrole), :is_admin, NOW())
        """),
        {
            "id": new_user_id,
            "org_id": inv.org_id,
            "email": email_normalized,
            "hashed_password": get_password_hash(password),
            "role": userrole_bind_value(user_role),
            "is_admin": (user_role in (UserRole.ADMIN, UserRole.OWNER)),
        },
    )
    uo = UserOrganization(
        user_id=new_user_id,
        org_id=inv.org_id,
        is_primary=True,
    )
    db.add(uo)
    inv.used_at = datetime.utcnow()
    db.commit()

    access_token_expires = timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={
            "sub": email_normalized,
            "org_id": str(inv.org_id),
            "user_id": str(new_user_id),
            "role": role_to_api(user_role),
        },
        expires_delta=access_token_expires,
    )
    return InviteAcceptResponse(
        access_token=access_token,
        token_type="bearer",
        org_id=inv.org_id,
        user_id=new_user_id,
        existing_user=False,
        message="Account created. You are now signed in.",
    )


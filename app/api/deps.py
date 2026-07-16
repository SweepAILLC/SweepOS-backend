import logging
import uuid
from typing import Optional

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.models.user import User, UserRole, parse_user_role_from_db
from app.models.organization_tab_permission import OrganizationTabPermission
from app.models.user_tab_permission import UserTabPermission
from app.core.security import decode_access_token

_logger = logging.getLogger(__name__)

security = HTTPBearer()


def _user_from_token(db: Session, token: str) -> User:
    """
    Resolve JWT + DB to a User-like proxy with selected_org_id set.
    Shared by get_current_user and long-running jobs that must not double-book get_db sessions.
    """
    payload = decode_access_token(token)
    if payload is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )
    email: str = payload.get("sub")
    org_id_from_token: Optional[str] = payload.get("org_id")
    user_id_from_token: Optional[str] = payload.get("user_id")

    if email is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication credentials",
        )

    # Use raw SQL to read user to avoid enum conversion issues.
    # Prefer user_id from token so we get the correct user row when same email has multiple rows (e.g. admin in one org, member in another).
    from sqlalchemy import text

    if user_id_from_token:
        try:
            user_id_uuid = uuid.UUID(user_id_from_token)
            user_row = db.execute(
                text("""
                    SELECT id, org_id, email, hashed_password, role, is_admin, created_at, fathom_api_key
                    FROM users
                    WHERE id = :user_id
                """),
                {"user_id": str(user_id_uuid)},
            ).fetchone()
        except (ValueError, TypeError):
            user_row = None
    else:
        user_row = None
    if user_row is None:
        user_row = db.execute(
            text("""
                SELECT id, org_id, email, hashed_password, role, is_admin, created_at, fathom_api_key
                FROM users
                WHERE email = :email
            """),
            {"email": email},
        ).fetchone()

    if user_row is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User not found",
        )

    class UserProxy:
        def __init__(self, user_id, org_id, email, hashed_password, role, is_admin, created_at, fathom_api_key=None):
            self.id = user_id
            self.org_id = org_id
            self.email = email
            self.hashed_password = hashed_password
            self.role_str = role  # Store raw role string to avoid enum conversion
            self.is_admin = is_admin
            self.created_at = created_at
            self.fathom_api_key = fathom_api_key
            self.role = parse_user_role_from_db(role)

    user = UserProxy(
        user_row[0],  # id
        user_row[1],  # org_id
        user_row[2],  # email
        user_row[3],  # hashed_password
        user_row[4],  # role
        user_row[5],  # is_admin
        user_row[6],  # created_at
        user_row[7] if len(user_row) > 7 else None,  # fathom_api_key
    )

    selected_org_id = user.org_id  # Default to user's primary org
    if org_id_from_token:
        try:
            org_id_uuid = uuid.UUID(org_id_from_token)
        except (ValueError, TypeError):
            _logger.warning("Invalid org_id format in token: %s", org_id_from_token)
            org_id_uuid = None

        if org_id_uuid:
            from app.services.org_user_context import user_has_email_org_access

            has_access = (
                str(user.org_id) == str(org_id_uuid)
                or user_has_email_org_access(db, user.email, org_id_uuid)
            )
            if not has_access:
                _logger.warning("User %s denied access to org %s", user.id, org_id_uuid)
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="User does not have access to this organization",
                )

            selected_org_id = org_id_uuid
            _logger.debug("User %s accessing org %s", user.id, selected_org_id)

    user.selected_org_id = selected_org_id

    from app.services.org_user_context import apply_selected_org_user_context

    apply_selected_org_user_context(user, db, selected_org_id)
    return user


def resolve_org_and_user_ids_for_checkin_sync(db: Session, token: str) -> tuple[uuid.UUID, uuid.UUID]:
    """(selected_org_id, user_id) with same access rules as get_current_user — for use inside a short-lived session."""
    user = _user_from_token(db, token)
    sid = user.selected_org_id
    uid = user.id
    if not isinstance(sid, uuid.UUID):
        sid = uuid.UUID(str(sid))
    if not isinstance(uid, uuid.UUID):
        uid = uuid.UUID(str(uid))
    return sid, uid


def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: Session = Depends(get_db)
) -> User:
    """
    Get current authenticated user and verify org_id matches token.
    This enforces org isolation: users can only access data from their org.
    Session is not bound to IP so users are not forced to re-login on IP change
    (e.g. switching networks or VPN); IP may still be logged for audit elsewhere.
    """
    return _user_from_token(db, credentials.credentials)


def get_selected_org_id(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
) -> uuid.UUID:
    """
    Get the selected org_id from the token (the org the user selected at login).
    Falls back to user.org_id if token doesn't have org_id.
    Use this in endpoints that need to filter by the selected org.
    """
    token = credentials.credentials
    payload = decode_access_token(token)
    if payload:
        org_id_from_token = payload.get("org_id")
        if org_id_from_token:
            try:
                selected_org_id = uuid.UUID(org_id_from_token)
                from app.services.org_user_context import user_has_email_org_access

                has_access = (
                    str(user.org_id) == str(selected_org_id)
                    or user_has_email_org_access(db, user.email, selected_org_id)
                )
                if has_access:
                    return selected_org_id
            except (ValueError, TypeError):
                pass
    
    # Fall back to user's org_id
    return user.org_id


def require_org_access(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
) -> uuid.UUID:
    """
    Dependency to extract and return org_id from token (selected org).
    Use this in endpoints that need to filter by org_id.
    Falls back to user.org_id if token doesn't have org_id.
    """
    return get_selected_org_id(credentials, user)


# Main org ID (default org created in migration)
MAIN_ORG_ID = uuid.UUID("00000000-0000-0000-0000-000000000001")


def require_admin(
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
) -> User:
    """
    Dependency to ensure user is an admin/owner AND is in the main org.
    Only users in the main org (Sweep Internal) can manage other organizations.
    """
    from app.core.config import settings
    
    # Check if user is the sudo admin (always allowed)
    if user.email == settings.SUDO_ADMIN_EMAIL:
        return user
    
    # User must be in the main org to manage other orgs
    if str(user.org_id) != str(MAIN_ORG_ID):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only users in the main organization can manage other organizations"
        )
    
    # Check if user has admin or owner role
    if user.role in (UserRole.ADMIN, UserRole.OWNER) or user.is_admin:
        return user
    
    raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="Admin access required"
    )


def require_admin_or_owner(
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
) -> User:
    """
    Dependency to ensure user is an admin or owner (for org-level permissions).
    This is less restrictive than require_admin - it allows admins/owners in any org,
    not just the main org. Use this for org-scoped operations like managing integrations.
    """
    from app.services.org_user_context import user_can_manage_org_integrations

    if not user_can_manage_org_integrations(user, db):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin or owner access required. Members cannot manage integrations.",
        )
    return user


def is_sudo_admin(user: User) -> bool:
    """True when the user is the configured platform sudo administrator."""
    from app.core.config import settings

    email = (getattr(user, "email", None) or "").strip().lower()
    sudo = (getattr(settings, "SUDO_ADMIN_EMAIL", None) or "").strip().lower()
    return bool(email and sudo and email == sudo)


def user_is_system_owner(user: User, db: Session) -> bool:
    """
    True for Sweep platform operators: sudo admin, or OWNER/ADMIN in Sweep Internal.
    Main-org membership alone (e.g. member) does not grant system-owner access.
    Independent of which org is currently selected in the JWT.
    """
    from app.core.config import settings
    from app.services.org_user_context import fetch_user_row_for_org

    email = (getattr(user, "email", None) or "").strip().lower()
    if not email:
        return False
    sudo = (getattr(settings, "SUDO_ADMIN_EMAIL", None) or "").strip().lower()
    if sudo and email == sudo:
        return True

    row = fetch_user_row_for_org(db, email, MAIN_ORG_ID)
    if not row:
        return False
    role = parse_user_role_from_db(row[4])
    return role in (UserRole.OWNER, UserRole.ADMIN)


def _tab_scope_org_id(user: User) -> uuid.UUID:
    """Org whose tab permissions apply — selected org from JWT, not the user's primary row."""
    raw = getattr(user, "selected_org_id", None) or user.org_id
    if isinstance(raw, uuid.UUID):
        return raw
    return uuid.UUID(str(raw))


def check_tab_access(
    tab_name: str,
    user: User,
    db: Session
) -> bool:
    """
    Check if a user has access to a specific tab.
    Returns True if user has access, False otherwise.
    
    Logic:
    1. Role-based restrictions:
       - 'owner' tab: Only OWNER role (and main org users)
    2. Users in main org always have access to all tabs (including owner)
    3. Check user-specific permissions first (overrides org permissions)
    4. Check organization-level permissions
    5. Default: all tabs enabled for new orgs (except role-restricted tabs)
    """
    scope_org_id = _tab_scope_org_id(user)

    # Resources (docs + org library) are available to every org member.
    if tab_name == "resources":
        return True

    # Role-based restrictions
    # 'owner' tab: Only OWNER role can access (unless in main org)
    if tab_name == 'owner':
        if str(scope_org_id) == str(MAIN_ORG_ID):
            return True  # Main org users always have access
        return user.role == UserRole.OWNER

    # Main org users always have access to all other tabs
    if str(scope_org_id) == str(MAIN_ORG_ID):
        return True

    # Finances UI uses the same permission rows as the legacy "stripe" tab name in DB
    lookup_tab = "stripe" if tab_name == "finances" else tab_name
    
    try:
        # Check user-specific permissions first
        user_permission = db.query(UserTabPermission).filter(
            UserTabPermission.user_id == user.id,
            UserTabPermission.tab_name == lookup_tab
        ).first()
        
        if user_permission is not None:
            return user_permission.enabled
        
        # Check organization-level permissions
        org_permission = db.query(OrganizationTabPermission).filter(
            OrganizationTabPermission.org_id == scope_org_id,
            OrganizationTabPermission.tab_name == lookup_tab
        ).first()
        
        if org_permission is not None:
            return org_permission.enabled
    except Exception as e:
        # If tables don't exist yet (migration not run), default to enabled
        # This allows the backend to start even before migrations are run
        import logging
        logging.warning(f"Tab permissions tables may not exist yet: {e}. Defaulting to enabled.")
    
    # Default: enable all tabs for existing orgs (backward compatibility)
    # For new orgs, we'll set defaults when creating them
    return True


def get_user_tab_permissions(
    user: User,
    db: Session
) -> dict[str, bool]:
    """
    Get all tab permissions for a user.
    Returns a dictionary mapping tab names to access booleans.
    """
    from app.api.users import AVAILABLE_TABS
    
    permissions = {}
    for tab in AVAILABLE_TABS:
        permissions[tab] = check_tab_access(tab, user, db)
    permissions["finances"] = check_tab_access("finances", user, db)
    
    return permissions

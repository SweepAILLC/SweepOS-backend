from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy.orm import Session
from typing import Optional
from app.db.session import get_db
from app.models.user import User, UserRole
from app.models.organization_tab_permission import OrganizationTabPermission
from app.models.user_tab_permission import UserTabPermission
from app.core.security import decode_access_token
import uuid

security = HTTPBearer()


def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: Session = Depends(get_db)
) -> User:
    """
    Get current authenticated user and verify org_id matches token.
    This enforces org isolation: users can only access data from their org.
    """
    token = credentials.credentials
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
                    SELECT id, org_id, email, hashed_password, role, is_admin, created_at
                    FROM users
                    WHERE id = :user_id
                """),
                {"user_id": str(user_id_uuid)}
            ).fetchone()
        except (ValueError, TypeError):
            user_row = None
    else:
        user_row = None
    if user_row is None:
        user_row = db.execute(
            text("""
                SELECT id, org_id, email, hashed_password, role, is_admin, created_at
                FROM users
                WHERE email = :email
            """),
            {"email": email}
        ).fetchone()
    
    if user_row is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User not found",
        )
    
    # Create a User-like object from the raw SQL result
    # We need to avoid SQLAlchemy enum conversion, so we'll create a proxy object
    class UserProxy:
        def __init__(self, user_id, org_id, email, hashed_password, role, is_admin, created_at):
            self.id = user_id
            self.org_id = org_id
            self.email = email
            self.hashed_password = hashed_password
            self.role_str = role  # Store raw role string to avoid enum conversion
            self.is_admin = is_admin
            self.created_at = created_at
            # Create a role property that returns a UserRole enum when accessed
            # Map database enum value to Python enum
            role_lower = role.lower() if role else "admin"
            if role == "member" or role == "MEMBER":
                role_lower = "member"
            elif role == "OWNER":
                role_lower = "owner"
            elif role == "ADMIN":
                role_lower = "admin"
            try:
                from app.models.user import UserRole
                self.role = UserRole(role_lower)
            except ValueError:
                # Fallback to ADMIN if role doesn't match
                self.role = UserRole.ADMIN
    
    user = UserProxy(
        user_row[0],  # id
        user_row[1],  # org_id
        user_row[2],  # email
        user_row[3],  # hashed_password
        user_row[4],  # role
        user_row[5],  # is_admin
        user_row[6]   # created_at
    )
    
    # Verify org_id matches (if present in token) and set selected_org_id attribute
    selected_org_id = user.org_id  # Default to user's primary org
    if org_id_from_token:
        # Convert org_id_from_token to UUID for proper comparison
        try:
            org_id_uuid = uuid.UUID(org_id_from_token)
        except (ValueError, TypeError) as e:
            # Invalid UUID format in token - log and use primary org
            print(f"[AUTH] Invalid org_id format in token: {org_id_from_token}, error: {e}")
            org_id_uuid = None
        
        if org_id_uuid:
            # Check if user has access to this org via UserOrganization table
            from app.models.user_organization import UserOrganization
            user_org = db.query(UserOrganization).filter(
                UserOrganization.user_id == user.id,
                UserOrganization.org_id == org_id_uuid
            ).first()
            
            # Also check if user.org_id matches (backward compatibility)
            if not user_org and user.org_id != org_id_uuid:
                print(f"[AUTH] User {user.id} does not have access to org {org_id_uuid}. User's primary org: {user.org_id}")
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="User does not have access to this organization",
                )
            
            # User has access - use the selected org from token
            selected_org_id = org_id_uuid
            print(f"[AUTH] User {user.id} accessing org {selected_org_id} (primary org: {user.org_id})")
    
    # Set selected_org_id as an attribute on the user object
    # This allows endpoints to access the selected org without needing a separate dependency
    user.selected_org_id = selected_org_id
    
    return user


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
                # Verify user has access to this org
                from app.models.user_organization import UserOrganization
                user_org = db.query(UserOrganization).filter(
                    UserOrganization.user_id == user.id,
                    UserOrganization.org_id == selected_org_id
                ).first()
                if user_org or str(user.org_id) == str(selected_org_id):
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
    if user.role.value in ['admin', 'owner'] or user.is_admin:
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
    from app.core.config import settings
    from app.models.user import UserRole
    
    # Check if user is the sudo admin (always allowed)
    if user.email == settings.SUDO_ADMIN_EMAIL:
        return user
    
    # Check if user has admin or owner role (or is_admin flag)
    # Handle both enum and string role values
    role_value = user.role.value if hasattr(user.role, 'value') else str(user.role)
    if role_value in ['admin', 'owner'] or user.is_admin or user.role == UserRole.ADMIN or user.role == UserRole.OWNER:
        return user
    
    raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="Admin or owner access required. Members cannot manage integrations."
    )


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
       - 'users' tab: Not accessible to MEMBER role
    2. Users in main org always have access to all tabs (including owner)
    3. Check user-specific permissions first (overrides org permissions)
    4. Check organization-level permissions
    5. Default: all tabs enabled for new orgs (except role-restricted tabs)
    """
    # Role-based restrictions
    # 'owner' tab: Only OWNER role can access (unless in main org)
    if tab_name == 'owner':
        if str(user.org_id) == str(MAIN_ORG_ID):
            return True  # Main org users always have access
        return user.role == UserRole.OWNER
    
    # 'users' tab: MEMBER role cannot access
    if tab_name == 'users':
        if user.role == UserRole.MEMBER:
            return False
    
    # Main org users always have access to all other tabs
    if str(user.org_id) == str(MAIN_ORG_ID):
        return True
    
    try:
        # Check user-specific permissions first
        user_permission = db.query(UserTabPermission).filter(
            UserTabPermission.user_id == user.id,
            UserTabPermission.tab_name == tab_name
        ).first()
        
        if user_permission is not None:
            return user_permission.enabled
        
        # Check organization-level permissions
        org_permission = db.query(OrganizationTabPermission).filter(
            OrganizationTabPermission.org_id == user.org_id,
            OrganizationTabPermission.tab_name == tab_name
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
    
    return permissions

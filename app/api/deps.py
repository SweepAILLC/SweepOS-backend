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
    
    if email is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication credentials",
        )
    
    user = db.query(User).filter(User.email == email).first()
    if user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User not found",
        )
    
    # Verify org_id matches (if present in token)
    if org_id_from_token:
        if str(user.org_id) != org_id_from_token:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="User organization mismatch",
            )
    
    return user


def require_org_access(user: User = Depends(get_current_user)) -> uuid.UUID:
    """
    Dependency to extract and return org_id from current user.
    Use this in endpoints that need to filter by org_id.
    """
    return user.org_id


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

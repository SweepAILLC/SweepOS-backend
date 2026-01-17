"""
User Management API
Allows organization admins to manage users within their organization.
"""
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from sqlalchemy import func
from typing import List, Optional
from uuid import UUID
import secrets
import string

from app.db.session import get_db
from app.api.deps import get_current_user, check_tab_access, get_user_tab_permissions
from app.models.user import User, UserRole
from app.models.organization import Organization
from app.models.organization_tab_permission import OrganizationTabPermission
from app.models.user_tab_permission import UserTabPermission
from app.core.security import get_password_hash
from app.schemas.user import User as UserSchema, UserCreate, UserUpdate
from app.schemas.permission import (
    OrganizationTabPermission as OrgTabPermissionSchema,
    OrganizationTabPermissionCreate,
    OrganizationTabPermissionUpdate,
    UserTabPermission as UserTabPermissionSchema,
    UserTabPermissionCreate,
    UserTabPermissionUpdate,
    TabAccessResponse
)

router = APIRouter()

# Available tabs
# Note: 'owner' tab is restricted to OWNER role only
# 'users' tab is restricted from MEMBER role
AVAILABLE_TABS = ['brevo', 'clients', 'stripe', 'funnels', 'users', 'owner']


@router.get("", response_model=List[UserSchema])
def list_users(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """List all users in the current user's organization"""
    # Only admins/owners can list users
    if current_user.role not in [UserRole.ADMIN, UserRole.OWNER] and not current_user.is_admin:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only admins can list users"
        )
    
    users = db.query(User).filter(
        User.org_id == current_user.org_id
    ).order_by(User.created_at.desc()).all()
    
    # Convert to schema with proper enum handling
    result = []
    for user in users:
        user_dict = {
            "id": user.id,
            "org_id": user.org_id,
            "email": user.email,
            "role": user.role.value if hasattr(user.role, 'value') else str(user.role),
            "is_admin": user.is_admin,
            "created_at": user.created_at
        }
        result.append(UserSchema(**user_dict))
    
    return result


@router.post("", response_model=UserSchema, status_code=status.HTTP_201_CREATED)
def create_user(
    user_data: UserCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Create a new user in the current user's organization"""
    # Only admins/owners can create users
    if current_user.role not in [UserRole.ADMIN, UserRole.OWNER] and not current_user.is_admin:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only admins can create users"
        )
    
    # Check if email already exists in this org
    existing_user = db.query(User).filter(
        User.email == user_data.email,
        User.org_id == current_user.org_id
    ).first()
    
    if existing_user:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="User with this email already exists in this organization"
        )
    
    # Generate password if not provided
    password = user_data.password
    if not password:
        alphabet = string.ascii_letters + string.digits + "!@#$%^&*"
        password = ''.join(secrets.choice(alphabet) for _ in range(12))
    
    # Determine role for new user
    user_role = UserRole.MEMBER  # Default to member
    if hasattr(user_data, 'role') and user_data.role:
        try:
            user_role = UserRole(user_data.role.lower())
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid role: {user_data.role}. Must be one of: owner, admin, member"
            )
    
    # Only OWNER can assign OWNER role
    if user_role == UserRole.OWNER and current_user.role != UserRole.OWNER:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only owners can assign the owner role"
        )
    
    # Create user
    new_user = User(
        org_id=current_user.org_id,
        email=user_data.email,
        hashed_password=get_password_hash(password),
        role=user_role,
        is_admin=(user_role in [UserRole.ADMIN, UserRole.OWNER])  # Set is_admin for backward compatibility
    )
    db.add(new_user)
    db.commit()
    db.refresh(new_user)
    
    # Return user with password (only on creation)
    user_dict = {
        "id": new_user.id,
        "org_id": new_user.org_id,
        "email": new_user.email,
        "role": new_user.role.value if hasattr(new_user.role, 'value') else str(new_user.role),
        "is_admin": new_user.is_admin,
        "created_at": new_user.created_at
    }
    result = UserSchema(**user_dict)
    
    # Include password in response for display (only on creation)
    result_dict = result.model_dump()
    result_dict['password'] = password
    return result_dict


@router.get("/{user_id}", response_model=UserSchema)
def get_user(
    user_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get a specific user in the current user's organization"""
    # Only admins/owners can view users
    if current_user.role not in [UserRole.ADMIN, UserRole.OWNER] and not current_user.is_admin:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only admins can view users"
        )
    
    user = db.query(User).filter(
        User.id == user_id,
        User.org_id == current_user.org_id
    ).first()
    
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )
    
    user_dict = {
        "id": user.id,
        "org_id": user.org_id,
        "email": user.email,
        "role": user.role.value if hasattr(user.role, 'value') else str(user.role),
        "is_admin": user.is_admin,
        "created_at": user.created_at
    }
    return UserSchema(**user_dict)


@router.patch("/{user_id}", response_model=UserSchema)
def update_user(
    user_id: UUID,
    user_update: UserUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Update a user in the current user's organization"""
    # Only admins/owners can update users
    if current_user.role not in [UserRole.ADMIN, UserRole.OWNER] and not current_user.is_admin:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only admins can update users"
        )
    
    user = db.query(User).filter(
        User.id == user_id,
        User.org_id == current_user.org_id
    ).first()
    
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )
    
    # Prevent users from modifying themselves (they should use /auth/me/settings)
    # Exception: allow role changes (but with restrictions)
    if user.id == current_user.id and user_update.role is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Use /auth/me/settings to update your own account"
        )
    
    # Role change restrictions
    if user_update.role is not None:
        new_role_str = user_update.role.lower()
        try:
            new_role = UserRole(new_role_str)
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid role: {user_update.role}. Must be one of: owner, admin, member"
            )
        
        # Only OWNER can assign OWNER role
        if new_role == UserRole.OWNER and current_user.role != UserRole.OWNER:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Only owners can assign the owner role"
            )
        
        # Prevent demoting the last owner in an org
        if user.role == UserRole.OWNER and new_role != UserRole.OWNER:
            owner_count = db.query(func.count(User.id)).filter(
                User.org_id == current_user.org_id,
                User.role == UserRole.OWNER
            ).scalar() or 0
            if owner_count <= 1:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Cannot demote the last owner in the organization"
                )
    
    # Update email if provided
    if user_update.email is not None:
        # Check if email is already taken in this org
        existing_user = db.query(User).filter(
            User.email == user_update.email,
            User.org_id == current_user.org_id,
            User.id != user_id
        ).first()
        if existing_user:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Email already in use in this organization"
            )
        user.email = user_update.email
    
    # Update password if provided
    if user_update.password is not None:
        user.hashed_password = get_password_hash(user_update.password)
    
    # Update role if provided
    if user_update.role is not None:
        new_role_str = user_update.role.lower()
        try:
            new_role = UserRole(new_role_str)
            user.role = new_role
            # Update is_admin flag for backward compatibility
            user.is_admin = (new_role in [UserRole.ADMIN, UserRole.OWNER])
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid role: {user_update.role}. Must be one of: owner, admin, member"
            )
    
    db.commit()
    db.refresh(user)
    
    user_dict = {
        "id": user.id,
        "org_id": user.org_id,
        "email": user.email,
        "role": user.role.value if hasattr(user.role, 'value') else str(user.role),
        "is_admin": user.is_admin,
        "created_at": user.created_at
    }
    return UserSchema(**user_dict)


@router.delete("/{user_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_user(
    user_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Delete a user from the current user's organization"""
    # Only admins/owners can delete users
    if current_user.role not in [UserRole.ADMIN, UserRole.OWNER] and not current_user.is_admin:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only admins can delete users"
        )
    
    user = db.query(User).filter(
        User.id == user_id,
        User.org_id == current_user.org_id
    ).first()
    
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )
    
    # Prevent users from deleting themselves
    if user.id == current_user.id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot delete your own account"
        )
    
    db.delete(user)
    db.commit()
    return None


# Tab Permissions Endpoints
@router.get("/tabs/access", response_model=dict[str, bool])
def get_my_tab_permissions(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get current user's tab permissions"""
    return get_user_tab_permissions(current_user, db)


@router.get("/tabs/{tab_name}/access", response_model=TabAccessResponse)
def check_my_tab_access(
    tab_name: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Check if current user has access to a specific tab"""
    has_access = check_tab_access(tab_name, current_user, db)
    return TabAccessResponse(
        tab_name=tab_name,
        has_access=has_access,
        reason=None if has_access else "This feature requires additional permissions. Please contact the Sweep OS team."
    )


# User Tab Permissions (for admins managing their team)
@router.get("/{user_id}/tabs", response_model=List[UserTabPermissionSchema])
def get_user_tab_permissions_list(
    user_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get tab permissions for a specific user (admin only)"""
    # Only admins/owners can view user permissions
    if current_user.role not in [UserRole.ADMIN, UserRole.OWNER] and not current_user.is_admin:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only admins can view user permissions"
        )
    
    # Verify user belongs to same org
    user = db.query(User).filter(
        User.id == user_id,
        User.org_id == current_user.org_id
    ).first()
    
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )
    
    permissions = db.query(UserTabPermission).filter(
        UserTabPermission.user_id == user_id
    ).all()
    
    return [UserTabPermissionSchema.model_validate(p, from_attributes=True) for p in permissions]


@router.post("/{user_id}/tabs", response_model=UserTabPermissionSchema, status_code=status.HTTP_201_CREATED)
def create_user_tab_permission(
    user_id: UUID,
    permission_data: UserTabPermissionCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Create or update tab permission for a user (admin only)"""
    # Only admins/owners can manage user permissions
    if current_user.role not in [UserRole.ADMIN, UserRole.OWNER] and not current_user.is_admin:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only admins can manage user permissions"
        )
    
    # Verify user belongs to same org
    user = db.query(User).filter(
        User.id == user_id,
        User.org_id == current_user.org_id
    ).first()
    
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )
    
    # Check if permission already exists
    existing = db.query(UserTabPermission).filter(
        UserTabPermission.user_id == user_id,
        UserTabPermission.tab_name == permission_data.tab_name
    ).first()
    
    if existing:
        existing.enabled = permission_data.enabled
        db.commit()
        db.refresh(existing)
        return UserTabPermissionSchema.model_validate(existing, from_attributes=True)
    
    # Create new permission
    permission = UserTabPermission(
        user_id=user_id,
        tab_name=permission_data.tab_name,
        enabled=permission_data.enabled
    )
    db.add(permission)
    db.commit()
    db.refresh(permission)
    return UserTabPermissionSchema.model_validate(permission, from_attributes=True)


@router.patch("/{user_id}/tabs/{tab_name}", response_model=UserTabPermissionSchema)
def update_user_tab_permission(
    user_id: UUID,
    tab_name: str,
    permission_update: UserTabPermissionUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Update tab permission for a user (admin only)"""
    # Only admins/owners can manage user permissions
    if current_user.role not in [UserRole.ADMIN, UserRole.OWNER] and not current_user.is_admin:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only admins can manage user permissions"
        )
    
    # Verify user belongs to same org
    user = db.query(User).filter(
        User.id == user_id,
        User.org_id == current_user.org_id
    ).first()
    
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )
    
    permission = db.query(UserTabPermission).filter(
        UserTabPermission.user_id == user_id,
        UserTabPermission.tab_name == tab_name
    ).first()
    
    if not permission:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Permission not found"
        )
    
    if permission_update.enabled is not None:
        permission.enabled = permission_update.enabled
    
    db.commit()
    db.refresh(permission)
    return UserTabPermissionSchema.model_validate(permission, from_attributes=True)


@router.delete("/{user_id}/tabs/{tab_name}", status_code=status.HTTP_204_NO_CONTENT)
def delete_user_tab_permission(
    user_id: UUID,
    tab_name: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Delete user-specific tab permission (falls back to org permissions)"""
    # Only admins/owners can manage user permissions
    if current_user.role not in [UserRole.ADMIN, UserRole.OWNER] and not current_user.is_admin:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only admins can manage user permissions"
        )
    
    # Verify user belongs to same org
    user = db.query(User).filter(
        User.id == user_id,
        User.org_id == current_user.org_id
    ).first()
    
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )
    
    permission = db.query(UserTabPermission).filter(
        UserTabPermission.user_id == user_id,
        UserTabPermission.tab_name == tab_name
    ).first()
    
    if permission:
        db.delete(permission)
        db.commit()
    
    return None


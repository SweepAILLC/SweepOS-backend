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
    
    # Get selected org_id from user object (set by get_current_user)
    org_id = getattr(current_user, 'selected_org_id', current_user.org_id)
    
    # Use raw SQL to read users to avoid SQLAlchemy enum conversion issues
    # Some users may have lowercase 'member' while enum expects uppercase 'MEMBER'
    from sqlalchemy import text
    users_result = db.execute(
        text("""
            SELECT id, org_id, email, role, is_admin, created_at
            FROM users
            WHERE org_id = :org_id
            ORDER BY created_at DESC
        """),
        {"org_id": org_id}
    ).fetchall()
    
    # Convert to schema with proper enum handling
    result = []
    for row in users_result:
        # Map database enum values (uppercase) to Python enum values (lowercase)
        role_db_value = row[3]  # role column
        role_python_value = role_db_value.lower() if role_db_value else "admin"
        
        # Handle case where database might have lowercase 'member' from old data
        if role_db_value == "member":
            role_python_value = "member"
        elif role_db_value == "MEMBER":
            role_python_value = "member"
        elif role_db_value == "OWNER":
            role_python_value = "owner"
        elif role_db_value == "ADMIN":
            role_python_value = "admin"
        
        user_dict = {
            "id": row[0],  # id
            "org_id": row[1],  # org_id
            "email": row[2],  # email
            "role": role_python_value,  # role (converted to lowercase)
            "is_admin": row[4],  # is_admin
            "created_at": row[5]  # created_at
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
    
    # Get selected org_id from user object (set by get_current_user)
    org_id = getattr(current_user, 'selected_org_id', current_user.org_id)
    
    # Check if email already exists in this org
    existing_user = db.query(User).filter(
        User.email == user_data.email,
        User.org_id == org_id
    ).first()
    
    if existing_user:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="User with this email already exists in this organization"
        )

    org = db.query(Organization).filter(Organization.id == org_id).first()
    if org and org.max_user_seats is not None:
        current_count = db.query(func.count(User.id)).filter(User.org_id == org_id).scalar() or 0
        if current_count >= org.max_user_seats:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Organization user limit reached ({org.max_user_seats} seats). Contact your system owner to increase the limit.",
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
    # Use org_id from selected org (not current_user.org_id)
    # Use raw SQL to insert role value directly to avoid SQLAlchemy enum name conversion
    from sqlalchemy import text
    import uuid as uuid_lib
    user_id = uuid_lib.uuid4()
    
    # Query database to get actual enum values
    enum_values_result = db.execute(
        text("SELECT unnest(enum_range(NULL::userrole))")
    ).fetchall()
    enum_values = [str(row[0]) for row in enum_values_result]
    
    # Map Python enum to actual database enum value
    if user_role == UserRole.OWNER:
        role_db_value = "OWNER"  # Always uppercase
    elif user_role == UserRole.ADMIN:
        role_db_value = "ADMIN"  # Always uppercase
    elif user_role == UserRole.MEMBER:
        # Check which case exists in database
        if "member" in enum_values:
            role_db_value = "member"  # Lowercase
        elif "MEMBER" in enum_values:
            role_db_value = "MEMBER"  # Uppercase
        else:
            # If neither exists, try to add it (migration may not have run)
            # For now, default to ADMIN and log warning
            print(f"[WARNING] MEMBER role not found in database enum. Available values: {enum_values}. Defaulting to ADMIN.")
            role_db_value = "ADMIN"
            user_role = UserRole.ADMIN  # Update to match
    else:
        role_db_value = "ADMIN"  # Default fallback
    
    db.execute(
        text("""
            INSERT INTO users (id, org_id, email, hashed_password, role, is_admin, created_at)
            VALUES (:id, :org_id, :email, :hashed_password, CAST(:role AS userrole), :is_admin, NOW())
        """),
        {
            "id": user_id,
            "org_id": org_id,
            "email": user_data.email,
            "hashed_password": get_password_hash(password),
            "role": role_db_value,
            "is_admin": (user_role in [UserRole.ADMIN, UserRole.OWNER])
        }
    )
    db.commit()
    
    # Fetch the created user using raw SQL to avoid enum conversion issues
    user_row = db.execute(
        text("""
            SELECT id, org_id, email, role, is_admin, created_at
            FROM users
            WHERE id = :user_id
        """),
        {"user_id": user_id}
    ).fetchone()
    
    # Create user_organization record for multi-org support
    from app.models.user_organization import UserOrganization
    user_org = UserOrganization(
        user_id=user_id,
        org_id=org_id,
        is_primary=True  # First org is always primary
    )
    db.add(user_org)
    db.commit()
    
    # Map database enum values to Python enum values
    role_db_value = user_row[3]  # role column
    role_python_value = role_db_value.lower() if role_db_value else "admin"
    if role_db_value == "member" or role_db_value == "MEMBER":
        role_python_value = "member"
    elif role_db_value == "OWNER":
        role_python_value = "owner"
    elif role_db_value == "ADMIN":
        role_python_value = "admin"
    
    # Return user with password (only on creation)
    user_dict = {
        "id": user_row[0],  # id
        "org_id": user_row[1],  # org_id
        "email": user_row[2],  # email
        "role": role_python_value,  # role (converted to lowercase)
        "is_admin": user_row[4],  # is_admin
        "created_at": user_row[5]  # created_at
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
    
    # Get selected org_id from user object (set by get_current_user)
    org_id = getattr(current_user, 'selected_org_id', current_user.org_id)
    
    # Use raw SQL to read user to avoid enum conversion issues
    from sqlalchemy import text
    user_row = db.execute(
        text("""
            SELECT id, org_id, email, role, is_admin, created_at
            FROM users
            WHERE id = :user_id AND org_id = :org_id
        """),
        {
            "user_id": user_id,
            "org_id": org_id
        }
    ).fetchone()
    
    if not user_row:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )
    
    # Map database enum values to Python enum values
    role_db_value = user_row[3]  # role column
    role_python_value = role_db_value.lower() if role_db_value else "admin"
    if role_db_value == "member" or role_db_value == "MEMBER":
        role_python_value = "member"
    elif role_db_value == "OWNER":
        role_python_value = "owner"
    elif role_db_value == "ADMIN":
        role_python_value = "admin"
    
    user_dict = {
        "id": user_row[0],  # id
        "org_id": user_row[1],  # org_id
        "email": user_row[2],  # email
        "role": role_python_value,  # role (converted to lowercase)
        "is_admin": user_row[4],  # is_admin
        "created_at": user_row[5]  # created_at
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
    
    # Get selected org_id from user object (set by get_current_user)
    org_id = getattr(current_user, 'selected_org_id', current_user.org_id)
    
    # Use raw SQL to read user to avoid enum conversion issues
    from sqlalchemy import text
    user_row = db.execute(
        text("""
            SELECT id, org_id, email, role, is_admin, created_at
            FROM users
            WHERE id = :user_id AND org_id = :org_id
        """),
        {
            "user_id": user_id,
            "org_id": org_id
        }
    ).fetchone()
    
    if not user_row:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )
    
    # Prevent users from modifying themselves (they should use /auth/me/settings)
    # Exception: allow role changes (but with restrictions)
    if user_id == current_user.id and user_update.role is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Use /auth/me/settings to update your own account"
        )
    
    # Get current role from database (handle both uppercase and lowercase)
    current_role_db = user_row[3]  # role column
    current_role_python = current_role_db.lower() if current_role_db else "admin"
    if current_role_db == "member" or current_role_db == "MEMBER":
        current_role_python = "member"
    elif current_role_db == "OWNER":
        current_role_python = "owner"
    elif current_role_db == "ADMIN":
        current_role_python = "admin"
    
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
        if current_role_python == "owner" and new_role != UserRole.OWNER:
            owner_count_result = db.execute(
                text("""
                    SELECT COUNT(*) FROM users
                    WHERE org_id = :org_id AND role = 'OWNER'
                """),
                {"org_id": org_id}
            ).scalar()
            owner_count = owner_count_result or 0
            if owner_count <= 1:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Cannot demote the last owner in the organization"
                )
    
    # Build update query dynamically
    update_fields = []
    update_params = {"user_id": user_id, "org_id": org_id}
    
    # Update email if provided
    if user_update.email is not None:
        # Check if email is already taken in this org
        existing_user_result = db.execute(
            text("""
                SELECT id FROM users
                WHERE email = :email AND org_id = :org_id AND id != :user_id
            """),
            {
                "email": user_update.email,
                "org_id": org_id,
                "user_id": user_id
            }
        ).fetchone()
        if existing_user_result:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Email already in use in this organization"
            )
        update_fields.append("email = :email")
        update_params["email"] = user_update.email
    
    # Update password if provided
    if user_update.password is not None:
        update_fields.append("hashed_password = :hashed_password")
        update_params["hashed_password"] = get_password_hash(user_update.password)
    
    # Update role if provided
    if user_update.role is not None:
        new_role_str = user_update.role.lower()
        new_role = UserRole(new_role_str)
        # Query database to get actual enum values
        enum_values_result = db.execute(
            text("SELECT unnest(enum_range(NULL::userrole))")
        ).fetchall()
        enum_values = [str(row[0]) for row in enum_values_result]
        
        # Map Python enum to actual database enum value
        if new_role == UserRole.OWNER:
            role_db_value = "OWNER"  # Always uppercase
        elif new_role == UserRole.ADMIN:
            role_db_value = "ADMIN"  # Always uppercase
        elif new_role == UserRole.MEMBER:
            # Check which case exists in database
            if "member" in enum_values:
                role_db_value = "member"  # Lowercase
            elif "MEMBER" in enum_values:
                role_db_value = "MEMBER"  # Uppercase
            else:
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail="MEMBER role not found in database. Please run migration 012."
                )
        else:
            role_db_value = "ADMIN"  # Default fallback
        
        update_fields.append("role = CAST(:role AS userrole)")
        update_params["role"] = role_db_value
        # Update is_admin flag for backward compatibility
        update_fields.append("is_admin = :is_admin")
        update_params["is_admin"] = (new_role in [UserRole.ADMIN, UserRole.OWNER])
    
    # Execute update if there are fields to update
    if update_fields:
        update_query = f"""
            UPDATE users
            SET {', '.join(update_fields)}
            WHERE id = :user_id AND org_id = :org_id
        """
        db.execute(text(update_query), update_params)
        db.commit()
    
    # Fetch updated user
    updated_user_row = db.execute(
        text("""
            SELECT id, org_id, email, role, is_admin, created_at
            FROM users
            WHERE id = :user_id AND org_id = :org_id
        """),
        {
            "user_id": user_id,
            "org_id": org_id
        }
    ).fetchone()
    
    # Map database enum values to Python enum values
    role_db_value = updated_user_row[3]  # role column
    role_python_value = role_db_value.lower() if role_db_value else "admin"
    if role_db_value == "member" or role_db_value == "MEMBER":
        role_python_value = "member"
    elif role_db_value == "OWNER":
        role_python_value = "owner"
    elif role_db_value == "ADMIN":
        role_python_value = "admin"
    
    user_dict = {
        "id": updated_user_row[0],  # id
        "org_id": updated_user_row[1],  # org_id
        "email": updated_user_row[2],  # email
        "role": role_python_value,  # role (converted to lowercase)
        "is_admin": updated_user_row[4],  # is_admin
        "created_at": updated_user_row[5]  # created_at
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
    
    # Get selected org_id from user object (set by get_current_user)
    org_id = getattr(current_user, 'selected_org_id', current_user.org_id)
    
    # Prevent users from deleting themselves
    if user_id == current_user.id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot delete your own account"
        )
    
    # Clear audit_logs references so FK does not block delete (user_id is nullable)
    from sqlalchemy import text
    db.execute(
        text("UPDATE audit_logs SET user_id = NULL WHERE user_id = :user_id"),
        {"user_id": user_id}
    )
    # Now delete the user
    result = db.execute(
        text("""
            DELETE FROM users
            WHERE id = :user_id AND org_id = :org_id
            RETURNING id
        """),
        {
            "user_id": user_id,
            "org_id": org_id
        }
    ).fetchone()

    if not result:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )
    
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
    
    # Get selected org_id from user object (set by get_current_user)
    org_id = getattr(current_user, 'selected_org_id', current_user.org_id)
    
    # Verify user belongs to same org
    user = db.query(User).filter(
        User.id == user_id,
        User.org_id == org_id
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
    
    # Get selected org_id from user object (set by get_current_user)
    org_id = getattr(current_user, 'selected_org_id', current_user.org_id)
    
    # Verify user belongs to same org
    user = db.query(User).filter(
        User.id == user_id,
        User.org_id == org_id
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
    
    # Get selected org_id from user object (set by get_current_user)
    org_id = getattr(current_user, 'selected_org_id', current_user.org_id)
    
    # Verify user belongs to same org
    user = db.query(User).filter(
        User.id == user_id,
        User.org_id == org_id
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
    
    # Get selected org_id from user object (set by get_current_user)
    org_id = getattr(current_user, 'selected_org_id', current_user.org_id)
    
    # Verify user belongs to same org
    user = db.query(User).filter(
        User.id == user_id,
        User.org_id == org_id
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


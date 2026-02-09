from fastapi import APIRouter, Depends, HTTPException, Request, status
from sqlalchemy.orm import Session
from sqlalchemy import func, text
from datetime import datetime, timedelta
from typing import List, Optional
from uuid import UUID
import uuid
from app.db.session import get_db
from app.models.user import User
from app.models.user_organization import UserOrganization
from app.models.organization import Organization
from app.schemas.user import UserLogin, Token, User as UserSchema, UserSettingsUpdate, LoginResponse
from app.schemas.organization import UserOrganizationResponse, OrganizationSwitchRequest
from app.schemas.invitation import InviteValidateResponse, InviteAcceptRequest, InviteAcceptResponse
from app.core.security import verify_password, create_access_token, get_password_hash
from app.core.config import settings
from app.core.rate_limit import rate_limit
from app.api.deps import get_current_user

router = APIRouter()


@router.post("/login", response_model=LoginResponse)
def login(
    user_credentials: UserLogin,
    db: Session = Depends(get_db)
):
    """
    Login endpoint. If user belongs to multiple organizations and org_id is not provided,
    returns a response indicating organization selection is needed.
    """
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
        text("""
            SELECT id, org_id, email, hashed_password, role, is_admin, created_at
            FROM users
            WHERE LOWER(email) = LOWER(:email)
        """),
        {"email": normalized_email}
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
    for row in users_result:
        user_id, org_id, email, hashed_password, role_db_value, is_admin, created_at = row
        if verify_password(normalized_password, hashed_password):
            # Create a minimal user-like object for matching
            # Map database enum value to Python enum
            role_lower = role_db_value.lower() if role_db_value else "admin"
            if role_db_value == "member" or role_db_value == "MEMBER":
                role_lower = "member"
            elif role_db_value == "OWNER":
                role_lower = "owner"
            elif role_db_value == "ADMIN":
                role_lower = "admin"
            
            from app.models.user import UserRole
            try:
                user_role_enum = UserRole(role_lower)
            except ValueError:
                user_role_enum = UserRole.ADMIN  # Fallback
            
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
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    # Use first matching user as primary
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
        
        # Create UserOrganization records for the primary user to access all orgs
        for org_id in unique_org_ids:
            existing_uo = db.query(UserOrganization).filter(
                UserOrganization.user_id == user.id,
                UserOrganization.org_id == org_id
            ).first()
            if not existing_uo:
                user_org = UserOrganization(
                    user_id=user.id,
                    org_id=org_id,
                    is_primary=(org_id == user.org_id)  # Current user's org is primary
                )
                db.add(user_org)
        
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
        # User belongs to multiple orgs
        if user_credentials.org_id is None:
            # Get organization details for selection
            org_ids = [uo.org_id for uo in user_orgs]
            orgs = db.query(Organization).filter(Organization.id.in_(org_ids)).all()
            org_dict = {org.id: org for org in orgs}
            
            organizations = []
            for uo in user_orgs:
                if uo.org_id in org_dict:
                    organizations.append({
                        "id": str(uo.org_id),
                        "name": org_dict[uo.org_id].name,
                        "is_primary": uo.is_primary
                    })
            
            # Sort by is_primary (primary first), then by name
            organizations.sort(key=lambda x: (not x["is_primary"], x["name"]))
            
            return LoginResponse(
                requires_org_selection=True,
                organizations=organizations
            )
        
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
            "role": user.role.value if hasattr(user.role, 'value') else str(user.role)
        },
        expires_delta=access_token_expires
    )
    return LoginResponse(
        requires_org_selection=False,
        access_token=access_token,
        token_type="bearer"
    )


@router.get("/me", response_model=UserSchema)
def get_current_user_info(current_user: User = Depends(get_current_user)):
    """Get current user info with proper enum serialization"""
    try:
        # Convert role enum to string for Pydantic serialization
        role_value = current_user.role.value if hasattr(current_user.role, 'value') else str(current_user.role)
        # Return the currently selected org (from token), not the user row's primary org_id
        org_id = getattr(current_user, "selected_org_id", current_user.org_id)
        return UserSchema(
            id=current_user.id,
            org_id=org_id,
            email=current_user.email,
            role=role_value,
            is_admin=current_user.is_admin,
            created_at=current_user.created_at
        )
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
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
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
        if not settings_data.current_password:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Current password is required to change password"
            )
        if not verify_password(settings_data.current_password, user_row.hashed_password):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Current password is incorrect"
            )
        user_row.hashed_password = get_password_hash(settings_data.new_password)
    
    db.commit()
    db.refresh(user_row)
    
    role_value = user_row.role.value if hasattr(user_row.role, "value") else str(user_row.role)
    return UserSchema(
        id=user_row.id,
        org_id=user_row.org_id,
        email=user_row.email,
        role=role_value,
        is_admin=user_row.is_admin,
        created_at=user_row.created_at,
    )


@router.get("/me/settings")
def get_user_settings(current_user: User = Depends(get_current_user)):
    """
    Get current user settings.
    Returns user info and default privacy settings.
    """
    return {
        "email": current_user.email,
        "role": current_user.role.value if hasattr(current_user.role, 'value') else str(current_user.role),
        "org_id": str(current_user.org_id),
        "created_at": current_user.created_at.isoformat() if current_user.created_at else None,
        # Default privacy settings (can be stored in DB later)
        "data_sharing_enabled": True,
        "analytics_enabled": True
    }


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
    # Normalize email
    normalized_email = email.lower().strip()
    
    # Find user by email
    user = db.query(User).filter(func.lower(User.email) == normalized_email).first()
    if not user:
        # Don't reveal if user exists for security
        return []
    
    # Get all organizations for this user
    user_orgs = db.query(UserOrganization).filter(
        UserOrganization.user_id == user.id
    ).all()
    
    # If no user_orgs, fall back to user.org_id (backward compatibility)
    if not user_orgs:
        org = db.query(Organization).filter(Organization.id == user.org_id).first()
        if org:
            return [UserOrganizationResponse(
                id=org.id,
                name=org.name,
                is_primary=True,
                created_at=org.created_at
            )]
        return []
    
    # Get organization details
    org_ids = [uo.org_id for uo in user_orgs]
    orgs = db.query(Organization).filter(Organization.id.in_(org_ids)).all()
    
    # Map to response
    org_dict = {org.id: org for org in orgs}
    result = []
    for uo in user_orgs:
        if uo.org_id in org_dict:
            result.append(UserOrganizationResponse(
                id=uo.org_id,
                name=org_dict[uo.org_id].name,
                is_primary=uo.is_primary,
                created_at=uo.created_at
            ))
    
    # Sort by is_primary (primary first), then by name
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
    # Verify user has access to the requested org
    user_org = db.query(UserOrganization).filter(
        UserOrganization.user_id == current_user.id,
        UserOrganization.org_id == switch_request.org_id
    ).first()
    
    # If no user_org found, check if user.org_id matches (backward compatibility)
    if not user_org:
        if current_user.org_id != switch_request.org_id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="User does not have access to this organization"
            )
    elif user_org.org_id != switch_request.org_id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User does not have access to this organization"
        )
    
    # Verify organization exists
    org = db.query(Organization).filter(Organization.id == switch_request.org_id).first()
    if not org:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Organization not found"
        )
    
    # Create new token with selected org_id
    access_token_expires = timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={
            "sub": current_user.email,
            "org_id": str(switch_request.org_id),
            "user_id": str(current_user.id),
            "role": current_user.role.value if hasattr(current_user.role, 'value') else str(current_user.role)
        },
        expires_delta=access_token_expires
    )
    return {"access_token": access_token, "token_type": "bearer"}


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
        # Existing user: add to org via UserOrganization
        already_in_org = db.query(UserOrganization).filter(
            UserOrganization.user_id == existing_user.id,
            UserOrganization.org_id == inv.org_id,
        ).first()
        if already_in_org:
            raise HTTPException(status_code=400, detail="You are already in this organization")
        uo = UserOrganization(
            user_id=existing_user.id,
            org_id=inv.org_id,
            is_primary=False,
        )
        db.add(uo)
        inv.used_at = datetime.utcnow()
        db.commit()
        # Issue token so frontend can set cookie and switch to this org
        access_token_expires = timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
        access_token = create_access_token(
            data={
                "sub": existing_user.email,
                "org_id": str(inv.org_id),
                "user_id": str(existing_user.id),
                "role": existing_user.role.value if hasattr(existing_user.role, "value") else str(existing_user.role),
            },
            expires_delta=access_token_expires,
        )
        return InviteAcceptResponse(
            access_token=access_token,
            token_type="bearer",
            org_id=inv.org_id,
            user_id=existing_user.id,
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

    try:
        user_role = UserRole(role_normalized)
    except ValueError:
        user_role = UserRole.MEMBER

    # Map Python enum to DB enum value (userrole has 'OWNER', 'ADMIN', 'member' - lowercase)
    enum_values_result = db.execute(text("SELECT unnest(enum_range(NULL::userrole))")).fetchall()
    enum_values = [str(row[0]) for row in enum_values_result]
    if user_role == UserRole.OWNER:
        role_db_value = "OWNER"
    elif user_role == UserRole.ADMIN:
        role_db_value = "ADMIN"
    elif user_role == UserRole.MEMBER:
        role_db_value = "member" if "member" in enum_values else ("MEMBER" if "MEMBER" in enum_values else "ADMIN")
    else:
        role_db_value = "ADMIN"

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
            "role": role_db_value,
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
            "role": user_role.value if hasattr(user_role, "value") else role_normalized,
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


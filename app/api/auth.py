from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from datetime import timedelta
from app.db.session import get_db
from app.models.user import User
from app.schemas.user import UserLogin, Token, User as UserSchema, UserSettingsUpdate
from app.core.security import verify_password, create_access_token
from app.core.config import settings
from app.api.deps import get_current_user

router = APIRouter()


@router.post("/login", response_model=Token)
def login(user_credentials: UserLogin, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.email == user_credentials.email).first()
    if not user or not verify_password(user_credentials.password, user.hashed_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token_expires = timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={
            "sub": user.email,
            "org_id": str(user.org_id),  # Include org_id in token for multi-tenant isolation
            "user_id": str(user.id),
            "role": user.role.value if hasattr(user.role, 'value') else str(user.role)
        },
        expires_delta=access_token_expires
    )
    return {"access_token": access_token, "token_type": "bearer"}


@router.get("/me", response_model=UserSchema)
def get_current_user_info(current_user: User = Depends(get_current_user)):
    """Get current user info with proper enum serialization"""
    try:
        # Convert role enum to string for Pydantic serialization
        role_value = current_user.role.value if hasattr(current_user.role, 'value') else str(current_user.role)
        
        # Build user dict manually to ensure proper types
        return UserSchema(
            id=current_user.id,
            org_id=current_user.org_id,
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
    """
    from app.core.security import verify_password, get_password_hash
    
    # Update email if provided
    if settings_data.email is not None and settings_data.email != current_user.email:
        # Check if email is already taken in this org
        existing_user = db.query(User).filter(
            User.email == settings_data.email,
            User.org_id == current_user.org_id,
            User.id != current_user.id
        ).first()
        if existing_user:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Email already in use in this organization"
            )
        current_user.email = settings_data.email
    
    # Update password if provided
    if settings_data.new_password is not None:
        if not settings_data.current_password:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Current password is required to change password"
            )
        # Verify current password
        if not verify_password(settings_data.current_password, current_user.hashed_password):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Current password is incorrect"
            )
        # Update password
        current_user.hashed_password = get_password_hash(settings_data.new_password)
    
    # Note: Privacy settings (data_sharing_enabled, analytics_enabled, marketing_emails_enabled)
    # would require a separate user_preferences table or JSON column on the user model.
    # For now, we'll skip these and they can be added later if needed.
    
    db.commit()
    db.refresh(current_user)
    
    # Convert role enum to string for Pydantic serialization
    user_dict = {
        "id": current_user.id,
        "org_id": current_user.org_id,
        "email": current_user.email,
        "role": current_user.role.value if hasattr(current_user.role, 'value') else str(current_user.role),
        "is_admin": current_user.is_admin,
        "created_at": current_user.created_at
    }
    return UserSchema(**user_dict)


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


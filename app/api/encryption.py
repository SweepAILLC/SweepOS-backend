"""
Encryption key management endpoints.
Admin-only endpoints for managing encryption keys and rotating tokens.
"""
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from app.db.session import get_db
from app.api.deps import get_current_user, require_admin
from app.models.user import User
from app.models.oauth_token import OAuthToken
from app.core.encryption import rotate_token
from typing import List, Dict, Any
import uuid

router = APIRouter()


@router.post("/encryption/rotate-tokens")
def rotate_all_tokens(
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin)
):
    """
    Rotate all encrypted tokens to use the current encryption key.
    
    This endpoint:
    1. Decrypts all tokens with old keys
    2. Re-encrypts them with the current key
    3. Updates the database
    
    SECURITY: Requires admin role. This operation can take time for large datasets.
    
    Returns:
        Summary of rotated tokens
    """
    from app.core.audit import log_security_event
    from app.models.audit_log import AuditEventType
    
    try:
        # Get all OAuth tokens
        all_tokens = db.query(OAuthToken).all()
        
        rotated_count = 0
        failed_count = 0
        errors = []
        
        for token in all_tokens:
            try:
                # Rotate the token (decrypt with old key, encrypt with new)
                new_encrypted = rotate_token(token.access_token)
                
                if new_encrypted != token.access_token:
                    token.access_token = new_encrypted
                    rotated_count += 1
                
                # Also rotate refresh token if present
                if token.refresh_token:
                    new_refresh = rotate_token(token.refresh_token)
                    if new_refresh != token.refresh_token:
                        token.refresh_token = new_refresh
                
            except Exception as e:
                failed_count += 1
                errors.append({
                    "token_id": str(token.id),
                    "provider": token.provider.value,
                    "error": str(e)
                })
                print(f"[ENCRYPTION] Failed to rotate token {token.id}: {e}")
        
        # Commit all changes
        db.commit()
        
        # Log the rotation event
        log_security_event(
            db=db,
            event_type=AuditEventType.API_KEY_CONNECTED,  # Reuse event type
            org_id=current_user.org_id,
            user_id=current_user.id,
            resource_type="encryption",
            resource_id="key_rotation",
            details={
                "rotated_count": rotated_count,
                "failed_count": failed_count,
                "total_tokens": len(all_tokens),
                "errors": errors[:10]  # Limit error details
            }
        )
        
        return {
            "success": True,
            "message": f"Token rotation complete. {rotated_count} tokens rotated, {failed_count} failed.",
            "rotated_count": rotated_count,
            "failed_count": failed_count,
            "total_tokens": len(all_tokens),
            "errors": errors if errors else None
        }
        
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error rotating tokens: {str(e)}"
        )


@router.get("/encryption/status")
def get_encryption_status(
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin)
):
    """
    Get encryption system status and statistics.
    
    Returns:
        Encryption system information including:
        - Key version information
        - Token statistics
        - Rotation status
    """
    try:
        from app.core.encryption_v2 import get_key_manager
        
        # Get key manager info
        manager = get_key_manager()
        current_version = manager.get_current_key_version()
        key_count = len(manager._keys)
        
        # Get token statistics
        all_tokens = db.query(OAuthToken).all()
        token_stats = {
            "total_tokens": len(all_tokens),
            "by_provider": {},
            "encrypted_count": 0
        }
        
        for token in all_tokens:
            provider = token.provider.value
            token_stats["by_provider"][provider] = token_stats["by_provider"].get(provider, 0) + 1
            
            # Check if token has version prefix (new format)
            if token.access_token and token.access_token.startswith('v'):
                token_stats["encrypted_count"] += 1
        
        return {
            "encryption_enabled": True,
            "key_version": current_version,
            "active_keys": key_count,
            "token_statistics": token_stats,
            "rotation_supported": True
        }
        
    except ImportError:
        # Fallback if v2 encryption not available
        return {
            "encryption_enabled": True,
            "key_version": 1,
            "active_keys": 1,
            "token_statistics": {
                "total_tokens": db.query(OAuthToken).count(),
                "by_provider": {},
                "encrypted_count": db.query(OAuthToken).count()
            },
            "rotation_supported": False,
            "note": "Using legacy encryption (v1). Upgrade to v2 for key rotation support."
        }
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error getting encryption status: {str(e)}"
        )



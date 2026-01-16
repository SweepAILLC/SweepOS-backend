from cryptography.fernet import Fernet
from app.core.config import settings
import base64


def get_encryption_key() -> bytes:
    """Get or generate encryption key for OAuth tokens"""
    if settings.ENCRYPTION_KEY:
        # ENCRYPTION_KEY should be a base64-encoded Fernet key
        try:
            # If it's a string, try to decode it as base64 first, then use as Fernet key
            if isinstance(settings.ENCRYPTION_KEY, str):
                # Check if it's already a valid Fernet key (32 bytes base64-encoded = 44 chars)
                if len(settings.ENCRYPTION_KEY) == 44:
                    # It's likely already a Fernet key, use it directly
                    return settings.ENCRYPTION_KEY.encode()
                else:
                    # Try to decode as base64
                    import base64
                    return base64.b64decode(settings.ENCRYPTION_KEY)
            else:
                # Already bytes
                return settings.ENCRYPTION_KEY
        except Exception as e:
            # If decoding fails, try using it directly as a Fernet key
            try:
                return settings.ENCRYPTION_KEY.encode() if isinstance(settings.ENCRYPTION_KEY, str) else settings.ENCRYPTION_KEY
            except Exception:
                raise ValueError(f"Invalid ENCRYPTION_KEY format: {str(e)}")
    # For development, generate a key if not set
    # In production, this should always be set via env var
    key = Fernet.generate_key()
    print(f"WARNING: Generated encryption key. Set ENCRYPTION_KEY={key.decode()} in .env")
    return key


def encrypt_token(token: str) -> str:
    """Encrypt a token for storage"""
    f = Fernet(get_encryption_key())
    return f.encrypt(token.encode()).decode()


def decrypt_token(encrypted_token: str, audit_context: dict = None) -> str:
    """
    Decrypt a stored token.
    
    Args:
        encrypted_token: Encrypted token string
        audit_context: Optional dict with audit info (org_id, user_id, etc.) for logging
    
    Returns:
        Decrypted token string
    """
    f = Fernet(get_encryption_key())
    decrypted = f.decrypt(encrypted_token.encode()).decode()
    
    # Log token access if audit context provided
    if audit_context:
        try:
            from app.core.audit import log_security_event
            from app.models.audit_log import AuditEventType
            
            # Only log if we have a database session
            db = audit_context.get('db')
            if db:
                log_security_event(
                    db=db,
                    event_type=AuditEventType.TOKEN_DECRYPTED,
                    org_id=audit_context.get('org_id'),
                    user_id=audit_context.get('user_id'),
                    resource_type=audit_context.get('resource_type', 'oauth_token'),
                    resource_id=audit_context.get('resource_id'),
                    ip_address=audit_context.get('ip_address'),
                    user_agent=audit_context.get('user_agent'),
                    details={
                        "token_prefix": decrypted[:10] + "..." if len(decrypted) > 10 else "***",
                        "token_length": len(decrypted)
                    }
                )
        except Exception as e:
            # Don't fail decryption if audit logging fails
            print(f"[ENCRYPTION] Failed to log token access: {str(e)}")
    
    return decrypted


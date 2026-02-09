"""
Encryption module with backward compatibility.
Uses enhanced encryption_v2 when available, falls back to legacy for compatibility.
"""
try:
    # Try to use enhanced encryption with key rotation support
    from app.core.encryption_v2 import encrypt_token, decrypt_token, rotate_token
    _USE_V2 = True
except ImportError:
    # Fall back to legacy encryption
    _USE_V2 = False
    from cryptography.fernet import Fernet
    from app.core.config import settings
    import base64
    
    def get_encryption_key() -> bytes:
        """Get or generate encryption key for OAuth tokens"""
        if settings.ENCRYPTION_KEY:
            try:
                if isinstance(settings.ENCRYPTION_KEY, str):
                    if len(settings.ENCRYPTION_KEY) == 44:
                        return settings.ENCRYPTION_KEY.encode()
                    else:
                        return base64.b64decode(settings.ENCRYPTION_KEY)
                else:
                    return settings.ENCRYPTION_KEY
            except Exception as e:
                try:
                    return settings.ENCRYPTION_KEY.encode() if isinstance(settings.ENCRYPTION_KEY, str) else settings.ENCRYPTION_KEY
                except Exception:
                    raise ValueError(f"Invalid ENCRYPTION_KEY format: {str(e)}")
        key = Fernet.generate_key()
        print(f"WARNING: Generated encryption key. Set ENCRYPTION_KEY={key.decode()} in .env")
        return key
    
    def encrypt_token(token: str) -> str:
        """Encrypt a token for storage"""
        f = Fernet(get_encryption_key())
        return f.encrypt(token.encode()).decode()
    
    def decrypt_token(encrypted_token: str, audit_context: dict = None) -> str:
        """Decrypt a stored token"""
        f = Fernet(get_encryption_key())
        decrypted = f.decrypt(encrypted_token.encode()).decode()
        
        if audit_context:
            try:
                from app.core.audit import log_security_event
                from app.models.audit_log import AuditEventType
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
                print(f"[ENCRYPTION] Failed to log token access: {str(e)}")
        
        return decrypted
    
    def rotate_token(encrypted_token: str) -> str:
        """Re-encrypt token with current key (legacy: no-op)"""
        return encrypted_token


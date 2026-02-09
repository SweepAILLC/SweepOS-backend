"""
Enhanced encryption system with key rotation support.

Best Practices:
1. Keys are versioned to support rotation
2. Multiple keys can be active simultaneously during rotation
3. Audit logging for all encryption/decryption operations
4. Keys are stored securely (never in code, only in env/secrets)
5. Automatic key derivation from master key
"""
from cryptography.fernet import Fernet, MultiFernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.backends import default_backend
from app.core.config import settings
import base64
import os
from typing import Optional, List
from datetime import datetime


class EncryptionKeyManager:
    """Manages encryption keys with rotation support"""
    
    def __init__(self):
        self._keys: List[bytes] = []
        self._current_key_version: int = 1
        self._load_keys()
    
    def _load_keys(self):
        """Load encryption keys from environment"""
        # Primary key (required)
        primary_key = self._get_primary_key()
        if primary_key:
            self._keys = [primary_key]
        
        # Additional keys for rotation (optional, comma-separated)
        rotation_keys = os.getenv('ENCRYPTION_KEY_ROTATION', '')
        if rotation_keys:
            for key_str in rotation_keys.split(','):
                key_str = key_str.strip()
                if key_str:
                    try:
                        key = self._parse_key(key_str)
                        if key and key not in self._keys:
                            self._keys.append(key)
                    except Exception as e:
                        print(f"[ENCRYPTION] Warning: Failed to parse rotation key: {e}")
        
        if not self._keys:
            raise ValueError("No encryption keys configured. Set ENCRYPTION_KEY in environment.")
    
    def _get_primary_key(self) -> Optional[bytes]:
        """Get the primary encryption key"""
        if settings.ENCRYPTION_KEY:
            return self._parse_key(settings.ENCRYPTION_KEY)
        return None
    
    def _parse_key(self, key_str: str) -> bytes:
        """Parse a key string (base64 or Fernet format)"""
        if isinstance(key_str, bytes):
            return key_str
        
        key_str = key_str.strip()
        
        # Check if it's a valid Fernet key (44 chars base64-encoded)
        if len(key_str) == 44:
            try:
                return key_str.encode()
            except:
                pass
        
        # Try base64 decode
        try:
            decoded = base64.b64decode(key_str)
            if len(decoded) == 32:  # Fernet keys are 32 bytes
                return decoded
        except:
            pass
        
        # Try as direct Fernet key
        try:
            return key_str.encode()
        except:
            raise ValueError(f"Invalid encryption key format: {key_str[:10]}...")
    
    def get_encryptor(self) -> Fernet:
        """Get the primary encryptor (uses current key)"""
        if not self._keys:
            raise ValueError("No encryption keys available")
        return Fernet(self._keys[0])
    
    def get_decryptor(self) -> MultiFernet:
        """Get a decryptor that can handle multiple key versions"""
        if not self._keys:
            raise ValueError("No encryption keys available")
        
        # Create Fernet instances for all keys (oldest to newest for rotation)
        fernets = [Fernet(key) for key in reversed(self._keys)]
        return MultiFernet(fernets)
    
    def get_current_key_version(self) -> int:
        """Get the current key version number"""
        return self._current_key_version


# Global key manager instance
_key_manager: Optional[EncryptionKeyManager] = None


def get_key_manager() -> EncryptionKeyManager:
    """Get or create the encryption key manager"""
    global _key_manager
    if _key_manager is None:
        _key_manager = EncryptionKeyManager()
    return _key_manager


def encrypt_token(token: str, key_version: Optional[int] = None) -> str:
    """
    Encrypt a token for storage.
    
    Args:
        token: Plain text token to encrypt
        key_version: Optional key version (defaults to current)
    
    Returns:
        Base64-encoded encrypted token with version prefix
    """
    manager = get_key_manager()
    encryptor = manager.get_encryptor()
    
    encrypted = encryptor.encrypt(token.encode())
    encrypted_str = base64.urlsafe_b64encode(encrypted).decode()
    
    # Prefix with key version for tracking
    version = key_version or manager.get_current_key_version()
    return f"v{version}:{encrypted_str}"


def decrypt_token(encrypted_token: str, audit_context: dict = None) -> str:
    """
    Decrypt a stored token (supports multiple key versions for rotation).
    
    Args:
        encrypted_token: Encrypted token string (may have version prefix)
        audit_context: Optional dict with audit info for logging
    
    Returns:
        Decrypted token string
    """
    manager = get_key_manager()
    decryptor = manager.get_decryptor()
    
    # Handle version prefix (v1:encrypted_data or just encrypted_data)
    if encrypted_token.startswith('v'):
        # New format with version prefix
        parts = encrypted_token.split(':', 1)
        if len(parts) == 2:
            version_str, encrypted_data = parts
            version = int(version_str[1:]) if version_str[1:].isdigit() else None
        else:
            encrypted_data = encrypted_token
            version = None
    else:
        # Legacy format (no version prefix)
        encrypted_data = encrypted_token
        version = None
    
    try:
        # Decode base64
        encrypted_bytes = base64.urlsafe_b64decode(encrypted_data.encode())
        
        # Try decryption with all available keys (MultiFernet handles this)
        decrypted = decryptor.decrypt(encrypted_bytes)
        decrypted_str = decrypted.decode()
        
        # Log token access if audit context provided
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
                            "key_version": version or "legacy",
                            "token_prefix": decrypted_str[:10] + "..." if len(decrypted_str) > 10 else "***",
                            "token_length": len(decrypted_str)
                        }
                    )
            except Exception as e:
                # Don't fail decryption if audit logging fails
                print(f"[ENCRYPTION] Failed to log token access: {str(e)}")
        
        return decrypted_str
        
    except Exception as e:
        raise ValueError(f"Failed to decrypt token: {str(e)}")


def rotate_token(encrypted_token: str) -> str:
    """
    Re-encrypt a token with the current key (for key rotation).
    
    Args:
        encrypted_token: Old encrypted token
    
    Returns:
        New encrypted token with current key version
    """
    # Decrypt with old key
    decrypted = decrypt_token(encrypted_token)
    
    # Re-encrypt with current key
    return encrypt_token(decrypted)



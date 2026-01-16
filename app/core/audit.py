"""
Audit logging for security events
"""
from sqlalchemy.orm import Session
from app.models.audit_log import AuditLog, AuditEventType
from typing import Optional
import uuid


def log_security_event(
    db: Session,
    event_type: AuditEventType,
    org_id: uuid.UUID,
    user_id: Optional[uuid.UUID] = None,
    resource_type: Optional[str] = None,
    resource_id: Optional[str] = None,
    ip_address: Optional[str] = None,
    user_agent: Optional[str] = None,
    details: Optional[dict] = None
):
    """
    Log a security event to the audit log.
    
    Args:
        db: Database session
        event_type: Type of security event
        org_id: Organization ID
        user_id: User ID (if applicable)
        resource_type: Type of resource (e.g., "stripe", "oauth_token")
        resource_id: ID of the resource (e.g., account_id, token_id)
        ip_address: IP address of the request
        user_agent: User agent string
        details: Additional details as a dictionary (will be JSON-encoded)
    """
    import json
    
    try:
        audit_log = AuditLog(
            org_id=org_id,
            user_id=user_id,
            event_type=event_type,
            resource_type=resource_type,
            resource_id=resource_id,
            ip_address=ip_address,
            user_agent=user_agent,
            details=json.dumps(details) if details else None
        )
        db.add(audit_log)
        db.commit()
    except Exception as e:
        # Don't fail the request if audit logging fails
        # But log the error for debugging
        print(f"[AUDIT] Failed to log security event: {str(e)}")
        db.rollback()


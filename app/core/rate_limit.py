"""
Simple in-memory rate limiting for API endpoints
"""
from functools import wraps
from fastapi import HTTPException, status, Request
from typing import Callable
from datetime import datetime, timedelta
from collections import defaultdict
import threading


# In-memory store for rate limiting
# Format: {identifier: [(timestamp, count), ...]}
_rate_limit_store = defaultdict(list)
_rate_limit_lock = threading.Lock()

# Cleanup old entries every 5 minutes
_last_cleanup = datetime.utcnow()
_cleanup_interval = timedelta(minutes=5)


def _cleanup_old_entries():
    """Remove entries older than the time window"""
    global _last_cleanup, _rate_limit_store
    
    now = datetime.utcnow()
    if now - _last_cleanup < _cleanup_interval:
        return
    
    with _rate_limit_lock:
        _last_cleanup = now
        cutoff_time = now - timedelta(hours=1)  # Keep last hour of data
        
        for key in list(_rate_limit_store.keys()):
            _rate_limit_store[key] = [
                (ts, count) for ts, count in _rate_limit_store[key]
                if ts > cutoff_time
            ]
            if not _rate_limit_store[key]:
                del _rate_limit_store[key]


def rate_limit(max_requests: int = 5, window_seconds: int = 300, identifier_func: Callable = None):
    """
    Rate limiting decorator for FastAPI endpoints.
    
    Args:
        max_requests: Maximum number of requests allowed
        window_seconds: Time window in seconds (default: 5 minutes)
        identifier_func: Function to extract identifier from request (default: uses user_id)
    
    Usage:
        @rate_limit(max_requests=5, window_seconds=300)
        @router.post("/endpoint")
        def my_endpoint(current_user: User = Depends(get_current_user)):
            ...
    """
    def decorator(func: Callable):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Find Request object in kwargs
            request = None
            user = None
            
            for arg in args:
                if isinstance(arg, Request):
                    request = arg
                    break
            
            for key, value in kwargs.items():
                if isinstance(value, Request):
                    request = value
                elif hasattr(value, 'id') and hasattr(value, 'org_id'):  # User object
                    user = value
            
            # Get identifier
            if identifier_func:
                identifier = identifier_func(request, user)
            elif user:
                identifier = f"user_{user.id}_{user.org_id}"
            elif request:
                # Fallback to IP address
                identifier = request.client.host if request.client else "unknown"
            else:
                identifier = "unknown"
            
            # Cleanup old entries
            _cleanup_old_entries()
            
            # Check rate limit
            now = datetime.utcnow()
            window_start = now - timedelta(seconds=window_seconds)
            
            with _rate_limit_lock:
                # Get requests in the current window
                recent_requests = [
                    ts for ts, _ in _rate_limit_store[identifier]
                    if ts > window_start
                ]
                
                if len(recent_requests) >= max_requests:
                    # Rate limit exceeded
                    from app.core.audit import log_security_event
                    from app.db.session import get_db
                    
                    # Try to log the event (non-blocking)
                    try:
                        from app.models.audit_log import AuditEventType
                        # Try to get db from kwargs
                        db = kwargs.get('db')
                        if db and user:
                            log_security_event(
                                db=db,
                                event_type=AuditEventType.RATE_LIMIT_EXCEEDED,
                                org_id=user.org_id,
                                user_id=user.id,
                                resource_type="api_endpoint",
                                resource_id=func.__name__,
                                ip_address=request.client.host if request else None,
                                user_agent=request.headers.get("user-agent") if request else None,
                                details={
                                    "endpoint": func.__name__,
                                    "max_requests": max_requests,
                                    "window_seconds": window_seconds,
                                    "recent_requests": len(recent_requests)
                                }
                            )
                    except Exception:
                        pass  # Don't fail if audit logging fails
                    
                    raise HTTPException(
                        status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                        detail=f"Rate limit exceeded: {max_requests} requests per {window_seconds} seconds. Please try again later."
                    )
                
                # Add current request
                _rate_limit_store[identifier].append((now, 1))
            
            # Call the original function
            return func(*args, **kwargs)
        
        return wrapper
    return decorator


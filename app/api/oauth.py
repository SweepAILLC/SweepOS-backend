from fastapi import APIRouter, Depends, HTTPException, Query, status, Request
from sqlalchemy.orm import Session
from urllib.parse import urlencode
import os
import uuid
import httpx
from app.db.session import get_db
from app.core.config import settings
from app.schemas.oauth import OAuthStartResponse, OAuthTokenResponse, DirectApiKeyRequest
from app.api.deps import get_current_user, require_admin, require_admin_or_owner
from app.models.user import User
from app.models.oauth_token import OAuthToken, OAuthProvider
from app.models.organization import Organization
from app.models.stripe_payment import StripePayment
from app.models.stripe_subscription import StripeSubscription
from app.models.stripe_treasury_transaction import StripeTreasuryTransaction
from app.models.stripe_event import StripeEvent
from app.core.encryption import encrypt_token
from datetime import datetime, timedelta

# Default org ID for v1 (internal only)
# TODO: For multi-tenant, get org_id from OAuth state parameter or session
DEFAULT_ORG_ID = uuid.UUID("00000000-0000-0000-0000-000000000001")

router = APIRouter()


@router.post("/stripe/start", response_model=OAuthStartResponse)
def start_stripe_oauth(
    current_user: User = Depends(get_current_user)
):
    # TEMPORARILY DISABLED: OAuth is disabled for deployment testing
    # Only API key authentication is available
    raise HTTPException(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        detail="OAuth authentication is temporarily disabled. Please use API key authentication instead."
    )
    
    # Original OAuth code commented out:
    """
    # For development: If test OAuth URL is set, use it directly
    # This allows using External test links without publishing the app
    if settings.STRIPE_TEST_OAUTH_URL:
        # Still include org_id in state for callback
        import secrets
        import base64
        import json
        import time
        state_data = {
            "org_id": str(current_user.org_id),
            "nonce": secrets.token_urlsafe(16),
            "timestamp": int(time.time())  # Add timestamp to prevent browser caching
        }
        state = base64.urlsafe_b64encode(json.dumps(state_data).encode()).decode()
        # Append state and prompt to test URL if it doesn't have query params
        separator = "&" if "?" in settings.STRIPE_TEST_OAUTH_URL else "?"
        # Add prompt=select_account to force account selection (prevents auto-connecting to cached account)
        return {"redirect_url": f"{settings.STRIPE_TEST_OAUTH_URL}{separator}state={state}&prompt=select_account"}
    
    # Production: Generate OAuth URL using Client ID
    # Debug: Check if client ID is set (handle empty strings and None)
    client_id = settings.STRIPE_OAUTH_CLIENT_ID
    if not client_id or (isinstance(client_id, str) and client_id.strip() == ""):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Stripe OAuth not configured. Either set STRIPE_TEST_OAUTH_URL (for development) or STRIPE_OAUTH_CLIENT_ID (for production) in .env file and restart the backend container."
        )
    
    # Also check redirect URI
    redirect_uri = settings.STRIPE_REDIRECT_URI
    if not redirect_uri or (isinstance(redirect_uri, str) and redirect_uri.strip() == ""):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="STRIPE_REDIRECT_URI is not set. Set it in .env file and restart the backend container."
        )
    
    # Generate state parameter with org_id for multi-tenant support
    import secrets
    import base64
    import json
    import time
    state_data = {
        "org_id": str(current_user.org_id),
        "nonce": secrets.token_urlsafe(16),  # CSRF protection
        "timestamp": int(time.time())  # Add timestamp to prevent browser caching
    }
    state = base64.urlsafe_b64encode(json.dumps(state_data).encode()).decode()
    
    # Stripe Apps OAuth 2.0 URL format
    # See: https://docs.stripe.com/stripe-apps/api-authentication/oauth
    params = {
        "client_id": settings.STRIPE_OAUTH_CLIENT_ID,
        "redirect_uri": settings.STRIPE_REDIRECT_URI,
        "state": state,  # Contains org_id for multi-tenant support
        "prompt": "select_account",  # Force account selection (prevents auto-connecting to cached account)
    }
    redirect_url = f"https://marketplace.stripe.com/oauth/v2/authorize?{urlencode(params)}"
    return {"redirect_url": redirect_url}


@router.get("/stripe/callback")
def stripe_oauth_callback(
    # TEMPORARILY DISABLED: OAuth is disabled for deployment testing
    code: str = Query(None),
    state: str = Query(None),
    error: str = Query(None),
    error_description: str = Query(None),
    db: Session = Depends(get_db)
):
    raise HTTPException(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        detail="OAuth authentication is temporarily disabled. Please use API key authentication instead."
    )
    
    # Original OAuth callback code commented out:
    """
def stripe_oauth_callback_original(
    code: str = Query(None),
    state: str = Query(None),
    error: str = Query(None),
    error_description: str = Query(None),
    db: Session = Depends(get_db)
):
    """
    Handle Stripe OAuth callback.
    Exchanges authorization code for access token and redirects to frontend.
    
    Note: This endpoint does NOT require authentication because OAuth callbacks
    are accessed directly from Stripe. The authorization code provides security.
    
    See: https://docs.stripe.com/stripe-apps/api-authentication/oauth
    """
    from fastapi.responses import RedirectResponse
    import httpx
    
    # Get frontend URL from settings
    frontend_url = settings.FRONTEND_URL
    
    # Handle OAuth errors
    if error:
        error_msg = error_description or error
        return RedirectResponse(
            url=f"{frontend_url}/?stripe_error={error}&error_description={error_msg}",
            status_code=302
        )
    
    # Code is required if no error
    if not code:
        return RedirectResponse(
            url=f"{frontend_url}/?stripe_error=no_code&error_description=No authorization code provided",
            status_code=302
        )
    
    if not settings.STRIPE_SECRET_KEY:
        return RedirectResponse(
            url=f"{frontend_url}/?stripe_error=configuration_error&error_description=Stripe not configured",
            status_code=302
        )
    
        # Extract org_id from state parameter for multi-tenant support
        org_id = DEFAULT_ORG_ID  # Fallback to default
        if state:
            try:
                import base64
                import json
                state_data = json.loads(base64.urlsafe_b64decode(state.encode()).decode())
                org_id = uuid.UUID(state_data.get("org_id", str(DEFAULT_ORG_ID)))
            except Exception as e:
                # If state parsing fails, log but continue with default
                print(f"[OAUTH] Warning: Failed to parse state parameter: {e}. Using default org.")
    
    try:
        # Exchange authorization code for access token
        # Note: For Stripe Apps OAuth, we use the application owner's secret key (from .env)
        # This is YOUR app's secret key, not the user's. Each org will get their own access token.
        # See: https://docs.stripe.com/stripe-apps/api-authentication/oauth#token-exchange
        response = httpx.post(
            "https://connect.stripe.com/oauth/token",
            data={
                "client_secret": settings.STRIPE_SECRET_KEY,  # Your app's secret key (from .env)
                "code": code,
                "grant_type": "authorization_code"
            },
            timeout=30.0  # Increased timeout for token exchange
        )
        
        if response.status_code != 200:
            error_data = response.json() if response.headers.get("content-type", "").startswith("application/json") else {}
            error_msg = error_data.get("error_description", f"HTTP {response.status_code}: {response.text}")
            return RedirectResponse(
                url=f"{frontend_url}/?stripe_error=token_exchange_failed&error_description={error_msg}",
                status_code=302
            )
        
        token_data = response.json()
        access_token = token_data.get("access_token")
        refresh_token = token_data.get("refresh_token")
        stripe_user_id = token_data.get("stripe_user_id")  # This is the connected account ID (the user's Stripe account)
        
        if not access_token:
            return RedirectResponse(
                url=f"{frontend_url}/?stripe_error=no_access_token&error_description=No access token in response",
                status_code=302
            )
        
        # Encrypt tokens before storing
        encrypted_token = encrypt_token(access_token)
        encrypted_refresh = encrypt_token(refresh_token) if refresh_token else None
        
        # Calculate expiration (Stripe access tokens typically expire, but for OAuth they may not)
        # Default to 1 year if not specified
        expires_at = datetime.utcnow() + timedelta(days=365)
        if "expires_in" in token_data:
            expires_at = datetime.utcnow() + timedelta(seconds=token_data["expires_in"])
        
        # CRITICAL: Check if token exists for this provider AND org (multi-tenant isolation)
        existing = db.query(OAuthToken).filter(
            OAuthToken.provider == OAuthProvider.STRIPE,
            OAuthToken.org_id == org_id
        ).first()
        
        if existing:
            existing.access_token = encrypted_token
            existing.refresh_token = encrypted_refresh
            existing.account_id = stripe_user_id or f"acct_{code[:10]}"
            existing.expires_at = expires_at
        else:
            # Store token with org_id for multi-tenant isolation
            oauth_token = OAuthToken(
                org_id=org_id,  # Use org_id from state parameter
                provider=OAuthProvider.STRIPE,
                account_id=stripe_user_id or f"acct_{code[:10]}",
                access_token=encrypted_token,
                refresh_token=encrypted_refresh,
                expires_at=expires_at
            )
            db.add(oauth_token)
        
        db.commit()
        
        # Trigger historical data sync automatically after OAuth connection in background thread
        # This prevents the redirect from being delayed during large syncs
        import threading
        def sync_in_background():
            # Create a new database session for the background thread
            from app.db.session import SessionLocal
            bg_db = SessionLocal()
            try:
                from app.services.stripe_sync_v2 import sync_stripe_incremental
                print(f"[OAUTH] Starting initial historical data sync (full backfill) for org {org_id}...")
                sync_result = sync_stripe_incremental(bg_db, org_id=org_id, force_full=True)
                if sync_result.get("error"):
                    print(f"[OAUTH] ❌ Historical sync error: {sync_result.get('error')}")
                else:
                    print(f"[OAUTH] ✅ Historical sync complete for org {org_id}:")
                    print(f"   - Customers: {sync_result.get('customers_synced', 0)} new, {sync_result.get('customers_updated', 0)} updated")
                    print(f"   - Subscriptions: {sync_result.get('subscriptions_synced', 0)} new, {sync_result.get('subscriptions_updated', 0)} updated")
                    print(f"   - Payments: {sync_result.get('payments_synced', 0)} new, {sync_result.get('payments_updated', 0)} updated")
            except Exception as e:
                import traceback
                print(f"[OAUTH] ❌ Historical sync failed: {str(e)}")
                traceback.print_exc()
            finally:
                bg_db.close()
        
        # Start sync in background thread
        sync_thread = threading.Thread(target=sync_in_background, daemon=True)
        sync_thread.start()
        
        # Redirect to frontend with success message (immediately, sync runs in background)
        return RedirectResponse(
            url=f"{frontend_url}/?stripe_connected=true",
            status_code=302
        )
        
    except httpx.HTTPError as e:
        db.rollback()
        return RedirectResponse(
            url=f"{frontend_url}/?stripe_error=network_error&error_description={str(e)}",
            status_code=302
        )
    except Exception as e:
        db.rollback()
        return RedirectResponse(
            url=f"{frontend_url}/?stripe_error=unknown_error&error_description={str(e)}",
            status_code=302
        )


@router.post("/stripe/callback/manual")
def stripe_oauth_callback_manual(
    code: str = Query(...),
    org_id: uuid.UUID = Query(...),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Manual callback endpoint for completing OAuth when redirect URL doesn't work.
    Use this when Stripe redirects to a URL you can't control (like localhost).
    
    Usage:
    1. Complete OAuth on Stripe and copy the authorization code from the URL
    2. Call this endpoint with the code and your org_id
    3. Example: POST /oauth/stripe/callback/manual?code=ac_xxxxx&org_id=your-org-id
    
    This endpoint requires authentication to ensure only the org owner can complete the connection.
    """
    from fastapi.responses import JSONResponse
    import httpx
    
    # Verify the user belongs to the org they're trying to connect
    if current_user.org_id != org_id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You can only connect Stripe for your own organization"
        )
    
    if not settings.STRIPE_SECRET_KEY:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Stripe not configured"
        )
    
    try:
        # Exchange authorization code for access token
        response = httpx.post(
            "https://connect.stripe.com/oauth/token",
            data={
                "client_secret": settings.STRIPE_SECRET_KEY,
                "code": code,
                "grant_type": "authorization_code"
            },
            timeout=30.0  # Increased timeout for token exchange
        )
        
        if response.status_code != 200:
            error_data = response.json() if response.headers.get("content-type", "").startswith("application/json") else {}
            error_msg = error_data.get("error_description", f"HTTP {response.status_code}: {response.text}")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Token exchange failed: {error_msg}"
            )
        
        token_data = response.json()
        access_token = token_data.get("access_token")
        refresh_token = token_data.get("refresh_token")
        stripe_user_id = token_data.get("stripe_user_id")
        
        if not access_token:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No access token in response"
            )
        
        # Encrypt tokens before storing
        encrypted_token = encrypt_token(access_token)
        encrypted_refresh = encrypt_token(refresh_token) if refresh_token else None
        
        expires_at = datetime.utcnow() + timedelta(days=365)
        if "expires_in" in token_data:
            expires_at = datetime.utcnow() + timedelta(seconds=token_data["expires_in"])
        
        # Check if token exists for this provider AND org
        existing = db.query(OAuthToken).filter(
            OAuthToken.provider == OAuthProvider.STRIPE,
            OAuthToken.org_id == org_id
        ).first()
        
        if existing:
            existing.access_token = encrypted_token
            existing.refresh_token = encrypted_refresh
            existing.account_id = stripe_user_id or f"acct_{code[:10]}"
            existing.expires_at = expires_at
        else:
            oauth_token = OAuthToken(
                org_id=org_id,
                provider=OAuthProvider.STRIPE,
                account_id=stripe_user_id or f"acct_{code[:10]}",
                access_token=encrypted_token,
                refresh_token=encrypted_refresh,
                expires_at=expires_at
            )
            db.add(oauth_token)
        
        db.commit()
        
        # Trigger initial historical data sync (full backfill) in background thread
        # This prevents the connection endpoint from timing out during large syncs
        import threading
        def sync_in_background():
            # Create a new database session for the background thread
            from app.db.session import SessionLocal
            bg_db = SessionLocal()
            try:
                from app.services.stripe_sync_v2 import sync_stripe_incremental
                print(f"[OAUTH] Starting initial historical data sync (full backfill) for org {org_id}...")
                sync_result = sync_stripe_incremental(bg_db, org_id=org_id, force_full=True)
                if sync_result.get("error"):
                    print(f"[OAUTH] ❌ Historical sync error: {sync_result.get('error')}")
                else:
                    print(f"[OAUTH] ✅ Historical sync complete for org {org_id}:")
                    print(f"   - Customers: {sync_result.get('customers_synced', 0)} new, {sync_result.get('customers_updated', 0)} updated")
                    print(f"   - Subscriptions: {sync_result.get('subscriptions_synced', 0)} new, {sync_result.get('subscriptions_updated', 0)} updated")
                    print(f"   - Payments: {sync_result.get('payments_synced', 0)} new, {sync_result.get('payments_updated', 0)} updated")
            except Exception as e:
                import traceback
                print(f"[OAUTH] ❌ Historical sync failed: {str(e)}")
                traceback.print_exc()
            finally:
                bg_db.close()
        
        # Start sync in background thread
        sync_thread = threading.Thread(target=sync_in_background, daemon=True)
        sync_thread.start()
        
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "success": True,
                "message": "Stripe connected successfully. Initial sync is running in the background.",
                "account_id": stripe_user_id,
                "org_id": str(org_id),
                "sync_in_progress": True
            }
        )
        
    except httpx.HTTPError as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Network error: {str(e)}"
        )
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error: {str(e)}"
        )


@router.post("/stripe/connect-direct")
def connect_stripe_direct(
    request: DirectApiKeyRequest,
    http_request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin)  # Require admin role
):
    """
    Connect Stripe directly using an API key (bypasses OAuth).
    Use this when you can't install the Stripe app on the publisher account.
    
    SECURITY: This endpoint requires admin role and is rate-limited.
    Direct API keys have full account access and cannot be revoked via Stripe.
    
    This endpoint:
    1. Validates the API key by making a test call to Stripe
    2. Retrieves the account ID
    3. Stores the API key as an OAuth token (encrypted)
    4. Triggers a historical data sync
    5. Logs the connection event for audit
    
    Args:
        request: Request body containing api_key (sk_test_..., sk_live_..., rk_test_..., or rk_live_...)
        http_request: HTTP request object for IP address and user agent
    
    Returns:
        Success message with account ID
    """
    from fastapi.responses import JSONResponse
    from app.core.rate_limit import _rate_limit_store, _rate_limit_lock, _cleanup_old_entries
    from app.core.audit import log_security_event
    from app.models.audit_log import AuditEventType
    from datetime import datetime, timedelta
    # Dynamic import for stripe
    try:
        import stripe
    except ImportError:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Stripe library is not installed. Please install it with: pip install stripe"
        )
    
    # Rate limiting: 3 attempts per 15 minutes per user
    _cleanup_old_entries()
    identifier = f"direct_api_key_{current_user.id}_{current_user.org_id}"
    now = datetime.utcnow()
    window_start = now - timedelta(seconds=900)  # 15 minutes
    
    with _rate_limit_lock:
        recent_requests = [
            ts for ts, _ in _rate_limit_store[identifier]
            if ts > window_start
        ]
        
        if len(recent_requests) >= 3:
            # Log rate limit event
            log_security_event(
                db=db,
                event_type=AuditEventType.RATE_LIMIT_EXCEEDED,
                org_id=current_user.org_id,
                user_id=current_user.id,
                resource_type="api_endpoint",
                resource_id="connect_stripe_direct",
                ip_address=http_request.client.host if http_request.client else None,
                user_agent=http_request.headers.get("user-agent"),
                details={
                    "endpoint": "connect_stripe_direct",
                    "max_requests": 3,
                    "window_seconds": 900,
                    "recent_requests": len(recent_requests)
                }
            )
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail="Rate limit exceeded: 3 direct API key connections per 15 minutes. Please try again later or use OAuth instead."
            )
        
        # Add current request
        _rate_limit_store[identifier].append((now, 1))
    
    api_key = request.api_key
    if not api_key or not api_key.strip():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="API key is required"
        )
    
    # Validate API key format
    api_key = api_key.strip()
    if not api_key.startswith(('sk_test_', 'sk_live_', 'rk_test_', 'rk_live_')):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid API key format. Must start with 'sk_test_', 'sk_live_', 'rk_test_', or 'rk_live_'"
        )
    
    try:
        # Test the API key by making a call to Stripe
        stripe.api_key = api_key
        account = stripe.Account.retrieve()
        account_id = account.id
        
        print(f"[DIRECT_CONNECT] Validated API key for account: {account_id}")
        
        # Log security event BEFORE storing (for audit trail)
        org_id = current_user.org_id
        log_security_event(
            db=db,
            event_type=AuditEventType.API_KEY_CONNECTED,
            org_id=org_id,
            user_id=current_user.id,
            resource_type="stripe",
            resource_id=account_id,
            ip_address=http_request.client.host if http_request.client else None,
            user_agent=http_request.headers.get("user-agent"),
            details={
                "account_id": account_id,
                "api_key_prefix": api_key[:10] + "..." if len(api_key) > 10 else "***",
                "mode": "test" if api_key.startswith(("sk_test_", "rk_test_")) else "live"
            }
        )
        
        # Encrypt the API key before storing
        encrypted_token = encrypt_token(api_key)
        
        # Check if token exists for this provider AND org
        existing = db.query(OAuthToken).filter(
            OAuthToken.provider == OAuthProvider.STRIPE,
            OAuthToken.org_id == org_id
        ).first()
        
        if existing:
            existing.access_token = encrypted_token
            existing.refresh_token = None  # Direct API keys don't have refresh tokens
            existing.account_id = account_id
            existing.expires_at = None  # API keys don't expire
            existing.scope = "direct_api_key"  # Mark as direct connection
        else:
            oauth_token = OAuthToken(
                org_id=org_id,
                provider=OAuthProvider.STRIPE,
                account_id=account_id,
                access_token=encrypted_token,
                refresh_token=None,  # Direct API keys don't have refresh tokens
                expires_at=None,  # API keys don't expire
                scope="direct_api_key"  # Mark as direct connection
            )
            db.add(oauth_token)
        
        db.commit()
        
        # Trigger initial historical data sync (full backfill) in background thread
        # This prevents the connection endpoint from timing out during large syncs
        # Also syncs Treasury Transactions and triggers reconciliation
        import threading
        def sync_in_background():
            # Create a new database session for the background thread
            from app.db.session import SessionLocal
            bg_db = SessionLocal()
            try:
                # Step 1: Sync old payment system (customers, subscriptions, payments)
                from app.services.stripe_sync_v2 import sync_stripe_incremental, reconcile_stripe_data
                print(f"[DIRECT_CONNECT] Starting initial historical data sync (full backfill) for org {org_id}...")
                sync_result = sync_stripe_incremental(bg_db, org_id=org_id, force_full=True)
                if sync_result.get("error"):
                    print(f"[DIRECT_CONNECT] ❌ Historical sync error: {sync_result.get('error')}")
                else:
                    print(f"[DIRECT_CONNECT] ✅ Historical sync complete for org {org_id}:")
                    print(f"   - Customers: {sync_result.get('customers_synced', 0)} new, {sync_result.get('customers_updated', 0)} updated")
                    print(f"   - Subscriptions: {sync_result.get('subscriptions_synced', 0)} new, {sync_result.get('subscriptions_updated', 0)} updated")
                    print(f"   - Payments: {sync_result.get('payments_synced', 0)} new, {sync_result.get('payments_updated', 0)} updated")
                
                # Step 2: Sync Treasury Transactions (new source of truth)
                try:
                    from app.services.stripe_treasury_sync import sync_treasury_transactions
                    from datetime import timedelta
                    created_since = datetime.utcnow() - timedelta(days=365)  # Sync last year
                    print(f"[DIRECT_CONNECT] Starting Treasury Transactions sync for org {org_id}...")
                    treasury_result = sync_treasury_transactions(
                        db=bg_db,
                        org_id=org_id,
                        financial_account_id=None,
                        limit=100,
                        created_since=created_since
                    )
                    print(f"[DIRECT_CONNECT] ✅ Treasury Transactions sync complete for org {org_id}:")
                    print(f"   - Transactions synced: {treasury_result.get('transactions_synced', 0)}")
                    print(f"   - Transactions updated: {treasury_result.get('transactions_updated', 0)}")
                    print(f"   - Clients created: {treasury_result.get('clients_created', 0)}")
                    print(f"   - Clients updated: {treasury_result.get('clients_updated', 0)}")
                except Exception as treasury_error:
                    print(f"[DIRECT_CONNECT] ⚠️ Treasury sync failed (non-critical): {str(treasury_error)}")
                    import traceback
                    traceback.print_exc()
                
                # Step 3: Reconcile/recalculate derived metrics
                try:
                    print(f"[DIRECT_CONNECT] Starting reconciliation for org {org_id}...")
                    reconcile_result = reconcile_stripe_data(bg_db, org_id=org_id)
                    print(f"[DIRECT_CONNECT] ✅ Reconciliation complete for org {org_id}:")
                    print(f"   - Clients reconciled: {reconcile_result.get('clients_reconciled', 0)}")
                    print(f"   - Revenue recalculated: ${reconcile_result.get('revenue_recalculated', 0):.2f}")
                except Exception as reconcile_error:
                    print(f"[DIRECT_CONNECT] ⚠️ Reconciliation failed (non-critical): {str(reconcile_error)}")
                    import traceback
                    traceback.print_exc()
                    
            except Exception as e:
                import traceback
                print(f"[DIRECT_CONNECT] ❌ Background sync failed: {str(e)}")
                traceback.print_exc()
            finally:
                bg_db.close()
        
        # Start sync in background thread
        sync_thread = threading.Thread(target=sync_in_background, daemon=True)
        sync_thread.start()
        
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "success": True,
                "message": "Stripe connected successfully using API key. Initial sync is running in the background.",
                "account_id": account_id,
                "org_id": str(org_id),
                "mode": "test" if api_key.startswith(("sk_test_", "rk_test_")) else "live",
                "sync_in_progress": True
            }
        )
        
    except stripe.error.AuthenticationError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Invalid API key: {str(e)}"
        )
    except stripe.error.StripeError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Stripe API error: {str(e)}"
        )
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error connecting Stripe: {str(e)}"
        )


@router.get("/stripe/verify")
def verify_stripe_connection(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Verify Stripe OAuth connection status for the current user's organization.
    Returns connection details including account ID and token status.
    """
    oauth_token = db.query(OAuthToken).filter(
        OAuthToken.provider == OAuthProvider.STRIPE,
        OAuthToken.org_id == current_user.org_id
    ).first()
    
    if not oauth_token:
        return {
            "connected": False,
            "message": "Stripe is not connected for this organization",
            "org_id": str(current_user.org_id)
        }
    
    # Check if token is expired
    is_expired = oauth_token.expires_at and oauth_token.expires_at < datetime.utcnow()
    
    return {
        "connected": not is_expired,
        "message": "Stripe is connected" if not is_expired else "Stripe token has expired",
        "account_id": oauth_token.account_id,
        "org_id": str(current_user.org_id),
        "expires_at": oauth_token.expires_at.isoformat() if oauth_token.expires_at else None,
        "is_expired": is_expired
    }


@router.get("/stripe/callback/manual")
def stripe_oauth_callback_manual_get(
    code: str = Query(...),
    db: Session = Depends(get_db)
):
    """
    Manual callback endpoint for localhost development.
    Use this if the automatic redirect doesn't work.
    
    Note: This endpoint does NOT require authentication for easier localhost development.
    The authorization code itself provides security.
    
    Usage: After completing OAuth on Stripe, copy the code from the URL and visit:
    http://localhost:8000/oauth/stripe/callback/manual?code=ac_xxxxx
    """
    from fastapi.responses import RedirectResponse
    import httpx
    
    frontend_url = settings.FRONTEND_URL
    
    if not settings.STRIPE_SECRET_KEY:
        return RedirectResponse(
            url=f"{frontend_url}/?stripe_error=configuration_error&error_description=Stripe not configured",
            status_code=302
        )
    
    try:
        # Exchange authorization code for access token
        response = httpx.post(
            "https://connect.stripe.com/oauth/token",
            data={
                "client_secret": settings.STRIPE_SECRET_KEY,
                "code": code,
                "grant_type": "authorization_code"
            },
            timeout=30.0  # Increased timeout for token exchange
        )
        
        if response.status_code != 200:
            error_data = response.json() if response.headers.get("content-type", "").startswith("application/json") else {}
            error_msg = error_data.get("error_description", f"HTTP {response.status_code}: {response.text}")
            return RedirectResponse(
                url=f"{frontend_url}/?stripe_error=token_exchange_failed&error_description={error_msg}",
                status_code=302
            )
        
        token_data = response.json()
        access_token = token_data.get("access_token")
        refresh_token = token_data.get("refresh_token")
        stripe_user_id = token_data.get("stripe_user_id")
        
        if not access_token:
            return RedirectResponse(
                url=f"{frontend_url}/?stripe_error=no_access_token&error_description=No access token in response",
                status_code=302
            )
        
        # Encrypt tokens before storing
        encrypted_token = encrypt_token(access_token)
        encrypted_refresh = encrypt_token(refresh_token) if refresh_token else None
        
        expires_at = datetime.utcnow() + timedelta(days=365)
        if "expires_in" in token_data:
            expires_at = datetime.utcnow() + timedelta(seconds=token_data["expires_in"])
        
        # For manual callback, we can't extract org_id from state
        # Use DEFAULT_ORG_ID as fallback (for development/testing)
        # In production, always use the regular callback which has state
        org_id = DEFAULT_ORG_ID
        
        # CRITICAL: Check if token exists for this provider AND org (multi-tenant isolation)
        existing = db.query(OAuthToken).filter(
            OAuthToken.provider == OAuthProvider.STRIPE,
            OAuthToken.org_id == org_id
        ).first()
        
        if existing:
            existing.access_token = encrypted_token
            existing.refresh_token = encrypted_refresh
            existing.account_id = stripe_user_id or f"acct_{code[:10]}"
            existing.expires_at = expires_at
        else:
            # Store token with org_id for multi-tenant isolation
            oauth_token = OAuthToken(
                org_id=org_id,
                provider=OAuthProvider.STRIPE,
                account_id=stripe_user_id or f"acct_{code[:10]}",
                access_token=encrypted_token,
                refresh_token=encrypted_refresh,
                expires_at=expires_at
            )
            db.add(oauth_token)
        
        db.commit()
        
        # Trigger historical data sync automatically after OAuth connection in background thread
        # This prevents the redirect from being delayed during large syncs
        import threading
        def sync_in_background():
            # Create a new database session for the background thread
            from app.db.session import SessionLocal
            bg_db = SessionLocal()
            try:
                from app.services.stripe_sync_v2 import sync_stripe_incremental
                print(f"[OAUTH] Starting initial historical data sync (full backfill) for org {org_id}...")
                sync_result = sync_stripe_incremental(bg_db, org_id=org_id, force_full=True)
                if sync_result.get("error"):
                    print(f"[OAUTH] ❌ Historical sync error: {sync_result.get('error')}")
                else:
                    print(f"[OAUTH] ✅ Historical sync complete for org {org_id}:")
                    print(f"   - Customers: {sync_result.get('customers_synced', 0)} new, {sync_result.get('customers_updated', 0)} updated")
                    print(f"   - Subscriptions: {sync_result.get('subscriptions_synced', 0)} new, {sync_result.get('subscriptions_updated', 0)} updated")
                    print(f"   - Payments: {sync_result.get('payments_synced', 0)} new, {sync_result.get('payments_updated', 0)} updated")
            except Exception as e:
                import traceback
                print(f"[OAUTH] ❌ Historical sync failed: {str(e)}")
                traceback.print_exc()
            finally:
                bg_db.close()
        
        # Start sync in background thread
        sync_thread = threading.Thread(target=sync_in_background, daemon=True)
        sync_thread.start()
        
        # Redirect to frontend with success message (immediately, sync runs in background)
        return RedirectResponse(
            url=f"{frontend_url}/?stripe_connected=true",
            status_code=302
        )
        
    except httpx.HTTPError as e:
        db.rollback()
        return RedirectResponse(
            url=f"{frontend_url}/?stripe_error=network_error&error_description={str(e)}",
            status_code=302
        )
    except Exception as e:
        db.rollback()
        return RedirectResponse(
            url=f"{frontend_url}/?stripe_error=unknown_error&error_description={str(e)}",
            status_code=302
        )


@router.post("/brevo/start", response_model=OAuthStartResponse)
def start_brevo_oauth(
    current_user: User = Depends(get_current_user)
):
    # TEMPORARILY DISABLED: OAuth is disabled for deployment testing
    # Only API key authentication is available
    raise HTTPException(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        detail="OAuth authentication is temporarily disabled. Please use API key authentication instead."
    )
    
    # Original OAuth code commented out:
    """
    Start Brevo OAuth flow.
    Generates authorization URL that redirects users to Brevo's authentication page.
    Uses only Brevo username/password (not Google/Apple SSO).
    """
    if not settings.BREVO_CLIENT_ID:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Brevo OAuth not configured. Set BREVO_CLIENT_ID in environment."
        )
    
    if not settings.BREVO_REDIRECT_URI:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="BREVO_REDIRECT_URI is not set. Set it in .env file."
        )
    
    # Generate state parameter with org_id for multi-tenant support
    # Use compact format to avoid HTTP 431 (Request Header Fields Too Large) errors
    import secrets
    import base64
    import json
    import time
    state_data = {
        "org_id": str(current_user.org_id),
        "nonce": secrets.token_urlsafe(8),  # Reduced from 16 to 8 bytes for smaller state
        "ts": int(time.time())  # Shorter key name, timestamp for CSRF protection
    }
    # Use compact JSON (no spaces) and base64 encode
    state = base64.urlsafe_b64encode(json.dumps(state_data, separators=(',', ':')).encode()).decode()
    
    # Use custom BREVO_LOGIN_URL if provided, otherwise use standard Brevo OAuth URL
    # According to Brevo docs: https://auth.brevo.com/realms/apiv3/protocol/openid-connect/auth
    # BREVO_LOGIN_URL should be the base URL WITHOUT query parameters
    # If query parameters are included, they will be stripped and replaced with our own
    base_url = settings.BREVO_LOGIN_URL or "https://auth.brevo.com/realms/apiv3/protocol/openid-connect/auth"
    
    # Remove any existing query parameters from base_url if provided
    # This ensures we always use our own parameters (including state)
    if "?" in base_url:
        base_url = base_url.split("?")[0]
        print(f"[BREVO OAUTH] ⚠️  WARNING: BREVO_LOGIN_URL contained query parameters. They have been stripped.")
        print(f"[BREVO OAUTH] ⚠️  BREVO_LOGIN_URL should only contain the base URL, e.g.:")
        print(f"[BREVO OAUTH] ⚠️  BREVO_LOGIN_URL=https://auth.brevo.com/realms/apiv3/protocol/openid-connect/auth")
        print(f"[BREVO OAUTH] ⚠️  Do NOT include query parameters like ?response_type=code&client_id=...")
    
    # Ensure redirect_uri matches exactly what's registered in Brevo
    # It must be URL-encoded in the query string, but the value itself should be the exact URI
    redirect_uri = settings.BREVO_REDIRECT_URI.strip()
    
    # Validate redirect URI format
    if not redirect_uri.startswith(('http://', 'https://')):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid BREVO_REDIRECT_URI format. Must start with http:// or https://. Current value: {redirect_uri[:50]}..."
        )
    
    # Check if redirect URI has a path (not just domain)
    from urllib.parse import urlparse
    parsed_uri = urlparse(redirect_uri)
    if not parsed_uri.path or parsed_uri.path == '/':
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid BREVO_REDIRECT_URI: Must include a path (e.g., /oauth/brevo/callback or /api/oauth/brevo/callback). Current value: {redirect_uri}"
        )
    
    # Warn if path doesn't include 'callback' or 'oauth'
    if 'callback' not in parsed_uri.path.lower() and 'oauth' not in parsed_uri.path.lower():
        print(f"[BREVO OAUTH] ⚠️  WARNING: Redirect URI path doesn't contain 'callback' or 'oauth': {parsed_uri.path}")
    
    # Debug logging - print full details
    print(f"\n{'='*80}")
    print(f"[BREVO OAUTH] Starting OAuth flow:")
    print(f"  - Client ID: {settings.BREVO_CLIENT_ID}")
    print(f"  - Redirect URI (raw): {redirect_uri}")
    print(f"  - Redirect URI (length): {len(redirect_uri)} characters")
    print(f"  - Base URL: {base_url}")
    print(f"  - Org ID: {current_user.org_id}")
    print(f"{'='*80}\n")
    
    # Build OAuth authorization URL according to Brevo documentation:
    # https://auth.brevo.com/realms/apiv3/protocol/openid-connect/auth?response_type=code&client_id={{YOUR_CLIENT_ID}}&redirect_uri={{YOUR_CALLBACK_URL}}&scope=openid
    # Note: We also include 'state' parameter for CSRF protection (standard OAuth 2.0 security practice)
    params = {
        "response_type": "code",  # Required: authorization code flow
        "client_id": settings.BREVO_CLIENT_ID,  # Required: OAuth application identifier
        "redirect_uri": redirect_uri,  # Required: callback URL (urlencode will encode this properly)
        "scope": "openid",  # Required: according to Brevo docs
        "state": state,  # Optional but recommended: CSRF protection and org_id tracking
    }
    
    # Build the OAuth URL with all parameters including state
    redirect_url = f"{base_url}?{urlencode(params)}"
    
    # Verify state parameter is in the URL (critical for security)
    if "state=" not in redirect_url:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to include state parameter in OAuth URL. This is required for security."
        )
    
    # Decode the redirect_uri from the generated URL to verify it's correct
    from urllib.parse import parse_qs, urlparse, unquote
    parsed_url = urlparse(redirect_url)
    query_params = parse_qs(parsed_url.query)
    redirect_uri_in_url = query_params.get('redirect_uri', [None])[0]
    if redirect_uri_in_url:
        redirect_uri_in_url = unquote(redirect_uri_in_url)
    
    # Log the full URL (truncated for readability)
    print(f"\n[BREVO OAUTH] ========== OAuth URL Generation ==========")
    print(f"[BREVO OAUTH] Client ID: {settings.BREVO_CLIENT_ID}")
    print(f"[BREVO OAUTH] Redirect URI from .env: {redirect_uri}")
    print(f"[BREVO OAUTH] Redirect URI length: {len(redirect_uri)} characters")
    print(f"[BREVO OAUTH] Redirect URI in generated URL: {redirect_uri_in_url}")
    print(f"[BREVO OAUTH] Match check: {'✅ MATCH' if redirect_uri == redirect_uri_in_url else '❌ MISMATCH'}")
    print(f"[BREVO OAUTH] State parameter: {'✅ PRESENT' if state and 'state=' in redirect_url else '❌ MISSING'}")
    print(f"[BREVO OAUTH] State length: {len(state) if state else 0} characters")
    print(f"[BREVO OAUTH] Full OAuth URL (first 400 chars):")
    print(f"  {redirect_url[:400]}...")
    print(f"[BREVO OAUTH] ==========================================\n")
    
    # Verify the redirect URI is correct before returning
    if len(redirect_uri) < 20 or '/callback' not in redirect_uri.lower():
        print(f"[BREVO OAUTH] ⚠️  WARNING: Redirect URI seems incomplete or missing callback path!")
        print(f"[BREVO OAUTH] ⚠️  Expected format: https://yourdomain.com/api/oauth/brevo/callback")
        print(f"[BREVO OAUTH] ⚠️  Current value: {redirect_uri}")
    
    if redirect_uri != redirect_uri_in_url:
        print(f"[BREVO OAUTH] ⚠️  WARNING: Redirect URI mismatch detected!")
        print(f"[BREVO OAUTH] ⚠️  From .env: {redirect_uri}")
        print(f"[BREVO OAUTH] ⚠️  In URL: {redirect_uri_in_url}")
    
    return {
        "redirect_url": redirect_url,
        "debug_info": {
            "redirect_uri_from_env": redirect_uri,
            "redirect_uri_in_url": redirect_uri_in_url,
            "redirect_uri_match": redirect_uri == redirect_uri_in_url,
            "redirect_uri_length": len(redirect_uri),
            "client_id": settings.BREVO_CLIENT_ID,
            "base_url": base_url,
            "full_oauth_url": redirect_url
        }
    }


@router.get("/brevo/callback")
def brevo_oauth_callback(
    code: str = Query(None),
    state: str = Query(None),
    error: str = Query(None),
    error_description: str = Query(None),
    db: Session = Depends(get_db)
):
    """
    Handle Brevo OAuth callback.
    Exchanges authorization code for access token and redirects to frontend.
    
    Note: This endpoint does NOT require authentication because OAuth callbacks
    are accessed directly from Brevo. The authorization code provides security.
    
    See: https://developers.brevo.com/docs/integration-part
    """
    from fastapi.responses import RedirectResponse
    import httpx
    
    # Get frontend URL from settings
    frontend_url = settings.FRONTEND_URL
    
    # Handle OAuth errors - redirect to dashboard with brevo tab active
    if error:
        error_msg = error_description or error
        return RedirectResponse(
            url=f"{frontend_url}/?brevo_error={error}&error_description={error_msg}&tab=brevo",
            status_code=302
        )
    
    # Code is required if no error
    if not code:
        return RedirectResponse(
            url=f"{frontend_url}/?brevo_error=no_code&error_description=No authorization code provided&tab=brevo",
            status_code=302
        )
    
    if not settings.BREVO_CLIENT_SECRET:
        return RedirectResponse(
            url=f"{frontend_url}/?brevo_error=configuration_error&error_description=Brevo not configured&tab=brevo",
            status_code=302
        )
    
    # Extract org_id from state parameter for multi-tenant support
    org_id = DEFAULT_ORG_ID  # Fallback to default
    if state:
        try:
            import base64
            import json
            state_data = json.loads(base64.urlsafe_b64decode(state.encode()).decode())
            # Support both old format (timestamp) and new format (ts)
            org_id = uuid.UUID(state_data.get("org_id", str(DEFAULT_ORG_ID)))
        except Exception as e:
            # If state parsing fails, log but continue with default
            print(f"[BREVO OAUTH] Warning: Failed to parse state parameter: {e}. Using default org.")
    
    try:
        # Exchange authorization code for access token
        # According to Brevo docs: POST to https://api.brevo.com/v3/token
        # Using data= parameter automatically URL-encodes the form data (matches curl --data-urlencode)
        # Required parameters per documentation:
        # - grant_type: "authorization_code"
        # - client_id: OAuth application identifier
        # - client_secret: Secret token for authentication
        # - code: Authorization code from callback (10-minute TTL per docs)
        # - redirect_uri: Must match exactly the redirect_uri used in authorization URL
        token_exchange_data = {
            "grant_type": "authorization_code",  # Required: OAuth grant type
            "client_id": settings.BREVO_CLIENT_ID,  # Required: OAuth application identifier
            "client_secret": settings.BREVO_CLIENT_SECRET,  # Required: Secret token
            "code": code,  # Required: Authorization code (10-minute TTL per docs)
            "redirect_uri": settings.BREVO_REDIRECT_URI  # Required: Must match authorization URL exactly
        }
        
        print(f"[BREVO OAUTH] Exchanging code for token (per Brevo docs):")
        print(f"  - Token endpoint: https://api.brevo.com/v3/token")
        print(f"  - Client ID: {settings.BREVO_CLIENT_ID}")
        print(f"  - Redirect URI: {settings.BREVO_REDIRECT_URI}")
        print(f"  - Code length: {len(code) if code else 0} characters")
        print(f"  - Note: Authorization code has 10-minute TTL (per Brevo docs)")
        
        response = httpx.post(
            "https://api.brevo.com/v3/token",
            data=token_exchange_data,  # httpx automatically URL-encodes when using data= with form data
            headers={
                "Content-Type": "application/x-www-form-urlencoded"
            },
            timeout=30.0
        )
        
        if response.status_code != 200:
            error_data = response.json() if response.headers.get("content-type", "").startswith("application/json") else {}
            error_msg = error_data.get("error_description", f"HTTP {response.status_code}: {response.text}")
            return RedirectResponse(
                url=f"{frontend_url}/?brevo_error=token_exchange_failed&error_description={error_msg}&tab=brevo",
                status_code=302
            )
        
        token_data = response.json()
        
        # Extract tokens according to Brevo documentation
        # Response includes: access_token, expires_in, refresh_token, token_type, id_token, session_state, scope
        access_token = token_data.get("access_token")
        refresh_token = token_data.get("refresh_token")
        expires_in = token_data.get("expires_in", 43200)  # Default 12 hours (43200 seconds) if not specified
        token_type = token_data.get("token_type", "Bearer")  # Usually "Bearer"
        id_token = token_data.get("id_token")  # JWT token with user info (optional)
        session_state = token_data.get("session_state")  # Session state (optional)
        scope = token_data.get("scope", "openid")  # Scopes granted
        
        if not access_token:
            print(f"[BREVO OAUTH] ❌ ERROR: No access_token in response")
            print(f"[BREVO OAUTH] Response keys: {list(token_data.keys())}")
            return RedirectResponse(
                url=f"{frontend_url}/?brevo_error=no_access_token&error_description=No access token in response&tab=brevo",
                status_code=302
            )
        
        print(f"[BREVO OAUTH] ✅ Token exchange successful:")
        print(f"  - Token type: {token_type}")
        print(f"  - Expires in: {expires_in} seconds ({expires_in / 3600:.1f} hours)")
        print(f"  - Scope: {scope}")
        print(f"  - Has refresh token: {bool(refresh_token)}")
        print(f"  - Has ID token: {bool(id_token)}")
        
        # Encrypt tokens before storing
        encrypted_token = encrypt_token(access_token)
        encrypted_refresh = encrypt_token(refresh_token) if refresh_token else None
        
        # Calculate expiration timestamp
        expires_at = datetime.utcnow() + timedelta(seconds=expires_in)
        
        # CRITICAL: Check if token exists for this provider AND org (multi-tenant isolation)
        existing = db.query(OAuthToken).filter(
            OAuthToken.provider == OAuthProvider.BREVO,
            OAuthToken.org_id == org_id
        ).first()
        
        if existing:
            existing.access_token = encrypted_token
            existing.refresh_token = encrypted_refresh
            existing.expires_at = expires_at
            existing.scope = scope  # Use extracted scope
            print(f"[BREVO OAUTH] Updated existing token for org {org_id}")
        else:
            # Store token with org_id for multi-tenant isolation
            oauth_token = OAuthToken(
                org_id=org_id,  # Use org_id from state parameter
                provider=OAuthProvider.BREVO,
                access_token=encrypted_token,
                refresh_token=encrypted_refresh,
                expires_at=expires_at,
                scope=scope  # Use extracted scope
            )
            db.add(oauth_token)
            print(f"[BREVO OAUTH] Created new token for org {org_id}")
        
        db.commit()
        
        # Redirect to frontend dashboard with success message and brevo tab active
        # This ensures users are sent back to the OS dashboard with state updated
        return RedirectResponse(
            url=f"{frontend_url}/?brevo_connected=true&tab=brevo",
            status_code=302
        )
        
    except httpx.HTTPError as e:
        db.rollback()
        return RedirectResponse(
            url=f"{frontend_url}/?brevo_error=network_error&error_description={str(e)}&tab=brevo",
            status_code=302
        )
    except Exception as e:
        db.rollback()
        import traceback
        traceback.print_exc()
        return RedirectResponse(
            url=f"{frontend_url}/?brevo_error=unknown_error&error_description={str(e)}&tab=brevo",
            status_code=302
        )


@router.post("/brevo/connect-direct")
def connect_brevo_direct(
    request: DirectApiKeyRequest,
    http_request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin)  # Require admin role
):
    """
    Connect Brevo directly using an API key (bypasses OAuth).
    Use this when OAuth is not working or you prefer API key authentication.
    
    SECURITY: This endpoint requires admin role and is rate-limited.
    Direct API keys have full account access.
    
    This endpoint:
    1. Validates the API key by making a test call to Brevo API
    2. Retrieves the account information
    3. Stores the API key as an OAuth token (encrypted)
    4. Logs the connection event for audit
    
    Args:
        request: Request body containing api_key (Brevo API key)
        http_request: HTTP request object for IP address and user agent
    
    Returns:
        Success message with account email
    """
    from fastapi.responses import JSONResponse
    from app.core.rate_limit import _rate_limit_store, _rate_limit_lock, _cleanup_old_entries
    from app.core.audit import log_security_event
    from app.models.audit_log import AuditEventType
    from datetime import datetime, timedelta
    import httpx
    
    # Rate limiting: 3 attempts per 15 minutes per user
    _cleanup_old_entries()
    identifier = f"brevo_direct_api_key_{current_user.id}_{current_user.org_id}"
    now = datetime.utcnow()
    window_start = now - timedelta(seconds=900)  # 15 minutes
    
    with _rate_limit_lock:
        if identifier not in _rate_limit_store:
            _rate_limit_store[identifier] = []
        recent_requests = [
            ts for ts, _ in _rate_limit_store[identifier]
            if ts > window_start
        ]
        
        if len(recent_requests) >= 3:
            # Log rate limit event
            log_security_event(
                db=db,
                event_type=AuditEventType.RATE_LIMIT_EXCEEDED,
                org_id=current_user.org_id,
                user_id=current_user.id,
                resource_type="api_endpoint",
                resource_id="connect_brevo_direct",
                ip_address=http_request.client.host if http_request.client else None,
                user_agent=http_request.headers.get("user-agent"),
                details={
                    "endpoint": "connect_brevo_direct",
                    "max_requests": 3,
                    "window_seconds": 900,
                    "recent_requests": len(recent_requests)
                }
            )
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail="Rate limit exceeded: 3 direct API key connections per 15 minutes. Please try again later or use OAuth instead."
            )
        
        # Add current request
        _rate_limit_store[identifier].append((now, 1))
    
    api_key = request.api_key
    if not api_key or not api_key.strip():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="API key is required"
        )
    
    # Validate and test the API key by making a call to Brevo API
    api_key = api_key.strip()
    
    try:
        # Test the API key by calling Brevo account endpoint
        # Brevo uses 'api-key' header, not 'Authorization: Bearer'
        response = httpx.get(
            "https://api.brevo.com/v3/account",
            headers={
                "api-key": api_key,
                "accept": "application/json"
            },
            timeout=10.0
        )
        
        if response.status_code != 200:
            error_data = response.json() if response.headers.get("content-type", "").startswith("application/json") else {}
            error_msg = error_data.get("message", f"HTTP {response.status_code}: {response.text}")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail=f"Invalid API key: {error_msg}"
            )
        
        account_data = response.json()
        account_email = account_data.get("email")
        account_id = account_email or "unknown"
        
        print(f"[BREVO DIRECT] Validated API key for account: {account_email}")
        
        # Log security event BEFORE storing (for audit trail)
        org_id = current_user.org_id
        log_security_event(
            db=db,
            event_type=AuditEventType.API_KEY_CONNECTED,
            org_id=org_id,
            user_id=current_user.id,
            resource_type="brevo",
            resource_id=account_id,
            ip_address=http_request.client.host if http_request.client else None,
            user_agent=http_request.headers.get("user-agent"),
            details={
                "account_email": account_email,
                "api_key_prefix": api_key[:10] + "..." if len(api_key) > 10 else "***",
                "method": "api_key"
            }
        )
        
        # Encrypt the API key before storing
        encrypted_token = encrypt_token(api_key)
        
        # Check if token exists for this provider AND org
        existing = db.query(OAuthToken).filter(
            OAuthToken.provider == OAuthProvider.BREVO,
            OAuthToken.org_id == org_id
        ).first()
        
        if existing:
            existing.access_token = encrypted_token
            existing.refresh_token = None  # API keys don't have refresh tokens
            existing.expires_at = None  # API keys don't expire
            existing.scope = "api_key"  # Mark as API key method
            existing.account_id = account_id
            print(f"[BREVO DIRECT] Updated existing connection for org {org_id}")
        else:
            # Store API key as OAuth token (encrypted)
            oauth_token = OAuthToken(
                org_id=org_id,
                provider=OAuthProvider.BREVO,
                access_token=encrypted_token,
                refresh_token=None,  # API keys don't have refresh tokens
                expires_at=None,  # API keys don't expire
                scope="api_key",  # Mark as API key method
                account_id=account_id
            )
            db.add(oauth_token)
            print(f"[BREVO DIRECT] Created new connection for org {org_id}")
        
        db.commit()
        
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "success": True,
                "message": "Brevo connected successfully using API key.",
                "account_email": account_email,
                "org_id": str(org_id),
                "method": "api_key"
            }
        )
        
    except HTTPException:
        raise
    except httpx.HTTPError as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Network error validating API key: {str(e)}"
        )
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error connecting Brevo: {str(e)}"
        )


@router.delete("/stripe/disconnect", status_code=status.HTTP_204_NO_CONTENT)
def disconnect_stripe(
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin_or_owner)
):
    """
    Disconnect Stripe (OAuth or API key) for the current user's organization.
    Removes the connection token and all stored Stripe data for this org so that
    reconnecting (e.g. with a different account or API key) starts with a clean slate
    and avoids duplicate payments from previous connection data.
    """
    org_id = getattr(current_user, "selected_org_id", current_user.org_id)

    # Delete stored Stripe data for this org so reconnect + resync doesn't mix with old data
    db.query(StripePayment).filter(StripePayment.org_id == org_id).delete(synchronize_session=False)
    db.query(StripeTreasuryTransaction).filter(StripeTreasuryTransaction.org_id == org_id).delete(synchronize_session=False)
    db.query(StripeSubscription).filter(StripeSubscription.org_id == org_id).delete(synchronize_session=False)
    db.query(StripeEvent).filter(StripeEvent.org_id == org_id).delete(synchronize_session=False)

    # Remove the connection token (OAuth or API key)
    oauth_token = db.query(OAuthToken).filter(
        OAuthToken.provider == OAuthProvider.STRIPE,
        OAuthToken.org_id == org_id
    ).first()
    if oauth_token:
        db.delete(oauth_token)

    db.commit()
    return None


@router.get("/brevo/debug")
def debug_brevo_config(
    current_user: User = Depends(get_current_user)
):
    """
    Debug endpoint to verify Brevo OAuth configuration.
    Helps troubleshoot redirect URI mismatches.
    """
    import secrets
    import base64
    import json
    import time
    
    # Generate state parameter (same as in start_brevo_oauth)
    # Use compact format to avoid HTTP 431 errors
    state_data = {
        "org_id": str(current_user.org_id),
        "nonce": secrets.token_urlsafe(8),  # Reduced for smaller state
        "ts": int(time.time())  # Shorter key name
    }
    state = base64.urlsafe_b64encode(json.dumps(state_data, separators=(',', ':')).encode()).decode()
    
    base_url = settings.BREVO_LOGIN_URL or "https://auth.brevo.com/realms/apiv3/protocol/openid-connect/auth"
    if "?" in base_url:
        base_url = base_url.split("?")[0]
    
    redirect_uri = settings.BREVO_REDIRECT_URI.strip()
    
    params = {
        "response_type": "code",
        "client_id": settings.BREVO_CLIENT_ID,
        "redirect_uri": redirect_uri,
        "scope": "openid",
        "state": state,
    }
    redirect_url = f"{base_url}?{urlencode(params)}"
    
    # Extract just the redirect_uri from the encoded URL to show what Brevo will see
    from urllib.parse import parse_qs, urlparse, unquote
    parsed = urlparse(redirect_url)
    query_params = parse_qs(parsed.query)
    redirect_uri_in_url_encoded = query_params.get('redirect_uri', [None])[0]
    redirect_uri_in_url = unquote(redirect_uri_in_url_encoded) if redirect_uri_in_url_encoded else None
    
    return {
        "configuration": {
            "client_id": settings.BREVO_CLIENT_ID,
            "client_secret_set": bool(settings.BREVO_CLIENT_SECRET),
            "redirect_uri_from_env": redirect_uri,
            "redirect_uri_length": len(redirect_uri),
            "redirect_uri_in_generated_url": redirect_uri_in_url,
            "redirect_uri_encoded": redirect_uri_in_url_encoded,
            "base_url": base_url,
            "frontend_url": settings.FRONTEND_URL,
        },
        "generated_url": {
            "full_url": redirect_url,
            "redirect_uri_parameter_decoded": redirect_uri_in_url,
            "redirect_uri_parameter_encoded": redirect_uri_in_url_encoded,
            "state_length": len(state),
        },
        "comparison": {
            "env_redirect_uri": redirect_uri,
            "url_decoded_redirect_uri": redirect_uri_in_url,
            "match": redirect_uri == redirect_uri_in_url,
            "exact_match_required": "The redirect_uri in Brevo dashboard must match EXACTLY (character for character)",
        },
        "instructions": {
            "step1": "Copy the 'redirect_uri_from_env' value above",
            "step2": "Go to https://app.brevo.com/settings/developers",
            "step3": "Find your OAuth application and check the Redirect URI field",
            "step4": "The values must match EXACTLY (character for character)",
            "step5": "Common issues: trailing slashes, http vs https, port numbers, domain mismatch",
        },
        "common_issues": [
            "Mismatch: http vs https",
            "Mismatch: trailing slash (callback vs callback/)",
            "Mismatch: port number",
            "Mismatch: domain or subdomain",
            "Redirect URI not accessible from internet (localhost without tunnel)",
            "Extra spaces or special characters in .env file"
        ]
    }


@router.delete("/brevo/disconnect", status_code=status.HTTP_204_NO_CONTENT)
def disconnect_brevo(
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin_or_owner)
):
    """
    Disconnect Brevo OAuth for the current user's organization.
    This allows users to connect a different Brevo account.
    """
    # Find and delete the OAuth token for this org
    oauth_token = db.query(OAuthToken).filter(
        OAuthToken.provider == OAuthProvider.BREVO,
        OAuthToken.org_id == current_user.org_id
    ).first()
    
    if oauth_token:
        db.delete(oauth_token)
        db.commit()
    
    return None


@router.post("/calcom/connect-direct")
def connect_calcom_direct(
    request: DirectApiKeyRequest,
    http_request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin)  # Require admin role
):
    """
    Connect Cal.com directly using an API key.
    
    SECURITY: This endpoint requires admin role and is rate-limited.
    Direct API keys have full account access.
    
    This endpoint:
    1. Validates the API key by making a test call to Cal.com API
    2. Retrieves the account information
    3. Stores the API key as an OAuth token (encrypted)
    4. Logs the connection event for audit
    
    Args:
        request: Request body containing api_key (Cal.com API key)
        http_request: HTTP request object for IP address and user agent
    
    Returns:
        Success message with account information
    """
    from fastapi.responses import JSONResponse
    from app.core.rate_limit import _rate_limit_store, _rate_limit_lock, _cleanup_old_entries
    from app.core.audit import log_security_event
    from app.models.audit_log import AuditEventType
    from datetime import datetime, timedelta
    import httpx
    
    # Rate limiting: 3 attempts per 15 minutes per user
    _cleanup_old_entries()
    identifier = f"calcom_direct_api_key_{current_user.id}_{current_user.org_id}"
    now = datetime.utcnow()
    window_start = now - timedelta(seconds=900)  # 15 minutes
    
    with _rate_limit_lock:
        if identifier not in _rate_limit_store:
            _rate_limit_store[identifier] = []
        recent_requests = [
            ts for ts, _ in _rate_limit_store[identifier]
            if ts > window_start
        ]
        
        if len(recent_requests) >= 3:
            # Log rate limit event
            log_security_event(
                db=db,
                event_type=AuditEventType.RATE_LIMIT_EXCEEDED,
                org_id=current_user.org_id,
                user_id=current_user.id,
                resource_type="api_endpoint",
                resource_id="connect_calcom_direct",
                ip_address=http_request.client.host if http_request.client else None,
                user_agent=http_request.headers.get("user-agent"),
                details={
                    "endpoint": "connect_calcom_direct",
                    "max_requests": 3,
                    "window_seconds": 900,
                    "recent_requests": len(recent_requests)
                }
            )
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail="Rate limit exceeded: 3 direct API key connections per 15 minutes. Please try again later."
            )
        
        # Add current request
        _rate_limit_store[identifier].append((now, 1))
    
    api_key = request.api_key
    if not api_key or not api_key.strip():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="API key is required"
        )
    
    api_key = api_key.strip()
    org_id = current_user.org_id
    
    # VALIDATION: Check if Calendly is already connected
    # Users should connect ONE calendar provider, not both
    from sqlalchemy import text
    calendly_result = db.execute(
        text("""
            SELECT id FROM oauth_tokens 
            WHERE provider = 'calendly'::oauthprovider
            AND org_id = :org_id 
            LIMIT 1
        """),
        {"org_id": org_id}
    ).first()
    
    if calendly_result:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Calendly is already connected. Please disconnect Calendly first before connecting Cal.com. You can only connect one calendar provider at a time."
        )
    
    # Validate and test the API key by making a call to Cal.com API
    
    try:
        # Test the API key by calling Cal.com API v2 user endpoint
        # According to Cal.com API v2 docs: https://cal.com/docs/api-reference/v2/introduction
        # Authentication: Authorization: Bearer {API_KEY}
        # Endpoint: GET /me (under v2)
        response = httpx.get(
            "https://api.cal.com/v2/me",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json"
            },
            timeout=10.0
        )
        
        if response.status_code != 200:
            error_data = {}
            try:
                if response.headers.get("content-type", "").startswith("application/json"):
                    error_data = response.json()
            except:
                pass
            
            # Extract error message from Cal.com API response
            error_msg = (
                error_data.get("message") or 
                error_data.get("error") or 
                (error_data.get("details", {}).get("message") if isinstance(error_data.get("details"), dict) else None) or
                f"HTTP {response.status_code}"
            )
            
            # Format a user-friendly error message
            if response.status_code == 401:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail=f"Invalid Cal.com API key. Please check that your API key is correct and has not expired. Error: {error_msg}"
                )
            else:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Cal.com API error: {error_msg}"
                )
        
        account_data = response.json()
        account_email = account_data.get("email") or account_data.get("username")
        account_id = account_data.get("id") or account_email or "unknown"
        account_name = account_data.get("name") or account_data.get("username")
        
        print(f"[CALCOM DIRECT] Validated API key for account: {account_email}")
        
        # Log security event BEFORE storing (for audit trail)
        org_id = current_user.org_id
        log_security_event(
            db=db,
            event_type=AuditEventType.API_KEY_CONNECTED,
            org_id=org_id,
            user_id=current_user.id,
            resource_type="calcom",
            resource_id=str(account_id),
            ip_address=http_request.client.host if http_request.client else None,
            user_agent=http_request.headers.get("user-agent"),
            details={
                "account_email": account_email,
                "account_name": account_name,
                "api_key_prefix": api_key[:10] + "..." if len(api_key) > 10 else "***",
                "method": "api_key"
            }
        )
        
        # Encrypt the API key before storing
        encrypted_token = encrypt_token(api_key)
        
        # Check if token exists for this provider AND org
        # Use raw SQL to bypass SQLAlchemy's enum name conversion
        from sqlalchemy import text
        existing_result = db.execute(
            text("""
                SELECT id FROM oauth_tokens 
                WHERE provider = 'calcom'::oauthprovider 
                AND org_id = :org_id 
                LIMIT 1
            """),
            {"org_id": org_id}
        ).first()
        
        # Check if token exists and get its ID (don't load the object to avoid enum validation)
        existing_id = None
        if existing_result:
            existing_id = existing_result[0]
        
        if existing_id:
            # Use raw SQL UPDATE to bypass SQLAlchemy's enum validation
            from sqlalchemy import text
            db.execute(
                text("""
                    UPDATE oauth_tokens 
                    SET access_token = :access_token,
                        refresh_token = NULL,
                        expires_at = NULL,
                        scope = :scope,
                        account_id = :account_id
                    WHERE id = :id
                """),
                {
                    "id": existing_id,
                    "access_token": encrypted_token,
                    "scope": "api_key",
                    "account_id": str(account_id)
                }
            )
            print(f"[CALCOM DIRECT] Updated existing connection for org {org_id}")
            # Don't load the object - it will trigger enum validation
            oauth_token = None
        else:
            # Store API key as OAuth token (encrypted)
            # Use raw SQL insert to bypass SQLAlchemy's enum name conversion
            # SQLAlchemy uses enum names (CALCOM) but database has lowercase value (calcom)
            from sqlalchemy import text
            import uuid as uuid_lib
            token_id = uuid_lib.uuid4()
            # Use bindparam with explicit type to bypass SQLAlchemy enum validation
            from sqlalchemy import bindparam
            db.execute(
                text("""
                    INSERT INTO oauth_tokens (id, org_id, provider, account_id, access_token, refresh_token, scope, expires_at, created_at)
                    VALUES (:id, :org_id, CAST(:provider AS oauthprovider), :account_id, :access_token, NULL, :scope, NULL, NOW())
                """),
                {
                    "id": token_id,
                    "org_id": org_id,
                    "provider": "calcom",  # Use string value directly
                    "account_id": str(account_id),
                    "access_token": encrypted_token,
                    "scope": "api_key"
                }
            )
            # Don't query back - just use the ID for response
            oauth_token = None  # We don't need the object, just the ID
            print(f"[CALCOM DIRECT] Created new connection for org {org_id}")
        
        db.commit()
        
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "success": True,
                "message": "Cal.com connected successfully using API key.",
                "account_email": account_email,
                "account_name": account_name,
                "org_id": str(org_id),
                "method": "api_key"
            }
        )
        
    except HTTPException:
        raise
    except httpx.HTTPError as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Network error validating API key: {str(e)}"
        )
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error connecting Cal.com: {str(e)}"
        )


@router.post("/calendly/connect-direct")
def connect_calendly_direct(
    request: DirectApiKeyRequest,
    http_request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin)  # Require admin role
):
    """
    Connect Calendly directly using an API key.
    
    SECURITY: This endpoint requires admin role and is rate-limited.
    Direct API keys have full account access.
    
    VALIDATION: Users can only connect ONE calendar provider (Cal.com OR Calendly, not both).
    If Cal.com is already connected, this will return an error.
    
    This endpoint:
    1. Validates that Cal.com is not already connected
    2. Validates the API key by making a test call to Calendly API
    3. Retrieves the account information
    4. Stores the API key as an OAuth token (encrypted)
    5. Logs the connection event for audit
    
    Args:
        request: Request body containing api_key (Calendly Personal Access Token)
        http_request: HTTP request object for IP address and user agent
    
    Returns:
        Success message with account information
    """
    from fastapi.responses import JSONResponse
    from app.core.rate_limit import _rate_limit_store, _rate_limit_lock, _cleanup_old_entries
    from app.core.audit import log_security_event
    from app.models.audit_log import AuditEventType
    from datetime import datetime, timedelta
    import httpx
    
    # Rate limiting: 3 attempts per 15 minutes per user
    _cleanup_old_entries()
    identifier = f"calendly_direct_api_key_{current_user.id}_{current_user.org_id}"
    now = datetime.utcnow()
    window_start = now - timedelta(seconds=900)  # 15 minutes
    
    with _rate_limit_lock:
        if identifier not in _rate_limit_store:
            _rate_limit_store[identifier] = []
        recent_requests = [
            ts for ts, _ in _rate_limit_store[identifier]
            if ts > window_start
        ]
        
        if len(recent_requests) >= 3:
            log_security_event(
                db=db,
                event_type=AuditEventType.RATE_LIMIT_EXCEEDED,
                org_id=current_user.org_id,
                user_id=current_user.id,
                resource_type="api_endpoint",
                resource_id="connect_calendly_direct",
                ip_address=http_request.client.host if http_request.client else None,
                user_agent=http_request.headers.get("user-agent"),
                details={
                    "endpoint": "connect_calendly_direct",
                    "max_requests": 3,
                    "window_seconds": 900,
                    "recent_requests": len(recent_requests)
                }
            )
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail="Rate limit exceeded: 3 direct API key connections per 15 minutes. Please try again later."
            )
        
        _rate_limit_store[identifier].append((now, 1))
    
    api_key = request.api_key
    if not api_key or not api_key.strip():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="API key is required"
        )
    
    api_key = api_key.strip()
    org_id = current_user.org_id
    
    # VALIDATION: Check if Cal.com is already connected
    # Users should connect ONE calendar provider, not both
    from sqlalchemy import text
    calcom_result = db.execute(
        text("""
            SELECT id FROM oauth_tokens 
            WHERE provider = 'calcom'::oauthprovider
            AND org_id = :org_id 
            LIMIT 1
        """),
        {"org_id": org_id}
    ).first()
    
    if calcom_result:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cal.com is already connected. Please disconnect Cal.com first before connecting Calendly. You can only connect one calendar provider at a time."
        )
    
    try:
        # Test the API key by calling Calendly API user endpoint
        # According to Calendly API docs: https://developer.calendly.com/api-docs/d7755e2f9e5fe-calendly-api
        # Authentication: Authorization: Bearer {PERSONAL_ACCESS_TOKEN}
        # Endpoint: GET /users/me
        response = httpx.get(
            "https://api.calendly.com/users/me",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json"
            },
            timeout=10.0
        )
        
        if response.status_code != 200:
            error_data = {}
            try:
                if response.headers.get("content-type", "").startswith("application/json"):
                    error_data = response.json()
            except:
                pass
            
            error_msg = (
                error_data.get("message") or 
                error_data.get("title") or
                f"HTTP {response.status_code}"
            )
            
            if response.status_code == 401:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail=f"Invalid Calendly API key. Please check that your Personal Access Token is correct and has not expired. Error: {error_msg}"
                )
            else:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Calendly API error: {error_msg}"
                )
        
        account_data = response.json()
        resource = account_data.get("resource", {})
        account_email = resource.get("email")
        account_id = resource.get("uri") or account_email or "unknown"
        account_name = resource.get("name") or resource.get("slug")
        
    except httpx.TimeoutException:
        raise HTTPException(
            status_code=status.HTTP_504_GATEWAY_TIMEOUT,
            detail="Calendly API request timed out. Please try again."
        )
    except httpx.NetworkError as e:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Unable to connect to Calendly API. Please check your internet connection and try again. Error: {str(e)}"
        )
    except Exception as e:
        import traceback
        print(f"[CALENDLY DIRECT] Unexpected error: {str(e)}")
        print(traceback.format_exc())
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Unexpected error connecting to Calendly: {str(e)}"
        )
    
    print(f"[CALENDLY DIRECT] Validated API key for account: {account_email}")
    
    # Log security event BEFORE storing (for audit trail)
    log_security_event(
        db=db,
        event_type=AuditEventType.API_KEY_CONNECTED,
        org_id=org_id,
        user_id=current_user.id,
        resource_type="calendly",
        resource_id=str(account_id),
        ip_address=http_request.client.host if http_request.client else None,
        user_agent=http_request.headers.get("user-agent"),
        details={
            "account_email": account_email,
            "account_name": account_name,
            "api_key_prefix": api_key[:10] + "..." if len(api_key) > 10 else "***",
            "method": "api_key"
        }
    )
    
    # Encrypt the API key before storing
    encrypted_token = encrypt_token(api_key)
    
    # Check if token exists for this provider AND org
    # Use raw SQL to bypass SQLAlchemy's enum name conversion
    existing_result = db.execute(
        text("""
            SELECT id FROM oauth_tokens 
            WHERE provider = 'calendly'::oauthprovider
            AND org_id = :org_id 
            LIMIT 1
        """),
        {"org_id": org_id}
    ).first()
    
    existing_id = None
    if existing_result:
        existing_id = existing_result[0]
    
    if existing_id:
        # Use raw SQL UPDATE to bypass SQLAlchemy's enum validation
        db.execute(
            text("""
                UPDATE oauth_tokens 
                SET access_token = :access_token,
                    refresh_token = NULL,
                    expires_at = NULL,
                    scope = :scope,
                    account_id = :account_id
                WHERE id = :id
            """),
            {
                "id": existing_id,
                "access_token": encrypted_token,
                "scope": "api_key",
                "account_id": str(account_id)
            }
        )
        print(f"[CALENDLY DIRECT] Updated existing connection for org {org_id}")
    else:
        # Store API key as OAuth token (encrypted)
        import uuid as uuid_lib
        token_id = uuid_lib.uuid4()
        db.execute(
            text("""
                INSERT INTO oauth_tokens (id, org_id, provider, account_id, access_token, refresh_token, scope, expires_at, created_at)
                VALUES (:id, :org_id, CAST(:provider AS oauthprovider), :account_id, :access_token, NULL, :scope, NULL, NOW())
            """),
            {
                "id": token_id,
                "org_id": org_id,
                "provider": "calendly",
                "account_id": str(account_id),
                "access_token": encrypted_token,
                "scope": "api_key"
            }
        )
        print(f"[CALENDLY DIRECT] Created new connection for org {org_id}")
    
    db.commit()
    
    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content={
            "success": True,
            "message": "Calendly connected successfully using API key.",
            "account_email": account_email,
            "account_name": account_name,
            "org_id": str(org_id),
            "method": "api_key"
        }
    )


@router.delete("/calendly/disconnect", status_code=status.HTTP_204_NO_CONTENT)
def disconnect_calendly(
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin_or_owner)
):
    """
    Disconnect Calendly for the current user's organization.
    This allows users to connect a different Calendly account or switch to Cal.com.
    """
    from sqlalchemy import text
    
    try:
        result = db.execute(
            text("""
                SELECT id FROM oauth_tokens 
                WHERE provider = 'calendly'::oauthprovider
                AND org_id = :org_id 
                LIMIT 1
            """),
            {"org_id": current_user.org_id}
        ).first()
        
        if result:
            token_id = result[0]
            print(f"[CALENDLY DISCONNECT] Found token with id: {token_id}, deleting...")
            
            db.execute(
                text("""
                    DELETE FROM oauth_tokens 
                    WHERE id = :token_id
                """),
                {"token_id": token_id}
            )
            db.commit()
            
            print(f"[CALENDLY DISCONNECT] Successfully deleted Calendly token")
            
            from app.core.audit import log_security_event
            from app.models.audit_log import AuditEventType
            log_security_event(
                db=db,
                event_type=AuditEventType.API_KEY_DISCONNECTED,
                org_id=current_user.org_id,
                user_id=current_user.id,
                resource_type="calendly_token",
                resource_id=str(token_id),
                details={
                    "provider": "calendly"
                }
            )
    
    except Exception as e:
        print(f"[CALENDLY DISCONNECT] Error: {str(e)}")
        db.rollback()
    
    return None


@router.delete("/calcom/disconnect", status_code=status.HTTP_204_NO_CONTENT)
def disconnect_calcom(
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin_or_owner)
):
    """
    Disconnect Cal.com for the current user's organization.
    This allows users to connect a different Cal.com account or switch to Calendly.
    """
    # Use raw SQL for both finding and deleting to bypass SQLAlchemy's enum validation
    from sqlalchemy import text
    
    try:
        # First, check if token exists
        result = db.execute(
            text("""
                SELECT id FROM oauth_tokens 
                WHERE provider = CAST('calcom' AS oauthprovider)
                AND org_id = :org_id 
                LIMIT 1
            """),
            {"org_id": current_user.org_id}
        ).first()
        
        if result:
            token_id = result[0]
            print(f"[CALCOM DISCONNECT] Found token with id: {token_id}, deleting...")
            
            # Delete using raw SQL to avoid loading the OAuthToken object
            db.execute(
                text("""
                    DELETE FROM oauth_tokens 
                    WHERE id = :token_id
                """),
                {"token_id": token_id}
            )
            db.commit()
            
            print(f"[CALCOM DISCONNECT] Successfully deleted Cal.com token")
            
            # Log the security event
            from app.core.audit import log_security_event
            from app.models.audit_log import AuditEventType
            log_security_event(
                db=db,
                event_type=AuditEventType.API_KEY_DISCONNECTED,
                org_id=current_user.org_id,
                user_id=current_user.id,
                resource_type="calcom_token",
                resource_id=str(token_id),
                details={"provider": "calcom"}
            )
        else:
            print(f"[CALCOM DISCONNECT] No Cal.com token found for org {current_user.org_id}")
        
        return None
        
    except Exception as e:
        db.rollback()
        print(f"[CALCOM DISCONNECT] Error disconnecting Cal.com: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error disconnecting Cal.com: {str(e)}"
        )

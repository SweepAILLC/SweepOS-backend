"""
Service for syncing historical Stripe data after OAuth connection.
This runs automatically when a user connects their Stripe account.
"""
import stripe
from decimal import Decimal
import json
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import and_
import uuid
import httpx

from app.core.config import settings
from app.core.encryption import decrypt_token, encrypt_token
from app.models.oauth_token import OAuthToken, OAuthProvider
from app.models.stripe_payment import StripePayment
from app.models.stripe_subscription import StripeSubscription
from app.models.client import Client


def sync_stripe_historical_data(db: Session, org_id: uuid.UUID = None, background: bool = False):
    """
    Sync historical data from Stripe API to populate the database.
    
    Args:
        db: Database session
        org_id: Organization ID to sync data for (required for multi-tenant)
        background: If True, runs in background (for async execution)
    
    Returns:
        dict with sync results
    """
    import uuid
    
    # Note: We don't need STRIPE_SECRET_KEY for syncing - we use the OAuth access token
    # STRIPE_SECRET_KEY is only needed for the initial OAuth token exchange
    
    try:
        # Check connection - filter by org_id for multi-tenant support
        query = db.query(OAuthToken).filter(
            OAuthToken.provider == OAuthProvider.STRIPE
        )
        
        if org_id:
            query = query.filter(OAuthToken.org_id == org_id)
        
        oauth_token = query.first()
        
        if not oauth_token:
            error_msg = "Stripe not connected via OAuth" + (f" for org {org_id}" if org_id else "")
            print(f"[SYNC] ❌ {error_msg}")
            return {"error": error_msg}
        
        print(f"[SYNC] Found OAuth token for org {oauth_token.org_id}, account: {oauth_token.account_id}")
        
        # Use the organization's OAuth token (decrypted) for API calls
        # This ensures each org accesses their own Stripe account
        try:
            decrypted_token = decrypt_token(oauth_token.access_token)
            stripe.api_key = decrypted_token  # Use the org's access token, not the app owner's key
            print(f"[SYNC] Successfully decrypted OAuth token, using for API calls")
            print(f"[SYNC] Token prefix: {decrypted_token[:10]}... (length: {len(decrypted_token)})")
            
            # Store decrypted_token for use in helper functions
            # Verify the token looks like a Stripe key (should start with sk_ or rk_)
            if not decrypted_token.startswith(('sk_', 'rk_')):
                print(f"[SYNC] ⚠️  WARNING: Token doesn't look like a Stripe API key! It should start with 'sk_' or 'rk_'")
            
            # Warn about restricted keys
            if decrypted_token.startswith('rk_'):
                print(f"[SYNC] ⚠️  NOTE: Using restricted key (rk_). Restricted keys have limited permissions.")
                print(f"[SYNC] ⚠️  If you see 0 results, the restricted key may not have 'read_customers' or 'read_subscriptions' permissions.")
                print(f"[SYNC] ⚠️  Check your Stripe App settings to ensure the OAuth scopes include these permissions.")
        except Exception as e:
            import traceback
            error_msg = f"Failed to decrypt OAuth token: {str(e)}"
            print(f"[SYNC] ❌ {error_msg}")
            print(traceback.format_exc())
            return {"error": error_msg}
        
        # Get org_id from oauth_token for multi-tenant support
        # Use the org_id from the token to ensure consistency
        org_id = oauth_token.org_id
        print(f"[SYNC] Syncing data for org {org_id}")
        
        # Helper function to refresh token
        def refresh_token(force: bool = False):
            """Refresh OAuth token if expired or force refresh"""
            nonlocal oauth_token, db
            
            # Direct API keys don't have refresh tokens and don't expire
            # Check if this is a direct API key connection (marked by scope)
            if oauth_token.scope == "direct_api_key":
                print(f"[SYNC] Using direct API key - no refresh needed")
                return False  # Direct API keys don't need refresh
            
            # Check if refresh is needed
            needs_refresh = force
            if not needs_refresh:
                # Refresh if expires_at is set and expired, or if expires_at is None (unknown expiration)
                if oauth_token.expires_at:
                    needs_refresh = oauth_token.expires_at < datetime.utcnow()
                else:
                    # If expires_at is None, we don't know when it expires, so don't auto-refresh
                    # But we'll try if we get an auth error
                    needs_refresh = False
            
            if not needs_refresh:
                return False  # No refresh needed
            
            if not oauth_token.refresh_token:
                raise Exception("OAuth token expired and no refresh token available. Please reconnect Stripe.")
            
            print(f"[SYNC] Refreshing OAuth token...")
            decrypted_refresh = decrypt_token(oauth_token.refresh_token)
            response = httpx.post(
                "https://connect.stripe.com/oauth/token",
                data={
                    "client_secret": settings.STRIPE_SECRET_KEY,
                    "refresh_token": decrypted_refresh,
                    "grant_type": "refresh_token"
                },
                timeout=10.0
            )
            
            if response.status_code != 200:
                error_data = response.json() if response.headers.get("content-type", "").startswith("application/json") else {}
                error_msg = error_data.get("error_description", f"HTTP {response.status_code}: {response.text}")
                
                # Check if refresh token is invalid/revoked
                if "does not exist" in error_msg or "invalid" in error_msg.lower() or "revoked" in error_msg.lower():
                    raise Exception(f"Refresh token is invalid or revoked. Please reconnect your Stripe account via the dashboard.")
                else:
                    raise Exception(f"Token refresh failed: {error_msg}")
            
            token_data = response.json()
            new_access_token = token_data.get("access_token")
            new_refresh_token = token_data.get("refresh_token", decrypted_refresh)
            
            if not new_access_token:
                raise Exception("No access token in refresh response")
            
            oauth_token.access_token = encrypt_token(new_access_token)
            if new_refresh_token != decrypted_refresh:
                oauth_token.refresh_token = encrypt_token(new_refresh_token)
            
            expires_at = datetime.utcnow() + timedelta(days=365)
            if "expires_in" in token_data:
                expires_at = datetime.utcnow() + timedelta(seconds=token_data["expires_in"])
            oauth_token.expires_at = expires_at
            
            db.commit()
            stripe.api_key = decrypt_token(oauth_token.access_token)
            print(f"[SYNC] ✅ Refreshed OAuth token successfully")
            return True
        
        # Helper function to refresh token if expired
        def refresh_token_if_needed():
            """Refresh OAuth token if it's expired (checks expires_at)"""
            return refresh_token(force=False)
        
        # Helper to retry API call with token refresh on auth errors
        def api_call_with_retry(api_func, *args, **kwargs):
            """Execute API call with automatic token refresh on authentication errors"""
            max_retries = 1
            for attempt in range(max_retries + 1):
                try:
                    return api_func(*args, **kwargs)
                except stripe.error.AuthenticationError as e:
                    if attempt < max_retries:
                        print(f"[SYNC] ⚠️  Authentication error (attempt {attempt + 1}): {str(e)}")
                        print(f"[SYNC] Attempting to refresh token and retry...")
                        try:
                            refresh_token(force=True)
                            continue  # Retry the API call
                        except Exception as refresh_error:
                            error_str = str(refresh_error)
                            print(f"[SYNC] ❌ Failed to refresh token: {error_str}")
                            
                            # Provide user-friendly error message
                            if "reconnect" in error_str.lower() or "invalid" in error_str.lower() or "revoked" in error_str.lower():
                                raise Exception(f"Stripe connection expired. Please reconnect your Stripe account via the dashboard.")
                            else:
                                raise Exception(f"Token expired and refresh failed: {error_str}. Please reconnect Stripe.")
                    else:
                        # Last attempt failed
                        raise Exception(f"Authentication failed after refresh attempt: {str(e)}. Please reconnect Stripe.")
        
        # Proactively refresh token if expired (now that refresh_token is defined)
        # Skip for direct API keys (they don't expire)
        if oauth_token.scope != "direct_api_key" and oauth_token.expires_at and oauth_token.expires_at < datetime.utcnow():
            print(f"[SYNC] Token expired at {oauth_token.expires_at}, attempting to refresh...")
            try:
                refresh_token(force=True)
            except Exception as e:
                error_msg = f"Failed to refresh expired token: {str(e)}"
                print(f"[SYNC] ❌ {error_msg}")
                return {"error": f"{error_msg}. Please reconnect Stripe."}
        
        # Try to verify the Stripe account connection (optional - may fail due to permissions)
        # This is just for debugging, we'll continue even if it fails
        try:
            refresh_token_if_needed()  # Check and refresh before API call
            account = stripe.Account.retrieve()
            print(f"[SYNC] ✅ Verified Stripe account connection")
            print(f"[SYNC] Account ID: {account.id}")
            print(f"[SYNC] Account type: {getattr(account, 'type', 'N/A')}")
            print(f"[SYNC] Account email: {getattr(account, 'email', 'N/A')}")
            print(f"[SYNC] Expected account ID from OAuth token: {oauth_token.account_id}")
            
            # Verify account ID matches
            if account.id != oauth_token.account_id:
                print(f"[SYNC] ⚠️  WARNING: Account ID mismatch! API returned {account.id} but OAuth token has {oauth_token.account_id}")
        except stripe.error.PermissionError as e:
            # Account.retrieve() requires special permissions that may not be granted
            # This is fine - we can still sync customers/subscriptions without it
            print(f"[SYNC] ⚠️  Could not verify account (permission error): {str(e)}")
            print(f"[SYNC] ⚠️  This is normal if the OAuth token doesn't have account read permissions")
            print(f"[SYNC] ⚠️  Continuing with sync using account ID from OAuth token: {oauth_token.account_id}")
        except stripe.error.StripeError as e:
            # Other Stripe errors - log but continue
            print(f"[SYNC] ⚠️  Could not verify account (Stripe error): {str(e)}")
            print(f"[SYNC] ⚠️  Continuing with sync using account ID from OAuth token: {oauth_token.account_id}")
        except Exception as e:
            # Unexpected errors - log but continue
            print(f"[SYNC] ⚠️  Could not verify account (unexpected error): {str(e)}")
            print(f"[SYNC] ⚠️  Continuing with sync using account ID from OAuth token: {oauth_token.account_id}")
        
        # Sync Customers
        print(f"[SYNC] Starting customer sync...")
        customers_synced = 0
        customers_updated = 0
        try:
            refresh_token_if_needed()  # Check and refresh before API call
            
            # Try listing customers with explicit parameters
            # For connected accounts, we should get all customers
            print(f"[SYNC] Calling stripe.Customer.list(limit=100)...")
            print(f"[SYNC] Using API key type: {'Restricted Key (rk_)' if decrypted_token.startswith('rk_') else 'Secret Key (sk_)'}")
            
            # For restricted keys, we might need to specify the account
            # But first, let's try the standard call
            customers = api_call_with_retry(stripe.Customer.list, limit=100)
            print(f"[SYNC] Retrieved customer list from Stripe")
            print(f"[SYNC] Customer list object type: {type(customers)}")
            print(f"[SYNC] Customer list has_data: {hasattr(customers, 'data')}")
            
            # Debug: Print the full response structure
            if hasattr(customers, 'data'):
                data_len = len(customers.data) if customers.data else 0
                print(f"[SYNC] Customer list data length: {data_len}")
                print(f"[SYNC] Customer list has_more: {getattr(customers, 'has_more', 'N/A')}")
                print(f"[SYNC] Customer list object_id: {getattr(customers, 'object', 'N/A')}")
                print(f"[SYNC] Customer list url: {getattr(customers, 'url', 'N/A')}")
                
                # Try to get raw response for debugging
                if hasattr(customers, 'to_dict'):
                    customers_dict = customers.to_dict()
                    print(f"[SYNC] Customer list keys: {list(customers_dict.keys()) if isinstance(customers_dict, dict) else 'N/A'}")
                
                if data_len > 0:
                    print(f"[SYNC] First customer ID: {customers.data[0].id if customers.data else 'N/A'}")
                else:
                    print(f"[SYNC] ⚠️  No customers found in initial response.")
                    print(f"[SYNC] ⚠️  NOTE: Using restricted key (rk_live_). Restricted keys may have limited permissions.")
                    print(f"[SYNC] ⚠️  If you have customers/subscriptions in Stripe but see 0 results, the restricted key may not have 'read_customers' permission.")
                    print(f"[SYNC] ⚠️  Will attempt to iterate through all pages using auto_paging_iter...")
        except stripe.error.StripeError as e:
            import traceback
            error_msg = f"Stripe API error listing customers: {str(e)}"
            print(f"[SYNC] ❌ {error_msg}")
            print(f"[SYNC] Error type: {type(e).__name__}")
            print(f"[SYNC] Error code: {getattr(e, 'code', 'N/A')}")
            print(traceback.format_exc())
            return {"error": error_msg}
        except Exception as e:
            import traceback
            error_msg = f"Failed to list customers from Stripe: {str(e)}"
            print(f"[SYNC] ❌ {error_msg}")
            print(traceback.format_exc())
            return {"error": error_msg}
        
        customer_count = 0
        # Process all customers using auto_paging_iter (this handles pagination automatically)
        print(f"[SYNC] Iterating through customers using auto_paging_iter...")
        try:
            for customer in customers.auto_paging_iter():
                customer_count += 1
                customer_email = getattr(customer, 'email', None)
                customer_id = customer.id
                print(f"[SYNC] Processing customer {customer_count}: {customer_id} ({customer_email or 'no email'})")
                
                # First, try to find existing client by stripe_customer_id (most reliable)
                client = db.query(Client).filter(
                    Client.stripe_customer_id == customer_id,
                    Client.org_id == org_id  # Multi-tenant filter
                ).first()
                
                # If not found by stripe_customer_id, try to find by email to avoid duplicates
                if not client and customer_email:
                    client = db.query(Client).filter(
                        Client.email == customer_email,
                        Client.org_id == org_id  # Multi-tenant filter
                    ).first()
                    
                    # If found by email, link the stripe_customer_id to avoid future duplicates
                    if client:
                        if not client.stripe_customer_id:
                            client.stripe_customer_id = customer_id
                            customers_updated += 1
                            print(f"[SYNC] Linked existing client {client.id} to Stripe customer {customer_id} by email {customer_email}")
                
                # If still not found, create a new client
                if not client:
                    name = getattr(customer, 'name', None) or ""
                    first_name = name.split()[0] if name else "Stripe"
                    last_name = " ".join(name.split()[1:]) if name and len(name.split()) > 1 else "Customer"
                    email = customer_email or f"{customer_id}@stripe.test"
                    
                    # Use org_id from oauth_token for multi-tenant support
                    client = Client(
                        org_id=oauth_token.org_id,
                        first_name=first_name,
                        last_name=last_name,
                        email=email,
                        stripe_customer_id=customer_id
                    )
                    db.add(client)
                    customers_synced += 1
                    print(f"[SYNC] ✅ Created new client for Stripe customer {customer_id} ({email})")
                else:
                    # Update existing client with latest info from Stripe
                    updated = False
                    if not client.email and customer_email:
                        client.email = customer_email
                        updated = True
                    if not client.stripe_customer_id:
                        client.stripe_customer_id = customer_id
                        updated = True
                    # Update name if missing
                    if not client.first_name or not client.last_name:
                        name = getattr(customer, 'name', None) or ""
                        if name:
                            if not client.first_name:
                                client.first_name = name.split()[0] if name else "Stripe"
                            if not client.last_name:
                                client.last_name = " ".join(name.split()[1:]) if name and len(name.split()) > 1 else "Customer"
                            updated = True
                    if updated:
                        customers_updated += 1
                        print(f"[SYNC] ✅ Updated existing client {client.id} for Stripe customer {customer_id}")
        except Exception as e:
            import traceback
            error_msg = f"Error processing customers: {str(e)}"
            print(f"[SYNC] ❌ {error_msg}")
            print(traceback.format_exc())
            # Don't return error, just log it and continue
        
        print(f"[SYNC] Processed {customer_count} customers from Stripe")
        
        db.commit()
        print(f"[SYNC] ✅ Customer sync complete: {customers_synced} new, {customers_updated} updated, {customer_count} total processed")
        
        # If no customers were found, warn the user
        if customer_count == 0:
            print(f"[SYNC] ⚠️  WARNING: No customers found in Stripe account!")
            print(f"[SYNC] ⚠️  Account ID: {oauth_token.account_id}")
            print(f"[SYNC] ⚠️  Token type: {'Restricted Key (rk_)' if decrypted_token.startswith('rk_') else 'Secret Key (sk_)'}")
            print(f"[SYNC] ⚠️  This could mean:")
            print(f"[SYNC] ⚠️    1. The Stripe account has no customers")
            print(f"[SYNC] ⚠️    2. The OAuth token is for a different Stripe account")
            print(f"[SYNC] ⚠️    3. There's a mode mismatch (test vs live)")
            print(f"[SYNC] ⚠️    4. Restricted key doesn't have 'read_customers' permission")
            print(f"[SYNC] ⚠️    5. You're connected to a Stripe Connect account that needs different API calls")
            print(f"[SYNC] ⚠️  ACTION: Check your Stripe Dashboard → Developers → API keys to verify:")
            print(f"[SYNC] ⚠️    - You're in the correct mode (test vs live)")
            print(f"[SYNC] ⚠️    - The account has customers/subscriptions")
            print(f"[SYNC] ⚠️    - If using restricted keys, they have the required permissions")
        # If no customers were synced or updated, it might mean they all already exist
        elif customers_synced == 0 and customers_updated == 0 and customer_count > 0:
            print(f"[SYNC] ℹ️  All {customer_count} customers already exist in the database with up-to-date information")
        
        # Sync Subscriptions
        print(f"[SYNC] Starting subscription sync...")
        subscriptions_synced = 0
        subscriptions_updated = 0
        
        try:
            refresh_token_if_needed()  # Check and refresh before API call
            
            # Try listing subscriptions with explicit parameters
            # status='all' should get all subscriptions regardless of status
            print(f"[SYNC] Calling stripe.Subscription.list(limit=100, status='all')...")
            subscriptions = api_call_with_retry(stripe.Subscription.list, limit=100, status='all')
            print(f"[SYNC] Retrieved subscription list from Stripe")
            print(f"[SYNC] Subscription list object type: {type(subscriptions)}")
            print(f"[SYNC] Subscription list has_data: {hasattr(subscriptions, 'data')}")
            
            # Debug: Print the full response structure
            if hasattr(subscriptions, 'data'):
                data_len = len(subscriptions.data) if subscriptions.data else 0
                print(f"[SYNC] Subscription list data length: {data_len}")
                print(f"[SYNC] Subscription list has_more: {getattr(subscriptions, 'has_more', 'N/A')}")
                print(f"[SYNC] Subscription list object_id: {getattr(subscriptions, 'object', 'N/A')}")
                print(f"[SYNC] Subscription list url: {getattr(subscriptions, 'url', 'N/A')}")
                
                # Try to get raw response for debugging
                if hasattr(subscriptions, 'to_dict'):
                    subscriptions_dict = subscriptions.to_dict()
                    print(f"[SYNC] Subscription list keys: {list(subscriptions_dict.keys()) if isinstance(subscriptions_dict, dict) else 'N/A'}")
                
                if data_len > 0:
                    print(f"[SYNC] First subscription ID: {subscriptions.data[0].id if subscriptions.data else 'N/A'}")
                    print(f"[SYNC] First subscription status: {subscriptions.data[0].status if subscriptions.data else 'N/A'}")
                else:
                    print(f"[SYNC] ⚠️  No subscriptions found in initial response. This might be normal if the account has no subscriptions.")
                    print(f"[SYNC] ⚠️  Will attempt to iterate through all pages using auto_paging_iter...")
        except stripe.error.StripeError as e:
            import traceback
            error_msg = f"Stripe API error listing subscriptions: {str(e)}"
            print(f"[SYNC] ❌ {error_msg}")
            print(f"[SYNC] Error type: {type(e).__name__}")
            print(f"[SYNC] Error code: {getattr(e, 'code', 'N/A')}")
            print(traceback.format_exc())
            return {"error": error_msg}
        except Exception as e:
            import traceback
            error_msg = f"Failed to list subscriptions from Stripe: {str(e)}"
            print(f"[SYNC] ❌ {error_msg}")
            print(traceback.format_exc())
            return {"error": error_msg}
        
        subscription_count = 0
        print(f"[SYNC] Iterating through subscriptions using auto_paging_iter...")
        
        # Debug: Check if auto_paging_iter is working
        try:
            # Try to manually check if there are more pages
            if hasattr(subscriptions, 'has_more') and subscriptions.has_more:
                print(f"[SYNC] ⚠️  Subscription list indicates there are more pages (has_more=True)")
            else:
                print(f"[SYNC] Subscription list indicates no more pages (has_more=False)")
            
            # Try to manually iterate the first page
            if hasattr(subscriptions, 'data') and subscriptions.data:
                print(f"[SYNC] Found {len(subscriptions.data)} subscriptions in first page")
                for idx, sub in enumerate(subscriptions.data):
                    print(f"[SYNC]   [{idx+1}] Subscription ID: {sub.id}, Status: {getattr(sub, 'status', 'N/A')}, Customer: {getattr(sub, 'customer', 'N/A')}")
        except Exception as e:
            print(f"[SYNC] ⚠️  Error inspecting subscription list: {e}")
        
        for sub_data in subscriptions.auto_paging_iter():
            subscription_count += 1
            print(f"[SYNC] Processing subscription {subscription_count}: {sub_data.id} (customer: {sub_data.customer})")
            # Find client (filter by org_id for multi-tenant)
            client = db.query(Client).filter(
                Client.stripe_customer_id == sub_data.customer,
                Client.org_id == org_id
            ).first()
            
            if not client:
                # Create client if missing - try to match by email first to avoid duplicates
                try:
                    customer_data = api_call_with_retry(stripe.Customer.retrieve, sub_data.customer)
                    customer_email = getattr(customer_data, 'email', None)
                    customer_id = customer_data.id
                    
                    # Try to find existing client by email to avoid duplicates
                    if customer_email:
                        client = db.query(Client).filter(
                            Client.email == customer_email,
                            Client.org_id == org_id
                        ).first()
                        
                        if client:
                            # Link the stripe_customer_id to the existing client
                            if not client.stripe_customer_id:
                                client.stripe_customer_id = customer_id
                                print(f"[SYNC] Linked existing client {client.id} to Stripe customer {customer_id} by email {customer_email}")
                    
                    # If still not found, create new client
                    if not client:
                        name = getattr(customer_data, 'name', None) or ""
                        first_name = name.split()[0] if name else "Stripe"
                        last_name = " ".join(name.split()[1:]) if name and len(name.split()) > 1 else "Customer"
                        email = customer_email or f"{customer_id}@stripe.test"
                        
                        client = Client(
                            org_id=org_id,
                            first_name=first_name,
                            last_name=last_name,
                            email=email,
                            stripe_customer_id=customer_id
                        )
                        db.add(client)
                        customers_synced += 1  # Count this as a synced customer
                        db.flush()
                        print(f"[SYNC] ✅ Created new client from subscription customer {customer_id} ({email})")
                except Exception as e:
                    import traceback
                    print(f"[SYNC] Error retrieving customer {sub_data.customer} from Stripe: {e}")
                    print(traceback.format_exc())
                    # Create a placeholder client so the subscription can be linked
                    client = Client(
                        org_id=org_id,
                        first_name="Stripe",
                        last_name=f"Customer {sub_data.customer[:8]}",
                        email=f"{sub_data.customer}@stripe.test",
                        stripe_customer_id=sub_data.customer
                    )
                    db.add(client)
                    customers_synced += 1  # Count this as a synced customer
                    db.flush()
                    print(f"[SYNC] ✅ Created placeholder client for subscription customer: {sub_data.customer}")
            
            # Calculate MRR from subscription items
            mrr = Decimal(0)
            try:
                # Convert Stripe object to dict to access items
                sub_dict = sub_data.to_dict() if hasattr(sub_data, 'to_dict') else dict(sub_data)
                items_data = sub_dict.get('items', {})
                
                if isinstance(items_data, dict):
                    items_list = items_data.get('data', [])
                elif isinstance(items_data, list):
                    items_list = items_data
                else:
                    items_list = []
                
                for item in items_list:
                    price = item.get('price', {}) if isinstance(item, dict) else getattr(item, 'price', None)
                    if price:
                        if isinstance(price, dict):
                            unit_amount = Decimal(price.get('unit_amount', 0) or 0)
                            quantity = Decimal(item.get('quantity', 1) or 1)
                            recurring = price.get('recurring', {})
                            interval = recurring.get('interval', 'month') if recurring else 'month'
                        else:
                            unit_amount = Decimal(getattr(price, 'unit_amount', None) or 0)
                            quantity = Decimal(getattr(item, 'quantity', 1) or 1)
                            recurring = getattr(price, 'recurring', None)
                            interval = getattr(recurring, 'interval', 'month') if recurring else 'month'
                        
                        # Convert to monthly
                        if interval == 'year':
                            unit_amount = unit_amount / Decimal(12)
                        elif interval == 'week':
                            unit_amount = unit_amount * Decimal(4.33)
                        elif interval == 'day':
                            unit_amount = unit_amount * Decimal(30)
                        
                        item_mrr = (unit_amount * quantity) / Decimal(100)
                        mrr += item_mrr
            except Exception:
                mrr = Decimal(0)
            
            # Extract plan_id
            plan_id = None
            try:
                sub_dict = sub_data.to_dict() if hasattr(sub_data, 'to_dict') else dict(sub_data)
                items_data = sub_dict.get('items', {})
                if isinstance(items_data, dict):
                    items_list = items_data.get('data', [])
                elif isinstance(items_data, list):
                    items_list = items_data
                else:
                    items_list = []
                
                if items_list and len(items_list) > 0:
                    price = items_list[0].get('price', {}) if isinstance(items_list[0], dict) else getattr(items_list[0], 'price', None)
                    if price:
                        plan_id = price.get('id') if isinstance(price, dict) else getattr(price, 'id', None)
            except:
                pass
            
            # Check if subscription exists (filter by org_id for multi-tenant)
            existing_sub = db.query(StripeSubscription).filter(
                StripeSubscription.stripe_subscription_id == sub_data.id,
                StripeSubscription.org_id == org_id
            ).first()
            
            if existing_sub:
                existing_sub.status = sub_data.status
                existing_sub.mrr = mrr
                existing_sub.current_period_start = datetime.fromtimestamp(sub_data.current_period_start)
                existing_sub.current_period_end = datetime.fromtimestamp(sub_data.current_period_end) if sub_data.current_period_end else None
                existing_sub.raw = json.loads(json.dumps(sub_data, default=str))
                existing_sub.updated_at = datetime.utcnow()
                subscriptions_updated += 1
            else:
                subscription = StripeSubscription(
                    org_id=org_id,
                    stripe_subscription_id=sub_data.id,
                    client_id=client.id,
                    status=sub_data.status,
                    current_period_start=datetime.fromtimestamp(sub_data.current_period_start),
                    current_period_end=datetime.fromtimestamp(sub_data.current_period_end) if sub_data.current_period_end else None,
                    plan_id=plan_id,
                    mrr=mrr,
                    raw=json.loads(json.dumps(sub_data, default=str)),
                    created_at=datetime.fromtimestamp(sub_data.created),
                    updated_at=datetime.utcnow()
                )
                db.add(subscription)
                subscriptions_synced += 1
        
        print(f"[SYNC] Processed {subscription_count} subscriptions from Stripe")
        db.commit()
        print(f"[SYNC] ✅ Subscription sync complete: {subscriptions_synced} new, {subscriptions_updated} updated, {subscription_count} total processed")
        
        # If no subscriptions were found, warn the user
        if subscription_count == 0:
            print(f"[SYNC] ⚠️  WARNING: No subscriptions found in Stripe account!")
            print(f"[SYNC] ⚠️  This could mean:")
            print(f"[SYNC] ⚠️    1. The Stripe account has no subscriptions")
            print(f"[SYNC] ⚠️    2. The OAuth token is for a different Stripe account")
            print(f"[SYNC] ⚠️    3. There's a mode mismatch (test vs live)")
            print(f"[SYNC] ⚠️    4. The account doesn't have the required permissions")
            print(f"[SYNC] ⚠️  Please verify you're connected to the correct Stripe account in the Stripe dashboard.")
        
        # Sync Payments (Charges and PaymentIntents)
        payments_synced = 0
        payments_updated = 0
        
        # Sync Charges - get all charges, newest first
        try:
            refresh_token_if_needed()  # Check and refresh before API call
            print(f"[SYNC] Calling stripe.Charge.list(limit=100)...")
            # Stripe returns charges in reverse chronological order (newest first) by default
            charges = api_call_with_retry(stripe.Charge.list, limit=100)
            print(f"[SYNC] Retrieved charges list from Stripe")
            if hasattr(charges, 'data'):
                data_len = len(charges.data) if charges.data else 0
                print(f"[SYNC] Charge list data length: {data_len}")
                if data_len > 0:
                    # Log the first (newest) charge for debugging
                    first_charge = charges.data[0]
                    first_charge_id = getattr(first_charge, 'id', 'N/A')
                    first_charge_status = getattr(first_charge, 'status', 'N/A')
                    first_charge_created = getattr(first_charge, 'created', None)
                    first_charge_date = datetime.fromtimestamp(first_charge_created) if first_charge_created else None
                    print(f"[SYNC] Newest charge: {first_charge_id}, status={first_charge_status}, created={first_charge_date}")
        except stripe.error.StripeError as e:
            import traceback
            error_msg = f"Stripe API error listing charges: {str(e)}"
            print(f"[SYNC] ❌ {error_msg}")
            print(traceback.format_exc())
            # Don't return error - continue with PaymentIntents
            charges = None
        except Exception as e:
            import traceback
            error_msg = f"Failed to list charges from Stripe: {str(e)}"
            print(f"[SYNC] ❌ {error_msg}")
            print(traceback.format_exc())
            # Don't return error - continue with PaymentIntents
            charges = None
        
        # Sync PaymentIntents (modern Stripe payment method)
        payment_intents = None
        try:
            refresh_token_if_needed()  # Check and refresh before API call
            print(f"[SYNC] Calling stripe.PaymentIntent.list(limit=100)...")
            payment_intents = api_call_with_retry(stripe.PaymentIntent.list, limit=100)
            print(f"[SYNC] Retrieved payment intents list from Stripe")
            if hasattr(payment_intents, 'data'):
                data_len = len(payment_intents.data) if payment_intents.data else 0
                print(f"[SYNC] PaymentIntent list data length: {data_len}")
        except stripe.error.StripeError as e:
            import traceback
            error_msg = f"Stripe API error listing payment intents: {str(e)}"
            print(f"[SYNC] ⚠️  {error_msg}")
            print(traceback.format_exc())
            # Continue - PaymentIntents might not be available
            payment_intents = None
        except Exception as e:
            import traceback
            error_msg = f"Failed to list payment intents from Stripe: {str(e)}"
            print(f"[SYNC] ⚠️  {error_msg}")
            print(traceback.format_exc())
            # Continue - PaymentIntents might not be available
            payment_intents = None
        
        # Process Charges
        if charges:
            print(f"[SYNC] Processing charges...")
            charge_count = 0
            for charge_data in charges.auto_paging_iter():
                charge_count += 1
                charge_id = charge_data.id
                charge_status = getattr(charge_data, 'status', None)
                charge_paid = getattr(charge_data, 'paid', False)
                charge_created = datetime.fromtimestamp(charge_data.created) if hasattr(charge_data, 'created') else None
                
                # Determine payment status from Stripe charge
                # Stripe Charge has a 'status' field: 'succeeded', 'pending', or 'failed'
                # Also check 'paid' boolean as fallback
                if charge_status:
                    if charge_status == 'succeeded':
                        payment_status = 'succeeded'
                    elif charge_status == 'failed':
                        payment_status = 'failed'
                    elif charge_status == 'pending':
                        payment_status = 'pending'
                    else:
                        # Fallback to 'paid' boolean
                        payment_status = 'succeeded' if charge_paid else 'failed'
                else:
                    # Fallback to 'paid' boolean if status not available
                    payment_status = 'succeeded' if charge_paid else 'failed'
                
                print(f"[SYNC] Processing charge {charge_count}: {charge_id}, status={charge_status}, paid={charge_paid}, payment_status={payment_status}, created={charge_created}")
                
                client = None
                if charge_data.customer:
                    client = db.query(Client).filter(
                        Client.stripe_customer_id == charge_data.customer,
                        Client.org_id == org_id  # Multi-tenant filter
                    ).first()
                
                existing_payment = db.query(StripePayment).filter(
                    StripePayment.stripe_id == charge_id,
                    StripePayment.org_id == org_id  # Multi-tenant filter
                ).first()
                
                if existing_payment:
                    # Update existing payment with latest data from Stripe
                    updated = False
                    if existing_payment.status != payment_status:
                        print(f"[SYNC] Updating payment {charge_id} status: {existing_payment.status} -> {payment_status}")
                        existing_payment.status = payment_status
                        updated = True
                    
                    # Update receipt URL if available
                    receipt_url = getattr(charge_data, 'receipt_url', None)
                    if receipt_url and existing_payment.receipt_url != receipt_url:
                        existing_payment.receipt_url = receipt_url
                        updated = True
                    
                    # Update raw event data
                    existing_payment.raw_event = json.loads(json.dumps(charge_data, default=str))
                    existing_payment.updated_at = datetime.utcnow()
                    
                    # Update client lifetime revenue if payment status changed to succeeded
                    if client and payment_status == 'succeeded' and existing_payment.status != 'succeeded':
                        # Only add if this is a new successful payment
                        client.lifetime_revenue_cents += charge_data.amount
                        updated = True
                    
                    if updated:
                        payments_updated += 1
                    continue
                
                # Payment doesn't exist - create new one
                subscription_id = None
                if charge_data.invoice:
                    try:
                        invoice = api_call_with_retry(stripe.Invoice.retrieve, charge_data.invoice)
                        if invoice.subscription:
                            subscription_id = invoice.subscription
                    except Exception as e:
                        # Log but continue - invoice retrieval failure shouldn't block payment sync
                        print(f"[SYNC] ⚠️  Could not retrieve invoice {charge_data.invoice}: {e}")
                        pass
                
                payment = StripePayment(
                    org_id=org_id,
                    stripe_id=charge_id,
                    client_id=client.id if client else None,
                    amount_cents=charge_data.amount,
                    currency=charge_data.currency,
                    status=payment_status,
                    type='charge',
                    subscription_id=subscription_id,
                    receipt_url=getattr(charge_data, 'receipt_url', None),
                    raw_event=json.loads(json.dumps(charge_data, default=str)),
                    created_at=charge_created or datetime.utcnow(),
                    updated_at=datetime.utcnow()
                )
                db.add(payment)
                print(f"[SYNC] ✅ Created new payment record: {charge_id}, status={payment_status}, amount=${charge_data.amount/100:.2f}")
                
                if client and payment_status == 'succeeded':
                    client.lifetime_revenue_cents += charge_data.amount
                
                payments_synced += 1
            
            print(f"[SYNC] Processed {charge_count} charges from Stripe")
            db.commit()  # Commit charges before processing PaymentIntents
            print(f"[SYNC] ✅ Charge sync complete: {payments_synced} new, {payments_updated} updated")
        
        # Process PaymentIntents
        if payment_intents:
            print(f"[SYNC] Processing PaymentIntents...")
            for pi_data in payment_intents.auto_paging_iter():
                client = None
                if pi_data.customer:
                    client = db.query(Client).filter(
                        Client.stripe_customer_id == pi_data.customer,
                        Client.org_id == org_id  # Multi-tenant filter
                    ).first()
                
                existing_payment = db.query(StripePayment).filter(
                    StripePayment.stripe_id == pi_data.id,
                    StripePayment.org_id == org_id  # Multi-tenant filter
                ).first()
                
                if existing_payment:
                    # Update existing payment
                    updated = False
                    new_status = pi_data.status
                    if existing_payment.status != new_status:
                        existing_payment.status = new_status
                        updated = True
                    
                    existing_payment.raw_event = json.loads(json.dumps(pi_data, default=str))
                    existing_payment.updated_at = datetime.utcnow()
                    
                    if updated:
                        payments_updated += 1
                    continue
                
                # Determine payment status from PaymentIntent status
                status_map = {
                    'succeeded': 'succeeded',
                    'processing': 'pending',
                    'requires_payment_method': 'failed',
                    'requires_confirmation': 'pending',
                    'requires_action': 'pending',
                    'canceled': 'failed',
                    'requires_capture': 'pending'
                }
                payment_status = status_map.get(pi_data.status, 'pending')
                
                payment = StripePayment(
                    org_id=org_id,
                    stripe_id=pi_data.id,
                    client_id=client.id if client else None,
                    amount_cents=pi_data.amount,
                    currency=pi_data.currency,
                    status=payment_status,
                    type='payment_intent',
                    subscription_id=pi_data.invoice if hasattr(pi_data, 'invoice') else None,
                    receipt_url=None,  # PaymentIntents don't have receipt_url directly
                    raw_event=json.loads(json.dumps(pi_data, default=str)),
                    created_at=datetime.fromtimestamp(pi_data.created),
                    updated_at=datetime.utcnow()
                )
                db.add(payment)
                
                if client and payment_status == 'succeeded':
                    client.lifetime_revenue_cents += pi_data.amount
                
                payments_synced += 1
        
        db.commit()
        
        # Update client MRR from active subscriptions (filter by org_id for multi-tenant)
        clients_updated = 0
        for client in db.query(Client).filter(
            Client.stripe_customer_id.isnot(None),
            Client.org_id == org_id
        ).all():
            active_subs = db.query(StripeSubscription).filter(
                and_(
                    StripeSubscription.client_id == client.id,
                    StripeSubscription.status == "active",
                    StripeSubscription.org_id == org_id
                )
            ).all()
            
            total_mrr = sum(float(sub.mrr) if sub.mrr else 0.0 for sub in active_subs)
            if client.estimated_mrr != Decimal(str(total_mrr)):
                client.estimated_mrr = Decimal(str(total_mrr))
                clients_updated += 1
        
        db.commit()
        
        # Include diagnostic counts
        return {
            "success": True,
            "customers_synced": customers_synced,
            "customers_updated": customers_updated,
            "subscriptions_synced": subscriptions_synced,
            "subscriptions_updated": subscriptions_updated,
            "payments_synced": payments_synced,
            "payments_updated": payments_updated,
            "clients_updated": clients_updated,
            "diagnostic": {
                "customers_found_from_stripe": customer_count,
                "subscriptions_found_from_stripe": subscription_count,
                "account_id": oauth_token.account_id,
                "org_id": str(org_id)
            }
        }
        
    except stripe.error.StripeError as e:
        db.rollback()
        import traceback
        error_msg = f"Stripe API error: {str(e)}"
        print(f"[SYNC] ❌ {error_msg}")
        print(traceback.format_exc())
        return {"error": error_msg}
    except Exception as e:
        db.rollback()
        import traceback
        error_msg = f"Error during sync: {str(e)}"
        print(f"[SYNC] ❌ {error_msg}")
        print(traceback.format_exc())
        return {"error": error_msg}


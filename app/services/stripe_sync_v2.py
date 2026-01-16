"""
Improved Stripe sync service with cursor-based incremental syncing.

Features:
- Initial historical backfill on connect
- Cursor-based incremental polling (only fetches updated objects)
- Idempotent upserts (prevents duplicates)
- Buffer time to handle webhook delays
- Never refetches full history after initial backfill
"""
import stripe
from decimal import Decimal
import json
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_, func as sa_func
from sqlalchemy.dialects.postgresql import insert
import uuid
import httpx

from app.core.config import settings
from app.core.encryption import decrypt_token
from app.models.oauth_token import OAuthToken, OAuthProvider
from app.models.stripe_payment import StripePayment
from app.models.stripe_subscription import StripeSubscription
from app.models.client import Client


# Buffer time to account for webhook delays (5 minutes)
SYNC_BUFFER_SECONDS = 300


def get_stripe_api_key(db: Session, org_id: uuid.UUID) -> str:
    """Get and decrypt Stripe API key for org"""
    oauth_token = db.query(OAuthToken).filter(
        OAuthToken.provider == OAuthProvider.STRIPE,
        OAuthToken.org_id == org_id
    ).first()
    
    if not oauth_token:
        raise ValueError(f"Stripe not connected for org {org_id}")
    
    return decrypt_token(oauth_token.access_token)


def refresh_token_if_needed(db: Session, oauth_token: OAuthToken) -> bool:
    """Refresh OAuth token if expired. Returns True if refreshed."""
    if oauth_token.scope == "direct_api_key":
        return False  # Direct API keys don't expire
    
    if not oauth_token.expires_at or oauth_token.expires_at > datetime.utcnow():
        return False  # Not expired
    
    if not oauth_token.refresh_token:
        raise Exception("OAuth token expired and no refresh token available. Please reconnect Stripe.")
    
    try:
        from app.core.encryption import decrypt_token, encrypt_token
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
            raise Exception(f"Token refresh failed: {response.text}")
        
        token_data = response.json()
        new_access_token = token_data.get("access_token")
        new_refresh_token = token_data.get("refresh_token", decrypted_refresh)
        
        oauth_token.access_token = encrypt_token(new_access_token)
        if new_refresh_token != decrypted_refresh:
            oauth_token.refresh_token = encrypt_token(new_refresh_token)
        
        expires_at = datetime.utcnow() + timedelta(days=365)
        if "expires_in" in token_data:
            expires_at = datetime.utcnow() + timedelta(seconds=token_data["expires_in"])
        oauth_token.expires_at = expires_at
        
        db.commit()
        return True
    except Exception as e:
        db.rollback()
        raise Exception(f"Failed to refresh token: {str(e)}")


def upsert_client(db: Session, customer_data, org_id: uuid.UUID) -> Client:
    """Idempotently upsert a client from Stripe customer data."""
    customer_id = customer_data.id
    customer_email = getattr(customer_data, 'email', None)
    
    # Try to find existing client by stripe_customer_id first
    client = db.query(Client).filter(
        Client.stripe_customer_id == customer_id,
        Client.org_id == org_id
    ).first()
    
    # If not found, try by email
    if not client and customer_email:
        client = db.query(Client).filter(
            Client.email == customer_email,
            Client.org_id == org_id
        ).first()
        
        # Link stripe_customer_id to existing client
        if client and not client.stripe_customer_id:
            client.stripe_customer_id = customer_id
    
    # Create or update client
    if client:
        # Update existing
        if not client.email and customer_email:
            client.email = customer_email
        if not client.stripe_customer_id:
            client.stripe_customer_id = customer_id
        client.updated_at = datetime.utcnow()
    else:
        # Create new
        client = Client(
            org_id=org_id,
            stripe_customer_id=customer_id,
            email=customer_email,
            first_name=getattr(customer_data, 'name', '').split()[0] if getattr(customer_data, 'name', None) else None,
            last_name=' '.join(getattr(customer_data, 'name', '').split()[1:]) if getattr(customer_data, 'name', None) and len(getattr(customer_data, 'name', '').split()) > 1 else None,
            lifecycle_state='active',
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow()
        )
        db.add(client)
    
    return client


def upsert_payment(db: Session, payment_data, org_id: uuid.UUID, payment_type: str = 'charge') -> StripePayment:
    """
    Idempotently upsert a payment using ON CONFLICT.
    Prevents duplicates by using unique constraint on (stripe_id, org_id).
    """
    payment_id = payment_data.id
    
    # Determine payment status
    if payment_type == 'charge':
        status = getattr(payment_data, 'status', 'succeeded' if getattr(payment_data, 'paid', False) else 'failed')
    elif payment_type == 'payment_intent':
        status_map = {
            'succeeded': 'succeeded',
            'processing': 'pending',
            'requires_payment_method': 'failed',
            'requires_confirmation': 'pending',
            'requires_action': 'pending',
            'canceled': 'failed',
            'requires_capture': 'pending'
        }
        status = status_map.get(payment_data.status, 'pending')
    else:  # invoice
        invoice_status = getattr(payment_data, 'status', None)
        paid = getattr(payment_data, 'paid', False)
        if invoice_status == 'paid' or paid:
            status = 'succeeded'
        elif invoice_status == 'uncollectible':
            status = 'failed'
        elif invoice_status in ('open', 'void') and not paid:
            # Check if there are failed payment attempts
            # If invoice has attempts and all failed, mark as failed
            if hasattr(payment_data, 'attempt_count') and payment_data.attempt_count > 0:
                if hasattr(payment_data, 'last_payment_error') and payment_data.last_payment_error:
                    status = 'failed'
                else:
                    status = 'pending'
            else:
                status = 'failed'
        else:
            status = 'pending'
    
    # Get client
    client = None
    if hasattr(payment_data, 'customer') and payment_data.customer:
        client = db.query(Client).filter(
            Client.stripe_customer_id == payment_data.customer,
            Client.org_id == org_id
        ).first()
    
    # Get subscription_id and invoice_id
    subscription_id = None
    invoice_id = None
    
    if payment_type == 'invoice':
        # For invoice type, the payment_data.id is the invoice ID
        invoice_id = payment_id
        if hasattr(payment_data, 'subscription') and payment_data.subscription:
            subscription_id = payment_data.subscription
    elif hasattr(payment_data, 'subscription') and payment_data.subscription:
        subscription_id = payment_data.subscription
    elif hasattr(payment_data, 'invoice') and payment_data.invoice:
        # Charge or payment intent linked to an invoice
        invoice_id = payment_data.invoice
        # Try to get subscription from invoice
        try:
            invoice = stripe.Invoice.retrieve(invoice_id)
            if invoice.subscription:
                subscription_id = invoice.subscription
        except:
            pass
    
    # Get amount
    if hasattr(payment_data, 'amount'):
        amount_cents = payment_data.amount
    elif hasattr(payment_data, 'amount_due'):
        amount_cents = payment_data.amount_due
    elif hasattr(payment_data, 'amount_paid'):
        amount_cents = payment_data.amount_paid
    else:
        amount_cents = 0
    
    # Get receipt URL
    receipt_url = getattr(payment_data, 'receipt_url', None) or getattr(payment_data, 'hosted_invoice_url', None)
    
    # Get created timestamp
    created_ts = getattr(payment_data, 'created', None)
    created_at = datetime.fromtimestamp(created_ts) if created_ts else datetime.utcnow()
    
    # Check for duplicate payments before inserting
    # 1. If this payment is linked to an invoice, check if we already have a charge for this invoice
    # 2. If this payment is linked to a subscription, check if we already have a payment for this subscription+invoice combo
    # Prefer charge records over invoice records to avoid double-counting
    
    # Check for duplicate invoice payments
    # IMPORTANT: Only deduplicate SUCCESSFUL payments. Failed payments (retry attempts) should all be stored.
    if invoice_id and payment_type == 'invoice' and status == 'succeeded':
        # Check if a SUCCESSFUL charge already exists for this invoice
        existing_charge = db.query(StripePayment).filter(
            StripePayment.invoice_id == invoice_id,
            StripePayment.org_id == org_id,
            StripePayment.type == 'charge',
            StripePayment.status == 'succeeded'  # Only check for successful payments
        ).first()
        
        if existing_charge:
            # Successful charge already exists, skip creating invoice record to avoid duplicate
            print(f"[SYNC] Skipping invoice {invoice_id} - charge {existing_charge.stripe_id} already exists")
            return existing_charge
    
    # Check for duplicate subscription payments (same subscription_id + invoice_id)
    # IMPORTANT: Only deduplicate SUCCESSFUL payments. Failed payments (retry attempts) should all be stored.
    if subscription_id and invoice_id and status == 'succeeded':
        # Check if we already have a SUCCESSFUL payment for this subscription+invoice combo
        existing_sub_payment = db.query(StripePayment).filter(
            StripePayment.subscription_id == subscription_id,
            StripePayment.invoice_id == invoice_id,
            StripePayment.org_id == org_id,
            StripePayment.status == 'succeeded'  # Only check for successful payments
        ).first()
        
        if existing_sub_payment and existing_sub_payment.stripe_id != payment_id:
            # Another successful payment already exists for this subscription+invoice combo
            # Prefer charge over invoice, prefer newer over older
            if payment_type == 'invoice' and existing_sub_payment.type == 'charge':
                print(f"[SYNC] Skipping invoice payment {payment_id} - charge {existing_sub_payment.stripe_id} already exists for subscription {subscription_id}")
                return existing_sub_payment
            elif payment_type == 'charge' and existing_sub_payment.type == 'invoice':
                # Update the existing invoice record to be a charge (better)
                print(f"[SYNC] Replacing invoice payment {existing_sub_payment.stripe_id} with charge {payment_id} for subscription {subscription_id}")
                # Will be handled by ON CONFLICT update below
    
    # Use PostgreSQL ON CONFLICT for idempotent upsert
    # Fallback to manual upsert if constraint doesn't exist (migration not run yet)
    try:
        stmt = insert(StripePayment).values(
            org_id=org_id,
            stripe_id=payment_id,
            client_id=client.id if client else None,
            amount_cents=amount_cents,
            currency=getattr(payment_data, 'currency', 'usd'),
            status=status,
            type=payment_type,
            subscription_id=subscription_id,
            invoice_id=invoice_id,
            receipt_url=receipt_url,
            raw_event=json.loads(json.dumps(payment_data, default=str)),
            created_at=created_at,
            updated_at=datetime.utcnow()
        )
        
        stmt = stmt.on_conflict_do_update(
            index_elements=['stripe_id', 'org_id'],
            set_=dict(
                status=stmt.excluded.status,
                amount_cents=stmt.excluded.amount_cents,
                currency=stmt.excluded.currency,
                client_id=stmt.excluded.client_id,
                subscription_id=stmt.excluded.subscription_id,
                invoice_id=stmt.excluded.invoice_id,
                receipt_url=stmt.excluded.receipt_url,
                raw_event=stmt.excluded.raw_event,
                updated_at=datetime.utcnow()
            )
        )
        
        db.execute(stmt)
    except Exception as e:
        # Fallback: manual upsert if constraint doesn't exist
        print(f"[SYNC] ON CONFLICT failed, using manual upsert: {str(e)}")
        existing_payment = db.query(StripePayment).filter(
            StripePayment.stripe_id == payment_id,
            StripePayment.org_id == org_id
        ).first()
        
        if existing_payment:
            # Update existing
            existing_payment.status = status
            existing_payment.amount_cents = amount_cents
            existing_payment.currency = getattr(payment_data, 'currency', 'usd')
            existing_payment.client_id = client.id if client else None
            existing_payment.subscription_id = subscription_id
            existing_payment.invoice_id = invoice_id
            existing_payment.receipt_url = receipt_url
            existing_payment.raw_event = json.loads(json.dumps(payment_data, default=str))
            existing_payment.updated_at = datetime.utcnow()
        else:
            # Check for duplicate invoice payments before creating
            if invoice_id and payment_type == 'invoice':
                existing_charge = db.query(StripePayment).filter(
                    StripePayment.invoice_id == invoice_id,
                    StripePayment.org_id == org_id,
                    StripePayment.type == 'charge'
                ).first()
                
                if existing_charge:
                    print(f"[SYNC] Skipping invoice {invoice_id} - charge {existing_charge.stripe_id} already exists")
                    return existing_charge
            
            # Create new
            payment = StripePayment(
                org_id=org_id,
                stripe_id=payment_id,
                client_id=client.id if client else None,
                amount_cents=amount_cents,
                currency=getattr(payment_data, 'currency', 'usd'),
                status=status,
                type=payment_type,
                subscription_id=subscription_id,
                invoice_id=invoice_id,
                receipt_url=receipt_url,
                raw_event=json.loads(json.dumps(payment_data, default=str)),
                created_at=created_at,
                updated_at=datetime.utcnow()
            )
            db.add(payment)
            db.flush()  # Flush to ensure payment is available for query
    
    # Get the payment record (after upsert)
    payment = db.query(StripePayment).filter(
        StripePayment.stripe_id == payment_id,
        StripePayment.org_id == org_id
    ).first()
    
    if not payment:
        raise Exception(f"Failed to retrieve payment {payment_id} after upsert")
    
    # Note: Client lifetime revenue is recalculated during reconciliation
    # to avoid double-counting during sync
    
    return payment


def upsert_subscription(db: Session, sub_data, org_id: uuid.UUID) -> StripeSubscription:
    """Idempotently upsert a subscription."""
    sub_id = sub_data.id
    
    # Check for duplicate subscription BEFORE processing
    # This ensures we don't process the same subscription multiple times
    existing_sub = db.query(StripeSubscription).filter(
        StripeSubscription.stripe_subscription_id == sub_id,
        StripeSubscription.org_id == org_id
    ).first()
    
    # Get subscription status
    subscription_status = getattr(sub_data, 'status', 'incomplete')
    
    # Calculate MRR - sum all subscription items
    # Stripe amounts are in cents, so we need to divide by 100 to get dollars
    mrr = Decimal('0')
    if subscription_status in ('active', 'trialing'):
        items_found = False
        
        # Try to get items from sub_data
        if hasattr(sub_data, 'items') and sub_data.items:
            items_data = sub_data.items.data if hasattr(sub_data.items, 'data') else []
            if not items_data and hasattr(sub_data.items, '__iter__'):
                # Try to iterate directly if it's a list
                items_data = list(sub_data.items)
            
            if items_data:
                items_found = True
                for item in items_data:
                    if hasattr(item, 'price') and item.price:
                        price_obj = item.price
                        # Handle both object and dict formats
                        if hasattr(price_obj, 'unit_amount'):
                            amount_cents = Decimal(str(price_obj.unit_amount or 0))
                        elif isinstance(price_obj, dict):
                            amount_cents = Decimal(str(price_obj.get('unit_amount', 0)))
                        else:
                            amount_cents = Decimal('0')
                        
                        # Get recurring interval
                        if hasattr(price_obj, 'recurring') and price_obj.recurring:
                            interval = price_obj.recurring.interval if hasattr(price_obj.recurring, 'interval') else 'month'
                        elif isinstance(price_obj, dict) and price_obj.get('recurring'):
                            interval = price_obj['recurring'].get('interval', 'month')
                        else:
                            interval = 'month'
                        
                        # Get quantity
                        if hasattr(item, 'quantity'):
                            quantity = Decimal(str(item.quantity or 1))
                        elif isinstance(item, dict):
                            quantity = Decimal(str(item.get('quantity', 1)))
                        else:
                            quantity = Decimal('1')
                        
                        # Calculate monthly amount for this item (convert cents to dollars)
                        item_mrr = Decimal('0')
                        amount_dollars = amount_cents / Decimal('100')  # Convert cents to dollars
                        if interval == 'year':
                            item_mrr = (amount_dollars * quantity) / Decimal('12')
                        elif interval == 'month':
                            item_mrr = amount_dollars * quantity
                        elif interval == 'week':
                            item_mrr = (amount_dollars * quantity) * Decimal('4.33')
                        elif interval == 'day':
                            item_mrr = (amount_dollars * quantity) * Decimal('30')
                        
                        mrr += item_mrr
                        print(f"[SYNC] Subscription {sub_id} item: amount_cents={amount_cents}, amount_dollars={amount_dollars}, interval={interval}, quantity={quantity}, item_mrr={item_mrr}, total_mrr={mrr}")
        
        # Fallback: Try to read from raw JSON if items weren't found
        if not items_found:
            try:
                # Try to get from raw data if it's already stored
                if existing_sub and existing_sub.raw:
                    raw_data = existing_sub.raw if isinstance(existing_sub.raw, dict) else json.loads(existing_sub.raw) if isinstance(existing_sub.raw, str) else {}
                    items = raw_data.get('items', {}).get('data', [])
                    if items:
                        print(f"[SYNC] Using raw JSON data for subscription {sub_id}")
                        for item in items:
                            price = item.get('price', {})
                            amount_cents = Decimal(str(price.get('unit_amount', 0) or 0))
                            quantity = Decimal(str(item.get('quantity', 1) or 1))
                            recurring = price.get('recurring', {})
                            interval = recurring.get('interval', 'month') if recurring else 'month'
                            
                            # Convert to monthly (cents to dollars)
                            amount_dollars = amount_cents / Decimal('100')
                            item_mrr = Decimal('0')
                            if interval == 'year':
                                item_mrr = (amount_dollars * quantity) / Decimal('12')
                            elif interval == 'month':
                                item_mrr = amount_dollars * quantity
                            elif interval == 'week':
                                item_mrr = (amount_dollars * quantity) * Decimal('4.33')
                            elif interval == 'day':
                                item_mrr = (amount_dollars * quantity) * Decimal('30')
                            
                            mrr += item_mrr
                            print(f"[SYNC] From raw JSON: amount_cents={amount_cents}, amount_dollars={amount_dollars}, interval={interval}, quantity={quantity}, item_mrr={item_mrr}, total_mrr={mrr}")
            except Exception as e:
                print(f"[SYNC] Error reading from raw JSON: {str(e)}")
        
        # Final fallback: Try to get amount from subscription metadata or plan
        if mrr == 0 and hasattr(sub_data, 'plan'):
            try:
                plan = sub_data.plan
                if plan:
                    amount_cents = Decimal(str(getattr(plan, 'amount', 0) or (plan.get('amount', 0) if isinstance(plan, dict) else 0)))
                    interval = getattr(plan, 'interval', 'month') if hasattr(plan, 'interval') else (plan.get('interval', 'month') if isinstance(plan, dict) else 'month')
                    amount_dollars = amount_cents / Decimal('100')
                    
                    if interval == 'year':
                        mrr = amount_dollars / Decimal('12')
                    elif interval == 'month':
                        mrr = amount_dollars
                    elif interval == 'week':
                        mrr = amount_dollars * Decimal('4.33')
                    elif interval == 'day':
                        mrr = amount_dollars * Decimal('30')
                    
                    print(f"[SYNC] Fallback to plan: amount_cents={amount_cents}, amount_dollars={amount_dollars}, interval={interval}, mrr={mrr}")
            except Exception as e:
                print(f"[SYNC] Error reading from plan: {str(e)}")
    
    print(f"[SYNC] Subscription {sub_id}: status={subscription_status}, final_calculated_mrr={mrr}")
    
    # Get client
    client = None
    if sub_data.customer:
        client = db.query(Client).filter(
            Client.stripe_customer_id == sub_data.customer,
            Client.org_id == org_id
        ).first()
    
    # Use ON CONFLICT for idempotent upsert
    # Always check for existing subscription first to prevent duplicates
    if existing_sub:
        # Update existing subscription
        existing_sub.status = subscription_status
        existing_sub.mrr = float(mrr)
        existing_sub.current_period_start = datetime.fromtimestamp(sub_data.current_period_start) if sub_data.current_period_start else None
        existing_sub.current_period_end = datetime.fromtimestamp(sub_data.current_period_end) if sub_data.current_period_end else None
        existing_sub.raw = json.loads(json.dumps(sub_data, default=str))
        existing_sub.updated_at = datetime.utcnow()
        if client and not existing_sub.client_id:
            existing_sub.client_id = client.id
        print(f"[SYNC] Updated existing subscription {sub_id}: status={subscription_status}, mrr={mrr}")
        db.flush()
        return existing_sub
    
    # Create new subscription
    stmt = insert(StripeSubscription).values(
        org_id=org_id,
        stripe_subscription_id=sub_id,
        client_id=client.id if client else None,
        status=subscription_status,
        mrr=float(mrr),
        current_period_start=datetime.fromtimestamp(sub_data.current_period_start) if sub_data.current_period_start else None,
        current_period_end=datetime.fromtimestamp(sub_data.current_period_end) if sub_data.current_period_end else None,
        raw=json.loads(json.dumps(sub_data, default=str)),
        created_at=datetime.fromtimestamp(sub_data.created) if sub_data.created else datetime.utcnow(),
        updated_at=datetime.utcnow()
    )
    
    # Check if unique constraint exists, if not use manual upsert
    try:
        stmt = stmt.on_conflict_do_update(
            index_elements=['stripe_subscription_id', 'org_id'],
            set_=dict(
                status=stmt.excluded.status,
                mrr=stmt.excluded.mrr,
                current_period_start=stmt.excluded.current_period_start,
                current_period_end=stmt.excluded.current_period_end,
                raw=stmt.excluded.raw,
                updated_at=datetime.utcnow()
            )
        )
        db.execute(stmt)
        print(f"[SYNC] Created/updated subscription {sub_id} via ON CONFLICT: status={subscription_status}, mrr={mrr}")
    except Exception as e:
        # Fallback: manual upsert if constraint doesn't exist
        print(f"[SYNC] ON CONFLICT failed for subscription, using manual upsert: {str(e)}")
        # Check again in case it was created by another process
        existing = db.query(StripeSubscription).filter(
            StripeSubscription.stripe_subscription_id == sub_id,
            StripeSubscription.org_id == org_id
        ).first()
        
        if existing:
            existing.status = subscription_status
            existing.mrr = float(mrr)
            existing.current_period_start = datetime.fromtimestamp(sub_data.current_period_start) if sub_data.current_period_start else None
            existing.current_period_end = datetime.fromtimestamp(sub_data.current_period_end) if sub_data.current_period_end else None
            existing.raw = json.loads(json.dumps(sub_data, default=str))
            existing.updated_at = datetime.utcnow()
            if client and not existing.client_id:
                existing.client_id = client.id
            print(f"[SYNC] Updated existing subscription {sub_id} via manual upsert: status={subscription_status}, mrr={mrr}")
        else:
            subscription = StripeSubscription(
                org_id=org_id,
                stripe_subscription_id=sub_id,
                client_id=client.id if client else None,
                status=subscription_status,
                mrr=float(mrr),
                current_period_start=datetime.fromtimestamp(sub_data.current_period_start) if sub_data.current_period_start else None,
                current_period_end=datetime.fromtimestamp(sub_data.current_period_end) if sub_data.current_period_end else None,
                raw=json.loads(json.dumps(sub_data, default=str)),
                created_at=datetime.fromtimestamp(sub_data.created) if sub_data.created else datetime.utcnow(),
                updated_at=datetime.utcnow()
            )
            db.add(subscription)
            print(f"[SYNC] Created new subscription {sub_id}: status={subscription_status}, mrr={mrr}")
        db.flush()  # Flush to ensure subscription is available for query
    
    subscription = db.query(StripeSubscription).filter(
        StripeSubscription.stripe_subscription_id == sub_id,
        StripeSubscription.org_id == org_id
    ).first()
    
    if not subscription:
        raise Exception(f"Failed to retrieve subscription {sub_id} after upsert")
    
    return subscription


def sync_stripe_incremental(db: Session, org_id: uuid.UUID, force_full: bool = False) -> dict:
    """
    Incremental sync of Stripe data.
    
    Args:
        db: Database session
        org_id: Organization ID
        force_full: If True, do full historical sync (only on first connect)
    
    Returns:
        dict with sync results
    """
    from sqlalchemy import func as sa_func
    
    oauth_token = db.query(OAuthToken).filter(
        OAuthToken.provider == OAuthProvider.STRIPE,
        OAuthToken.org_id == org_id
    ).first()
    
    if not oauth_token:
        return {"error": "Stripe not connected"}
    
    # Refresh token if needed
    refresh_token_if_needed(db, oauth_token)
    
    # Get API key
    api_key = get_stripe_api_key(db, org_id)
    stripe.api_key = api_key
    
    # Determine sync window
    if force_full or not oauth_token.last_sync_at:
        # Full historical sync (first time or forced)
        sync_start = None  # Fetch all
        print(f"[SYNC] Starting full historical sync for org {org_id}")
    else:
        # Incremental sync: fetch objects updated since last_sync minus buffer
        sync_start = oauth_token.last_sync_at - timedelta(seconds=SYNC_BUFFER_SECONDS)
        print(f"[SYNC] Starting incremental sync for org {org_id} since {sync_start}")
    
    results = {
        "customers_synced": 0,
        "customers_updated": 0,
        "subscriptions_synced": 0,
        "subscriptions_updated": 0,
        "payments_synced": 0,
        "payments_updated": 0,
        "is_full_sync": force_full or not oauth_token.last_sync_at
    }
    
    try:
        # Sync customers
        print(f"[SYNC] Syncing customers...")
        customer_params = {"limit": 100}
        if sync_start:
            customer_params["created"] = {"gte": int(sync_start.timestamp())}
        
        try:
            customers = stripe.Customer.list(**customer_params)
            for customer in customers.auto_paging_iter():
                try:
                    client = upsert_client(db, customer, org_id)
                    if client.stripe_customer_id == customer.id:
                        results["customers_synced"] += 1
                    else:
                        results["customers_updated"] += 1
                except Exception as e:
                    print(f"[SYNC] Error upserting customer {customer.id}: {str(e)}")
                    import traceback
                    traceback.print_exc()
                    continue
        except Exception as e:
            print(f"[SYNC] Error listing customers: {str(e)}")
            import traceback
            traceback.print_exc()
            # Continue with other syncs even if customers fail
        
        # Sync subscriptions
        print(f"[SYNC] Syncing subscriptions...")
        sub_params = {"limit": 100, "status": "all"}
        if sync_start:
            sub_params["created"] = {"gte": int(sync_start.timestamp())}
        
        try:
            subscriptions = stripe.Subscription.list(**sub_params)
            for sub in subscriptions.auto_paging_iter():
                try:
                    # Ensure customer exists
                    if sub.customer:
                        try:
                            customer = stripe.Customer.retrieve(sub.customer)
                            upsert_client(db, customer, org_id)
                        except:
                            pass
                    
                    subscription = upsert_subscription(db, sub, org_id)
                    if not sync_start or (subscription.created_at and subscription.created_at >= sync_start):
                        results["subscriptions_synced"] += 1
                    else:
                        results["subscriptions_updated"] += 1
                except Exception as e:
                    print(f"[SYNC] Error upserting subscription {sub.id}: {str(e)}")
                    import traceback
                    traceback.print_exc()
                    continue
        except Exception as e:
            print(f"[SYNC] Error listing subscriptions: {str(e)}")
            import traceback
            traceback.print_exc()
            # Continue with other syncs
        
        # Sync charges
        print(f"[SYNC] Syncing charges...")
        charge_params = {"limit": 100}
        if sync_start:
            charge_params["created"] = {"gte": int(sync_start.timestamp())}
        
        try:
            charges = stripe.Charge.list(**charge_params)
            for charge in charges.auto_paging_iter():
                try:
                    # Ensure customer exists
                    if charge.customer:
                        try:
                            customer = stripe.Customer.retrieve(charge.customer)
                            upsert_client(db, customer, org_id)
                        except:
                            pass
                    
                    payment = upsert_payment(db, charge, org_id, 'charge')
                    if payment:
                        # Debug: Log failed charge payments to track retry attempts
                        if payment.status == 'failed' and payment.subscription_id:
                            print(f"[SYNC] Failed charge payment: charge_id={charge.id}, subscription_id={payment.subscription_id}, invoice_id={payment.invoice_id}, created={payment.created_at}")
                        if not sync_start or (payment.created_at and payment.created_at >= sync_start):
                            results["payments_synced"] += 1
                        else:
                            results["payments_updated"] += 1
                except Exception as e:
                    print(f"[SYNC] Error upserting charge {charge.id}: {str(e)}")
                    import traceback
                    traceback.print_exc()
                    continue
        except Exception as e:
            print(f"[SYNC] Error listing charges: {str(e)}")
            import traceback
            traceback.print_exc()
            # Continue with other syncs
        
        # Sync payment intents
        print(f"[SYNC] Syncing payment intents...")
        pi_params = {"limit": 100}
        if sync_start:
            pi_params["created"] = {"gte": int(sync_start.timestamp())}
        
        try:
            payment_intents = stripe.PaymentIntent.list(**pi_params)
            for pi in payment_intents.auto_paging_iter():
                try:
                    if pi.customer:
                        try:
                            customer = stripe.Customer.retrieve(pi.customer)
                            upsert_client(db, customer, org_id)
                        except:
                            pass
                    
                    payment = upsert_payment(db, pi, org_id, 'payment_intent')
                    if payment:
                        # Debug: Log failed payment intent payments to track retry attempts
                        if payment.status == 'failed' and payment.subscription_id:
                            print(f"[SYNC] Failed payment_intent payment: pi_id={pi.id}, subscription_id={payment.subscription_id}, invoice_id={payment.invoice_id}, created={payment.created_at}")
                        if not sync_start or (payment.created_at and payment.created_at >= sync_start):
                            results["payments_synced"] += 1
                        else:
                            results["payments_updated"] += 1
                except Exception as e:
                    print(f"[SYNC] Error upserting payment intent {pi.id}: {str(e)}")
                    import traceback
                    traceback.print_exc()
                    continue
        except Exception as e:
            print(f"[SYNC] Error listing payment intents: {str(e)}")
            import traceback
            traceback.print_exc()
            # Continue with other syncs
        
        # Sync invoices
        print(f"[SYNC] Syncing invoices...")
        invoice_params = {"limit": 100}  # Remove "status": "all" - it's invalid
        if sync_start:
            invoice_params["created"] = {"gte": int(sync_start.timestamp())}
        
        try:
            invoices = stripe.Invoice.list(**invoice_params)
            for invoice in invoices.auto_paging_iter():
                try:
                    if invoice.customer:
                        try:
                            customer = stripe.Customer.retrieve(invoice.customer)
                            upsert_client(db, customer, org_id)
                        except:
                            pass
                    
                    payment = upsert_payment(db, invoice, org_id, 'invoice')
                    if payment:
                        # Debug: Log failed invoice payments to track retry attempts
                        if payment.status == 'failed' and payment.subscription_id:
                            print(f"[SYNC] Failed invoice payment: invoice_id={invoice.id}, subscription_id={payment.subscription_id}, stripe_id={payment.stripe_id}, created={payment.created_at}")
                        if not sync_start or (payment.created_at and payment.created_at >= sync_start):
                            results["payments_synced"] += 1
                        else:
                            results["payments_updated"] += 1
                except Exception as e:
                    print(f"[SYNC] Error upserting invoice {invoice.id}: {str(e)}")
                    import traceback
                    traceback.print_exc()
                    continue
        except Exception as e:
            print(f"[SYNC] Error listing invoices: {str(e)}")
            import traceback
            traceback.print_exc()
            # Continue - don't fail entire sync if invoices fail
        
        # Update last_sync_at
        oauth_token.last_sync_at = datetime.utcnow()
        db.commit()
        
        print(f"[SYNC] ✅ Sync complete: {results}")
        return results
        
    except Exception as e:
        db.rollback()
        import traceback
        error_msg = f"Sync failed: {str(e)}"
        print(f"[SYNC] ❌ {error_msg}")
        print(traceback.format_exc())
        return {"error": error_msg, **results}


def reconcile_stripe_data(db: Session, org_id: uuid.UUID) -> dict:
    """
    Manual reconciliation: Recompute derived analytics from raw data.
    This doesn't refetch from Stripe, just recalculates from existing data.
    """
    from sqlalchemy import func as sa_func
    
    results = {
        "clients_reconciled": 0,
        "revenue_recalculated": 0
    }
    
    # Recalculate client lifetime revenue from all succeeded payments
    # Deduplicate payments by (subscription_id, invoice_id) or (invoice_id)
    # Prefer charge records over invoice records
    
    clients = db.query(Client).filter(Client.org_id == org_id).all()
    for client in clients:
        # Get all succeeded payments for this client
        all_payments = db.query(StripePayment).filter(
            StripePayment.client_id == client.id,
            StripePayment.status == 'succeeded',
            StripePayment.org_id == org_id
        ).all()
        
        # Deduplicate: group by (subscription_id, invoice_id) or (invoice_id)
        seen = set()
        deduplicated_payments = []
        
        # Sort: prefer charge over invoice, then by updated_at (most recent first)
        all_payments.sort(key=lambda p: (
            0 if p.type == 'charge' else 1,
            -(p.updated_at.timestamp() if p.updated_at else 0)
        ))
        
        for payment in all_payments:
            if payment.subscription_id and payment.invoice_id:
                key = (payment.subscription_id, payment.invoice_id)
            elif payment.invoice_id:
                key = (None, payment.invoice_id)
            else:
                key = payment.stripe_id
            
            if key not in seen:
                seen.add(key)
                deduplicated_payments.append(payment)
        
        total_revenue = sum(p.amount_cents for p in deduplicated_payments)
        
        if client.lifetime_revenue_cents != total_revenue:
            client.lifetime_revenue_cents = total_revenue
            results["revenue_recalculated"] += 1
        
        results["clients_reconciled"] += 1
    
    db.commit()
    return results


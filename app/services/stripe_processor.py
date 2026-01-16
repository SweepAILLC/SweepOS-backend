"""
Background processor for Stripe webhook events.
Processes events and updates database records.
"""
from sqlalchemy.orm import Session
from sqlalchemy import and_
from app.models.stripe_payment import StripePayment
from app.models.stripe_subscription import StripeSubscription
from app.models.client import Client
from app.models.recommendation import Recommendation
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Dict, Any
import uuid


def process_stripe_event(db: Session, event: Dict[str, Any], org_id: uuid.UUID):
    """
    Process a Stripe webhook event and update database.
    
    Handles:
    - invoice.payment_succeeded / charge.succeeded -> create/update payment, update client lifetime revenue
    - invoice.payment_failed / payment_intent.payment_failed -> create failed payment, create recovery recommendation
    - customer.subscription.* -> create/update subscription, update MRR
    - charge.refunded -> mark payment as refunded
    """
    
    event_type = event.get("type")
    data = event.get("data", {}).get("object", {})
    
    print(f"Processing event type: {event_type}")
    print(f"Event data keys: {list(data.keys())[:20] if isinstance(data, dict) else 'Not a dict'}")
    print(f"Event structure: type={event_type}, has_data={bool(data)}, data_type={type(data)}")
    
    try:
        if event_type in ["invoice.payment_succeeded", "charge.succeeded", "invoice.paid"]:
            print(f"Handling successful payment event: {event_type}")
            _process_successful_payment(db, data, event, event_type, org_id)
        
        elif event_type in ["invoice.payment_failed", "payment_intent.payment_failed", "charge.failed"]:
            print(f"Handling failed payment event: {event_type}")
            _process_failed_payment(db, data, event, event_type, org_id)
        
        elif event_type.startswith("customer.subscription."):
            print(f"Handling subscription event: {event_type}")
            _process_subscription_event(db, data, event_type, org_id)
        
        elif event_type == "charge.refunded":
            print(f"Handling refund event: {event_type}")
            _process_refund(db, data, org_id)
        
        elif event_type == "customer.created":
            print(f"Handling customer created event: {event_type}")
            _process_customer_created(db, data, org_id)
        
        elif event_type == "customer.updated":
            print(f"Handling customer updated event: {event_type}")
            _process_customer_updated(db, data, org_id)
        
        else:
            print(f"Event type {event_type} not handled - skipping")
    except Exception as e:
        # Log the error but don't crash
        import traceback
        print(f"ERROR processing event {event_type}: {str(e)}")
        print(traceback.format_exc())
        raise  # Re-raise so webhook handler knows it failed


def _process_successful_payment(db: Session, data: Dict[str, Any], event: Dict[str, Any], event_type: str, org_id: uuid.UUID):
    """Process successful payment - create/update payment record and update client lifetime revenue"""
    
    # For invoice events, the charge ID is in data.charge
    # For charge events, the charge ID is in data.id
    if event_type.startswith("invoice."):
        # Invoice event - get charge from invoice
        # For invoice.payment_succeeded, charge might be a string ID or an object
        charge_data = data.get("charge")
        if isinstance(charge_data, dict):
            payment_id = charge_data.get("id")
        elif isinstance(charge_data, str):
            payment_id = charge_data
        else:
            # Fallback: use invoice ID as payment identifier if no charge yet
            payment_id = data.get("id")  # Use invoice ID temporarily
            print(f"Warning: No charge ID in invoice {payment_id}, using invoice ID")
        
        amount_cents = data.get("amount_paid", 0) or data.get("amount_due", 0)  # Amount paid in cents
        subscription_id = data.get("subscription")
        customer_id = data.get("customer")
        currency = data.get("currency", "usd")
        receipt_url = data.get("hosted_invoice_url")
        
        # If this invoice has a subscription, try to create/update subscription with MRR from invoice amount
        # Test events might not have subscription.created events, so we derive MRR from invoice
        if subscription_id and amount_cents > 0:
            print(f"[PAYMENT] Invoice has subscription {subscription_id}, will update subscription MRR from invoice amount")
    else:
        # Charge event
        payment_id = data.get("id")
        amount_cents = data.get("amount", 0)
        # Try to get subscription from invoice if charge has invoice field
        invoice_data = data.get("invoice")
        subscription_id = None
        if invoice_data:
            # If invoice is an object (expanded), get subscription from it
            if isinstance(invoice_data, dict):
                subscription_id = invoice_data.get("subscription")
            # If invoice is just an ID string, try to find a payment created from that invoice
            elif isinstance(invoice_data, str):
                # Look for a payment that was created from this invoice (invoice ID as stripe_id)
                invoice_payment = db.query(StripePayment).filter(
                    StripePayment.stripe_id == invoice_data
                ).first()
                if invoice_payment and invoice_payment.subscription_id:
                    subscription_id = invoice_payment.subscription_id
                    print(f"Found subscription_id {subscription_id} from invoice payment {invoice_data}")
        customer_id = data.get("customer")
        currency = data.get("currency", "usd")
        receipt_url = data.get("receipt_url")
    
    if not payment_id:
        print(f"No payment ID found in event {event_type}, data keys: {list(data.keys())[:10]}")
        return
    
    if not customer_id:
        print(f"No customer ID found in event {event_type}, data keys: {list(data.keys())[:10]}")
        return
    
    print(f"Processing payment: ID={payment_id}, Amount=${amount_cents/100:.2f}, Customer={customer_id}")
    
    # Find or create client by Stripe customer ID
    # First try to find by stripe_customer_id
    client = db.query(Client).filter(
        Client.stripe_customer_id == customer_id,
        Client.org_id == org_id
    ).first()
    
    if not client:
        # Try to get customer email from Stripe to match existing clients
        try:
            import stripe
            from app.core.encryption import decrypt_token
            from app.models.oauth_token import OAuthToken, OAuthProvider
            
            # Get the org's OAuth token to access their Stripe account
            oauth_token = db.query(OAuthToken).filter(
                OAuthToken.provider == OAuthProvider.STRIPE,
                OAuthToken.org_id == org_id
            ).first()
            
            if oauth_token:
                decrypted_token = decrypt_token(oauth_token.access_token)
                stripe.api_key = decrypted_token
                
                # Try to retrieve customer from Stripe to get email
                try:
                    customer_data = stripe.Customer.retrieve(customer_id)
                    customer_email = getattr(customer_data, 'email', None)
                    
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
                                print(f"[WEBHOOK] Linked existing client {client.id} to Stripe customer {customer_id} by email {customer_email}")
                except Exception as e:
                    print(f"[WEBHOOK] Could not retrieve customer {customer_id} from Stripe: {e}")
        except Exception as e:
            print(f"[WEBHOOK] Error matching customer by email: {e}")
    
    if not client:
        print(f"Warning: No client found for Stripe customer {customer_id}. Creating client...")
        # Create a client for this Stripe customer (for test data)
        # Client model has first_name, last_name, email (not name)
        client = Client(
            org_id=org_id,
            first_name=f"Stripe",
            last_name=f"Customer {customer_id[:8]}",
            email=f"customer_{customer_id[:8]}@stripe.test",
            stripe_customer_id=customer_id
        )
        db.add(client)
        db.flush()  # Flush to get the client ID
        print(f"Created client {client.id} for Stripe customer {customer_id}")
    
    # If subscription_id is still None, try to find active subscription for this client
    if not subscription_id and client:
        active_sub = db.query(StripeSubscription).filter(
            and_(
                StripeSubscription.org_id == org_id,
                StripeSubscription.client_id == client.id,
                StripeSubscription.status == "active"
            )
        ).first()
        if active_sub:
            subscription_id = active_sub.stripe_subscription_id
            print(f"Linking payment to active subscription {subscription_id} for client {client.id}")
    
    # Use idempotent upsert with ON CONFLICT (prevents duplicates)
    from sqlalchemy.dialects.postgresql import insert
    import json
    
    payment_type = 'charge' if not event_type.startswith("invoice.") else 'invoice'
    created_ts = data.get('created', int(datetime.utcnow().timestamp()))
    created_at = datetime.fromtimestamp(created_ts) if created_ts else datetime.utcnow()
    
    stmt = insert(StripePayment).values(
        org_id=org_id,
        stripe_id=payment_id,
        client_id=client.id if client else None,
        amount_cents=amount_cents,
        currency=currency,
        status="succeeded",
        type=payment_type,
        subscription_id=subscription_id,
        receipt_url=receipt_url,
        raw_event=event,
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
            receipt_url=stmt.excluded.receipt_url,
            raw_event=stmt.excluded.raw_event,
            updated_at=datetime.utcnow()
        )
    )
    
    db.execute(stmt)
    
    # Note: Client lifetime revenue is recalculated during reconciliation to avoid double-counting
    # This prevents double-counting when webhooks and sync both process the same payment
    
    # If invoice has subscription, ensure subscription exists with MRR
    # Test events might not trigger subscription.created, so create/update from invoice
    if subscription_id and amount_cents > 0:
            existing_sub = db.query(StripeSubscription).filter(
                and_(
                    StripeSubscription.stripe_subscription_id == subscription_id,
                    StripeSubscription.org_id == org_id
                )
            ).first()
            
            # Calculate MRR from invoice amount (assume monthly for test data)
            # For invoices, the amount is typically the monthly charge
            invoice_mrr = Decimal(amount_cents) / Decimal(100)  # Convert cents to dollars
            
            if existing_sub:
                # Update existing subscription MRR if it's 0
                if existing_sub.mrr == 0 or existing_sub.mrr is None:
                    existing_sub.mrr = invoice_mrr
                    existing_sub.status = "active"  # Assume active if payment succeeded
                    print(f"Updated subscription {subscription_id} MRR from invoice: ${float(invoice_mrr):.2f}")
            else:
                # Create subscription from invoice data
                print(f"Creating subscription {subscription_id} from invoice with MRR: ${float(invoice_mrr):.2f}")
                subscription = StripeSubscription(
                    org_id=org_id,
                    stripe_subscription_id=subscription_id,
                    client_id=client.id if client else None,
                    status="active",
                    current_period_start=datetime.utcnow(),
                    current_period_end=datetime.utcnow() + timedelta(days=30),  # Default 30 days
                    mrr=invoice_mrr,
                    raw=data,
                    created_at=datetime.utcnow()
                )
                db.add(subscription)
    
    try:
        db.commit()
        print(f"✅ Successfully processed {event_type} event - payment {payment_id} committed to database")
        
        # Move client back to active if they received a payment (automation rule)
        if client:
            try:
                from app.services.client_automation import move_client_to_active_on_payment
                if move_client_to_active_on_payment(db, client):
                    db.commit()
                    print(f"[CLIENT_AUTOMATION] ✅ Moved client {client.id} back to ACTIVE after payment")
            except Exception as automation_error:
                # Don't fail payment processing if automation fails
                print(f"[CLIENT_AUTOMATION] ⚠️  Error in automation: {str(automation_error)}")
    except Exception as commit_error:
        print(f"❌ ERROR committing payment {payment_id}: {str(commit_error)}")
        db.rollback()
        raise


def _process_failed_payment(db: Session, data: Dict[str, Any], event: Dict[str, Any], event_type: str, org_id: uuid.UUID):
    """Process failed payment - create payment record and recovery recommendation"""
    
    # For invoice events, get charge from invoice
    # For charge/payment_intent events, use the ID directly
    if event_type.startswith("invoice."):
        payment_id = data.get("charge")  # Charge ID from invoice
        amount_cents = data.get("amount_due", 0)
        subscription_id = data.get("subscription")
        customer_id = data.get("customer")
        currency = data.get("currency", "usd")
        receipt_url = data.get("hosted_invoice_url")
    else:
        payment_id = data.get("id")
        amount_cents = data.get("amount", 0)
        subscription_id = None
        customer_id = data.get("customer")
        currency = data.get("currency", "usd")
        receipt_url = None
    
    if not payment_id:
        print(f"No payment ID found in failed payment event {event_type}")
        return
    
    if not customer_id:
        print(f"No customer ID found in failed payment event {event_type}")
        return
    
    client = db.query(Client).filter(
        and_(
            Client.stripe_customer_id == customer_id,
            Client.org_id == org_id
        )
    ).first()
    
    # Check if payment already exists (filter by org_id to avoid cross-org conflicts)
    existing_payment = db.query(StripePayment).filter(
        and_(
            StripePayment.stripe_id == payment_id,
            StripePayment.org_id == org_id
        )
    ).first()
    
    if existing_payment:
        existing_payment.status = "failed"
        existing_payment.raw_event = event
        existing_payment.updated_at = datetime.utcnow()
    else:
        payment = StripePayment(
            org_id=org_id,
            stripe_id=payment_id,
            client_id=client.id if client else None,
            amount_cents=amount_cents,
            currency=currency,
            status="failed",
            type="charge" if not event_type.startswith("invoice.") else "invoice",
            subscription_id=subscription_id,
            receipt_url=receipt_url,
            raw_event=event
        )
        db.add(payment)
    
    # Create recovery recommendation
    if client:
        recommendation = Recommendation(
            org_id=org_id,
            client_id=client.id,
            type="payment_recovery",
            payload={
                "stripe_payment_id": payment_id,
                "amount_cents": amount_cents,
                "currency": data.get("currency", "usd"),
                "invoice_url": data.get("hosted_invoice_url"),
                "customer_id": customer_id,
                "subscription_id": subscription_id
            },
            status="PENDING"
        )
        db.add(recommendation)
    
    try:
        db.commit()
        print(f"✅ Successfully processed {event_type} event - failed payment {payment_id} committed")
    except Exception as commit_error:
        print(f"❌ ERROR committing failed payment {payment_id}: {str(commit_error)}")
        db.rollback()
        raise


def _process_subscription_event(db: Session, data: Dict[str, Any], event_type: str, org_id: uuid.UUID):
    """Process subscription events - create/update subscription and update MRR"""
    
    subscription_id = data.get("id")
    if not subscription_id:
        return
    
    customer_id = data.get("customer")
    client = None
    if customer_id:
        client = db.query(Client).filter(
            and_(
                Client.stripe_customer_id == customer_id,
                Client.org_id == org_id
            )
        ).first()
    
    # Determine status
    status_map = {
        "customer.subscription.created": "active",
        "customer.subscription.updated": data.get("status", "active"),
        "customer.subscription.deleted": "canceled",
    }
    subscription_status = status_map.get(event_type, data.get("status", "active"))
    
    # Calculate MRR from subscription items
    mrr = Decimal(0)
    items = data.get("items", {}).get("data", [])
    print(f"[SUBSCRIPTION] Processing subscription {subscription_id}, items count: {len(items)}")
    
    if not items:
        # Fallback: try to get amount from subscription directly (for test events)
        # Test events might not have items, but might have amount or plan info
        amount = data.get("plan", {}).get("amount") or data.get("amount")
        if amount:
            print(f"[SUBSCRIPTION] No items found, using direct amount: {amount}")
            interval = data.get("plan", {}).get("interval", "month") or data.get("interval", "month")
            unit_amount = Decimal(amount)
            if interval == "year":
                unit_amount = unit_amount / Decimal(12)
            elif interval == "week":
                unit_amount = unit_amount * Decimal(4.33)
            elif interval == "day":
                unit_amount = unit_amount * Decimal(30)
            mrr = unit_amount / Decimal(100)  # Convert from cents
            print(f"[SUBSCRIPTION] Calculated MRR from direct amount: ${float(mrr):.2f}")
    
    for item in items:
        price = item.get("price", {})
        unit_amount = Decimal(price.get("unit_amount", 0) or 0)
        quantity = Decimal(item.get("quantity", 1) or 1)
        
        print(f"[SUBSCRIPTION] Item: unit_amount={unit_amount}, quantity={quantity}")
        
        # Convert to monthly if needed
        interval = price.get("recurring", {}).get("interval", "month")
        if interval == "year":
            unit_amount = unit_amount / Decimal(12)
        elif interval == "week":
            unit_amount = unit_amount * Decimal(4.33)
        elif interval == "day":
            unit_amount = unit_amount * Decimal(30)
        
        item_mrr = unit_amount * quantity / Decimal(100)  # Convert from cents
        mrr += item_mrr
        print(f"[SUBSCRIPTION] Item MRR: ${float(item_mrr):.2f}, Total MRR so far: ${float(mrr):.2f}")
    
    print(f"[SUBSCRIPTION] Final calculated MRR: ${float(mrr):.2f}")
    
    # Parse dates
    period_start = None
    period_end = None
    if data.get("current_period_start"):
        period_start = datetime.fromtimestamp(data.get("current_period_start"))
    if data.get("current_period_end"):
        period_end = datetime.fromtimestamp(data.get("current_period_end"))
    
    # Get plan ID (from first item)
    plan_id = None
    if items and items[0].get("price"):
        plan_id = items[0]["price"].get("id")
    
    # Check if subscription exists (filter by org_id to avoid cross-org conflicts)
    existing_sub = db.query(StripeSubscription).filter(
        and_(
            StripeSubscription.stripe_subscription_id == subscription_id,
            StripeSubscription.org_id == org_id
        )
    ).first()
    
    if existing_sub:
        existing_sub.status = subscription_status
        existing_sub.current_period_start = period_start
        existing_sub.current_period_end = period_end
        existing_sub.mrr = mrr
        existing_sub.raw = data
        existing_sub.updated_at = datetime.utcnow()
    else:
        subscription = StripeSubscription(
            org_id=org_id,
            stripe_subscription_id=subscription_id,
            client_id=client.id if client else None,
            status=subscription_status,
            current_period_start=period_start,
            current_period_end=period_end,
            plan_id=plan_id,
            mrr=mrr,
            raw=data,
            created_at=datetime.utcnow()  # Explicitly set created_at
        )
        db.add(subscription)
        print(f"Created subscription: {subscription_id}, mrr: ${float(mrr):.2f}, status: {subscription_status}, created_at: {subscription.created_at}")
    
    # Update client estimated MRR
    if client:
        # Sum all active subscriptions for this client (filter by org_id)
        total_mrr = db.query(StripeSubscription).filter(
            and_(
                StripeSubscription.org_id == org_id,
                StripeSubscription.client_id == client.id,
                StripeSubscription.status == "active"
            )
        ).with_entities(
            db.func.sum(StripeSubscription.mrr)
        ).scalar() or Decimal(0)
        
        client.estimated_mrr = total_mrr
        db.add(client)
    
    try:
        db.commit()
        print(f"✅ Successfully processed {event_type} event - failed payment {payment_id} committed")
    except Exception as commit_error:
        print(f"❌ ERROR committing failed payment {payment_id}: {str(commit_error)}")
        db.rollback()
        raise


def _process_customer_created(db: Session, data: Dict[str, Any], org_id: uuid.UUID):
    """
    Handle customer.created webhook event.
    Creates or updates a client record when a new customer is created in Stripe.
    """
    customer_id = data.get("id")
    if not customer_id:
        print("[WEBHOOK] customer.created: No customer ID in event data")
        return
    
    customer_email = data.get("email")
    customer_name = data.get("name", "")
    
    # Parse name into first and last name
    if customer_name:
        name_parts = customer_name.split()
        first_name = name_parts[0] if name_parts else "Stripe"
        last_name = " ".join(name_parts[1:]) if len(name_parts) > 1 else "Customer"
    else:
        first_name = "Stripe"
        last_name = f"Customer {customer_id[:8]}"
    
    email = customer_email or f"{customer_id}@stripe.test"
    
    # First, try to find existing client by stripe_customer_id
    client = db.query(Client).filter(
        Client.stripe_customer_id == customer_id,
        Client.org_id == org_id
    ).first()
    
    # If not found by stripe_customer_id, try to find by email to avoid duplicates
    if not client and customer_email:
        client = db.query(Client).filter(
            Client.email == customer_email,
            Client.org_id == org_id
        ).first()
        
        # If found by email, link the stripe_customer_id
        if client:
            if not client.stripe_customer_id:
                client.stripe_customer_id = customer_id
                print(f"[WEBHOOK] customer.created: Linked existing client {client.id} to Stripe customer {customer_id} by email {customer_email}")
    
    # If still not found, create a new client
    if not client:
        client = Client(
            org_id=org_id,
            first_name=first_name,
            last_name=last_name,
            email=email,
            stripe_customer_id=customer_id,
            lifecycle_state="cold_lead"  # New customers start as cold leads
        )
        db.add(client)
        db.flush()  # Flush to get the client ID
        print(f"[WEBHOOK] customer.created: ✅ Created new client {client.id} for Stripe customer {customer_id} ({email})")
    else:
        # Update existing client with latest info from Stripe
        updated = False
        if not client.email and customer_email:
            client.email = customer_email
            updated = True
        if not client.stripe_customer_id:
            client.stripe_customer_id = customer_id
            updated = True
        if not client.first_name or not client.last_name:
            if customer_name:
                if not client.first_name:
                    client.first_name = first_name
                if not client.last_name:
                    client.last_name = last_name
                updated = True
        if updated:
            print(f"[WEBHOOK] customer.created: Updated existing client {client.id} for Stripe customer {customer_id}")
    
    db.commit()


def _process_customer_updated(db: Session, data: Dict[str, Any], org_id: uuid.UUID):
    """
    Handle customer.updated webhook event.
    Updates an existing client record when customer information changes in Stripe.
    """
    customer_id = data.get("id")
    if not customer_id:
        print("[WEBHOOK] customer.updated: No customer ID in event data")
        return
    
    # Find client by stripe_customer_id
    client = db.query(Client).filter(
        Client.stripe_customer_id == customer_id,
        Client.org_id == org_id
    ).first()
    
    if not client:
        # If client doesn't exist, treat it like a customer.created event
        print(f"[WEBHOOK] customer.updated: Client not found for customer {customer_id}, creating new client")
        _process_customer_created(db, data, org_id)
        return
    
    # Update client with latest info from Stripe
    customer_email = data.get("email")
    customer_name = data.get("name", "")
    
    updated = False
    if customer_email and client.email != customer_email:
        client.email = customer_email
        updated = True
    
    if customer_name:
        name_parts = customer_name.split()
        first_name = name_parts[0] if name_parts else "Stripe"
        last_name = " ".join(name_parts[1:]) if len(name_parts) > 1 else "Customer"
        
        if client.first_name != first_name:
            client.first_name = first_name
            updated = True
        if client.last_name != last_name:
            client.last_name = last_name
            updated = True
    
    if updated:
        db.commit()
        print(f"[WEBHOOK] customer.updated: ✅ Updated client {client.id} for Stripe customer {customer_id}")


def _process_refund(db: Session, data: Dict[str, Any], org_id: uuid.UUID):
    """Process refund - mark payment as refunded"""
    
    charge_id = data.get("charge") or data.get("id")
    if not charge_id:
        return
    
    # Find payment by charge ID and org_id
    payment = db.query(StripePayment).filter(
        StripePayment.stripe_id == charge_id,
        StripePayment.org_id == org_id
    ).first()
    
    if payment:
        payment.status = "refunded"
        payment.updated_at = datetime.utcnow()
        try:
            db.commit()
            print(f"✅ Successfully processed refund event - payment {charge_id} marked as refunded")
        except Exception as commit_error:
            print(f"❌ ERROR committing refund for payment {charge_id}: {str(commit_error)}")
            db.rollback()
            raise


from fastapi import APIRouter, Depends, HTTPException, status, Query, Body
from sqlalchemy.orm import Session
from sqlalchemy import desc, func, or_, and_
from typing import List, Optional
from uuid import UUID
import uuid
from datetime import datetime, timezone
from app.db.session import get_db
from app.models.client import Client, LifecycleState
from app.models.stripe_payment import StripePayment
from app.models.stripe_treasury_transaction import StripeTreasuryTransaction, TreasuryTransactionStatus
from app.models.manual_payment import ManualPayment
from app.models.client_checkin import ClientCheckIn
from app.schemas.client import Client as ClientSchema, ClientCreate, ClientUpdate
from app.api.deps import get_current_user, get_selected_org_id
from app.models.user import User
from app.services.client_automation import update_client_progress, update_client_lifecycle_state, process_client_automation

router = APIRouter()


@router.get("", response_model=List[ClientSchema])
def list_clients(
    lifecycle_state: Optional[LifecycleState] = Query(None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    try:
        # Get selected org_id from user object (set by get_current_user)
        org_id = getattr(current_user, 'selected_org_id', current_user.org_id)
        
        # CRITICAL: Filter by org_id for multi-tenant isolation (use selected org from token)
        query = db.query(Client).filter(Client.org_id == org_id)
        if lifecycle_state:
            query = query.filter(Client.lifecycle_state == lifecycle_state)
        clients = query.all()

        # Trigger client automation in a background thread to avoid blocking the API request
        import threading
        def run_client_automation_in_background():
            from app.db.session import SessionLocal
            bg_db = SessionLocal()
            try:
                print(f"[CLIENT_API] Starting background client automation for org {org_id}...")
                result = process_client_automation(bg_db, org_id=org_id)
                print(f"[CLIENT_API] Background client automation complete: {result.get('progress_updates', 0)} progress updates, {result.get('state_changes', 0)} state changes")
            except Exception as e:
                import traceback
                print(f"[CLIENT_API] ❌ Background client automation failed: {str(e)}")
                traceback.print_exc()
            finally:
                bg_db.close()

        automation_thread = threading.Thread(target=run_client_automation_in_background, daemon=True)
        automation_thread.start()
        
        # Convert each client with proper error handling
        result = []
        for client in clients:
            try:
                # Use Pydantic's model_validate with from_attributes to handle Decimal conversion
                validated = ClientSchema.model_validate(client, from_attributes=True)
                result.append(validated)
            except Exception as client_error:
                from pydantic import ValidationError
                import traceback
                print(f"ERROR validating client {client.id}: {str(client_error)}")
                print(f"Client data: estimated_mrr={client.estimated_mrr} (type: {type(client.estimated_mrr)}), email={client.email}")
                if isinstance(client_error, ValidationError):
                    print(f"Validation errors: {client_error.errors()}")
                    for error in client_error.errors():
                        print(f"  - Field: {error.get('loc')}, Error: {error.get('msg')}, Input: {error.get('input')}")
                print(traceback.format_exc())
                # Skip invalid clients for now
                continue
        
        return result
    except Exception as e:
        import traceback
        print(f"ERROR in list_clients: {str(e)}")
        print(traceback.format_exc())
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error loading clients: {str(e)}"
        )


@router.post("", response_model=ClientSchema, status_code=status.HTTP_201_CREATED)
def create_client(
    client_data: ClientCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    # Get selected org_id from user object (set by get_current_user)
    org_id = getattr(current_user, 'selected_org_id', current_user.org_id)
    
    # Check for duplicate client by email
    if client_data.email:
        existing_client = db.query(Client).filter(
            Client.email == client_data.email,
            Client.org_id == org_id
        ).first()
        
        if existing_client:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Client with email {client_data.email} already exists (ID: {existing_client.id})"
            )
    
    # CRITICAL: Set org_id from selected org (token)
    client_dict = client_data.model_dump()
    client_dict['org_id'] = org_id
    client = Client(**client_dict)
    
    # Update program dates if program fields are set
    # Handle end_date -> duration calculation
    if client.program_start_date and client.program_end_date:
        # Calculate duration from start and end dates
        duration = (client.program_end_date - client.program_start_date).days
        if duration > 0:
            client.program_duration_days = duration
        else:
            # Invalid: end date before start date
            client.program_end_date = None
            client.program_duration_days = None
    
    # Update program dates (handles duration -> end_date if needed)
    if client.program_start_date or client.program_duration_days or client.program_end_date:
        client.update_program_dates()
        # Calculate initial progress
        client.program_progress_percent = client.calculate_progress()
    
    db.add(client)
    db.commit()
    db.refresh(client)
    return client


@router.get("/{client_id}", response_model=ClientSchema)
def get_client(
    client_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get client details"""
    # Get selected org_id from user object (set by get_current_user)
    org_id = getattr(current_user, 'selected_org_id', current_user.org_id)
    
    # CRITICAL: Filter by org_id for multi-tenant isolation (use selected org from token)
    client = db.query(Client).filter(
        Client.id == client_id,
        Client.org_id == org_id
    ).first()
    if not client:
        raise HTTPException(status_code=404, detail="Client not found")
    
    # Update progress if program is set
    if client.program_start_date and client.program_duration_days:
        old_state = client.lifecycle_state.value if hasattr(client.lifecycle_state, 'value') else str(client.lifecycle_state)
        progress_updated = update_client_progress(db, client)
        state_updated = update_client_lifecycle_state(db, client)
        if progress_updated or state_updated:
            db.commit()
            # Refresh the client object to get updated state
            db.refresh(client)
            new_state = client.lifecycle_state.value if hasattr(client.lifecycle_state, 'value') else str(client.lifecycle_state)
            print(f"[CLIENT_API] Client {client.id} ({client.email}) updated: progress={client.program_progress_percent}%, state={old_state} → {new_state}")
            if old_state != new_state:
                print(f"[CLIENT_API] ✅ State change confirmed: {old_state} → {new_state}")
    
    return client


@router.get("/{client_id}/payments")
def get_client_payments(
    client_id: str,
    limit: int = Query(50, ge=1, le=100),
    merged_client_ids: Optional[str] = Query(None, description="Comma-separated list of merged client IDs"),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get payments for a specific client, including merged clients if specified"""
    # Get selected org_id from user object (set by get_current_user)
    org_id = getattr(current_user, 'selected_org_id', current_user.org_id)
    
    # CRITICAL: Filter by org_id for multi-tenant isolation (use selected org from token)
    client = db.query(Client).filter(
        Client.id == client_id,
        Client.org_id == org_id
    ).first()
    if not client:
        raise HTTPException(status_code=404, detail="Client not found")
    
    # Determine which client IDs to fetch payments for
    client_ids_to_fetch = [client_id]
    merged_clients_list = [client]  # Track all merged clients for email collection
    
    # If merged_client_ids is provided, use those
    if merged_client_ids:
        client_ids_to_fetch = [cid.strip() for cid in merged_client_ids.split(',') if cid.strip()]
        # Fetch all merged clients to get their emails
        merged_clients_list = db.query(Client).filter(
            Client.id.in_([UUID(cid) for cid in client_ids_to_fetch if cid]),
            Client.org_id == current_user.org_id
        ).all()
        print(f"[CLIENT_PAYMENTS] Using provided merged_client_ids: {client_ids_to_fetch}")
    # Check if client has merged_client_ids in meta field (from frontend)
    elif client.meta and isinstance(client.meta, dict) and client.meta.get('merged_client_ids'):
        merged_ids_from_meta = client.meta.get('merged_client_ids', [])
        if isinstance(merged_ids_from_meta, list) and len(merged_ids_from_meta) > 0:
            # Convert to UUIDs and fetch all merged clients
            try:
                merged_uuids = [UUID(cid) for cid in merged_ids_from_meta if cid]
                merged_clients_list = db.query(Client).filter(
                    Client.id.in_(merged_uuids),
                    Client.org_id == current_user.org_id
                ).all()
                client_ids_to_fetch = [str(c.id) for c in merged_clients_list]
                print(f"[CLIENT_PAYMENTS] Found merged_client_ids in meta: {client_ids_to_fetch}")
            except (ValueError, TypeError) as e:
                print(f"[CLIENT_PAYMENTS] Error parsing merged_client_ids from meta: {e}")
                merged_clients_list = [client]
    # Otherwise, automatically check if client has same email as other clients (for merged clients)
    elif client.email:
        # Normalize email for comparison (lowercase, trim, remove all whitespace)
        import re
        normalized_email = re.sub(r'\s+', '', client.email.lower().strip())
        
        # Find all clients with the same email (fetch all and filter in Python for better normalization)
        all_clients_with_email = db.query(Client).filter(
            and_(
                Client.org_id == current_user.org_id,
                Client.email.isnot(None)
            )
        ).all()
        
        # Filter clients with matching normalized email
        clients_with_same_email = [
            c for c in all_clients_with_email
            if c.email and re.sub(r'\s+', '', c.email.lower().strip()) == normalized_email
        ]
        
        if len(clients_with_same_email) > 1:
            # Multiple clients with same email - fetch payments from all
            merged_clients_list = clients_with_same_email
            client_ids_to_fetch = [str(c.id) for c in clients_with_same_email]
            print(f"[CLIENT_PAYMENTS] Auto-detected {len(clients_with_same_email)} clients with email '{normalized_email}', fetching payments from all: {client_ids_to_fetch}")
        else:
            print(f"[CLIENT_PAYMENTS] Only 1 client found with email '{normalized_email}', using single client")
    
    # Fetch payments from all relevant client IDs
    # Convert client_ids to UUIDs for query
    client_uuids = []
    for cid in client_ids_to_fetch:
        try:
            client_uuids.append(UUID(cid))
        except ValueError:
            print(f"[CLIENT_PAYMENTS] Invalid client ID: {cid}")
    
    if not client_uuids:
        client_uuids = [UUID(client_id)]
    
    # Fetch all payments from merged clients (no limit yet, we'll deduplicate first)
    # Also fetch payments by email matching if client has an email
    all_payments = []
    
    # 1. Fetch payments by client_id (directly linked)
    payments_by_client_id = db.query(StripePayment).filter(
        StripePayment.client_id.in_(client_uuids),
        StripePayment.org_id == org_id
    ).order_by(desc(StripePayment.created_at)).all()
    all_payments.extend(payments_by_client_id)
    
    # 2. Fetch payments by email matching (for payments not yet linked to client)
    # Collect all emails from all merged clients
    import re
    all_merged_emails = set()
    for merged_client in merged_clients_list:
        if merged_client.email:
            normalized_email = re.sub(r'\s+', '', merged_client.email.lower().strip())
            all_merged_emails.add(normalized_email)
    
    if all_merged_emails:
        print(f"[CLIENT_PAYMENTS] Checking payments for {len(all_merged_emails)} unique emails from merged clients: {list(all_merged_emails)}")
        
        # Fetch ALL StripePayments (not just unlinked ones) to check for email matches
        # This handles cases where payment is linked to wrong client but email matches merged clients
        all_stripe_payments = db.query(StripePayment).filter(
            StripePayment.org_id == current_user.org_id
        ).all()
        
        # Track which payments we've already added (by ID) to avoid duplicates
        added_payment_ids = {p.id for p in all_payments}
        
        for payment in all_stripe_payments:
            # Skip if already added (from client_id matching)
            if payment.id in added_payment_ids:
                continue
            
            # Check if customer email matches in raw_event
            customer_email = None
            if payment.raw_event:
                if isinstance(payment.raw_event, dict):
                    # Check various paths where email might be (including receipt_email for invoices)
                    customer_email = (
                        payment.raw_event.get('customer_email') or
                        payment.raw_event.get('receipt_email') or  # Common for invoices
                        (payment.raw_event.get('customer', {}).get('email') if isinstance(payment.raw_event.get('customer'), dict) else None) or
                        (payment.raw_event.get('data', {}).get('object', {}).get('customer_email') if isinstance(payment.raw_event.get('data'), dict) else None) or
                        (payment.raw_event.get('data', {}).get('object', {}).get('receipt_email') if isinstance(payment.raw_event.get('data'), dict) else None) or  # For invoice events
                        (payment.raw_event.get('data', {}).get('object', {}).get('customer', {}).get('email') if isinstance(payment.raw_event.get('data', {}).get('object', {}).get('customer'), dict) else None)
                    )
            
            # Also check if payment is linked to a client with matching email
            if not customer_email and payment.client_id:
                linked_client = db.query(Client).filter(Client.id == payment.client_id).first()
                if linked_client and linked_client.email:
                    customer_email = linked_client.email
            
            if customer_email:
                normalized_payment_email = re.sub(r'\s+', '', customer_email.lower().strip())
                if normalized_payment_email in all_merged_emails:
                    all_payments.append(payment)
                    added_payment_ids.add(payment.id)
                    payment_type = getattr(payment, 'type', 'unknown')
                    invoice_id = getattr(payment, 'invoice_id', None)
                    print(f"[CLIENT_PAYMENTS] Found payment {payment.stripe_id} (type: {payment_type}, invoice_id: {invoice_id}) by email match: {customer_email} (linked to client_id: {payment.client_id})")
                else:
                    # Log when email doesn't match (for debugging)
                    payment_type = getattr(payment, 'type', 'unknown')
                    if payment_type == 'invoice':
                        print(f"[CLIENT_PAYMENTS] Invoice payment {payment.stripe_id} has email {customer_email} but doesn't match merged client emails: {list(all_merged_emails)}")
        
        # 3. Fetch from Treasury Transactions by email
        treasury_transactions = db.query(StripeTreasuryTransaction).filter(
            StripeTreasuryTransaction.org_id == org_id,
            StripeTreasuryTransaction.customer_email.isnot(None)
        ).all()
        
        for transaction in treasury_transactions:
            if transaction.customer_email:
                normalized_transaction_email = re.sub(r'\s+', '', transaction.customer_email.lower().strip())
                if normalized_transaction_email in all_merged_emails:
                    # Convert Treasury Transaction to payment-like object for deduplication
                    # Create a simple object that mimics StripePayment structure
                    class TreasuryPayment:
                        def __init__(self, transaction):
                            self.id = transaction.id
                            self.stripe_id = transaction.stripe_transaction_id
                            self.client_id = transaction.client_id
                            self.amount_cents = abs(transaction.amount) if transaction.amount > 0 else 0
                            self.currency = transaction.currency or 'usd'
                            self.status = 'succeeded' if transaction.status == TreasuryTransactionStatus.POSTED else 'pending'
                            self.type = 'treasury_transaction'
                            self.subscription_id = None
                            self.invoice_id = None
                            self.receipt_url = None
                            self.created_at = transaction.created or transaction.posted_at
                            self.raw_event = transaction.raw_data
                    
                    all_payments.append(TreasuryPayment(transaction))
    
    # 4. Fetch manual payments for all merged clients
    manual_payments = db.query(ManualPayment).filter(
        ManualPayment.client_id.in_(client_uuids),
        ManualPayment.org_id == org_id
    ).order_by(desc(ManualPayment.payment_date)).all()
    
    # Convert manual payments to payment-like objects
    class ManualPaymentWrapper:
        def __init__(self, manual_payment):
            self.id = manual_payment.id
            self.stripe_id = None  # Manual payments don't have stripe_id
            self.client_id = manual_payment.client_id
            self.amount_cents = manual_payment.amount_cents
            self.currency = manual_payment.currency or 'usd'
            self.status = 'succeeded'  # Manual payments are always succeeded
            self.type = 'manual_payment'
            self.subscription_id = None
            self.invoice_id = None
            self.receipt_url = manual_payment.receipt_url
            self.created_at = manual_payment.payment_date
            self.description = manual_payment.description
            self.payment_method = manual_payment.payment_method
    
    for manual_payment in manual_payments:
        all_payments.append(ManualPaymentWrapper(manual_payment))
    
    # Apply the EXACT same deduplication logic as the recent payments table
    def deduplicate_payments(payments_list):
        """Deduplicate payments using EXACT same logic as recent payments table (/api/stripe/payments)"""
        seen_stripe_ids = set()  # Track exact duplicates by stripe_id
        deduplicated = []
        payment_map = {}  # Track best payment for each key
        
        # Sort: prefer charge over payment_intent over invoice, then by created_at (most recent first)
        # This matches the recent payments table sorting exactly
        payments_list.sort(key=lambda p: (
            {'charge': 0, 'payment_intent': 1, 'invoice': 2, 'manual_payment': 3}.get(getattr(p, 'type', None), 4),
            -(getattr(p, 'created_at', None).timestamp() if getattr(p, 'created_at', None) else 0)
        ))
        
        for payment in payments_list:
            stripe_id = getattr(payment, 'stripe_id', None)
            payment_id = getattr(payment, 'id', None)
            payment_type = getattr(payment, 'type', None)
            payment_status = getattr(payment, 'status', None)
            
            # For manual payments (no stripe_id), use payment id
            if payment_type == 'manual_payment':
                if payment_id and str(payment_id) not in seen_stripe_ids:
                    seen_stripe_ids.add(str(payment_id))
                    deduplicated.append(payment)
                continue
            
            # First check for exact duplicate by stripe_id (matches recent payments logic)
            if stripe_id and stripe_id in seen_stripe_ids:
                print(f"[CLIENT_PAYMENTS] Skipping exact duplicate payment {stripe_id} (same stripe_id)")
                continue
            if stripe_id:
                seen_stripe_ids.add(stripe_id)
            
            # Only deduplicate successful payments - keep all failed payments (matches recent payments)
            if payment_status != 'succeeded':
                deduplicated.append(payment)
                continue
            
            # Create deduplication key - EXACT same logic as recent payments table
            subscription_id = getattr(payment, 'subscription_id', None)
            invoice_id = getattr(payment, 'invoice_id', None)
            
            # Priority: (subscription_id, invoice_id) > invoice_id > subscription_id > stripe_id (standalone, no dedup)
            if subscription_id and invoice_id:
                # Group by subscription_id + invoice_id - payments for same subscription+invoice are duplicates
                key = ('subscription_invoice', subscription_id, invoice_id)
            elif invoice_id:
                # Group by invoice_id - all payments for same invoice are duplicates
                key = ('invoice', invoice_id)
            elif subscription_id:
                # Subscription payment without invoice_id (shouldn't happen, but handle gracefully)
                key = ('subscription', subscription_id)
            else:
                # Standalone payment (no subscription or invoice) - keep all (no deduplication)
                deduplicated.append(payment)
                continue
            
            # Check if we've seen this key before (matches recent payments logic)
            if key not in payment_map:
                # First payment with this key - keep it (it's the best one due to sorting)
                payment_map[key] = payment
                deduplicated.append(payment)
            else:
                # Duplicate key - we already have a better payment (due to sorting)
                existing_payment = payment_map[key]
                print(f"[CLIENT_PAYMENTS] Skipping duplicate payment {stripe_id} (type: {payment_type}) - keeping {getattr(existing_payment, 'stripe_id', None)} (type: {getattr(existing_payment, 'type', None)}) for {key[0]} {key[1]}")
        
        return deduplicated
    
    # Deduplicate payments
    deduplicated_payments = deduplicate_payments(all_payments)
    
    # Apply limit after deduplication
    payments = deduplicated_payments[:limit]
    
    # Calculate total revenue from deduplicated payments (succeeded only)
    # Manual payments are always succeeded, Stripe payments need to check status
    succeeded_deduplicated = [
        p for p in deduplicated_payments 
        if getattr(p, 'status', None) == "succeeded" or getattr(p, 'type', None) == "manual_payment"
    ]
    total_revenue_cents = sum(getattr(p, 'amount_cents', 0) for p in succeeded_deduplicated)
    
    print(f"[CLIENT_PAYMENTS] Fetched {len(all_payments)} payments (by client_id: {len(payments_by_client_id)}, by email: {len(all_payments) - len(payments_by_client_id)}), {len(deduplicated_payments)} after deduplication, showing {len(payments)} (total revenue from succeeded: ${total_revenue_cents/100:.2f})")
    
    # Convert payments to response format
    payment_responses = []
    for payment in payments:
        # Handle both StripePayment objects and TreasuryPayment objects
        payment_id = getattr(payment, 'id', None)
        stripe_id = getattr(payment, 'stripe_id', None)
        amount_cents = getattr(payment, 'amount_cents', 0)
        currency = getattr(payment, 'currency', 'usd')
        payment_status = getattr(payment, 'status', 'unknown')
        created_at = getattr(payment, 'created_at', None)
        receipt_url = getattr(payment, 'receipt_url', None)
        subscription_id = getattr(payment, 'subscription_id', None)
        invoice_id = getattr(payment, 'invoice_id', None)
        payment_type = getattr(payment, 'type', None)
        description = getattr(payment, 'description', None)
        payment_method = getattr(payment, 'payment_method', None)
        
        # Manual payments may not have stripe_id, so check payment_id instead
        if payment_id:
            # Format created_at
            created_at_str = None
            if created_at:
                if hasattr(created_at, 'isoformat'):
                    created_at_str = created_at.isoformat()
                elif hasattr(created_at, 'strftime'):
                    created_at_str = created_at.strftime('%Y-%m-%dT%H:%M:%S')
                else:
                    created_at_str = str(created_at)
            
            payment_responses.append({
                "id": str(payment_id),
                "stripe_id": stripe_id,  # Will be None for manual payments
                "amount_cents": amount_cents or 0,
                "amount": (amount_cents or 0) / 100.0,
                "currency": currency,
                "status": payment_status,
                "created_at": created_at_str,
                "receipt_url": receipt_url,
                "subscription_id": subscription_id,
                "invoice_id": invoice_id,
                "type": payment_type,
                "description": description,  # For manual payments
                "payment_method": payment_method,  # For manual payments
            })
    
    return {
        "client_id": client_id,
        "total_amount_paid_cents": total_revenue_cents,
        "total_amount_paid": total_revenue_cents / 100.0,
        "payments": payment_responses
    }


@router.post("/{client_id}/manual-payment", status_code=status.HTTP_201_CREATED)
def create_manual_payment(
    client_id: str,
    amount: float = Query(..., ge=0.01, description="Payment amount in dollars"),
    payment_date: Optional[str] = Query(None, description="Payment date (ISO format). Defaults to now."),
    description: Optional[str] = Query(None, description="Payment description/notes"),
    payment_method: Optional[str] = Query(None, description="Payment method (e.g., cash, check, bank_transfer)"),
    receipt_url: Optional[str] = Query(None, description="Optional receipt/document URL"),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Create a manual payment transaction for a client.
    Manual payments affect cash collected totals and revenue contributors,
    but do NOT appear in Stripe dashboard or failed payment queues.
    """
    from datetime import datetime, timezone
    
    # Get selected org_id from user object (set by get_current_user)
    org_id = getattr(current_user, 'selected_org_id', current_user.org_id)
    
    try:
        client_uuid = UUID(client_id)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid client ID format"
        )
    
    # Verify client exists and belongs to user's org (use selected org from token)
    client = db.query(Client).filter(
        Client.id == client_uuid,
        Client.org_id == org_id
    ).first()
    
    if not client:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Client not found"
        )
    
    # Parse payment date
    # The frontend sends ISO strings with timezone info (from toISOString())
    # We preserve the timezone-aware datetime to maintain the user's local date
    if payment_date:
        try:
            # Handle ISO format strings (may include 'Z' or timezone offset)
            if payment_date.endswith('Z'):
                payment_date = payment_date.replace('Z', '+00:00')
            payment_datetime = datetime.fromisoformat(payment_date)
            # If no timezone info, assume it's already in UTC (shouldn't happen with toISOString())
            if payment_datetime.tzinfo is None:
                payment_datetime = payment_datetime.replace(tzinfo=timezone.utc)
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid payment_date format: {str(e)}"
            )
    else:
        payment_datetime = datetime.now(timezone.utc)
    
    # Convert amount to cents
    amount_cents = int(round(amount * 100))
    
    # Create manual payment
    manual_payment = ManualPayment(
        org_id=org_id,
        client_id=client_uuid,
        amount_cents=amount_cents,
        currency="usd",
        payment_date=payment_datetime,
        description=description,
        payment_method=payment_method,
        receipt_url=receipt_url,
        created_by=current_user.id
    )
    
    db.add(manual_payment)
    db.commit()
    db.refresh(manual_payment)
    
    return {
        "id": str(manual_payment.id),
        "client_id": client_id,
        "amount_cents": manual_payment.amount_cents,
        "amount": manual_payment.amount_cents / 100.0,
        "currency": manual_payment.currency,
        "payment_date": manual_payment.payment_date.isoformat(),
        "description": manual_payment.description,
        "payment_method": manual_payment.payment_method,
        "receipt_url": manual_payment.receipt_url,
        "status": "succeeded",
        "type": "manual_payment"
    }


@router.delete("/{client_id}/manual-payment/{payment_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_manual_payment(
    client_id: str,
    payment_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Delete a manual payment transaction.
    Only manual payments can be deleted (not Stripe payments).
    """
    # Get selected org_id from user object (set by get_current_user)
    org_id = getattr(current_user, 'selected_org_id', current_user.org_id)
    
    try:
        client_uuid = UUID(client_id)
        payment_uuid = UUID(payment_id)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid client ID or payment ID format"
        )
    
    # Verify client exists and belongs to user's org (use selected org from token)
    client = db.query(Client).filter(
        Client.id == client_uuid,
        Client.org_id == org_id
    ).first()
    
    if not client:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Client not found"
        )
    
    # Find and delete the manual payment
    manual_payment = db.query(ManualPayment).filter(
        ManualPayment.id == payment_uuid,
        ManualPayment.client_id == client_uuid,
        ManualPayment.org_id == org_id
    ).first()
    
    if not manual_payment:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Manual payment not found"
        )
    
    db.delete(manual_payment)
    db.commit()
    
    return None


@router.patch("/{client_id}", response_model=ClientSchema)
def update_client(
    client_id: str,
    client_update: ClientUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    # CRITICAL: Filter by org_id for multi-tenant isolation
    client = db.query(Client).filter(
        Client.id == client_id,
        Client.org_id == current_user.org_id
    ).first()
    if not client:
        raise HTTPException(status_code=404, detail="Client not found")
    
    try:
        update_data = client_update.model_dump(exclude_unset=True)
        
        # Apply updates
        for field, value in update_data.items():
            setattr(client, field, value)
        
        # Handle program fields updates
        # If program fields are being cleared (set to None), clear all program-related fields
        if 'program_start_date' in update_data and update_data['program_start_date'] is None:
            # Clearing program - also clear related fields
            client.program_start_date = None
            client.program_duration_days = None
            client.program_end_date = None
            client.program_progress_percent = None
            print(f"[UPDATE_CLIENT] Cleared program fields for client {client.id}")
        elif 'program_end_date' in update_data and update_data['program_end_date'] is None:
            # Clearing program end date - also clear related fields
            client.program_end_date = None
            client.program_duration_days = None
            client.program_progress_percent = None
            print(f"[UPDATE_CLIENT] Cleared program end date for client {client.id}")
        elif 'program_duration_days' in update_data and update_data['program_duration_days'] is None:
            # Clearing program duration - also clear related fields
            client.program_duration_days = None
            client.program_end_date = None
            client.program_progress_percent = None
            print(f"[UPDATE_CLIENT] Cleared program duration for client {client.id}")
        elif 'program_start_date' in update_data or 'program_end_date' in update_data or 'program_duration_days' in update_data:
            # Program fields are being set/updated
            try:
                # If end_date is provided, calculate duration from start and end
                if 'program_end_date' in update_data and update_data['program_end_date']:
                    end_date = update_data['program_end_date']
                    if isinstance(end_date, str):
                        end_date = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
                    client.program_end_date = end_date
                    
                    # Calculate duration if start date exists
                    if client.program_start_date:
                        duration = (client.program_end_date - client.program_start_date).days
                        if duration > 0:
                            client.program_duration_days = duration
                        else:
                            print(f"[UPDATE_CLIENT] Warning: End date is before start date for client {client.id}")
                            client.program_end_date = None
                            client.program_duration_days = None
                
                # Update program dates (this handles duration -> end_date calculation if needed)
                client.update_program_dates()
                
                # Recalculate progress
                progress = client.calculate_progress()
                client.program_progress_percent = progress
            except Exception as e:
                import traceback
                print(f"[UPDATE_CLIENT] Error updating program dates/progress: {e}")
                traceback.print_exc()
                # Don't fail the update if progress calculation fails, but log it
                # Set progress to None if calculation fails
                if not client.program_start_date or not client.program_duration_days:
                    client.program_progress_percent = None
        
        # Handle explicit program_progress_percent updates
        if 'program_progress_percent' in update_data:
            client.program_progress_percent = update_data['program_progress_percent']
        
        db.commit()
        db.refresh(client)
        return client
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        import traceback
        traceback.print_exc()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error updating client: {str(e)}"
        )


@router.delete("/{client_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_client(
    client_id: str,
    delete_merged: bool = Query(False, description="If true and client is merged, delete all merged clients"),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Delete a client. If delete_merged is True and client has same email as others, delete all clients with that email.
    
    Before deleting, sets client_id to NULL in all related records (payments, subscriptions, events, etc.)
    to avoid foreign key constraint violations.
    """
    # CRITICAL: Filter by org_id for multi-tenant isolation
    client = db.query(Client).filter(
        Client.id == client_id,
        Client.org_id == current_user.org_id
    ).first()
    if not client:
        raise HTTPException(status_code=404, detail="Client not found")
    
    client_ids_to_delete = [client_id]
    
    # If delete_merged is True and client has an email, find and delete all clients with same email
    if delete_merged and client.email:
        import re
        normalized_email = re.sub(r'\s+', '', client.email.lower().strip())
        
        # Find all clients with the same email
        all_clients_with_email = db.query(Client).filter(
            and_(
                Client.org_id == current_user.org_id,
                Client.email.isnot(None)
            )
        ).all()
        
        # Filter clients with matching normalized email
        clients_with_same_email = [
            c for c in all_clients_with_email
            if c.email and re.sub(r'\s+', '', c.email.lower().strip()) == normalized_email
        ]
        
        if len(clients_with_same_email) > 1:
            client_ids_to_delete = [str(c.id) for c in clients_with_same_email]
            print(f"[DELETE_CLIENT] Deleting {len(clients_with_same_email)} clients with email '{normalized_email}': {client_ids_to_delete}")
    
    # Before deleting, set client_id to NULL in all related records to avoid foreign key violations
    from app.models.stripe_subscription import StripeSubscription
    from app.models.event import Event
    from app.models.funnel import Funnel
    from app.models.recommendation import Recommendation
    
    deleted_count = 0
    for cid in client_ids_to_delete:
        try:
            client_uuid = UUID(cid)
            client_to_delete = db.query(Client).filter(
                Client.id == client_uuid,
                Client.org_id == current_user.org_id
            ).first()
            
            if not client_to_delete:
                continue
            
            # Set client_id to NULL in related tables
            # Stripe Payments
            db.query(StripePayment).filter(
                StripePayment.client_id == client_uuid,
                StripePayment.org_id == current_user.org_id
            ).update({StripePayment.client_id: None}, synchronize_session=False)
            
            # Stripe Subscriptions
            db.query(StripeSubscription).filter(
                StripeSubscription.client_id == client_uuid,
                StripeSubscription.org_id == current_user.org_id
            ).update({StripeSubscription.client_id: None}, synchronize_session=False)
            
            # Events
            db.query(Event).filter(
                Event.client_id == client_uuid,
                Event.org_id == current_user.org_id
            ).update({Event.client_id: None}, synchronize_session=False)
            
            # Funnels
            db.query(Funnel).filter(
                Funnel.client_id == client_uuid,
                Funnel.org_id == current_user.org_id
            ).update({Funnel.client_id: None}, synchronize_session=False)
            
            # Recommendations
            db.query(Recommendation).filter(
                Recommendation.client_id == client_uuid,
                Recommendation.org_id == current_user.org_id
            ).update({Recommendation.client_id: None}, synchronize_session=False)
            
            # Now delete the client
            db.delete(client_to_delete)
            deleted_count += 1
            print(f"[DELETE_CLIENT] Set client_id to NULL in related records and deleted client {cid}")
            
        except ValueError:
            print(f"[DELETE_CLIENT] Invalid client ID: {cid}")
        except Exception as e:
            print(f"[DELETE_CLIENT] Error deleting client {cid}: {str(e)}")
            import traceback
            traceback.print_exc()
            db.rollback()
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Error deleting client: {str(e)}"
            )
    
    db.commit()
    print(f"[DELETE_CLIENT] Successfully deleted {deleted_count} client(s)")
    
    return None


@router.post("/automation/process", status_code=status.HTTP_200_OK)
def process_client_automation_endpoint(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Manually trigger client automation processing.
    Updates progress and lifecycle states for all clients with programs.
    This can also be called via a scheduled task/cron job.
    """
    try:
        result = process_client_automation(db, org_id=current_user.org_id)
        return {
            "success": True,
            "message": "Client automation processed successfully",
            **result
        }
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error processing automation: {str(e)}"
        )


# Check-in endpoints - MUST be before /{client_id}/check-ins to avoid route conflicts
@router.post("/check-ins/sync")
def sync_check_ins(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Sync calendar events (Cal.com/Calendly) with clients by matching attendee emails.
    Creates or updates check-in records for matching clients.
    """
    print(f"[CHECKIN SYNC API] ===== ENDPOINT CALLED =====")
    print(f"[CHECKIN SYNC API] User: {current_user.id}, Org: {current_user.org_id}")
    
    try:
        from app.services.checkin_sync import sync_all_checkins
        print(f"[CHECKIN SYNC API] ✅ Successfully imported sync_all_checkins")
    except ImportError as e:
        import traceback
        error_trace = traceback.format_exc()
        print(f"[CHECKIN SYNC API] ❌ Import error: {str(e)}")
        print(f"[CHECKIN SYNC API] Traceback:\n{error_trace}")
        traceback.print_exc()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to import checkin_sync service: {str(e)}"
        )
    
    try:
        print(f"[CHECKIN SYNC API] Calling sync_all_checkins...")
        results = sync_all_checkins(db, current_user.org_id, current_user.id)
        print(f"[CHECKIN SYNC API] ✅ Sync completed successfully")
        return {
            "success": True,
            "message": f"Synced {results['total']} check-ins",
            "calcom": results["calcom"],
            "calendly": results["calendly"],
            "total": results["total"]
        }
    except Exception as e:
        import traceback
        error_trace = traceback.format_exc()
        print(f"[CHECKIN SYNC API] ❌ Error: {str(e)}")
        print(f"[CHECKIN SYNC API] Traceback:\n{error_trace}")
        traceback.print_exc()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error syncing check-ins: {str(e)}"
        )


@router.get("/{client_id}/check-ins")
def get_client_check_ins(
    client_id: str,
    limit: int = Query(50, ge=1, le=100),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Get check-in history for a client, ordered by start_time (most recent first).
    """
    try:
        client_uuid = UUID(client_id)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid client ID format"
        )
    
    # Verify client exists and belongs to user's org
    client = db.query(Client).filter(
        Client.id == client_uuid,
        Client.org_id == current_user.org_id
    ).first()
    
    if not client:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Client not found"
        )
    
    # Get check-ins for this client
    check_ins = db.query(ClientCheckIn).filter(
        ClientCheckIn.client_id == client_uuid,
        ClientCheckIn.org_id == current_user.org_id
    ).order_by(desc(ClientCheckIn.start_time)).limit(limit).all()
    
    return [
        {
            "id": str(checkin.id),
            "event_id": checkin.event_id,
            "event_uri": checkin.event_uri,
            "provider": checkin.provider,
            "title": checkin.title,
            "start_time": checkin.start_time.isoformat() if checkin.start_time else None,
            "end_time": checkin.end_time.isoformat() if checkin.end_time else None,
            "location": checkin.location,
            "meeting_url": checkin.meeting_url,
            "attendee_email": checkin.attendee_email,
            "attendee_name": checkin.attendee_name,
            "completed": checkin.completed,
            "cancelled": checkin.cancelled,
            "created_at": checkin.created_at.isoformat() if checkin.created_at else None,
        }
        for checkin in check_ins
    ]


@router.patch("/check-ins/{check_in_id}")
def update_check_in(
    check_in_id: str,
    completed: Optional[bool] = Body(None),
    cancelled: Optional[bool] = Body(None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Update a check-in (mark as completed/cancelled or update other fields).
    Accepts JSON body with optional 'completed' and 'cancelled' boolean fields.
    """
    try:
        check_in_uuid = UUID(check_in_id)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid check-in ID format"
        )
    
    # Get check-in and verify it belongs to user's org
    check_in = db.query(ClientCheckIn).filter(
        and_(
            ClientCheckIn.id == check_in_uuid,
            ClientCheckIn.org_id == current_user.org_id
        )
    ).first()
    
    if not check_in:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Check-in not found"
        )
    
    # Update fields
    if completed is not None:
        check_in.completed = completed
    if cancelled is not None:
        check_in.cancelled = cancelled
    
    check_in.updated_at = datetime.now(timezone.utc)
    
    try:
        db.commit()
        db.refresh(check_in)
        
        return {
            "id": str(check_in.id),
            "event_id": check_in.event_id,
            "event_uri": check_in.event_uri,
            "provider": check_in.provider,
            "title": check_in.title,
            "start_time": check_in.start_time.isoformat() if check_in.start_time else None,
            "end_time": check_in.end_time.isoformat() if check_in.end_time else None,
            "location": check_in.location,
            "meeting_url": check_in.meeting_url,
            "attendee_email": check_in.attendee_email,
            "attendee_name": check_in.attendee_name,
            "completed": check_in.completed,
            "cancelled": check_in.cancelled,
            "created_at": check_in.created_at.isoformat() if check_in.created_at else None,
        }
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to update check-in: {str(e)}"
        )


@router.delete("/check-ins/{check_in_id}")
def delete_check_in(
    check_in_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Delete a check-in.
    """
    try:
        check_in_uuid = UUID(check_in_id)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid check-in ID format"
        )
    
    # Get check-in and verify it belongs to user's org
    check_in = db.query(ClientCheckIn).filter(
        and_(
            ClientCheckIn.id == check_in_uuid,
            ClientCheckIn.org_id == current_user.org_id
        )
    ).first()
    
    if not check_in:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Check-in not found"
        )
    
    try:
        db.delete(check_in)
        db.commit()
        return {"success": True, "message": "Check-in deleted successfully"}
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to delete check-in: {str(e)}"
        )


@router.post("/{client_id}/check-ins")
def create_manual_check_in(
    client_id: str,
    title: str = Body(...),
    start_time: str = Body(...),
    end_time: Optional[str] = Body(None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Create a manual check-in for a client (for days without calendar bookings).
    """
    try:
        client_uuid = UUID(client_id)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid client ID format"
        )
    
    # Verify client exists and belongs to user's org
    client = db.query(Client).filter(
        Client.id == client_uuid,
        Client.org_id == current_user.org_id
    ).first()
    
    if not client:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Client not found"
        )
    
    # Parse datetime strings
    try:
        start_datetime = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
        end_datetime = datetime.fromisoformat(end_time.replace('Z', '+00:00')) if end_time else None
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid datetime format: {str(e)}"
        )
    
    # Create manual check-in
    manual_check_in = ClientCheckIn(
        org_id=current_user.org_id,
        client_id=client_uuid,
        event_id=f"manual_{uuid.uuid4()}",  # Generate unique ID for manual check-ins
        provider="manual",
        title=title,
        start_time=start_datetime,
        end_time=end_datetime,
        attendee_email=client.email or "",
        attendee_name=f"{client.first_name} {client.last_name}".strip(),
        completed=False,
        cancelled=False
    )
    
    try:
        db.add(manual_check_in)
        db.commit()
        db.refresh(manual_check_in)
        
        return {
            "id": str(manual_check_in.id),
            "event_id": manual_check_in.event_id,
            "event_uri": manual_check_in.event_uri,
            "provider": manual_check_in.provider,
            "title": manual_check_in.title,
            "start_time": manual_check_in.start_time.isoformat() if manual_check_in.start_time else None,
            "end_time": manual_check_in.end_time.isoformat() if manual_check_in.end_time else None,
            "location": manual_check_in.location,
            "meeting_url": manual_check_in.meeting_url,
            "attendee_email": manual_check_in.attendee_email,
            "attendee_name": manual_check_in.attendee_name,
            "completed": manual_check_in.completed,
            "cancelled": manual_check_in.cancelled,
            "created_at": manual_check_in.created_at.isoformat() if manual_check_in.created_at else None,
        }
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create check-in: {str(e)}"
        )


@router.get("/{client_id}/check-ins/next")
def get_next_check_in(
    client_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Get the next upcoming check-in for a client.
    Returns None if no upcoming check-ins.
    """
    try:
        client_uuid = UUID(client_id)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid client ID format"
        )
    
    # Verify client exists and belongs to user's org
    client = db.query(Client).filter(
        Client.id == client_uuid,
        Client.org_id == current_user.org_id
    ).first()
    
    if not client:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Client not found"
        )
    
    # Get next upcoming check-in (not completed, not cancelled, in the future)
    now = datetime.now(timezone.utc)
    next_checkin = db.query(ClientCheckIn).filter(
        ClientCheckIn.client_id == client_uuid,
        ClientCheckIn.org_id == current_user.org_id,
        ClientCheckIn.completed == False,
        ClientCheckIn.cancelled == False,
        ClientCheckIn.start_time > now
    ).order_by(ClientCheckIn.start_time).first()
    
    if not next_checkin:
        return None
    
    return {
        "id": str(next_checkin.id),
        "event_id": next_checkin.event_id,
        "event_uri": next_checkin.event_uri,
        "provider": next_checkin.provider,
        "title": next_checkin.title,
        "start_time": next_checkin.start_time.isoformat() if next_checkin.start_time else None,
        "end_time": next_checkin.end_time.isoformat() if next_checkin.end_time else None,
        "location": next_checkin.location,
        "meeting_url": next_checkin.meeting_url,
        "attendee_email": next_checkin.attendee_email,
        "attendee_name": next_checkin.attendee_name,
    }


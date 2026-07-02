"""Clients API — payments routes."""
from __future__ import annotations

import logging
import re
import uuid
from datetime import datetime, timedelta, timezone
from threading import Lock as ThreadingLock
from typing import List, Optional, Tuple
from uuid import UUID

import httpx
from fastapi import APIRouter, BackgroundTasks, Body, Depends, HTTPException, Query, Request, status
from fastapi.security import HTTPAuthorizationCredentials
from sqlalchemy import and_, desc, func, or_
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, defer
from sqlalchemy.orm.attributes import flag_modified
from starlette.concurrency import run_in_threadpool

from app.api.deps import get_current_user, security
from app.api.clients.helpers import (
    LOG,
    WHOP_PAID_STATUSES,
    effective_org_id,
    merge_client_meta_from_duplicates,
    normalize_email,
    client_created_sort_key,
    load_whop_payments,
    org_checkin_sync_lock,
    refresh_call_insights_after_checkin_sync,
    scope_org_id,
    sync_check_ins_in_worker,
    user_pipeline_priorities,
    brevo_merged_stats_for_client,
    fetch_brevo_email_stats,
    merge_brevo_stats,
)
from app.core.config import settings
from app.core.rate_limit import check_sliding_window
from app.db.session import get_db, SessionLocal
from app.long_jobs import schedule_background_work
from app.models.calendar_booking_sales import CalendarBookingSales
from app.models.client import Client, LifecycleState
from app.models.client_checkin import ClientCheckIn
from app.models.manual_payment import ManualPayment
from app.models.organization import Organization
from app.models.stripe_payment import StripePayment
from app.models.stripe_subscription import StripeSubscription
from app.models.stripe_treasury_transaction import StripeTreasuryTransaction, TreasuryTransactionStatus
from app.models.user import User
from app.models.whop_payment import WhopPayment
from app.utils.stripe_helpers import extract_email_from_payment_raw
from app.utils.stripe_ids import normalize_stripe_id_for_dedup
from app.services.terminal_metrics_service import invalidate_terminal_monthly_trends_cache

router = APIRouter()


from app.models.stripe_payment import StripePayment
from app.models.manual_payment import ManualPayment


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
            Client.org_id == org_id
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
                    Client.org_id == org_id
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
                Client.org_id == org_id,
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
    # Collect all emails from all merged clients (primary email + emails list)
    import re
    all_merged_emails = set()
    for merged_client in merged_clients_list:
        all_merged_emails |= merged_client.get_all_emails_normalized()
    
    if all_merged_emails:
        print(f"[CLIENT_PAYMENTS] Checking payments for {len(all_merged_emails)} unique emails from merged clients: {list(all_merged_emails)}")
        
        # Fetch ALL StripePayments (not just unlinked ones) to check for email matches
        # This handles cases where payment is linked to wrong client but email matches merged clients
        all_stripe_payments = db.query(StripePayment).filter(
            StripePayment.org_id == org_id
        ).all()
        
        # Track which payments we've already added (by ID) to avoid duplicates
        added_payment_ids = {p.id for p in all_payments}
        
        for payment in all_stripe_payments:
            # Skip if already added (from client_id matching)
            if payment.id in added_payment_ids:
                continue
            
            # Check if customer email matches in raw_event (Charge, PaymentIntent, Invoice, or webhook format)
            customer_email = extract_email_from_payment_raw(payment.raw_event) if payment.raw_event else None

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
            
            norm_stripe = normalize_stripe_id_for_dedup(stripe_id) if stripe_id else None
            if norm_stripe and norm_stripe in seen_stripe_ids:
                print(f"[CLIENT_PAYMENTS] Skipping exact duplicate payment {stripe_id} (same normalized id)")
                continue
            if norm_stripe:
                seen_stripe_ids.add(norm_stripe)
            
            if payment_status != 'succeeded':
                deduplicated.append(payment)
                continue
            
            subscription_id = getattr(payment, 'subscription_id', None)
            invoice_id = getattr(payment, 'invoice_id', None)
            
            if subscription_id and invoice_id:
                key = ('subscription_invoice', normalize_stripe_id_for_dedup(subscription_id), normalize_stripe_id_for_dedup(invoice_id))
            elif invoice_id:
                key = ('invoice', normalize_stripe_id_for_dedup(invoice_id))
            elif subscription_id:
                key = ('subscription', normalize_stripe_id_for_dedup(subscription_id))
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

    try:
        from app.services.client_automation import apply_automatic_lifecycle_for_client

        apply_automatic_lifecycle_for_client(db, client)
        db.commit()
        db.refresh(client)
    except Exception as lc_err:
        print(f"[MANUAL PAYMENT] lifecycle update skipped for {client.id}: {lc_err}")

    invalidate_terminal_monthly_trends_cache(org_id)

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


@router.patch("/{client_id}/manual-payment/{payment_id}")
def update_manual_payment(
    client_id: str,
    payment_id: str,
    amount: float = Query(..., ge=0.01, description="Payment amount in dollars"),
    payment_date: Optional[str] = Query(None, description="Payment date (ISO format). Defaults to existing."),
    description: Optional[str] = Query(None, description="Payment description/notes"),
    payment_method: Optional[str] = Query(None, description="Payment method (e.g., cash, check, bank_transfer)"),
    receipt_url: Optional[str] = Query(None, description="Optional receipt/document URL"),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Update a manual payment transaction."""
    from datetime import timezone

    org_id = getattr(current_user, "selected_org_id", current_user.org_id)

    try:
        client_uuid = UUID(client_id)
        payment_uuid = UUID(payment_id)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid client ID or payment ID format",
        )

    client = db.query(Client).filter(
        Client.id == client_uuid,
        Client.org_id == org_id,
    ).first()
    if not client:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Client not found")

    manual_payment = db.query(ManualPayment).filter(
        ManualPayment.id == payment_uuid,
        ManualPayment.client_id == client_uuid,
        ManualPayment.org_id == org_id,
    ).first()
    if not manual_payment:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Manual payment not found")

    if payment_date:
        try:
            if payment_date.endswith("Z"):
                payment_date = payment_date.replace("Z", "+00:00")
            payment_datetime = datetime.fromisoformat(payment_date)
            if payment_datetime.tzinfo is None:
                payment_datetime = payment_datetime.replace(tzinfo=timezone.utc)
            manual_payment.payment_date = payment_datetime
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid payment_date format: {str(e)}",
            )

    manual_payment.amount_cents = int(round(amount * 100))
    if description is not None:
        manual_payment.description = description or None
    if payment_method is not None:
        manual_payment.payment_method = payment_method or None
    if receipt_url is not None:
        manual_payment.receipt_url = receipt_url or None

    db.commit()
    db.refresh(manual_payment)
    invalidate_terminal_monthly_trends_cache(org_id)

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
        "type": "manual_payment",
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
    invalidate_terminal_monthly_trends_cache(org_id)
    
    return None


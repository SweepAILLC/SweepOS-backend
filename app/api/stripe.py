"""
Enhanced Stripe Analytics API - uses database data from webhook-processed events.
Provides KPIs, revenue timeline, subscriptions, payments, and failed payments queue.
"""
from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.orm import Session
from sqlalchemy import func, and_, or_, desc, select
from typing import List, Optional
from datetime import datetime, timedelta
from collections import defaultdict
import time
from decimal import Decimal
import uuid

from app.db.session import get_db
from app.api.deps import get_current_user
from app.models.user import User
from app.models.oauth_token import OAuthToken, OAuthProvider
from app.models.stripe_payment import StripePayment
from app.models.stripe_subscription import StripeSubscription
from app.models.client import Client
from app.models.recommendation import Recommendation
from app.schemas.stripe import (
    StripeSummaryResponse,
    StripeConnectionStatus,
    StripeKPIsResponse,
    StripeRevenueTimelineResponse,
    StripeSubscriptionResponse,
    StripePaymentResponse,
    StripeFailedPaymentResponse,
    StripeClientRevenueResponse,
    StripeChurnResponse,
    StripeTopCustomersResponse,
    RevenueTimelinePoint,
    MRRTrendResponse,
    MRRTrendPoint,
)

router = APIRouter()


def check_stripe_connected(db: Session, org_id: uuid.UUID) -> bool:
    """Check if Stripe is connected via OAuth for a specific org"""
    oauth_token = db.query(OAuthToken).filter(
        OAuthToken.provider == OAuthProvider.STRIPE,
        OAuthToken.org_id == org_id
    ).first()
    
    if not oauth_token:
        return False
    
    if oauth_token.expires_at and oauth_token.expires_at < datetime.utcnow():
        return False
    
    return True


@router.get("/status", response_model=StripeConnectionStatus)
def get_stripe_connection_status(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Check if Stripe is connected via OAuth."""
    # CRITICAL: Filter by org_id for multi-tenant isolation
    oauth_token = db.query(OAuthToken).filter(
        OAuthToken.provider == OAuthProvider.STRIPE,
        OAuthToken.org_id == current_user.org_id
    ).first()
    
    if oauth_token and oauth_token.access_token:
        return StripeConnectionStatus(
            connected=True,
            message="Stripe is connected.",
            account_id=oauth_token.account_id
        )
    return StripeConnectionStatus(connected=False, message="Stripe is not connected.")


@router.get("/debug", status_code=status.HTTP_200_OK)
def debug_stripe_data(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Diagnostic endpoint to check what Stripe data is stored in the database.
    """
    org_id = current_user.org_id
    
    # Count clients
    total_clients = db.query(Client).filter(Client.org_id == org_id).count()
    clients_with_stripe = db.query(Client).filter(
        Client.org_id == org_id,
        Client.stripe_customer_id.isnot(None)
    ).count()
    
    # Count subscriptions
    total_subs = db.query(StripeSubscription).filter(StripeSubscription.org_id == org_id).count()
    active_subs = db.query(StripeSubscription).filter(
        StripeSubscription.org_id == org_id,
        StripeSubscription.status == "active"
    ).count()
    
    # Count payments
    total_payments = db.query(StripePayment).filter(StripePayment.org_id == org_id).count()
    
    # Get sample data
    sample_clients = db.query(Client).filter(
        Client.org_id == org_id,
        Client.stripe_customer_id.isnot(None)
    ).limit(5).all()
    
    sample_subs = db.query(StripeSubscription).filter(
        StripeSubscription.org_id == org_id
    ).order_by(StripeSubscription.created_at.desc()).limit(5).all()
    
    return {
        "org_id": str(org_id),
        "summary": {
            "total_clients": total_clients,
            "clients_with_stripe_id": clients_with_stripe,
            "total_subscriptions": total_subs,
            "active_subscriptions": active_subs,
            "total_payments": total_payments,
        },
        "sample_clients": [
            {
                "id": str(c.id),
                "name": f"{c.first_name} {c.last_name}",
                "email": c.email,
                "stripe_customer_id": c.stripe_customer_id,
                "lifecycle_state": c.lifecycle_state,
            }
            for c in sample_clients
        ],
        "sample_subscriptions": [
            {
                "id": str(s.id),
                "stripe_subscription_id": s.stripe_subscription_id,
                "status": s.status,
                "mrr": float(s.mrr) if s.mrr else 0.0,
                "client_id": str(s.client_id) if s.client_id else None,
            }
            for s in sample_subs
        ]
    }


@router.post("/sync", status_code=status.HTTP_200_OK)
def sync_stripe_data(
    force_full: bool = Query(False, description="Force full historical sync (only needed on first connect)"),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Unified sync endpoint: Incremental cursor-based sync with idempotent upserts.
    
    Features:
    - Initial historical backfill on first connect (force_full=True)
    - Incremental sync: Only fetches objects updated since last sync (minus buffer)
    - Idempotent upserts: Prevents duplicates using unique constraints
    - Updates last_sync_at timestamp
    - Never refetches full history after initial backfill
    
    Args:
        force_full: If True, performs full historical sync (only needed on first connect)
    """
    if not check_stripe_connected(db, current_user.org_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Stripe not connected. Please connect Stripe via OAuth first."
        )
    
    from app.services.stripe_sync_v2 import sync_stripe_incremental
    
    try:
        print(f"[API] Sync requested by user {current_user.id} for org {current_user.org_id} (force_full={force_full})")
        sync_result = sync_stripe_incremental(db, org_id=current_user.org_id, force_full=force_full)
        
        if sync_result.get("error"):
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=sync_result.get("error")
            )
        
        response_data = {
            "success": True,
            "message": "Stripe data synced successfully",
            "is_full_sync": sync_result.get("is_full_sync", False),
            "results": {
                "customers_synced": sync_result.get("customers_synced", 0),
                "customers_updated": sync_result.get("customers_updated", 0),
                "subscriptions_synced": sync_result.get("subscriptions_synced", 0),
                "subscriptions_updated": sync_result.get("subscriptions_updated", 0),
                "payments_synced": sync_result.get("payments_synced", 0),
                "payments_updated": sync_result.get("payments_updated", 0),
            }
        }
        
        return response_data
    except HTTPException:
        # Re-raise HTTP exceptions as-is
        raise
    except Exception as e:
        import traceback
        error_detail = str(e)
        print(f"[API] ❌ Error syncing Stripe data: {error_detail}")
        print(traceback.format_exc())
        
        # Provide more helpful error messages
        if "decrypt" in error_detail.lower() or "encryption" in error_detail.lower() or "InvalidToken" in error_detail:
            error_detail = "Encryption key mismatch. Your OAuth tokens were encrypted with a different key. Please set ENCRYPTION_KEY in your .env file and reconnect Stripe. See README.md for instructions."
        
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=error_detail
        )


@router.post("/reconcile", status_code=status.HTTP_200_OK)
def reconcile_stripe_data(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Manual reconciliation: Recompute derived analytics from existing data.
    
    This doesn't refetch from Stripe - it recalculates:
    - Client lifetime revenue from all succeeded payments
    - Other derived metrics
    
    Use this if you suspect data inconsistencies.
    """
    if not check_stripe_connected(db, current_user.org_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Stripe not connected."
        )
    
    from app.services.stripe_sync_v2 import reconcile_stripe_data
    
    try:
        print(f"[API] Reconciliation requested by user {current_user.id} for org {current_user.org_id}")
        result = reconcile_stripe_data(db, org_id=current_user.org_id)
        
        return {
            "success": True,
            "message": "Reconciliation complete",
            "clients_reconciled": result.get("clients_reconciled", 0),
            "revenue_recalculated": result.get("revenue_recalculated", 0)
        }
    except Exception as e:
        import traceback
        print(f"[API] ❌ Reconciliation error: {str(e)}")
        print(traceback.format_exc())
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to reconcile Stripe data: {str(e)}"
        )


@router.get("/summary", response_model=StripeSummaryResponse)
def get_stripe_summary(
    range_days: int = Query(30, alias="range", ge=1, le=365),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Get comprehensive Stripe financial summary with KPIs.
    Uses database data from webhook-processed events.
    """
    if not check_stripe_connected(db, current_user.org_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Stripe not connected. Please connect Stripe via OAuth first."
        )
    
    # CRITICAL: All queries must filter by org_id for multi-tenant isolation
    org_id = current_user.org_id
    
    # Calculate date range
    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=range_days)
    prev_start_date = start_date - timedelta(days=range_days)
    
    # Get current MRR (from active and trialing subscriptions)
    # Include both "active" and "trialing" status subscriptions
    active_subs = db.query(StripeSubscription).filter(
        StripeSubscription.status.in_(["active", "trialing"]),
        StripeSubscription.org_id == org_id
    ).all()
    
    # Calculate MRR manually from subscriptions - sum all MRR values
    # If MRR is 0 or None, try to calculate from raw JSON data
    manual_mrr = 0.0
    import json
    from decimal import Decimal
    
    for sub in active_subs:
        mrr_value = 0.0
        
        # First try the stored MRR value
        if sub.mrr is not None:
            try:
                mrr_value = float(sub.mrr) if hasattr(sub.mrr, '__float__') else float(str(sub.mrr))
            except:
                mrr_value = 0.0
        
        # If MRR is 0, try to calculate from raw JSON
        if mrr_value == 0.0 and sub.raw:
            try:
                raw_data = sub.raw if isinstance(sub.raw, dict) else json.loads(sub.raw) if isinstance(sub.raw, str) else {}
                items = raw_data.get('items', {}).get('data', [])
                
                if items:
                    sub_mrr = Decimal('0')
                    for item in items:
                        price = item.get('price', {})
                        amount_cents = Decimal(str(price.get('unit_amount', 0) or 0))
                        quantity = Decimal(str(item.get('quantity', 1) or 1))
                        recurring = price.get('recurring', {})
                        interval = recurring.get('interval', 'month') if recurring else 'month'
                        
                        # Convert cents to dollars
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
                        
                        sub_mrr += item_mrr
                    
                    mrr_value = float(sub_mrr)
                    print(f"[DEBUG] Calculated MRR from raw JSON for {sub.stripe_subscription_id}: ${mrr_value:.2f}")
                    
                    # Update the subscription MRR if it was 0
                    if sub.mrr == 0 or sub.mrr is None:
                        sub.mrr = mrr_value
                        db.flush()
            except Exception as e:
                print(f"[DEBUG] Error calculating MRR from raw JSON for {sub.stripe_subscription_id}: {str(e)}")
        
        manual_mrr += mrr_value
        print(f"[DEBUG] Sub {sub.stripe_subscription_id}: status={sub.status}, mrr={mrr_value} (stored: {sub.mrr}, type: {type(sub.mrr)})")
    
    # Commit any MRR updates
    db.commit()
    
    # Also try SQL sum for comparison
    current_mrr_result = db.query(func.sum(StripeSubscription.mrr)).filter(
        and_(
            StripeSubscription.status.in_(["active", "trialing"]),
            StripeSubscription.org_id == org_id
        )
    ).scalar()
    
    # Use manual calculation if SQL sum returns None or 0
    if current_mrr_result is None:
        current_mrr = manual_mrr
    else:
        current_mrr = float(current_mrr_result) if hasattr(current_mrr_result, '__float__') else float(str(current_mrr_result))
        # If SQL sum is 0 but we have subscriptions with MRR, use manual calculation
        if current_mrr == 0.0 and len(active_subs) > 0 and manual_mrr > 0:
            current_mrr = manual_mrr
    
    print(f"[DEBUG] Active/trialing subscriptions count: {len(active_subs)}")
    print(f"[DEBUG] Manual MRR calculation: ${manual_mrr:.2f}")
    print(f"[DEBUG] SQL sum MRR result: {current_mrr_result}, type: {type(current_mrr_result)}")
    print(f"[DEBUG] Final MRR: ${current_mrr:.2f}")
    
    # Get previous period MRR for comparison
    prev_mrr_result = db.query(func.sum(StripeSubscription.mrr)).filter(
        and_(
            StripeSubscription.status.in_(["active", "trialing"]),
            StripeSubscription.org_id == org_id,
            StripeSubscription.updated_at >= prev_start_date,
            StripeSubscription.updated_at < start_date
        )
    ).scalar() or Decimal(0)
    prev_mrr = float(prev_mrr_result)
    mrr_change = current_mrr - prev_mrr
    mrr_change_percent = (mrr_change / prev_mrr * 100) if prev_mrr > 0 else 0
    
    # Calculate ARR
    arr = current_mrr * 12
    
    # Count new subscriptions in period
    new_subscriptions = db.query(func.count(StripeSubscription.id)).filter(
        and_(
            StripeSubscription.org_id == org_id,
            StripeSubscription.created_at >= start_date,
            StripeSubscription.created_at <= end_date
        )
    ).scalar() or 0
    
    # Count churned subscriptions in period
    # Churn = canceled subscription where customer has NO new active subscription (no upsell)
    canceled_subs = db.query(StripeSubscription).filter(
        and_(
            StripeSubscription.org_id == org_id,
            StripeSubscription.status == "canceled",
            StripeSubscription.updated_at >= start_date,
            StripeSubscription.updated_at <= end_date
        )
    ).all()
    
    churned_subscriptions = 0
    for canceled_sub in canceled_subs:
        # Check if customer has a new active subscription created after cancellation
        # This would indicate an upsell/replacement, not a true churn
        has_replacement = db.query(StripeSubscription).filter(
            and_(
                StripeSubscription.org_id == org_id,
                StripeSubscription.client_id == canceled_sub.client_id,
                StripeSubscription.status == "active",
                StripeSubscription.created_at > canceled_sub.updated_at
            )
        ).first()
        
        # Only count as churn if no replacement subscription exists
        if not has_replacement:
            churned_subscriptions += 1
    
    # Count unique failed payments in period (grouped by subscription_id + client_id)
    # This avoids counting duplicate retry attempts as separate failures
    failed_payment_records = db.query(StripePayment).filter(
        and_(
            StripePayment.org_id == org_id,
            StripePayment.status == "failed",
            StripePayment.created_at >= start_date,
            StripePayment.created_at <= end_date
        )
    ).all()
    
    # Group by subscription_id + client_id to count unique failures
    unique_failures = set()
    for payment in failed_payment_records:
        # Use subscription_id if available, otherwise use client_id only
        group_key = (payment.subscription_id, payment.client_id)
        unique_failures.add(group_key)
    
    failed_payments = len(unique_failures)
    
    # Define deduplication function - used by both revenue calculation and recent payments
    # This ensures revenue matches exactly what users see in the recent payments table
    def deduplicate_payments(payments_list):
        """Deduplicate payments using same logic as recent payments table"""
        seen = set()
        deduplicated = []
        
        # Sort: prefer charge over payment_intent over invoice, then by created_at (most recent first)
        # Also prefer payments with subscription_id over those without (for same invoice_id)
        # This matches the exact sorting used in recent payments
        payments_list.sort(key=lambda p: (
            0 if p.type == 'charge' else (1 if p.type == 'payment_intent' else 2),  # Charges first, then payment_intents, then invoices
            0 if p.subscription_id else 1,  # Payments with subscription_id first
            -(p.created_at.timestamp() if p.created_at else 0)  # Most recent first
        ))
        
        # Track invoice_ids that have been seen with subscription_id
        # If we see an invoice_id with subscription_id, we should skip it if we see it again without subscription_id
        invoice_ids_with_sub = set()
        
        # Track payments without invoice/subscription by (amount, client_id, time_window)
        # to catch payment_intent/charge duplicates
        standalone_payments_seen = {}  # key: (amount_cents, client_id, time_bucket) -> stripe_id
        
        for payment in payments_list:
            # Create deduplication key - same logic as recent payments
            if payment.subscription_id and payment.invoice_id:
                key = (payment.subscription_id, payment.invoice_id)
                # Mark this invoice_id as having a subscription_id
                invoice_ids_with_sub.add(payment.invoice_id)
            elif payment.invoice_id:
                # If this invoice_id was already seen with a subscription_id, skip this one
                if payment.invoice_id in invoice_ids_with_sub:
                    print(f"[DEBUG] Skipping payment {payment.stripe_id} with invoice_id {payment.invoice_id} (already have one with subscription_id)")
                    continue
                key = (None, payment.invoice_id)
            else:
                # No subscription or invoice - need to check for payment_intent/charge duplicates
                # Group by amount, client_id, and time window (within 30 seconds)
                if payment.created_at:
                    # Round to nearest 30 seconds to group nearby payments
                    time_bucket = int(payment.created_at.timestamp() / 30) * 30
                    standalone_key = (payment.amount_cents, payment.client_id, time_bucket)
                    
                    # Check if we've seen a payment with same amount, client, and time window
                    if standalone_key in standalone_payments_seen:
                        existing_id = standalone_payments_seen[standalone_key]
                        print(f"[DEBUG] Skipping duplicate standalone payment {payment.stripe_id} (type: {payment.type}) - matches {existing_id} (same amount ${payment.amount_cents/100:.2f}, client, time window)")
                        continue
                    
                    # Mark this payment as seen
                    standalone_payments_seen[standalone_key] = payment.stripe_id
                
                # Use stripe_id as key for payments without invoice/subscription
                key = payment.stripe_id
            
            if key not in seen:
                seen.add(key)
                deduplicated.append(payment)
            else:
                print(f"[DEBUG] Skipping duplicate payment {payment.stripe_id} with key {key}")
        
        return deduplicated
    
    # Calculate revenue using EXACT same query and logic as recent payments table
    # For time-filtered revenue, we need to filter FIRST, then deduplicate
    # This ensures payment_intent/charge pairs are correctly handled within the time range
    
    # Get ALL succeeded payments (for recent payments table - no date filter)
    all_succeeded_payments = db.query(StripePayment).filter(
        and_(
            StripePayment.org_id == org_id,
            StripePayment.status == "succeeded"
        )
    ).order_by(desc(StripePayment.created_at)).all()
    
    # Deduplicate ALL payments (for recent payments table and total revenue)
    deduplicated_all_payments = deduplicate_payments(all_succeeded_payments)
    
    # Calculate revenue from ALL deduplicated payments (same set that recent payments uses)
    total_revenue = sum(p.amount_cents for p in deduplicated_all_payments) / 100.0
    
    # For period revenue, filter FIRST by date range, then deduplicate
    # This ensures payment_intent/charge pairs are correctly deduplicated within the time window
    payments_in_range = [
        p for p in all_succeeded_payments
        if p.created_at and p.created_at >= start_date and p.created_at <= end_date
    ]
    
    # Deduplicate the filtered payments
    deduplicated_payments_in_range = deduplicate_payments(payments_in_range)
    
    # Calculate period revenue from deduplicated, filtered payments
    revenue = sum(p.amount_cents for p in deduplicated_payments_in_range) / 100.0
    
    print(f"[DEBUG] Revenue calculation (using EXACT same payments as recent payments table):")
    print(f"  - All succeeded payments: {len(all_succeeded_payments)} total")
    print(f"  - After deduplication: {len(deduplicated_all_payments)} (same as recent payments uses)")
    print(f"  - In date range ({start_date} to {end_date}): {len(deduplicated_payments_in_range)}")
    print(f"  - Revenue (period): ${revenue:.2f}")
    print(f"  - Total revenue (all deduplicated): ${total_revenue:.2f}")
    
    # Debug: Print each payment in deduplicated set with details
    print(f"[DEBUG] Deduplicated payments breakdown (first 20):")
    for i, p in enumerate(deduplicated_all_payments[:20]):
        print(f"  {i+1}. ID: {p.stripe_id}, Amount: ${(p.amount_cents or 0)/100:.2f}, Type: {p.type}, Sub: {p.subscription_id}, Invoice: {p.invoice_id}, Created: {p.created_at}")
    
    # Debug: Check for potential duplicates by invoice_id
    invoice_ids_seen = {}
    for p in deduplicated_all_payments:
        if p.invoice_id:
            if p.invoice_id in invoice_ids_seen:
                print(f"[WARNING] Duplicate invoice_id {p.invoice_id}: existing={invoice_ids_seen[p.invoice_id]}, new={p.stripe_id}")
            else:
                invoice_ids_seen[p.invoice_id] = p.stripe_id
    
    # Debug: Print each payment in deduplicated set with details
    print(f"[DEBUG] Deduplicated payments breakdown:")
    for i, p in enumerate(deduplicated_all_payments[:20]):  # Print first 20
        print(f"  {i+1}. ID: {p.stripe_id}, Amount: ${(p.amount_cents or 0)/100:.2f}, Type: {p.type}, Sub: {p.subscription_id}, Invoice: {p.invoice_id}, Created: {p.created_at}")
    
    # Debug: Check for potential duplicates by invoice_id
    invoice_ids_seen = {}
    duplicate_invoices = []
    for p in deduplicated_all_payments:
        if p.invoice_id:
            if p.invoice_id in invoice_ids_seen:
                duplicate_invoices.append((p.invoice_id, invoice_ids_seen[p.invoice_id], p.stripe_id))
                print(f"[WARNING] Duplicate invoice_id {p.invoice_id}: existing={invoice_ids_seen[p.invoice_id]}, new={p.stripe_id}")
            else:
                invoice_ids_seen[p.invoice_id] = p.stripe_id
    
    if duplicate_invoices:
        print(f"[ERROR] Found {len(duplicate_invoices)} duplicate invoices in deduplicated set!")
        for inv_id, existing_id, new_id in duplicate_invoices:
            existing_p = next((p for p in deduplicated_all_payments if p.stripe_id == existing_id), None)
            new_p = next((p for p in deduplicated_all_payments if p.stripe_id == new_id), None)
            if existing_p and new_p:
                print(f"  Invoice {inv_id}: existing amount=${(existing_p.amount_cents or 0)/100:.2f}, new amount=${(new_p.amount_cents or 0)/100:.2f}")
    
    # Debug: Check for potential duplicates by (subscription_id, invoice_id) pair
    sub_invoice_pairs_seen = {}
    duplicate_pairs = []
    for p in deduplicated_all_payments:
        if p.subscription_id and p.invoice_id:
            key = (p.subscription_id, p.invoice_id)
            if key in sub_invoice_pairs_seen:
                duplicate_pairs.append((key, sub_invoice_pairs_seen[key], p.stripe_id))
                print(f"[WARNING] Duplicate (sub, invoice) {key}: existing={sub_invoice_pairs_seen[key]}, new={p.stripe_id}")
            else:
                sub_invoice_pairs_seen[key] = p.stripe_id
    
    if duplicate_pairs:
        print(f"[ERROR] Found {len(duplicate_pairs)} duplicate (sub, invoice) pairs in deduplicated set!")
    
    # Get active subscriptions count (include trialing)
    active_subscriptions = db.query(func.count(StripeSubscription.id)).filter(
        and_(
            StripeSubscription.org_id == org_id,
            StripeSubscription.status.in_(["active", "trialing"])
        )
    ).scalar() or 0
    
    # Get total customers (unique clients with Stripe customer ID)
    total_customers = db.query(func.count(func.distinct(Client.id))).filter(
        and_(
            Client.org_id == org_id,
            Client.stripe_customer_id.isnot(None)
        )
    ).scalar() or 0
    
    # Calculate average client LTV (average total spend of all customers)
    # Get all customers with revenue data
    customers_with_revenue = db.query(Client).filter(
        and_(
            Client.org_id == org_id,
            Client.stripe_customer_id.isnot(None)
        )
    ).all()
    
    if customers_with_revenue:
        total_lifetime_revenue = sum(
            (client.lifetime_revenue_cents or 0) / 100.0 
            for client in customers_with_revenue
        )
        average_client_ltv = total_lifetime_revenue / len(customers_with_revenue)
    else:
        average_client_ltv = 0.0
    
    # Get recent subscriptions, invoices, customers, payments for backward compatibility
    from app.schemas.stripe import StripeSubscription as StripeSubscriptionSchema, StripePayment as StripePaymentSchema, StripeInvoice as StripeInvoiceSchema, StripeCustomer as StripeCustomerSchema
    
    # Debug: Check total counts
    total_payments_count = db.query(func.count(StripePayment.id)).filter(
        StripePayment.org_id == org_id
    ).scalar() or 0
    total_subscriptions_count = db.query(func.count(StripeSubscription.id)).filter(
        StripeSubscription.org_id == org_id
    ).scalar() or 0
    active_subs_with_mrr = db.query(StripeSubscription).filter(
        and_(
            StripeSubscription.org_id == org_id,
            StripeSubscription.status.in_(["active", "trialing"])
        )
    ).all()
    
    print(f"[DEBUG] Total payments in DB: {total_payments_count}")
    print(f"[DEBUG] Total subscriptions in DB: {total_subscriptions_count}")
    print(f"[DEBUG] Active/trialing subscriptions: {len(active_subs_with_mrr)}")
    for sub in active_subs_with_mrr:
        mrr_val = float(sub.mrr) if sub.mrr is not None else 0.0
        print(f"[DEBUG]   - Sub {sub.stripe_subscription_id}: status={sub.status}, mrr={mrr_val} (type: {type(sub.mrr)}, value: {sub.mrr})")
    
    # Manual MRR calculation to verify
    manual_mrr_debug = sum(float(sub.mrr) if sub.mrr is not None else 0.0 for sub in active_subs_with_mrr)
    print(f"[DEBUG] Manual MRR calculation (debug): ${manual_mrr_debug:.2f}")
    
    print(f"[DEBUG] Date range: {start_date} to {end_date}")
    print(f"[DEBUG] Current MRR query result: {current_mrr_result} (type: {type(current_mrr_result)})")
    
    recent_subscriptions = db.query(StripeSubscription).filter(
        StripeSubscription.org_id == org_id
    ).order_by(
        desc(StripeSubscription.created_at)
    ).limit(10).all()
    
    # Get recent payments using EXACT same deduplicated set as revenue calculation
    # This ensures revenue matches what users see in the recent payments table
    # Use the SAME deduplicated set that revenue uses, just take top 10
    recent_payments = deduplicated_all_payments[:10]
    
    print(f"[DEBUG] Recent payments: showing top {len(recent_payments)} of {len(deduplicated_all_payments)} deduplicated payments")
    print(f"[DEBUG] Recent subscriptions found: {len(recent_subscriptions)}")
    if recent_payments:
        print(f"[DEBUG] First payment: created_at={recent_payments[0].created_at}, amount={recent_payments[0].amount_cents}, status={recent_payments[0].status}")
        print(f"[DEBUG] Payment in date range? {recent_payments[0].created_at >= start_date and recent_payments[0].created_at <= end_date}")
    if recent_subscriptions:
        print(f"[DEBUG] First subscription: created_at={recent_subscriptions[0].created_at}, mrr={recent_subscriptions[0].mrr}, status={recent_subscriptions[0].status}")
    
    # Format subscriptions for response
    subscriptions_list = []
    for sub in recent_subscriptions:
        try:
            subscriptions_list.append(StripeSubscriptionSchema(
                id=sub.stripe_subscription_id,
                status=sub.status,
                amount=int(float(sub.mrr) * 100) if sub.mrr else 0,  # Convert to cents
                current_period_start=int(sub.current_period_start.timestamp()) if sub.current_period_start else 0,
                current_period_end=int(sub.current_period_end.timestamp()) if sub.current_period_end else 0,
                customer_id=str(sub.client_id) if sub.client_id else ""
            ))
        except Exception as e:
            # Skip invalid subscriptions
            print(f"Error formatting subscription {sub.id}: {e}")
            continue
    
    # Format payments for response
    payments_list = []
    for payment in recent_payments:
        try:
            payments_list.append(StripePaymentSchema(
                id=payment.stripe_id,
                amount=payment.amount_cents,
                status=payment.status,
                created_at=int(payment.created_at.timestamp()) if payment.created_at else 0
            ))
        except Exception as e:
            # Skip invalid payments
            print(f"Error formatting payment {payment.id}: {e}")
            continue
    
    response_data = {
        "total_mrr": current_mrr,
        "total_arr": arr,
        "mrr_change": mrr_change,
        "mrr_change_percent": mrr_change_percent,
        "new_subscriptions": new_subscriptions,
        "churned_subscriptions": churned_subscriptions,
        "failed_payments": failed_payments,
        # Use period-filtered revenue (deduplicated payments within date range)
        # This matches what the user expects when they select a time range
        "last_30_days_revenue": revenue,
        "active_subscriptions": active_subscriptions,
        "total_customers": total_customers,
        "average_client_ltv": average_client_ltv,
        "subscriptions": subscriptions_list,
        "invoices": [],  # Empty for now - invoices not stored separately
        "customers": [],  # Empty for now - use clients endpoint
        "payments": payments_list
    }
    print(f"[DEBUG] Response data: total_mrr={response_data['total_mrr']}, revenue={response_data['last_30_days_revenue']}, arr={response_data['total_arr']}")
    return StripeSummaryResponse(**response_data)


@router.get("/kpis", response_model=StripeKPIsResponse)
def get_stripe_kpis(
    range_days: int = Query(30, alias="range", ge=1, le=365),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get top-line KPI cards with time-range selection"""
    if not check_stripe_connected(db, current_user.org_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Stripe not connected."
        )
    
    # Use summary endpoint logic
    summary = get_stripe_summary(range_days=range_days, current_user=current_user, db=db)
    
    return StripeKPIsResponse(
        mrr=summary.total_mrr,
        mrr_change=summary.mrr_change,
        mrr_change_percent=summary.mrr_change_percent,
        new_subscriptions=summary.new_subscriptions,
        churned_subscriptions=summary.churned_subscriptions,
        failed_payments=summary.failed_payments,
        revenue=summary.last_30_days_revenue
    )


@router.get("/revenue-timeline", response_model=StripeRevenueTimelineResponse)
def get_revenue_timeline(
    range_days: int = Query(30, alias="range", ge=1, le=365),
    group_by: str = Query("day", regex="^(day|week)$"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get daily/weekly revenue timeline chart data using same deduplication logic as total revenue"""
    if not check_stripe_connected(db, current_user.org_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Stripe not connected."
        )
    
    # CRITICAL: Filter by org_id for multi-tenant isolation
    org_id = current_user.org_id
    
    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=range_days)
    
    # Use the SAME deduplication function as the summary endpoint
    # This ensures revenue timeline matches total revenue calculation exactly
    def deduplicate_payments(payments_list):
        """Deduplicate payments using same logic as recent payments table"""
        seen = set()
        deduplicated = []
        
        # Sort: prefer charge over payment_intent over invoice, then by created_at (most recent first)
        payments_list.sort(key=lambda p: (
            0 if p.type == 'charge' else (1 if p.type == 'payment_intent' else 2),
            0 if p.subscription_id else 1,
            -(p.created_at.timestamp() if p.created_at else 0)
        ))
        
        invoice_ids_with_sub = set()
        standalone_payments_seen = {}
        
        for payment in payments_list:
            if payment.subscription_id and payment.invoice_id:
                key = (payment.subscription_id, payment.invoice_id)
                invoice_ids_with_sub.add(payment.invoice_id)
            elif payment.invoice_id:
                if payment.invoice_id in invoice_ids_with_sub:
                    continue
                key = (None, payment.invoice_id)
            else:
                if payment.created_at:
                    time_bucket = int(payment.created_at.timestamp() / 30) * 30
                    standalone_key = (payment.amount_cents, payment.client_id, time_bucket)
                    
                    if standalone_key in standalone_payments_seen:
                        continue
                    
                    standalone_payments_seen[standalone_key] = payment.stripe_id
                
                key = payment.stripe_id
            
            if key not in seen:
                seen.add(key)
                deduplicated.append(payment)
        
        return deduplicated
    
    # Get ALL succeeded payments in the date range
    all_payments = db.query(StripePayment).filter(
        and_(
            StripePayment.org_id == org_id,
            StripePayment.status == "succeeded",
            StripePayment.created_at >= start_date,
            StripePayment.created_at <= end_date
        )
    ).all()
    
    # Deduplicate using the same logic as summary endpoint
    deduplicated_payments = deduplicate_payments(all_payments)
    
    # Group deduplicated payments by day/week
    revenue_by_period = defaultdict(float)
    
    for payment in deduplicated_payments:
        if not payment.created_at:
            continue
        
        if group_by == "day":
            period_key = payment.created_at.date()
        else:  # week
            # Get start of week (Monday)
            days_since_monday = payment.created_at.weekday()
            week_start = payment.created_at.date() - timedelta(days=days_since_monday)
            period_key = week_start
        
        revenue_by_period[period_key] += (payment.amount_cents or 0) / 100.0
    
    # Convert to timeline format, sorted by date
    timeline = [
        {
            "date": str(period_date),
            "revenue": revenue_by_period[period_date]
        }
        for period_date in sorted(revenue_by_period.keys())
    ]
    
    print(f"[REVENUE_TIMELINE] Using {len(deduplicated_payments)} deduplicated payments (from {len(all_payments)} total) for date range {start_date.date()} to {end_date.date()}")
    print(f"[REVENUE_TIMELINE] Total revenue in timeline: ${sum(p['revenue'] for p in timeline):.2f}")
    
    return StripeRevenueTimelineResponse(timeline=timeline, group_by=group_by)


@router.get("/subscriptions", response_model=List[StripeSubscriptionResponse])
def get_subscriptions(
    status_filter: Optional[str] = Query(None, alias="status"),
    search: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get subscriptions table with search, sort, pagination"""
    if not check_stripe_connected(db, current_user.org_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Stripe not connected."
        )
    
    # CRITICAL: Filter by org_id for multi-tenant isolation
    query = db.query(StripeSubscription).filter(
        StripeSubscription.org_id == current_user.org_id
    ).join(Client, StripeSubscription.client_id == Client.id, isouter=True)
    
    # Apply status filter
    if status_filter:
        query = query.filter(StripeSubscription.status == status_filter)
    
    # Apply search (by client name or subscription ID)
    if search:
        search_term = f"%{search}%"
        query = query.filter(
            or_(
                StripeSubscription.stripe_subscription_id.ilike(search_term),
                Client.first_name.ilike(search_term),
                Client.last_name.ilike(search_term),
                Client.email.ilike(search_term)
            )
        )
    
    # Pagination
    total = query.count()
    subscriptions = query.order_by(desc(StripeSubscription.created_at)).offset(
        (page - 1) * page_size
    ).limit(page_size).all()
    
    result = []
    for sub in subscriptions:
        client = db.query(Client).filter(Client.id == sub.client_id).first() if sub.client_id else None
        result.append(StripeSubscriptionResponse(
            id=str(sub.id),
            stripe_subscription_id=sub.stripe_subscription_id,
            client_id=str(sub.client_id) if sub.client_id else None,
            client_name=f"{client.first_name or ''} {client.last_name or ''}".strip() if client else None,
            client_email=client.email if client else None,
            status=sub.status,
            plan_id=sub.plan_id,
            mrr=float(sub.mrr),
            start_date=sub.created_at,
            current_period_end=sub.current_period_end,
            estimated_lifetime_value=None  # TODO: Calculate from payment history
        ))
    
    return result


@router.get("/payments", response_model=List[StripePaymentResponse])
def get_payments(
    status_filter: Optional[str] = Query(None, alias="status"),
    range_days: Optional[int] = Query(None, alias="range", ge=1),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get payments table with filters, deduplicated"""
    if not check_stripe_connected(db, current_user.org_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Stripe not connected."
        )
    
    # CRITICAL: Filter by org_id for multi-tenant isolation
    query = db.query(StripePayment).filter(
        StripePayment.org_id == current_user.org_id
    ).join(Client, StripePayment.client_id == Client.id, isouter=True)
    
    if status_filter:
        query = query.filter(StripePayment.status == status_filter)
    
    # Filter by date range if provided (None means all time)
    if range_days is not None:
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=range_days)
        query = query.filter(
            and_(
                StripePayment.created_at >= start_date,
                StripePayment.created_at <= end_date
            )
        )
    
    # Get all payments, then deduplicate
    all_payments = query.order_by(desc(StripePayment.created_at)).all()
    
    # Deduplicate: group by (subscription_id, invoice_id) or (invoice_id)
    seen = set()
    deduplicated = []
    
    # Sort: prefer charge over invoice, then by created_at (most recent first)
    all_payments.sort(key=lambda p: (
        0 if p.type == 'charge' else 1,
        -(p.created_at.timestamp() if p.created_at else 0)
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
            deduplicated.append(payment)
    
    # Apply pagination after deduplication
    total = len(deduplicated)
    payments = deduplicated[(page - 1) * page_size:page * page_size]
    
    result = []
    for payment in payments:
        client = db.query(Client).filter(Client.id == payment.client_id).first() if payment.client_id else None
        result.append(StripePaymentResponse(
            id=str(payment.id),
            stripe_id=payment.stripe_id,
            client_id=str(payment.client_id) if payment.client_id else None,
            client_name=f"{client.first_name or ''} {client.last_name or ''}".strip() if client else None,
            client_email=client.email if client else None,
            amount_cents=payment.amount_cents or 0,
            currency=payment.currency or "usd",
            status=payment.status,
            subscription_id=payment.subscription_id,
            receipt_url=payment.receipt_url,
            created_at=int(payment.created_at.timestamp()) if payment.created_at else 0
        ))
    
    return result


@router.get("/failed-payments", response_model=List[StripeFailedPaymentResponse])
def get_failed_payments(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get failed payments queue"""
    if not check_stripe_connected(db, current_user.org_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Stripe not connected."
        )
    
    # CRITICAL: Filter by org_id for multi-tenant isolation
    org_id = current_user.org_id
    
    # Get all failed payments
    all_failed_payments = db.query(StripePayment).filter(
        and_(
            StripePayment.org_id == org_id,
            or_(
                StripePayment.status == "failed",
                StripePayment.status == "past_due"
            )
        )
    ).order_by(desc(StripePayment.created_at)).all()
    
    print(f"[DEBUG] Total failed payments found: {len(all_failed_payments)}")
    for p in all_failed_payments[:20]:  # Print first 20
        print(f"  - ID: {p.stripe_id}, Sub: {p.subscription_id}, Client: {p.client_id}, Invoice: {p.invoice_id}, Created: {p.created_at}, Status: {p.status}")
    
    # Debug: Check for duplicate subscription_ids
    sub_counts = {}
    for p in all_failed_payments:
        if p.subscription_id:
            sub_counts[p.subscription_id] = sub_counts.get(p.subscription_id, 0) + 1
    print(f"[DEBUG] Subscription IDs with multiple failed payments: {[(sub, count) for sub, count in sub_counts.items() if count > 1]}")
    
    # Group failed payments by subscription_id + client_id to condense retry attempts
    # For subscription payments: group by (subscription_id, client_id) - all retries for same subscription
    # For non-subscription payments: group by (None, client_id, invoice_id) if invoice_id exists,
    #   otherwise group by (None, client_id) - this ensures retries for same invoice are grouped
    grouped_payments = {}
    
    for payment in all_failed_payments:
        # Create grouping key for retry attempts
        # For subscription payments, group all retries together regardless of invoice_id
        # Use subscription_id as the primary grouping key - all retries for same subscription should group
        if payment.subscription_id:
            # Group by subscription_id only (not client_id) since retries for same subscription should group
            # even if client_id is different or None
            group_key = (payment.subscription_id,)
        elif payment.invoice_id:
            # For non-subscription payments with invoice, group by invoice to catch retries
            group_key = (None, payment.invoice_id)
        elif payment.client_id:
            # For standalone payments without subscription or invoice, group by client only
            group_key = (None, payment.client_id)
        else:
            # No subscription, invoice, or client - use stripe_id as unique key
            group_key = (payment.stripe_id,)
        
        if group_key not in grouped_payments:
            grouped_payments[group_key] = {
                'payments': [],
                'first_attempt': payment.created_at,
                'latest_attempt': payment.created_at
            }
            print(f"[DEBUG] New group created: key={group_key}, payment_id={payment.stripe_id}, sub={payment.subscription_id}, client={payment.client_id}, invoice={payment.invoice_id}")
        else:
            existing_count = len(grouped_payments[group_key]['payments'])
            print(f"[DEBUG] Adding to existing group: key={group_key}, payment_id={payment.stripe_id}, existing_count={existing_count}, sub={payment.subscription_id}, client={payment.client_id}, invoice={payment.invoice_id}")
        
        grouped_payments[group_key]['payments'].append(payment)
        
        # Track first and latest attempt dates
        if payment.created_at < grouped_payments[group_key]['first_attempt']:
            grouped_payments[group_key]['first_attempt'] = payment.created_at
        if payment.created_at > grouped_payments[group_key]['latest_attempt']:
            grouped_payments[group_key]['latest_attempt'] = payment.created_at
    
    # Convert grouped payments to result list
    result = []
    for group_key, group_data in grouped_payments.items():
        # Handle different group_key formats
        # group_key can be:
        #   - (subscription_id,) - 1-tuple for subscription payments
        #   - (None, invoice_id) - 2-tuple for invoice payments
        #   - (None, client_id) - 2-tuple for client payments
        #   - (stripe_id,) - 1-tuple for unique payments
        
        subscription_id = None
        client_id = None
        invoice_id = None
        
        if len(group_key) == 1:
            # Single value tuple: (subscription_id,) or (stripe_id,)
            value = group_key[0]
            if value and value.startswith('sub_'):
                subscription_id = value
            else:
                # Skip unique payments (stripe_id) - no grouping needed
                continue
        elif len(group_key) == 2:
            # Two value tuple: (None, invoice_id) or (None, client_id)
            first, second = group_key
            if first is None:
                if second and second.startswith('in_'):
                    invoice_id = second
                else:
                    client_id = second
        
        # Use the most recent payment as the representative payment
        representative_payment = max(group_data['payments'], key=lambda p: p.created_at)
        attempt_count = len(group_data['payments'])
        
        # Get client_id from representative payment if not already set
        if not client_id:
            client_id = representative_payment.client_id
        
        # Get subscription_id from representative payment if not already set
        if not subscription_id:
            subscription_id = representative_payment.subscription_id
        
        print(f"[DEBUG] Failed payment group: key={group_key}, attempt_count={attempt_count}, subscription_id={subscription_id}, client_id={client_id}")
        
        client = db.query(Client).filter(Client.id == representative_payment.client_id).first() if representative_payment.client_id else None
        
        # Check if recovery recommendation exists (filter by org_id)
        recovery = db.query(Recommendation).filter(
            and_(
                Recommendation.org_id == org_id,
                Recommendation.client_id == representative_payment.client_id,
                Recommendation.type == "payment_recovery",
                Recommendation.status == "PENDING"
            )
        ).first()
        
        result.append(StripeFailedPaymentResponse(
            id=str(representative_payment.id),
            stripe_id=representative_payment.stripe_id,
            client_id=str(representative_payment.client_id) if representative_payment.client_id else None,
            client_name=f"{client.first_name or ''} {client.last_name or ''}".strip() if client else None,
            client_email=client.email if client else None,
            amount_cents=representative_payment.amount_cents or 0,
            currency=representative_payment.currency or "usd",
            status=representative_payment.status,
            subscription_id=representative_payment.subscription_id,
            receipt_url=representative_payment.receipt_url,
            created_at=int(group_data['latest_attempt'].timestamp()) if group_data['latest_attempt'] else 0,
            has_recovery_recommendation=recovery is not None,
            recovery_recommendation_id=str(recovery.id) if recovery else None,
            attempt_count=attempt_count,
            first_attempt_at=int(group_data['first_attempt'].timestamp()) if group_data['first_attempt'] else 0,
            latest_attempt_at=int(group_data['latest_attempt'].timestamp()) if group_data['latest_attempt'] else 0
        ))
    
    # Sort by latest attempt date (most recent first) and apply pagination
    result.sort(key=lambda x: x.latest_attempt_at, reverse=True)
    total = len(result)
    paginated_result = result[(page - 1) * page_size:page * page_size]
    
    return paginated_result


@router.get("/client/{client_id}/revenue", response_model=StripeClientRevenueResponse)
def get_client_revenue(
    client_id: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get single-client revenue panel"""
    if not check_stripe_connected(db, current_user.org_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Stripe not connected."
        )
    
    from uuid import UUID
    try:
        client_uuid = UUID(client_id)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid client ID"
        )
    
    client = db.query(Client).filter(Client.id == client_uuid).first()
    if not client:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Client not found"
        )
    
    # Get lifetime revenue
    lifetime_revenue_cents = client.lifetime_revenue_cents or 0
    
    # Get current subscription
    current_subscription = db.query(StripeSubscription).filter(
        and_(
            StripeSubscription.client_id == client_uuid,
            StripeSubscription.status == "active"
        )
    ).first()
    
    # Get payment history
    payments = db.query(StripePayment).filter(
        StripePayment.client_id == client_uuid
    ).order_by(desc(StripePayment.created_at)).limit(20).all()
    
    payment_history = [
        {
            "id": str(p.id),
            "amount_cents": p.amount_cents,
            "status": p.status,
            "created_at": p.created_at,
            "receipt_url": p.receipt_url
        }
        for p in payments
    ]
    
    return StripeClientRevenueResponse(
        client_id=client_id,
        client_name=f"{client.first_name or ''} {client.last_name or ''}".strip(),
        client_email=client.email,
        lifetime_revenue_cents=lifetime_revenue_cents,
        current_subscription_id=current_subscription.stripe_subscription_id if current_subscription else None,
        current_mrr=float(current_subscription.mrr) if current_subscription else 0.0,
        next_invoice_date=current_subscription.current_period_end if current_subscription else None,
        payment_history=payment_history
    )


@router.get("/churn", response_model=StripeChurnResponse)
def get_churn_analytics(
    months: int = Query(6, ge=1, le=12),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get subscription churn & cohort snapshot"""
    if not check_stripe_connected(db, current_user.org_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Stripe not connected."
        )
    
    # Calculate monthly churn rate for last N months
    churn_data = []
    cohort_data = []
    
    for i in range(months):
        month_start = datetime.utcnow().replace(day=1) - timedelta(days=30 * i)
        month_end = (month_start + timedelta(days=32)).replace(day=1) - timedelta(seconds=1)
        
        # CRITICAL: Filter by org_id for multi-tenant isolation
        org_id = current_user.org_id
        
        # Count canceled subscriptions in this month
        # Churn = canceled subscription where customer has NO new active subscription (no upsell)
        canceled_subs = db.query(StripeSubscription).filter(
            and_(
                StripeSubscription.org_id == org_id,
                StripeSubscription.status == "canceled",
                StripeSubscription.updated_at >= month_start,
                StripeSubscription.updated_at <= month_end
            )
        ).all()
        
        canceled = 0
        for canceled_sub in canceled_subs:
            # Check if customer has a new active subscription created after cancellation
            # This would indicate an upsell/replacement, not a true churn
            has_replacement = db.query(StripeSubscription).filter(
                and_(
                    StripeSubscription.org_id == org_id,
                    StripeSubscription.client_id == canceled_sub.client_id,
                    StripeSubscription.status == "active",
                    StripeSubscription.created_at > canceled_sub.updated_at
                )
            ).first()
            
            # Only count as churn if no replacement subscription exists
            if not has_replacement:
                canceled += 1
        
        # Count active subscriptions at start of month
        active = db.query(func.count(StripeSubscription.id)).filter(
            and_(
                StripeSubscription.org_id == org_id,
                StripeSubscription.status == "active",
                StripeSubscription.created_at < month_end
            )
        ).scalar() or 0
        
        # Count new subscriptions in this month
        new_subs = db.query(func.count(StripeSubscription.id)).filter(
            and_(
                StripeSubscription.org_id == org_id,
                StripeSubscription.created_at >= month_start,
                StripeSubscription.created_at <= month_end
            )
        ).scalar() or 0
        
        churn_rate = (canceled / active * 100) if active > 0 else 0
        
        churn_data.append({
            "month": month_start.strftime("%Y-%m"),
            "churn_rate": churn_rate,
            "canceled": canceled,
            "active": active
        })
        
        cohort_data.append({
            "month": month_start.strftime("%Y-%m"),
            "new_subscriptions": new_subs,
            "churned": canceled  # This is now true churn (canceled without replacement)
        })
    
    return StripeChurnResponse(
        churn_by_month=churn_data,
        cohort_snapshot=cohort_data
    )


@router.get("/top-customers", response_model=StripeTopCustomersResponse)
def get_top_customers(
    days: int = Query(90, ge=1, le=365),
    limit: int = Query(5, ge=1, le=20),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get top customers by revenue and recent refunds"""
    if not check_stripe_connected(db, current_user.org_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Stripe not connected."
        )
    
    start_date = datetime.utcnow() - timedelta(days=days)
    
    # CRITICAL: Filter by org_id for multi-tenant isolation
    org_id = current_user.org_id
    
    # Top customers by revenue
    top_customers_query = db.query(
        Client.id,
        Client.first_name,
        Client.last_name,
        Client.email,
        func.sum(StripePayment.amount_cents).label("total_revenue_cents")
    ).join(
        StripePayment, Client.id == StripePayment.client_id
    ).filter(
        and_(
            Client.org_id == org_id,
            StripePayment.org_id == org_id,
            StripePayment.status == "succeeded",
            StripePayment.created_at >= start_date
        )
    ).group_by(Client.id).order_by(desc("total_revenue_cents")).limit(limit).all()
    
    top_customers = [
        {
            "client_id": str(row.id),
            "name": f"{row.first_name or ''} {row.last_name or ''}".strip(),
            "email": row.email,
            "revenue_cents": int(row.total_revenue_cents or 0)
        }
        for row in top_customers_query
    ]
    
    # Recent refunds (filter by org_id)
    refunds = db.query(StripePayment).filter(
        and_(
            StripePayment.org_id == org_id,
            StripePayment.status == "refunded",
            StripePayment.created_at >= start_date
        )
    ).order_by(desc(StripePayment.created_at)).limit(10).all()
    
    recent_refunds = [
        {
            "id": str(p.id),
            "stripe_id": p.stripe_id,
            "amount_cents": p.amount_cents,
            "created_at": p.created_at,
            "client_id": str(p.client_id) if p.client_id else None
        }
        for p in refunds
    ]
    
    return StripeTopCustomersResponse(
        top_customers=top_customers,
        recent_refunds=recent_refunds
    )


@router.get("/mrr-trend", response_model=MRRTrendResponse)
def get_mrr_trend(
    range_days: int = Query(90, alias="range", ge=7, le=365),
    group_by: str = Query("day", regex="^(day|week|month)$"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get MRR trend over time for charting"""
    if not check_stripe_connected(db, current_user.org_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Stripe not connected."
        )
    
    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=range_days)
    
    # CRITICAL: Filter by org_id for multi-tenant isolation
    org_id = current_user.org_id
    
    # Get ALL subscriptions that could have been active during the range
    # Simplest approach: Get all subscriptions created before or during the range
    # The is_subscription_active_on_date function will determine if they were active on each date
    all_subs = db.query(StripeSubscription).filter(
        and_(
            StripeSubscription.org_id == org_id,
            # Get all subscriptions created before or during the range
            # This ensures we include past subscriptions that were active during the range
            StripeSubscription.created_at <= end_date
        )
    ).all()
    
    print(f"[MRR_TREND] Found {len(all_subs)} subscriptions to check for date range {start_date.date()} to {end_date.date()}")
    
    # Debug: Print subscription statuses and date ranges
    status_counts = {}
    canceled_count = 0
    active_count = 0
    for sub in all_subs:
        status_counts[sub.status] = status_counts.get(sub.status, 0) + 1
        if sub.status == "canceled":
            canceled_count += 1
        elif sub.status in ["active", "trialing", "past_due"]:
            active_count += 1
    
    print(f"[MRR_TREND] Subscription statuses: {status_counts}")
    print(f"[MRR_TREND] Active/trialing/past_due: {active_count}, Canceled: {canceled_count}")
    
    # Debug: Check how many canceled subscriptions have period_end info
    canceled_with_period_end = sum(1 for sub in all_subs if sub.status == "canceled" and sub.current_period_end)
    print(f"[MRR_TREND] Canceled subscriptions with current_period_end: {canceled_with_period_end}/{canceled_count}")
    
    trend_data = []
    
    def is_subscription_active_on_date(sub, check_date):
        """Check if a subscription was active on a given date"""
        check_date_only = check_date.date() if isinstance(check_date, datetime) else check_date
        
        # Subscription must have been created on or before this date
        if sub.created_at and sub.created_at.date() > check_date_only:
            return False
        
        # If subscription is currently active/trialing/past_due, it was active on this date
        # (assuming it was created before the check date, which we already verified)
        if sub.status in ["active", "trialing", "past_due"]:
            return True
        
        # If subscription is canceled, check when it was last active
        if sub.status == "canceled":
            # For canceled subscriptions, use current_period_end as the last day it was active
            # In Stripe, when a subscription is canceled, it remains active until the end of the current period
            if sub.current_period_end:
                # Subscription was active until the end of its period
                # So it was active on check_date if check_date <= period_end
                if sub.current_period_end.date() >= check_date_only:
                    return True
                return False
            
            # If no current_period_end, use updated_at as a fallback
            # updated_at is when the subscription status was last updated (likely when canceled)
            if sub.updated_at:
                # If updated after check date, subscription was still active on check date
                if sub.updated_at.date() > check_date_only:
                    return True
                # If updated on or before check date, subscription was not active
                return False
            
            # If we have no date information, be conservative and assume it was active
            # if it was created before the check date (better to show historical data)
            return True
        
        # For other statuses (incomplete, incomplete_expired, unpaid, etc.), 
        # check if they were in an active period on this date
        if sub.current_period_end and sub.current_period_end.date() >= check_date_only:
            # Was in an active period on this date
            return True
        
        # For subscriptions with unclear status, if created before check date and no clear end,
        # assume they might have been active (conservative approach for historical data)
        return True
    
    if group_by == "day":
        # Group by day - calculate MRR for each day
        current = start_date
        while current <= end_date:
            day_end = current + timedelta(days=1)
            
            # Sum MRR of subscriptions that were active on this day
            day_mrr = 0.0
            active_count = 0
            for sub in all_subs:
                if is_subscription_active_on_date(sub, current):
                    mrr_value = float(sub.mrr) if sub.mrr else 0.0
                    day_mrr += mrr_value
                    active_count += 1
            
            trend_data.append(MRRTrendPoint(
                date=current.strftime("%Y-%m-%d"),
                mrr=day_mrr,
                subscriptions_count=active_count
            ))
            
            current = day_end
    
    elif group_by == "week":
        # Group by week
        current = start_date
        while current <= end_date:
            week_end = current + timedelta(days=7)
            
            week_mrr = 0.0
            active_count = 0
            for sub in all_subs:
                # Use the start of the week as the check date
                if is_subscription_active_on_date(sub, current):
                    mrr_value = float(sub.mrr) if sub.mrr else 0.0
                    week_mrr += mrr_value
                    active_count += 1
            
            trend_data.append(MRRTrendPoint(
                date=current.strftime("%Y-%m-%d"),
                mrr=week_mrr,
                subscriptions_count=active_count
            ))
            
            current = week_end
    
    else:  # month
        # Group by month
        current = start_date.replace(day=1)
        while current <= end_date:
            if current.month == 12:
                month_end = current.replace(year=current.year + 1, month=1, day=1) - timedelta(seconds=1)
            else:
                month_end = current.replace(month=current.month + 1, day=1) - timedelta(seconds=1)
            
            month_mrr = 0.0
            active_count = 0
            for sub in all_subs:
                # Use the start of the month as the check date
                if is_subscription_active_on_date(sub, current):
                    mrr_value = float(sub.mrr) if sub.mrr else 0.0
                    month_mrr += mrr_value
                    active_count += 1
            
            trend_data.append(MRRTrendPoint(
                date=current.strftime("%Y-%m-%d"),
                mrr=month_mrr,
                subscriptions_count=active_count
            ))
            
            if current.month == 12:
                current = current.replace(year=current.year + 1, month=1, day=1)
            else:
                current = current.replace(month=current.month + 1, day=1)
    
    # Calculate current and previous MRR for comparison
    current_mrr = trend_data[-1].mrr if trend_data else 0.0
    previous_mrr = trend_data[-2].mrr if len(trend_data) >= 2 else current_mrr
    growth_percent = ((current_mrr - previous_mrr) / previous_mrr * 100) if previous_mrr > 0 else 0.0
    
    return MRRTrendResponse(
        trend_data=trend_data,
        current_mrr=current_mrr,
        previous_mrr=previous_mrr,
        growth_percent=growth_percent
    )


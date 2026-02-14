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
from app.models.stripe_treasury_transaction import StripeTreasuryTransaction, TreasuryTransactionStatus
from app.models.client import Client
from app.models.recommendation import Recommendation, RecommendationStatus
from app.utils.stripe_ids import normalize_stripe_id, normalize_stripe_id_for_dedup
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
    DuplicatePaymentsResponse,
    DuplicatePaymentGroup,
    DuplicatePaymentEntry,
    MergeDuplicatesRequest,
    MergeDuplicatesResponse,
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
    # Get selected org_id from user object (set by get_current_user)
    org_id = getattr(current_user, 'selected_org_id', current_user.org_id)
    
    # CRITICAL: Filter by org_id for multi-tenant isolation (use selected org from token)
    oauth_token = db.query(OAuthToken).filter(
        OAuthToken.provider == OAuthProvider.STRIPE,
        OAuthToken.org_id == org_id
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
    # Get selected org_id from user object (set by get_current_user)
    org_id = getattr(current_user, 'selected_org_id', current_user.org_id)
    
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


@router.post("/sync-treasury", status_code=status.HTTP_200_OK)
def sync_treasury_transactions(
    financial_account_id: Optional[str] = Query(None, description="Financial account ID to sync (optional)"),
    days_back: int = Query(30, ge=1, le=365, description="Number of days to sync back"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Sync Treasury Transactions from Stripe API.
    This is the new source of truth for payments and client generation.
    Treasury Transactions provide a unified view of all money movements.
    """
    if not check_stripe_connected(db, current_user.org_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Stripe not connected. Please connect Stripe via OAuth first."
        )
    
    from app.services.stripe_treasury_sync import sync_treasury_transactions
    
    try:
        created_since = datetime.utcnow() - timedelta(days=days_back)
        print(f"[API] Treasury sync requested by user {current_user.id} for org {current_user.org_id} (days_back={days_back})")
        
        sync_result = sync_treasury_transactions(
            db=db,
            org_id=current_user.org_id,
            financial_account_id=financial_account_id,
            limit=100,
            created_since=created_since
        )
        
        if sync_result.get("errors"):
            # Return partial success if there were errors
            return {
                "success": True,
                "message": "Treasury transactions synced with some errors",
                "results": {
                    "transactions_synced": sync_result.get("transactions_synced", 0),
                    "transactions_updated": sync_result.get("transactions_updated", 0),
                    "clients_created": sync_result.get("clients_created", 0),
                    "clients_updated": sync_result.get("clients_updated", 0),
                    "errors": sync_result.get("errors", [])
                }
            }
        
        return {
            "success": True,
            "message": "Treasury transactions synced successfully",
            "results": {
                "transactions_synced": sync_result.get("transactions_synced", 0),
                "transactions_updated": sync_result.get("transactions_updated", 0),
                "clients_created": sync_result.get("clients_created", 0),
                "clients_updated": sync_result.get("clients_updated", 0),
            }
        }
    except Exception as e:
        import traceback
        error_detail = str(e)
        print(f"[API] ❌ Error syncing Treasury transactions: {error_detail}")
        print(traceback.format_exc())
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error syncing Treasury transactions: {error_detail}"
        )


@router.post("/sync", status_code=status.HTTP_200_OK)
def sync_stripe_data(
    force_full: bool = Query(False, description="Force full historical sync (only needed on first connect)"),
    sync_recent: bool = Query(False, description="Sync payments from last 24 hours (useful for catching missed payments)"),
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
        print(f"[API] Sync requested by user {current_user.id} for org {current_user.org_id} (force_full={force_full}, sync_recent={sync_recent})")
        
        # If sync_recent is True, temporarily modify last_sync_at to look back 24 hours
        if sync_recent:
            from app.models.oauth_token import OAuthToken, OAuthProvider
            from datetime import timedelta
            oauth_token = db.query(OAuthToken).filter(
                OAuthToken.provider == OAuthProvider.STRIPE,
                OAuthToken.org_id == current_user.org_id
            ).first()
            
            if oauth_token:
                # Temporarily set last_sync_at to 24 hours ago to force sync of recent payments
                original_last_sync = oauth_token.last_sync_at
                oauth_token.last_sync_at = datetime.utcnow() - timedelta(hours=24)
                db.commit()
                print(f"[API] Temporarily set last_sync_at to {oauth_token.last_sync_at} to sync recent payments")
        
        sync_result = sync_stripe_incremental(db, org_id=current_user.org_id, force_full=force_full)
        
        # Restore original last_sync_at if we modified it
        if sync_recent and 'original_last_sync' in locals() and original_last_sync is not None:
            oauth_token = db.query(OAuthToken).filter(
                OAuthToken.provider == OAuthProvider.STRIPE,
                OAuthToken.org_id == current_user.org_id
            ).first()
            if oauth_token:
                oauth_token.last_sync_at = original_last_sync
                db.commit()
                print(f"[API] Restored last_sync_at to {original_last_sync}")
        
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


@router.delete("/payments/{payment_id}")
def delete_payment(
    payment_id: str,
    use_treasury: bool = Query(True, description="Delete from Treasury Transactions if True, otherwise from StripePayment"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Delete a payment that is known to be false/incorrect.
    
    This endpoint allows deletion of payments from either:
    - Treasury Transactions (if use_treasury=True)
    - StripePayment table (if use_treasury=False)
    
    After deletion, triggers reconciliation to recalculate derived metrics.
    """
    if not check_stripe_connected(db, current_user.org_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Stripe not connected."
        )
    
    org_id = current_user.org_id
    
    try:
        if use_treasury:
            # Delete from Treasury Transactions
            transaction = db.query(StripeTreasuryTransaction).filter(
                and_(
                    StripeTreasuryTransaction.id == uuid.UUID(payment_id),
                    StripeTreasuryTransaction.org_id == org_id
                )
            ).first()
            
            if not transaction:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Treasury transaction with ID {payment_id} not found."
                )
            
            transaction_id = transaction.stripe_transaction_id
            db.delete(transaction)
            db.commit()
            
            print(f"[DELETE] Deleted Treasury transaction {transaction_id} (ID: {payment_id}) for org {org_id}")
            
        else:
            # Delete from StripePayment table
            payment = db.query(StripePayment).filter(
                and_(
                    StripePayment.id == uuid.UUID(payment_id),
                    StripePayment.org_id == org_id
                )
            ).first()
            
            if not payment:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Payment with ID {payment_id} not found."
                )
            
            stripe_id = payment.stripe_id
            db.delete(payment)
            db.commit()
            
            print(f"[DELETE] Deleted StripePayment {stripe_id} (ID: {payment_id}) for org {org_id}")
        
        # Trigger reconciliation to recalculate derived metrics
        from app.services.stripe_sync_v2 import reconcile_stripe_data
        try:
            reconcile_result = reconcile_stripe_data(db, org_id=org_id)
            print(f"[DELETE] Reconciliation complete: {reconcile_result.get('clients_reconciled', 0)} clients reconciled")
        except Exception as reconcile_error:
            print(f"[DELETE] Warning: Reconciliation failed after deletion: {str(reconcile_error)}")
            # Don't fail the deletion if reconciliation fails
        
        return {
            "success": True,
            "message": f"Payment deleted successfully. Reconciliation triggered.",
            "payment_id": payment_id,
            "reconciliation": {
                "clients_reconciled": reconcile_result.get("clients_reconciled", 0) if 'reconcile_result' in locals() else 0,
                "revenue_recalculated": reconcile_result.get("revenue_recalculated", 0) if 'reconcile_result' in locals() else 0
            }
        }
        
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid payment ID format: {payment_id}"
        )
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        import traceback
        print(f"[DELETE] Error deleting payment: {str(e)}")
        print(traceback.format_exc())
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to delete payment: {str(e)}"
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
    # Get selected org_id from user object (set by get_current_user)
    org_id = getattr(current_user, 'selected_org_id', current_user.org_id)
    
    if not check_stripe_connected(db, org_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Stripe not connected. Please connect Stripe via OAuth first."
        )
    
    # CRITICAL: All queries must filter by org_id for multi-tenant isolation (use selected org from token)
    
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
    
    # Count new customers in period (first successful payment in period, any type: subscription, invoice, one-off)
    # First payment date per client from StripePayment (succeeded)
    payment_first = db.query(
        StripePayment.client_id,
        func.min(StripePayment.created_at).label("first_ts"),
    ).filter(
        and_(
            StripePayment.org_id == org_id,
            StripePayment.status == "succeeded",
            StripePayment.client_id.isnot(None),
        )
    ).group_by(StripePayment.client_id).all()
    first_by_client = {}
    for (cid, ts) in payment_first:
        if cid and ts:
            first_by_client[cid] = min(first_by_client.get(cid, ts), ts) if cid in first_by_client else ts
    # First payment date per client from Treasury (posted, inbound)
    try:
        from sqlalchemy import text
        treasury_first = db.query(
            StripeTreasuryTransaction.client_id,
            func.min(
                func.coalesce(StripeTreasuryTransaction.posted_at, StripeTreasuryTransaction.created)
            ).label("first_ts"),
        ).filter(
            and_(
                StripeTreasuryTransaction.org_id == org_id,
                StripeTreasuryTransaction.client_id.isnot(None),
                StripeTreasuryTransaction.amount > 0,
            )
        ).filter(
            text("stripe_treasury_transactions.status = 'posted'::treasurytransactionstatus")
        ).group_by(StripeTreasuryTransaction.client_id).all()
        for (cid, ts) in treasury_first:
            if cid and ts:
                first_by_client[cid] = min(first_by_client.get(cid, ts), ts) if cid in first_by_client else ts
    except Exception:
        pass
    new_customers = sum(
        1 for _, first_ts in first_by_client.items()
        if start_date <= first_ts <= end_date
    )
    
    # Count churned clients in period (client-based churn, not subscription-based)
    # Churn = unique clients who churned in this period
    # - Subscription churn: immediate when subscription period ends
    # - One-off payment churn: 30-day grace period after last payment
    churned_client_ids = set()
    
    # 1. SUBSCRIPTION CHURN: Clients whose subscription ended in this period
    canceled_subs = db.query(StripeSubscription).filter(
        and_(
            StripeSubscription.org_id == org_id,
            StripeSubscription.status == "canceled",
            StripeSubscription.current_period_end.isnot(None),
            StripeSubscription.current_period_end >= start_date,
            StripeSubscription.current_period_end <= end_date,
            StripeSubscription.client_id.isnot(None)
        )
    ).all()
    
    for canceled_sub in canceled_subs:
        client_id = canceled_sub.client_id
        if not client_id:
            continue
            
        period_end_date = canceled_sub.current_period_end
        
        # Check if client has a new active subscription created after period ended
        has_replacement = db.query(StripeSubscription).filter(
            and_(
                StripeSubscription.org_id == org_id,
                StripeSubscription.client_id == client_id,
                StripeSubscription.status == "active",
                StripeSubscription.created_at > period_end_date
            )
        ).first()
        
        # Only count as churn if no replacement subscription exists
        if not has_replacement:
            churned_client_ids.add(client_id)
    
    # 2. ONE-OFF PAYMENT CHURN: Clients whose last payment was 30+ days before end_date
    # and they haven't made a new payment since
    grace_period_cutoff = end_date - timedelta(days=30)
    
    # Get clients with one-off payments
    clients_with_payments = db.query(
        StripePayment.client_id,
        func.max(StripePayment.created_at).label('last_payment_date')
    ).filter(
        and_(
            StripePayment.org_id == org_id,
            StripePayment.status == 'succeeded',
            StripePayment.client_id.isnot(None),
            StripePayment.subscription_id.is_(None)  # One-off payments only
        )
    ).group_by(StripePayment.client_id).all()
    
    for client_id, last_payment_date in clients_with_payments:
        if not client_id or not last_payment_date:
            continue
        
        # Skip if already counted as subscription churn
        if client_id in churned_client_ids:
            continue
        
        # Check if last payment was before grace period cutoff
        if last_payment_date < grace_period_cutoff:
            # Check if client made any payment after last_payment_date but before end_date
            has_renewal = db.query(StripePayment).filter(
                and_(
                    StripePayment.org_id == org_id,
                    StripePayment.client_id == client_id,
                    StripePayment.status == 'succeeded',
                    StripePayment.created_at > last_payment_date,
                    StripePayment.created_at <= end_date
                )
            ).first()
            
            if not has_renewal:
                # Churn date is last_payment_date + 30 days
                churn_date = last_payment_date + timedelta(days=30)
                # Only count if churn date falls within the period
                if start_date <= churn_date <= end_date:
                    churned_client_ids.add(client_id)
    
    # Count unique churned clients
    churned_subscriptions = len(churned_client_ids)
    
    # Count unique failed payments in period
    # Try Treasury Transactions first, fall back to old payment system
    failed_payments = 0
    try:
        from sqlalchemy import text
        # Check if Treasury Transactions exist
        treasury_count = db.query(StripeTreasuryTransaction).filter(
            StripeTreasuryTransaction.org_id == org_id
        ).count()
        
        if treasury_count > 0:
            # Use Treasury Transactions
            failed_transaction_records = db.query(StripeTreasuryTransaction).filter(
                StripeTreasuryTransaction.org_id == org_id
            ).filter(
                text("stripe_treasury_transactions.status = 'void'::treasurytransactionstatus")
            ).filter(
                StripeTreasuryTransaction.created >= start_date,
                StripeTreasuryTransaction.created <= end_date
            ).all()
            
            unique_failures = set()
            for transaction in failed_transaction_records:
                group_key = normalize_stripe_id_for_dedup(transaction.flow_id or transaction.stripe_transaction_id) or (transaction.flow_id or transaction.stripe_transaction_id)
                unique_failures.add(group_key)
            failed_payments = len(unique_failures)
        else:
            print(f"[FAILED PAYMENTS COUNT] No Treasury Transactions, using StripePayment")
            failed_payment_records = db.query(StripePayment).filter(
                and_(
                    StripePayment.org_id == org_id,
                    StripePayment.status == "failed",
                    StripePayment.created_at >= start_date,
                    StripePayment.created_at <= end_date
                )
            ).all()
            unique_failures = set()
            for payment in failed_payment_records:
                group_key = (normalize_stripe_id_for_dedup(payment.subscription_id) if payment.subscription_id else None, payment.client_id)
                unique_failures.add(group_key)
            failed_payments = len(unique_failures)
    except Exception as e:
        db.rollback()
        print(f"[FAILED PAYMENTS COUNT] Error querying failed transactions: {str(e)}, falling back to StripePayment")
        try:
            failed_payment_records = db.query(StripePayment).filter(
                and_(
                    StripePayment.org_id == org_id,
                    StripePayment.status == "failed",
                    StripePayment.created_at >= start_date,
                    StripePayment.created_at <= end_date
                )
            ).all()
            unique_failures = set()
            for payment in failed_payment_records:
                group_key = (normalize_stripe_id_for_dedup(payment.subscription_id) if payment.subscription_id else None, payment.client_id)
                unique_failures.add(group_key)
            
            failed_payments = len(unique_failures)
        except Exception as e2:
            print(f"[FAILED PAYMENTS COUNT] Error with fallback: {str(e2)}")
            failed_payments = 0
    
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
        
        # Track invoice_ids (normalized) that have been seen with subscription_id
        invoice_ids_with_sub = set()
        
        # Track payments without invoice/subscription by (amount, client_id, time_window)
        # Use normalized stripe_id so pi_xxx and ch_xxx for same payment match
        standalone_payments_seen = {}  # key: (amount_cents, client_id, time_bucket) -> normalized stripe_id
        
        for payment in payments_list:
            # Create deduplication key using normalized IDs (first 17 chars of suffix for dedup)
            if payment.subscription_id and payment.invoice_id:
                key = (normalize_stripe_id_for_dedup(payment.subscription_id), normalize_stripe_id_for_dedup(payment.invoice_id))
                invoice_ids_with_sub.add(normalize_stripe_id_for_dedup(payment.invoice_id))
            elif payment.invoice_id:
                norm_inv = normalize_stripe_id_for_dedup(payment.invoice_id)
                if norm_inv in invoice_ids_with_sub:
                    print(f"[DEBUG] Skipping payment {payment.stripe_id} with invoice_id {payment.invoice_id} (already have one with subscription_id)")
                    continue
                key = (None, norm_inv)
            else:
                if payment.created_at:
                    time_bucket = int(payment.created_at.timestamp() / 30) * 30
                    standalone_key = (payment.amount_cents, payment.client_id, time_bucket)
                    norm_stripe = normalize_stripe_id_for_dedup(payment.stripe_id)
                    if standalone_key in standalone_payments_seen:
                        existing_id = standalone_payments_seen[standalone_key]
                        print(f"[DEBUG] Skipping duplicate standalone payment {payment.stripe_id} (type: {payment.type}) - matches {existing_id} (same amount ${payment.amount_cents/100:.2f}, client, time window)")
                        continue
                    standalone_payments_seen[standalone_key] = norm_stripe
                key = normalize_stripe_id_for_dedup(payment.stripe_id) if payment.stripe_id else payment.stripe_id
            
            if key not in seen:
                seen.add(key)
                deduplicated.append(payment)
            else:
                print(f"[DEBUG] Skipping duplicate payment {payment.stripe_id} with key {key}")
        
        return deduplicated
    
    # Calculate revenue using Treasury Transactions as source of truth
    # Get ALL posted Treasury Transactions (for recent payments table - no date filter)
    # Use raw SQL to avoid SQLAlchemy enum name conversion
    # Handle case where Treasury Transactions table might not exist or have no data
    use_treasury_for_revenue = True
    all_succeeded_transactions = []
    
    try:
        from sqlalchemy import text
        # Check if Treasury Transactions exist
        treasury_count = db.query(StripeTreasuryTransaction).filter(
            StripeTreasuryTransaction.org_id == org_id
        ).count()
        
        if treasury_count == 0:
            print(f"[REVENUE] No Treasury Transactions found, falling back to StripePayment")
            use_treasury_for_revenue = False
        else:
            # Query transactions using raw SQL for status to avoid enum conversion issues
            all_succeeded_transactions = db.query(StripeTreasuryTransaction).filter(
                StripeTreasuryTransaction.org_id == org_id
            ).filter(
                text("stripe_treasury_transactions.status = 'posted'::treasurytransactionstatus")
            ).filter(
                StripeTreasuryTransaction.amount > 0  # Only inbound transactions (positive amounts)
            ).order_by(desc(StripeTreasuryTransaction.posted_at), desc(StripeTreasuryTransaction.created)).all()
            
            if len(all_succeeded_transactions) == 0:
                print(f"[REVENUE] No posted Treasury Transactions found, falling back to StripePayment")
                use_treasury_for_revenue = False
    except Exception as e:
        # If Treasury Transactions table doesn't exist or query fails, rollback and fall back to old system
        db.rollback()
        import traceback
        print(f"[REVENUE] Error querying Treasury Transactions: {str(e)}")
        print(f"[REVENUE] Traceback: {traceback.format_exc()}")
        print(f"[REVENUE] Falling back to StripePayment system.")
        use_treasury_for_revenue = False
    
    # Process transactions or fall back to old payment system
    if use_treasury_for_revenue and all_succeeded_transactions:
        # Deduplicate by stripe_transaction_id (exact duplicates) and flow_id (same payment flow)
        seen_transaction_ids = set()
        seen_flows = set()
        deduplicated_all_transactions = []
        
        # Sort by posted_at (most recent first)
        all_succeeded_transactions.sort(key=lambda t: (t.posted_at or t.created).timestamp() if (t.posted_at or t.created) else 0, reverse=True)
        
        for transaction in all_succeeded_transactions:
            norm_txn = normalize_stripe_id_for_dedup(transaction.stripe_transaction_id)
            if norm_txn and norm_txn in seen_transaction_ids:
                print(f"[REVENUE] Skipping duplicate transaction {transaction.stripe_transaction_id} (same normalized id)")
                continue
            if norm_txn:
                seen_transaction_ids.add(norm_txn)
            
            norm_flow = normalize_stripe_id_for_dedup(transaction.flow_id) if transaction.flow_id else None
            if norm_flow:
                if norm_flow in seen_flows:
                    print(f"[REVENUE] Skipping duplicate transaction {transaction.stripe_transaction_id} (same flow_id: {transaction.flow_id})")
                    continue
                seen_flows.add(norm_flow)
            
            deduplicated_all_transactions.append(transaction)
        
        # Calculate revenue from ALL deduplicated transactions (same set that recent payments uses)
        total_revenue = sum(abs(t.amount) for t in deduplicated_all_transactions) / 100.0
        
        # For period revenue, filter FIRST by date range, then deduplicate
        transactions_in_range = [
            t for t in deduplicated_all_transactions
            if (t.posted_at or t.created) and (t.posted_at or t.created) >= start_date and (t.posted_at or t.created) <= end_date
        ]
        
        # Calculate period revenue from filtered transactions
        revenue = sum(abs(t.amount) for t in transactions_in_range) / 100.0
        
        # Convert transactions to payment-like objects for compatibility with existing code
        class PaymentLike:
            def __init__(self, transaction):
                self.id = transaction.id
                self.stripe_id = transaction.stripe_transaction_id
                self.amount_cents = abs(transaction.amount)
                self.currency = transaction.currency
                self.status = "succeeded"
                self.created_at = transaction.posted_at or transaction.created
                self.subscription_id = transaction.flow_id
                self.invoice_id = None
                self.type = "treasury_transaction"
                self.client_id = transaction.client_id
        
        deduplicated_all_payments = [PaymentLike(t) for t in deduplicated_all_transactions]
        
        print(f"[TREASURY REVENUE] Revenue calculation (using Treasury Transactions as source of truth):")
        print(f"  - All posted transactions: {len(all_succeeded_transactions)} total")
        print(f"  - After deduplication: {len(deduplicated_all_transactions)} (same as recent payments uses)")
        print(f"  - In date range ({start_date} to {end_date}): {len(transactions_in_range)}")
        print(f"  - Revenue (period): ${revenue:.2f}")
        print(f"  - Total revenue (all deduplicated): ${total_revenue:.2f}")
    else:
        # Fallback to old payment system for revenue calculation
        print(f"[REVENUE] Using StripePayment system for revenue calculation")
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
        payments_in_range = [
            p for p in all_succeeded_payments
            if p.created_at and p.created_at >= start_date and p.created_at <= end_date
        ]
        
        # Deduplicate the filtered payments
        deduplicated_payments_in_range = deduplicate_payments(payments_in_range)
        
        # Calculate period revenue from deduplicated, filtered payments
        revenue = sum(p.amount_cents for p in deduplicated_payments_in_range) / 100.0
        
        print(f"[PAYMENT REVENUE] Revenue calculation (using StripePayment system):")
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
    
    # Calculate average client LTV using same dedup as assign modal / TopRevenueContributors:
    # one "customer" per normalized email, else per stripe_customer_id, else per client id.
    customers_with_revenue = db.query(Client).filter(
        and_(
            Client.org_id == org_id,
            Client.stripe_customer_id.isnot(None)
        )
    ).all()

    def _normalize_email(email: Optional[str]) -> Optional[str]:
        if not email:
            return None
        s = email.replace(" ", "").lower().strip()
        return s if s else None

    # Group by same key as frontend: email > stripe_customer_id > client id; sum revenue per group
    _group_revenue_cents: dict[str, int] = {}
    for client in customers_with_revenue:
        norm_email = _normalize_email(client.email)
        key = (
            f"email:{norm_email}"
            if norm_email
            else f"stripe:{client.stripe_customer_id}"
            if client.stripe_customer_id
            else f"id:{client.id}"
        )
        _group_revenue_cents[key] = _group_revenue_cents.get(key, 0) + (client.lifetime_revenue_cents or 0)

    if _group_revenue_cents:
        total_lifetime_revenue = sum(cents / 100.0 for cents in _group_revenue_cents.values())
        num_unique_customers = len(_group_revenue_cents)
        average_client_ltv = total_lifetime_revenue / num_unique_customers
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
        "new_customers": new_customers,
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
    # Get selected org_id from user object (set by get_current_user)
    org_id = getattr(current_user, 'selected_org_id', current_user.org_id)
    
    if not check_stripe_connected(db, org_id):
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
        new_customers=summary.new_customers,
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
    # Get selected org_id from user object (set by get_current_user)
    org_id = getattr(current_user, 'selected_org_id', current_user.org_id)
    
    if not check_stripe_connected(db, org_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Stripe not connected."
        )
    
    # CRITICAL: Filter by org_id for multi-tenant isolation (use selected org from token)
    
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
                key = (normalize_stripe_id_for_dedup(payment.subscription_id), normalize_stripe_id_for_dedup(payment.invoice_id))
                invoice_ids_with_sub.add(normalize_stripe_id_for_dedup(payment.invoice_id))
            elif payment.invoice_id:
                norm_inv = normalize_stripe_id_for_dedup(payment.invoice_id)
                if norm_inv in invoice_ids_with_sub:
                    continue
                key = (None, norm_inv)
            else:
                if payment.created_at:
                    time_bucket = int(payment.created_at.timestamp() / 30) * 30
                    standalone_key = (payment.amount_cents, payment.client_id, time_bucket)
                    if standalone_key in standalone_payments_seen:
                        continue
                    standalone_payments_seen[standalone_key] = normalize_stripe_id_for_dedup(payment.stripe_id)
                key = normalize_stripe_id_for_dedup(payment.stripe_id) if payment.stripe_id else payment.stripe_id
            
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
    # Get selected org_id from user object (set by get_current_user)
    org_id = getattr(current_user, 'selected_org_id', current_user.org_id)
    
    if not check_stripe_connected(db, org_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Stripe not connected."
        )
    
    # CRITICAL: Filter by org_id for multi-tenant isolation (use selected org from token)
    query = db.query(StripeSubscription).filter(
        StripeSubscription.org_id == org_id
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
    use_treasury: bool = Query(True, description="Use Treasury Transactions API as source of truth"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Get payments table with filters, deduplicated.
    Uses Treasury Transactions API as source of truth when use_treasury=True.
    """
    # Get selected org_id from user object (set by get_current_user)
    org_id = getattr(current_user, 'selected_org_id', current_user.org_id)
    
    if not check_stripe_connected(db, org_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Stripe not connected."
        )
    
    # Use Treasury Transactions if requested
    if use_treasury:
        from app.models.stripe_treasury_transaction import StripeTreasuryTransaction, TreasuryTransactionStatus
        
        # Check if Treasury Transactions table has any data for this org
        from sqlalchemy import text
        treasury_count = db.query(StripeTreasuryTransaction).filter(
            StripeTreasuryTransaction.org_id == org_id
        ).count()
        
        # If no Treasury Transactions exist, fall back to old payment system
        if treasury_count == 0:
            print(f"[PAYMENTS] No Treasury Transactions found for org {org_id}, falling back to StripePayment")
            use_treasury = False
        else:
            print(f"[PAYMENTS] Found {treasury_count} Treasury Transactions for org {org_id}, using Treasury Transactions")
        
    if use_treasury:
        # CRITICAL: Filter by org_id for multi-tenant isolation (use selected org from token)
        # Use raw SQL to avoid SQLAlchemy enum name conversion
        from sqlalchemy import text
        query = db.query(StripeTreasuryTransaction).filter(
            StripeTreasuryTransaction.org_id == org_id
        ).filter(
            text("stripe_treasury_transactions.status = 'posted'::treasurytransactionstatus")
        ).join(Client, StripeTreasuryTransaction.client_id == Client.id, isouter=True)
        
        if status_filter:
            # Map status filter to Treasury status - use raw SQL
            if status_filter == "succeeded":
                query = query.filter(text("stripe_treasury_transactions.status = 'posted'::treasurytransactionstatus"))
            elif status_filter == "failed":
                query = query.filter(text("stripe_treasury_transactions.status = 'void'::treasurytransactionstatus"))
            elif status_filter == "pending":
                query = query.filter(text("stripe_treasury_transactions.status = 'open'::treasurytransactionstatus"))
        
        # Filter by date range if provided
        if range_days is not None:
            end_date = datetime.utcnow()
            start_date = end_date - timedelta(days=range_days)
            query = query.filter(
                and_(
                    StripeTreasuryTransaction.posted_at >= start_date,
                    StripeTreasuryTransaction.posted_at <= end_date
                )
            )
        
        # Get all transactions, then deduplicate
        all_transactions = query.order_by(desc(StripeTreasuryTransaction.posted_at or StripeTreasuryTransaction.created)).all()

        # Only inbound (positive amount)
        inbound = [t for t in all_transactions if t.amount > 0]

        # First pass: one transaction per normalized id (same as StripePayment path).
        # Canonical id = normalized flow_id when present, else normalized stripe_transaction_id,
        # so trxn_xxx / ic_xxx / obt_xxx with same suffix collapse to one.
        def canonical_id(t):
            n_flow = normalize_stripe_id_for_dedup(t.flow_id) if t.flow_id else None
            n_txn = normalize_stripe_id_for_dedup(t.stripe_transaction_id) if t.stripe_transaction_id else None
            return n_flow or n_txn or ("_no_id_%s" % t.id)

        by_canonical = {}
        for t in inbound:
            cid = canonical_id(t)
            if cid not in by_canonical:
                by_canonical[cid] = t
            else:
                existing = by_canonical[cid]
                ex_ts = (existing.posted_at or existing.created).timestamp() if (existing.posted_at or existing.created) else 0
                new_ts = (t.posted_at or t.created).timestamp() if (t.posted_at or t.created) else 0
                if new_ts > ex_ts:
                    by_canonical[cid] = t

        deduplicated = list(by_canonical.values())
        deduplicated.sort(key=lambda t: (t.posted_at or t.created).timestamp() if (t.posted_at or t.created) else 0, reverse=True)
        
        # Apply pagination after deduplication
        total = len(deduplicated)
        transactions = deduplicated[(page - 1) * page_size:page * page_size]
        
        result = []
        for transaction in transactions:
            client = db.query(Client).filter(Client.id == transaction.client_id).first() if transaction.client_id else None
            
            # Map Treasury transaction to payment response
            # Use flow_id or transaction_id for subscription column
            display_id = transaction.flow_id or transaction.stripe_transaction_id
            if transaction.flow_type and transaction.flow_type.value in ['received_credit', 'inbound_transfer']:
                display_id = f"Flow: {display_id}"
            else:
                display_id = f"Transaction: {display_id}"
            
            result.append(StripePaymentResponse(
                id=str(transaction.id),
                stripe_id=transaction.stripe_transaction_id,
                client_id=str(transaction.client_id) if transaction.client_id else None,
                client_name=f"{client.first_name or ''} {client.last_name or ''}".strip() if client else None,
                client_email=(client.email if client else None) or transaction.customer_email,
                amount_cents=abs(transaction.amount),  # Use absolute value for display
                currency=transaction.currency or "usd",
                status="succeeded" if str(transaction.status) == "posted" else str(transaction.status),
                subscription_id=display_id,
                receipt_url=None,  # Treasury transactions don't have receipt URLs
                created_at=int((transaction.posted_at or transaction.created).timestamp()) if (transaction.posted_at or transaction.created) else 0
            ))
        
        return result
    
    # Fallback to old payment system (when use_treasury=False)
    # CRITICAL: Filter by org_id for multi-tenant isolation (use selected org from token)
    query = db.query(StripePayment).filter(
        StripePayment.org_id == org_id
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
    
    # First pass: one payment per normalized stripe_id (first 17 chars of suffix for dedup)
    type_priority = {'charge': 0, 'payment_intent': 1, 'invoice': 2}
    by_norm_id = {}
    no_stripe_id = []
    for p in all_payments:
        norm = normalize_stripe_id_for_dedup(p.stripe_id) if p.stripe_id else None
        if not norm:
            no_stripe_id.append(p)
            continue
        if norm not in by_norm_id:
            by_norm_id[norm] = p
        else:
            existing = by_norm_id[norm]
            ex_pri = type_priority.get(existing.type, 3)
            new_pri = type_priority.get(p.type, 3)
            ex_ts = existing.created_at.timestamp() if existing.created_at else 0.0
            new_ts = p.created_at.timestamp() if p.created_at else 0.0
            if new_pri < ex_pri or (new_pri == ex_pri and new_ts > ex_ts):
                by_norm_id[norm] = p
    all_payments = list(by_norm_id.values()) + no_stripe_id

    seen_stripe_ids = set()
    deduplicated = []
    payment_map = {}

    all_payments.sort(key=lambda p: (
        {'charge': 0, 'payment_intent': 1, 'invoice': 2}.get(p.type, 3),
        -(p.created_at.timestamp() if p.created_at else 0)
    ))

    for payment in all_payments:
        norm_stripe = normalize_stripe_id_for_dedup(payment.stripe_id) if payment.stripe_id else None
        if norm_stripe and norm_stripe in seen_stripe_ids:
            continue
        if norm_stripe:
            seen_stripe_ids.add(norm_stripe)

        if payment.status != 'succeeded':
            deduplicated.append(payment)
            continue

        if payment.subscription_id and payment.invoice_id:
            key = ('subscription_invoice', normalize_stripe_id_for_dedup(payment.subscription_id), normalize_stripe_id_for_dedup(payment.invoice_id))
        elif payment.invoice_id:
            key = ('invoice', normalize_stripe_id_for_dedup(payment.invoice_id))
        elif payment.subscription_id:
            key = ('subscription', normalize_stripe_id_for_dedup(payment.subscription_id))
        else:
            deduplicated.append(payment)
            continue

        if key not in payment_map:
            payment_map[key] = payment
            deduplicated.append(payment)
        else:
            existing_payment = payment_map[key]
            print(f"[PAYMENTS] Skipping duplicate payment {payment.stripe_id} (type: {payment.type}) - keeping {existing_payment.stripe_id} (type: {existing_payment.type}) for {key[0]} {key[1]}")
    
    # Sort deduplicated payments by date (most recent first) before pagination
    deduplicated.sort(key=lambda p: p.created_at.timestamp() if p.created_at else 0, reverse=True)
    
    # Apply pagination after deduplication
    total = len(deduplicated)
    payments = deduplicated[(page - 1) * page_size:page * page_size]
    
    result = []
    for payment in payments:
        client = db.query(Client).filter(Client.id == payment.client_id).first() if payment.client_id else None
        
        # For subscription_id field: use subscription_id if available, otherwise invoice_id, otherwise stripe_id
        display_subscription_id = payment.subscription_id
        if not display_subscription_id:
            # If no subscription_id, show invoice_id if available
            if payment.invoice_id:
                display_subscription_id = f"Invoice: {payment.invoice_id}"
            else:
                # If no invoice_id either, show the payment ID
                display_subscription_id = f"Payment: {payment.stripe_id}"
        
        result.append(StripePaymentResponse(
            id=str(payment.id),
            stripe_id=payment.stripe_id,
            client_id=str(payment.client_id) if payment.client_id else None,
            client_name=f"{client.first_name or ''} {client.last_name or ''}".strip() if client else None,
            client_email=client.email if client else None,
            amount_cents=payment.amount_cents or 0,
            currency=payment.currency or "usd",
            status=payment.status,
            subscription_id=display_subscription_id,
            receipt_url=payment.receipt_url,
            created_at=int(payment.created_at.timestamp()) if payment.created_at else 0
        ))
    
    return result


@router.get("/failed-payments", response_model=List[StripeFailedPaymentResponse])
def get_failed_payments(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    use_treasury: bool = Query(True, description="Use Treasury Transactions API as source of truth"),
    exclude_resolved: bool = Query(False, description="Exclude resolved payments (for terminal queue)"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Get failed payments queue.
    Uses Treasury Transactions API as source of truth when use_treasury=True.
    Failed payments are transactions with status='void' or negative amounts that failed.
    
    If exclude_resolved=True, only returns payments that haven't been resolved (no REJECTED recommendations).
    This is used for the terminal queue, while the Stripe dashboard shows all failed payments.
    """
    # Get selected org_id from user object (set by get_current_user)
    org_id = getattr(current_user, 'selected_org_id', current_user.org_id)
    
    if not check_stripe_connected(db, org_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Stripe not connected."
        )
    
    # CRITICAL: Filter by org_id for multi-tenant isolation (use selected org from token)
    
    # Use Treasury Transactions if requested
    if use_treasury:
        # Check if Treasury Transactions table has any data for this org
        from sqlalchemy import text
        treasury_count = db.query(StripeTreasuryTransaction).filter(
            StripeTreasuryTransaction.org_id == org_id
        ).count()
        
        # If no Treasury Transactions exist, fall back to old payment system
        if treasury_count == 0:
            print(f"[FAILED PAYMENTS] No Treasury Transactions found for org {org_id}, falling back to StripePayment")
            use_treasury = False
        else:
            print(f"[FAILED PAYMENTS] Found {treasury_count} Treasury Transactions for org {org_id}, using Treasury Transactions")
    
    if use_treasury:
        # Get voided transactions (failed payments)
        # Use raw SQL to avoid SQLAlchemy enum name conversion
        from sqlalchemy import text
        all_failed_transactions = db.query(StripeTreasuryTransaction).filter(
            StripeTreasuryTransaction.org_id == org_id
        ).filter(
            text("stripe_treasury_transactions.status = 'void'::treasurytransactionstatus")
        ).order_by(desc(StripeTreasuryTransaction.created)).all()
        
        # Group by flow_id to condense retry attempts
        grouped_transactions = {}
        
        for transaction in all_failed_transactions:
            if transaction.flow_id:
                group_key = normalize_stripe_id_for_dedup(transaction.flow_id)
            elif transaction.customer_email:
                group_key = f"email_{transaction.customer_email}"
            else:
                group_key = normalize_stripe_id_for_dedup(transaction.stripe_transaction_id) or transaction.stripe_transaction_id
            
            if group_key not in grouped_transactions:
                grouped_transactions[group_key] = {
                    'transactions': [],
                    'first_attempt': transaction.created,
                    'latest_attempt': transaction.created
                }
            
            grouped_transactions[group_key]['transactions'].append(transaction)
            
            # Track first and latest attempt dates
            if transaction.created < grouped_transactions[group_key]['first_attempt']:
                grouped_transactions[group_key]['first_attempt'] = transaction.created
            if transaction.created > grouped_transactions[group_key]['latest_attempt']:
                grouped_transactions[group_key]['latest_attempt'] = transaction.created
        
        # Convert grouped transactions to result list
        result = []
        for group_key, group_data in grouped_transactions.items():
            # Use the FIRST transaction (earliest) as the representative to show initial date
            representative = min(group_data['transactions'], key=lambda t: t.created)
            attempt_count = len(group_data['transactions'])
            
            client = db.query(Client).filter(Client.id == representative.client_id).first() if representative.client_id else None
            
            # Check if recovery recommendation exists
            recovery = None
            rejected_recovery = None
            if representative.client_id:
                recovery = db.query(Recommendation).filter(
                    and_(
                        Recommendation.org_id == org_id,
                        Recommendation.client_id == representative.client_id,
                        Recommendation.type == "payment_recovery",
                        Recommendation.status == "PENDING"
                    )
                ).first()
            
            # Check if payment is resolved (by payment_id in payload - works for all cases)
            if exclude_resolved:
                from sqlalchemy import text
                payment_id_str = str(representative.id)
                
                # First check by client_id if available
                if representative.client_id:
                    rejected_recovery = db.query(Recommendation).filter(
                        and_(
                            Recommendation.org_id == org_id,
                            Recommendation.client_id == representative.client_id,
                            Recommendation.type == "payment_recovery",
                            Recommendation.status == "REJECTED"
                        )
                    ).first()
                
                # Also check by payment_id in payload (works for all cases, including no client_id)
                if not rejected_recovery:
                    rejected_by_payment_id = db.query(Recommendation).filter(
                        and_(
                            Recommendation.org_id == org_id,
                            Recommendation.type == "payment_recovery",
                            Recommendation.status == "REJECTED",
                            text("recommendations.payload->>'payment_id' = :payment_id")
                        )
                    ).params(payment_id=payment_id_str).first()
                    
                    if rejected_by_payment_id:
                        rejected_recovery = rejected_by_payment_id
            
            # Skip resolved payments if exclude_resolved is True
            if exclude_resolved and rejected_recovery:
                continue
            
            result.append(StripeFailedPaymentResponse(
                id=str(representative.id),
                stripe_id=representative.stripe_transaction_id,
                client_id=str(representative.client_id) if representative.client_id else None,
                client_name=f"{client.first_name or ''} {client.last_name or ''}".strip() if client else None,
                client_email=client.email if client else representative.customer_email,
                amount_cents=abs(representative.amount),
                currency=representative.currency or "usd",
                status="failed",
                subscription_id=representative.flow_id or representative.stripe_transaction_id,
                receipt_url=None,
                created_at=int(group_data['first_attempt'].timestamp()) if group_data['first_attempt'] else 0,  # Use first attempt date
                has_recovery_recommendation=recovery is not None,
                recovery_recommendation_id=str(recovery.id) if recovery else None,
                attempt_count=attempt_count,
                first_attempt_at=int(group_data['first_attempt'].timestamp()) if group_data['first_attempt'] else 0,
                latest_attempt_at=int(group_data['latest_attempt'].timestamp()) if group_data['latest_attempt'] else 0
            ))
        
        # Sort by latest attempt (most recent first) before pagination
        result.sort(key=lambda r: r.latest_attempt_at, reverse=True)
        
        # Apply pagination
        total = len(result)
        failed_payments = result[(page - 1) * page_size:page * page_size]
        
        return failed_payments
    
    # Fallback to old payment system
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
        # Group using normalized IDs (first 17 chars of suffix for dedup)
        if payment.subscription_id:
            group_key = ('sub', normalize_stripe_id_for_dedup(payment.subscription_id))
        elif payment.invoice_id:
            group_key = (None, normalize_stripe_id_for_dedup(payment.invoice_id))
        elif payment.client_id:
            group_key = (None, payment.client_id)
        else:
            group_key = ('stripe', normalize_stripe_id_for_dedup(payment.stripe_id) if payment.stripe_id else payment.stripe_id)
        
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
        
        representative_payment = min(group_data['payments'], key=lambda p: p.created_at)
        attempt_count = len(group_data['payments'])
        
        subscription_id = None
        client_id = None
        invoice_id = None
        
        if len(group_key) == 2 and group_key[0] == 'sub':
            subscription_id = representative_payment.subscription_id
        elif len(group_key) == 2 and group_key[0] == 'stripe':
            continue
        elif len(group_key) == 2:
            first, second = group_key
            if first is None:
                if isinstance(second, uuid.UUID):
                    client_id = second
                else:
                    invoice_id = representative_payment.invoice_id
        
        if not client_id:
            client_id = representative_payment.client_id
        if not subscription_id:
            subscription_id = representative_payment.subscription_id
        
        print(f"[DEBUG] Failed payment group: key={group_key}, attempt_count={attempt_count}, subscription_id={subscription_id}, client_id={client_id}")
        
        client = db.query(Client).filter(Client.id == representative_payment.client_id).first() if representative_payment.client_id else None
        
        # Check if recovery recommendation exists (filter by org_id)
        recovery = None
        rejected_recovery = None
        if representative_payment.client_id:
            recovery = db.query(Recommendation).filter(
                and_(
                    Recommendation.org_id == org_id,
                    Recommendation.client_id == representative_payment.client_id,
                    Recommendation.type == "payment_recovery",
                    Recommendation.status == "PENDING"
                )
            ).first()
            
        # Check if payment is resolved (by payment_id in payload - works for all cases)
        if exclude_resolved:
            from sqlalchemy import text
            payment_id_str = str(representative_payment.id)
            
            # First check by client_id if available
            if representative_payment.client_id:
                rejected_recovery = db.query(Recommendation).filter(
                    and_(
                        Recommendation.org_id == org_id,
                        Recommendation.client_id == representative_payment.client_id,
                        Recommendation.type == "payment_recovery",
                        Recommendation.status == "REJECTED"
                    )
                ).first()
            
            # Also check by payment_id in payload (works for all cases, including no client_id)
            if not rejected_recovery:
                rejected_by_payment_id = db.query(Recommendation).filter(
                    and_(
                        Recommendation.org_id == org_id,
                        Recommendation.type == "payment_recovery",
                        Recommendation.status == "REJECTED",
                        text("recommendations.payload->>'payment_id' = :payment_id")
                    )
                ).params(payment_id=payment_id_str).first()
                
                if rejected_by_payment_id:
                    rejected_recovery = rejected_by_payment_id
        
        # Skip resolved payments if exclude_resolved is True
        if exclude_resolved and rejected_recovery:
            continue
        
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
            created_at=int(group_data['first_attempt'].timestamp()) if group_data['first_attempt'] else 0,  # Use first attempt date
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
    # Get selected org_id from user object (set by get_current_user)
    org_id = getattr(current_user, 'selected_org_id', current_user.org_id)
    
    if not check_stripe_connected(db, org_id):
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
    
    # Verify client belongs to selected org
    client = db.query(Client).filter(
        Client.id == client_uuid,
        Client.org_id == org_id
    ).first()
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
    # Get selected org_id from user object (set by get_current_user)
    org_id = getattr(current_user, 'selected_org_id', current_user.org_id)
    
    if not check_stripe_connected(db, org_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Stripe not connected."
        )
    
    # Calculate monthly churn rate for last N months
    # NEW LOGIC: Track churn by client lifecycle, not by subscription
    # - Subscription churn: immediate when subscription period ends
    # - One-off payment churn: 30-day grace period after last payment
    # - A client can only churn once per lifecycle
    # - A new lifecycle starts when a client makes a payment after churning
    churn_data = []
    cohort_data = []
    
    # CRITICAL: Filter by org_id for multi-tenant isolation (use selected org from token)
    # First payment date per client (any type) for new-customers-per-month
    payment_first_all = db.query(
        StripePayment.client_id,
        func.min(StripePayment.created_at).label("first_ts"),
    ).filter(
        and_(
            StripePayment.org_id == org_id,
            StripePayment.status == "succeeded",
            StripePayment.client_id.isnot(None),
        )
    ).group_by(StripePayment.client_id).all()
    first_by_client_churn = {}
    for (cid, ts) in payment_first_all:
        if cid and ts:
            first_by_client_churn[cid] = min(first_by_client_churn.get(cid, ts), ts) if cid in first_by_client_churn else ts
    try:
        from sqlalchemy import text
        treasury_first_all = db.query(
            StripeTreasuryTransaction.client_id,
            func.min(
                func.coalesce(StripeTreasuryTransaction.posted_at, StripeTreasuryTransaction.created)
            ).label("first_ts"),
        ).filter(
            and_(
                StripeTreasuryTransaction.org_id == org_id,
                StripeTreasuryTransaction.client_id.isnot(None),
                StripeTreasuryTransaction.amount > 0,
            )
        ).filter(
            text("stripe_treasury_transactions.status = 'posted'::treasurytransactionstatus")
        ).group_by(StripeTreasuryTransaction.client_id).all()
        for (cid, ts) in treasury_first_all:
            if cid and ts:
                first_by_client_churn[cid] = min(first_by_client_churn.get(cid, ts), ts) if cid in first_by_client_churn else ts
    except Exception:
        pass
    
    # Track clients who have churned (to prevent double-counting in same lifecycle)
    # Key: client_id, Value: (churn_date, lifecycle_start_date)
    # We'll process months from oldest to newest to track lifecycles properly
    client_churn_history = {}  # client_id -> list of (churn_date, next_payment_date) tuples
    
    # Process months from oldest to newest (reverse order)
    for i in reversed(range(months)):
        month_start = datetime.utcnow().replace(day=1) - timedelta(days=30 * i)
        month_end = (month_start + timedelta(days=32)).replace(day=1) - timedelta(seconds=1)
        
        # Track churned clients (unique per lifecycle)
        # A client can only churn once per lifecycle
        churned_client_ids = set()
        
        # 1. SUBSCRIPTION CHURN: Find clients whose subscription ended in this month
        # A subscription cancel = immediate churn and end of lifecycle
        canceled_subs = db.query(StripeSubscription).filter(
            and_(
                StripeSubscription.org_id == org_id,
                StripeSubscription.status == "canceled",
                StripeSubscription.current_period_end.isnot(None),
                StripeSubscription.current_period_end >= month_start,
                StripeSubscription.current_period_end <= month_end,
                StripeSubscription.client_id.isnot(None)
            )
        ).all()
        
        for canceled_sub in canceled_subs:
            client_id = canceled_sub.client_id
            if not client_id:
                continue
                
            period_end_date = canceled_sub.current_period_end
            
            # Check if client has a new active subscription created after period ended
            # This would indicate an upsell/replacement, not a true churn
            has_replacement = db.query(StripeSubscription).filter(
                and_(
                    StripeSubscription.org_id == org_id,
                    StripeSubscription.client_id == client_id,
                    StripeSubscription.status == "active",
                    StripeSubscription.created_at > period_end_date
                )
            ).first()
            
            # Only count as churn if no replacement subscription exists
            if not has_replacement:
                # Check lifecycle: Has this client already churned in a previous month?
                # If they made a payment/subscription AFTER a previous churn, they started a new lifecycle
                # We need to check if there was a previous churn and if they've made payments since
                
                # Find the most recent payment/subscription before this churn
                # If there was a gap (previous churn), this is a new lifecycle
                # For now, we'll count subscription churn immediately (each cancel = new lifecycle end)
                churned_client_ids.add(client_id)
        
        # 2. ONE-OFF PAYMENT CHURN: Find clients whose last payment was 30+ days before month_end
        # and they haven't made a new payment since
        # Calculate the cutoff date: 30 days before month_end
        grace_period_cutoff = month_end - timedelta(days=30)
        
        # Get all clients who have made one-off payments (no subscription)
        # Group by client and get their last payment date
        clients_with_payments = db.query(
            StripePayment.client_id,
            func.max(StripePayment.created_at).label('last_payment_date')
        ).filter(
            and_(
                StripePayment.org_id == org_id,
                StripePayment.status == 'succeeded',
                StripePayment.client_id.isnot(None),
                # Only consider one-off payments (no subscription_id)
                StripePayment.subscription_id.is_(None)
            )
        ).group_by(StripePayment.client_id).all()
        
        for client_id, last_payment_date in clients_with_payments:
            if not client_id or not last_payment_date:
                continue
            
            # Skip if already counted as subscription churn
            if client_id in churned_client_ids:
                continue
            
            # Check if last payment was before the grace period cutoff
            if last_payment_date < grace_period_cutoff:
                # Check if client made any payment after last_payment_date but before month_end
                # If they did, they didn't churn (they renewed within grace period)
                has_renewal = db.query(StripePayment).filter(
                    and_(
                        StripePayment.org_id == org_id,
                        StripePayment.client_id == client_id,
                        StripePayment.status == 'succeeded',
                        StripePayment.created_at > last_payment_date,
                        StripePayment.created_at <= month_end
                    )
                ).first()
                
                if not has_renewal:
                    # Client churned: last payment was 30+ days ago and no renewal
                    # The churn date is last_payment_date + 30 days
                    churn_date = last_payment_date + timedelta(days=30)
                    
                    # Only count churn if the churn date falls within this month
                    if month_start <= churn_date <= month_end:
                        # Check lifecycle: Has this client already churned in this lifecycle?
                        can_churn = True
                        if client_id in client_churn_history:
                            # Client has churned before - check if they made a payment after last churn
                            last_churn_info = client_churn_history[client_id]
                            if last_churn_info:
                                last_churn_date = last_churn_info[0]
                                # Check if client made any payment after last churn but before this churn
                                has_payment_after_churn = db.query(StripePayment).filter(
                                    and_(
                                        StripePayment.org_id == org_id,
                                        StripePayment.client_id == client_id,
                                        StripePayment.status == 'succeeded',
                                        StripePayment.created_at > last_churn_date,
                                        StripePayment.created_at <= churn_date
                                    )
                                ).first()
                                
                                has_sub_after_churn = db.query(StripeSubscription).filter(
                                    and_(
                                        StripeSubscription.org_id == org_id,
                                        StripeSubscription.client_id == client_id,
                                        StripeSubscription.created_at > last_churn_date,
                                        StripeSubscription.created_at <= churn_date
                                    )
                                ).first()
                                
                                # If no payment/subscription after last churn, they're still in same lifecycle
                                if not has_payment_after_churn and not has_sub_after_churn:
                                    can_churn = False  # Already churned in this lifecycle
                        
                        if can_churn:
                            churned_client_ids.add(client_id)
                            # Record this churn for lifecycle tracking
                            client_churn_history[client_id] = (churn_date, None)
        
        # Count unique churned clients (one churn per lifecycle)
        canceled = len(churned_client_ids)
        
        # Count active clients at start of month (clients with active subscriptions OR recent payments)
        # Active = has active subscription OR made payment in last 30 days (as of month_start)
        active_subscription_clients = db.query(func.count(func.distinct(StripeSubscription.client_id))).filter(
            and_(
                StripeSubscription.org_id == org_id,
                StripeSubscription.status == "active",
                StripeSubscription.client_id.isnot(None),
                StripeSubscription.created_at < month_end
            )
        ).scalar() or 0
        
        # Clients with payments in the 30 days before month_start (for one-off payment clients)
        payment_cutoff = month_start - timedelta(days=30)
        active_payment_clients = db.query(func.count(func.distinct(StripePayment.client_id))).filter(
            and_(
                StripePayment.org_id == org_id,
                StripePayment.status == 'succeeded',
                StripePayment.client_id.isnot(None),
                StripePayment.subscription_id.is_(None),  # One-off payments only
                StripePayment.created_at >= payment_cutoff,
                StripePayment.created_at < month_start
            )
        ).scalar() or 0
        
        # Combine active clients (union of subscription and payment clients)
        # We'll use subscription count as primary metric for now
        active = active_subscription_clients
        
        # Count new customers in this month (first payment in month, any type)
        new_customers_month = sum(
            1 for _, first_ts in first_by_client_churn.items()
            if month_start <= first_ts <= month_end
        )
        
        churn_rate = (canceled / active * 100) if active > 0 else 0
        
        # Append data (will be reversed later to show newest first)
        churn_data.append({
            "month": month_start.strftime("%Y-%m"),
            "churn_rate": churn_rate,
            "canceled": canceled,
            "active": active
        })
        
        cohort_data.append({
            "month": month_start.strftime("%Y-%m"),
            "new_customers": new_customers_month,
            "churned": canceled  # Unique clients who churned in this month
        })
    
    # Reverse to show newest month first (since we processed oldest to newest)
    churn_data.reverse()
    cohort_data.reverse()
    
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
    # Get selected org_id from user object (set by get_current_user)
    org_id = getattr(current_user, 'selected_org_id', current_user.org_id)
    
    if not check_stripe_connected(db, org_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Stripe not connected."
        )
    
    start_date = datetime.utcnow() - timedelta(days=days)
    
    # CRITICAL: Filter by org_id for multi-tenant isolation (use selected org from token)
    
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
    # Get selected org_id from user object (set by get_current_user)
    org_id = getattr(current_user, 'selected_org_id', current_user.org_id)
    
    if not check_stripe_connected(db, org_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Stripe not connected."
        )
    
    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=range_days)
    
    # CRITICAL: Filter by org_id for multi-tenant isolation (use selected org from token)
    
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


def _build_duplicate_group(key: str, payments: list, recommended_keep) -> DuplicatePaymentGroup:
    total_amount = sum(p.amount_cents for p in payments)
    payments_detail = [
        DuplicatePaymentEntry(
            payment_id=str(p.id),
            stripe_id=p.stripe_id or "",
            suffix=normalize_stripe_id(p.stripe_id) if p.stripe_id else "",
            type=p.type,
            amount_cents=p.amount_cents or 0
        )
        for p in payments
    ]
    return DuplicatePaymentGroup(
        key=key,
        payment_ids=[str(p.id) for p in payments],
        payments_detail=payments_detail,
        count=len(payments),
        total_amount_cents=total_amount,
        recommended_keep_id=str(recommended_keep.id)
    )


@router.get("/payments/duplicates", response_model=DuplicatePaymentsResponse)
def find_duplicate_payments(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Find duplicate payments using: (1) same normalized stripe_id suffix, (2) same subscription+invoice, (3) same invoice.
    Returns groups with full stripe_id and suffix per payment. Full ids shown (no ellipsis) for debugging.
    """
    org_id = getattr(current_user, 'selected_org_id', current_user.org_id)

    if not check_stripe_connected(db, org_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Stripe not connected."
        )

    all_payments = db.query(StripePayment).filter(
        StripePayment.org_id == org_id,
        StripePayment.status == 'succeeded'
    ).all()

    seen_payment_sets = set()  # frozenset of payment ids, so we don't show the same duplicate set twice
    groups = []
    total_duplicates = 0

    def add_group(key: str, payments: list):
        if len(payments) <= 1:
            return
        ids_set = frozenset(str(p.id) for p in payments)
        if ids_set in seen_payment_sets:
            return
        seen_payment_sets.add(ids_set)
        payments.sort(key=lambda p: (
            {'charge': 0, 'payment_intent': 1, 'invoice': 2}.get(p.type, 3),
            -(p.updated_at.timestamp() if p.updated_at else 0)
        ))
        groups.append(_build_duplicate_group(key, list(payments), payments[0]))
        nonlocal total_duplicates
        total_duplicates += len(payments) - 1

    # (1) Group by normalized stripe_id for dedup (first 17 chars of suffix)
    by_suffix = defaultdict(list)
    for p in all_payments:
        if p.stripe_id:
            s = normalize_stripe_id_for_dedup(p.stripe_id)
            if s:
                by_suffix[s].append(p)
    for suffix_key, payments in by_suffix.items():
        add_group(f"suffix:{suffix_key}", payments)

    # (2) Group by (subscription_id, invoice_id) normalized for dedup
    by_sub_inv = defaultdict(list)
    for p in all_payments:
        if p.subscription_id and p.invoice_id:
            k = (normalize_stripe_id_for_dedup(p.subscription_id), normalize_stripe_id_for_dedup(p.invoice_id))
            by_sub_inv[k].append(p)
    for (ns, ni), payments in by_sub_inv.items():
        add_group(f"subscription_invoice:{ns}:{ni}", payments)

    # (3) Group by invoice_id normalized for dedup
    by_inv = defaultdict(list)
    for p in all_payments:
        if p.invoice_id:
            k = normalize_stripe_id_for_dedup(p.invoice_id)
            if k:
                by_inv[k].append(p)
    for inv_key, payments in by_inv.items():
        add_group(f"invoice:{inv_key}", payments)

    return DuplicatePaymentsResponse(
        total_groups=len(groups),
        total_duplicates=total_duplicates,
        groups=groups
    )


@router.post("/payments/merge-duplicates", response_model=MergeDuplicatesResponse)
def merge_duplicate_payments(
    request: MergeDuplicatesRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Delete duplicate payments and reconcile data.
    
    This will:
    1. Delete the specified payment IDs
    2. Optionally run reconciliation to recalculate lifetime revenue and other metrics
    
    WARNING: This permanently deletes payments. Make sure you're deleting the correct duplicates.
    """
    # Get selected org_id from user object (set by get_current_user)
    org_id = getattr(current_user, 'selected_org_id', current_user.org_id)
    
    if not check_stripe_connected(db, org_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Stripe not connected."
        )
    
    if not request.payment_ids:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No payment IDs provided"
        )
    
    deleted_count = 0
    errors = []
    
    try:
        # Delete each payment (only if it belongs to this org)
        for payment_id_str in request.payment_ids:
            try:
                payment_id = uuid.UUID(payment_id_str)
                payment = db.query(StripePayment).filter(
                    StripePayment.id == payment_id,
                    StripePayment.org_id == org_id
                ).first()
                
                if not payment:
                    errors.append(f"Payment {payment_id_str} not found or doesn't belong to your organization")
                    continue
                
                # Log before deletion
                print(f"[MERGE] Deleting duplicate payment {payment.stripe_id} (id: {payment_id_str})")
                
                db.delete(payment)
                deleted_count += 1
                
            except ValueError:
                errors.append(f"Invalid payment ID: {payment_id_str}")
            except Exception as e:
                errors.append(f"Error deleting payment {payment_id_str}: {str(e)}")
        
        # Commit deletions
        db.commit()
        print(f"[MERGE] Deleted {deleted_count} duplicate payments")
        
        # Run reconciliation if requested
        reconciliation_result = None
        if request.auto_reconcile:
            try:
                from app.services.stripe_sync_v2 import reconcile_stripe_data
                print(f"[MERGE] Running reconciliation after deletion...")
                reconciliation_result = reconcile_stripe_data(db, org_id=org_id)
                print(f"[MERGE] Reconciliation complete: {reconciliation_result}")
            except Exception as e:
                print(f"[MERGE] ⚠️  Reconciliation failed (non-fatal): {str(e)}")
                import traceback
                traceback.print_exc()
        
        if errors:
            print(f"[MERGE] ⚠️  Encountered {len(errors)} errors: {errors}")
        
        return MergeDuplicatesResponse(
            deleted_count=deleted_count,
            reconciliation=reconciliation_result
        )
        
    except Exception as e:
        db.rollback()
        import traceback
        error_msg = f"Failed to merge duplicates: {str(e)}"
        print(f"[MERGE] ❌ {error_msg}")
        print(traceback.format_exc())
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=error_msg
        )


@router.patch("/payments/{payment_id}/assign", status_code=status.HTTP_200_OK)
def assign_payment_to_client(
    payment_id: str,
    client_id: str = Query(..., description="Client ID to assign this payment to"),
    auto_reconcile: bool = Query(True, description="Automatically reconcile after assignment"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Assign a payment to a specific client.
    
    Accepts either StripePayment.id or StripeTreasuryTransaction.id so that assigning
    works when the Recent payments list is sourced from Treasury (e.g. time range view).
    After assignment, optionally runs reconciliation to update client lifetime revenue.
    """
    # Get selected org_id from user object (set by get_current_user)
    org_id = getattr(current_user, 'selected_org_id', current_user.org_id)
    
    if not check_stripe_connected(db, org_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Stripe not connected."
        )
    
    try:
        payment_uuid = uuid.UUID(payment_id)
        payment = db.query(StripePayment).filter(
            StripePayment.id == payment_uuid,
            StripePayment.org_id == org_id
        ).first()
        transaction = None
        if not payment:
            # Recent payments list may be from Treasury when use_treasury=True (e.g. 30-day view)
            transaction = db.query(StripeTreasuryTransaction).filter(
                StripeTreasuryTransaction.id == payment_uuid,
                StripeTreasuryTransaction.org_id == org_id
            ).first()
            if not transaction:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Payment with ID {payment_id} not found or doesn't belong to your organization."
                )
        
        # Validate client exists and belongs to this org
        client_uuid = uuid.UUID(client_id)
        client = db.query(Client).filter(
            Client.id == client_uuid,
            Client.org_id == org_id
        ).first()
        
        if not client:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Client with ID {client_id} not found or doesn't belong to your organization."
            )
        
        if payment:
            payment.client_id = client_uuid
            payment.updated_at = datetime.utcnow()
            db.commit()
            print(f"[ASSIGN] Assigned StripePayment {payment.stripe_id} (ID: {payment_id}) to client {client.email or client.id} (ID: {client_id})")
        else:
            transaction.client_id = client_uuid
            transaction.updated_at = datetime.utcnow()
            db.commit()
            print(f"[ASSIGN] Assigned Treasury transaction {transaction.stripe_transaction_id} (ID: {payment_id}) to client {client.email or client.id} (ID: {client_id})")
        
        # Run reconciliation if requested
        reconciliation_result = None
        if auto_reconcile:
            try:
                from app.services.stripe_sync_v2 import reconcile_stripe_data
                print(f"[ASSIGN] Running reconciliation after assignment...")
                reconciliation_result = reconcile_stripe_data(db, org_id=org_id)
                print(f"[ASSIGN] Reconciliation complete: {reconciliation_result}")
            except Exception as e:
                print(f"[ASSIGN] ⚠️  Reconciliation failed (non-fatal): {str(e)}")
                import traceback
                traceback.print_exc()
        
        return {
            "success": True,
            "message": f"Payment assigned to {client.first_name or ''} {client.last_name or ''}".strip() or client.email or "client",
            "payment_id": payment_id,
            "client_id": client_id,
            "client_name": f"{client.first_name or ''} {client.last_name or ''}".strip() or client.email or "Unknown",
            "reconciliation": reconciliation_result
        }
        
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid payment ID or client ID format."
        )
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        import traceback
        error_msg = f"Failed to assign payment: {str(e)}"
        print(f"[ASSIGN] ❌ {error_msg}")
        print(traceback.format_exc())
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=error_msg
        )


@router.post("/failed-payments/{payment_id}/resolve", status_code=status.HTTP_200_OK)
def resolve_failed_payment_alert(
    payment_id: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Resolve a failed payment alert by marking it as resolved.
    
    This marks the associated recommendation as rejected, which removes it from the terminal queue
    but keeps it visible in the Stripe dashboard. The payment itself is NOT deleted.
    """
    # Get selected org_id from user object (set by get_current_user)
    org_id = getattr(current_user, 'selected_org_id', current_user.org_id)
    
    if not check_stripe_connected(db, org_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Stripe not connected."
        )
    
    try:
        # Find the payment to get client_id for recommendation lookup
        payment_uuid = uuid.UUID(payment_id)
        client_id_for_recommendation = None
        
        # Try StripePayment table first
        payment = db.query(StripePayment).filter(
            StripePayment.id == payment_uuid,
            StripePayment.org_id == org_id
        ).first()
        
        if payment:
            client_id_for_recommendation = payment.client_id
            stripe_id = payment.stripe_id
            print(f"[RESOLVE] Resolving failed payment alert for StripePayment {stripe_id} (ID: {payment_id})")
        else:
            # Try Treasury Transactions table
            transaction = db.query(StripeTreasuryTransaction).filter(
                and_(
                    StripeTreasuryTransaction.id == payment_uuid,
                    StripeTreasuryTransaction.org_id == org_id
                )
            ).first()
            
            if transaction:
                client_id_for_recommendation = transaction.client_id
                transaction_id = transaction.stripe_transaction_id
                print(f"[RESOLVE] Resolving failed payment alert for Treasury transaction {transaction_id} (ID: {payment_id})")
            else:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Payment with ID {payment_id} not found or doesn't belong to your organization."
                )
        
        # Mark any associated recovery recommendations as rejected, or create one if it doesn't exist
        if client_id_for_recommendation:
            recommendation = db.query(Recommendation).filter(
                and_(
                    Recommendation.org_id == org_id,
                    Recommendation.client_id == client_id_for_recommendation,
                    Recommendation.type == "payment_recovery",
                    Recommendation.status == "PENDING"
                )
            ).first()
            
            if recommendation:
                # Mark existing recommendation as rejected
                recommendation.status = RecommendationStatus.REJECTED
                db.commit()
                print(f"[RESOLVE] Marked recommendation {recommendation.id} as rejected (payment resolved)")
            else:
                # Check if there's already a rejected recommendation
                existing_rejected = db.query(Recommendation).filter(
                    and_(
                        Recommendation.org_id == org_id,
                        Recommendation.client_id == client_id_for_recommendation,
                        Recommendation.type == "payment_recovery",
                        Recommendation.status == "REJECTED"
                    )
                ).first()
                
                if not existing_rejected:
                    # Create a rejected recommendation to mark this payment as resolved
                    # This ensures the filtering logic works even if no recommendation existed before
                    # Store payment_id in payload so we can find it even if client_id changes
                    resolved_recommendation = Recommendation(
                        org_id=org_id,
                        client_id=client_id_for_recommendation,
                        type="payment_recovery",
                        status=RecommendationStatus.REJECTED,
                        payload={
                            "resolved_manually": True,
                            "payment_id": payment_id,
                            "stripe_id": payment.stripe_id if payment else transaction.stripe_transaction_id if 'transaction' in locals() else None,
                            "resolved_at": datetime.utcnow().isoformat()
                        }
                    )
                    db.add(resolved_recommendation)
                    db.commit()
                    print(f"[RESOLVE] Created rejected recommendation {resolved_recommendation.id} to mark payment {payment_id} as resolved")
                else:
                    # Update existing rejected recommendation to ensure payment_id is in payload
                    if not existing_rejected.payload or existing_rejected.payload.get("payment_id") != payment_id:
                        existing_rejected.payload = existing_rejected.payload or {}
                        existing_rejected.payload["payment_id"] = payment_id
                        db.commit()
                        print(f"[RESOLVE] Updated rejected recommendation {existing_rejected.id} with payment_id {payment_id}")
                    else:
                        print(f"[RESOLVE] Payment {payment_id} already has a rejected recommendation")
        else:
            # Payment has no client_id - we can't create a recommendation without a client
            # For payments without client_id, we need to track resolution differently
            # We'll create a special recommendation with client_id=None but store payment_id in payload
            # Then update the filtering logic to check for these special recommendations
            from sqlalchemy import text
            existing_resolved = db.query(Recommendation).filter(
                and_(
                    Recommendation.org_id == org_id,
                    Recommendation.client_id.is_(None),
                    Recommendation.type == "payment_recovery",
                    Recommendation.status == "REJECTED",
                    text("recommendations.payload->>'payment_id' = :payment_id")
                )
            ).params(payment_id=payment_id).first()
            
            if not existing_resolved:
                # Create a special recommendation with client_id=None to track resolved payments without clients
                resolved_recommendation = Recommendation(
                    org_id=org_id,
                    client_id=None,  # No client, but we track by payment_id in payload
                    type="payment_recovery",
                    status=RecommendationStatus.REJECTED,
                    payload={
                        "resolved_manually": True,
                        "payment_id": payment_id,
                        "stripe_id": payment.stripe_id if payment else transaction.stripe_transaction_id if 'transaction' in locals() else None,
                        "resolved_at": datetime.utcnow().isoformat(),
                        "no_client": True
                    }
                )
                db.add(resolved_recommendation)
                db.commit()
                print(f"[RESOLVE] Created rejected recommendation {resolved_recommendation.id} to mark payment {payment_id} (no client) as resolved")
            else:
                print(f"[RESOLVE] Payment {payment_id} (no client) already has a rejected recommendation")
        
        return {
            "success": True,
            "message": "Failed payment alert resolved. It will no longer appear in the terminal queue but remains in the Stripe dashboard.",
            "payment_id": payment_id
        }
        
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid payment ID format."
        )
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        import traceback
        error_msg = f"Failed to resolve failed payment alert: {str(e)}"
        print(f"[RESOLVE] ❌ {error_msg}")
        print(traceback.format_exc())
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=error_msg
        )


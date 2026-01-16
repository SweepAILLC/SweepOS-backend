"""
Enhanced Stripe Analytics API - uses database data from webhook-processed events.
Provides KPIs, revenue timeline, subscriptions, payments, and failed payments queue.
"""
from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.orm import Session
from sqlalchemy import func, and_, or_, desc
from typing import List, Optional
from datetime import datetime, timedelta
from decimal import Decimal

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
)

router = APIRouter()


def check_stripe_connected(db: Session) -> bool:
    """Check if Stripe is connected via OAuth"""
    oauth_token = db.query(OAuthToken).filter(
        OAuthToken.provider == OAuthProvider.STRIPE
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
    oauth_token = db.query(OAuthToken).filter(
        OAuthToken.provider == OAuthProvider.STRIPE
    ).first()
    
    if oauth_token and oauth_token.access_token:
        return StripeConnectionStatus(
            connected=True,
            message="Stripe is connected.",
            account_id=oauth_token.account_id
        )
    return StripeConnectionStatus(connected=False, message="Stripe is not connected.")


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
    if not check_stripe_connected(db):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Stripe not connected. Please connect Stripe via OAuth first."
        )
    
    # Calculate date range
    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=range_days)
    prev_start_date = start_date - timedelta(days=range_days)
    
    # Get current MRR (from active subscriptions)
    current_mrr_result = db.query(func.sum(StripeSubscription.mrr)).filter(
        StripeSubscription.status == "active"
    ).scalar() or Decimal(0)
    current_mrr = float(current_mrr_result)
    
    # Get previous period MRR for comparison
    prev_mrr_result = db.query(func.sum(StripeSubscription.mrr)).filter(
        and_(
            StripeSubscription.status == "active",
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
            StripeSubscription.created_at >= start_date,
            StripeSubscription.created_at <= end_date
        )
    ).scalar() or 0
    
    # Count churned subscriptions in period
    churned_subscriptions = db.query(func.count(StripeSubscription.id)).filter(
        and_(
            StripeSubscription.status == "canceled",
            StripeSubscription.updated_at >= start_date,
            StripeSubscription.updated_at <= end_date
        )
    ).scalar() or 0
    
    # Count failed payments in period
    failed_payments = db.query(func.count(StripePayment.id)).filter(
        and_(
            StripePayment.status == "failed",
            StripePayment.created_at >= start_date,
            StripePayment.created_at <= end_date
        )
    ).scalar() or 0
    
    # Calculate revenue for period
    revenue_result = db.query(func.sum(StripePayment.amount_cents)).filter(
        and_(
            StripePayment.status == "succeeded",
            StripePayment.created_at >= start_date,
            StripePayment.created_at <= end_date
        )
    ).scalar() or 0
    revenue = float(revenue_result) / 100.0  # Convert from cents
    
    # Get active subscriptions count
    active_subscriptions = db.query(func.count(StripeSubscription.id)).filter(
        StripeSubscription.status == "active"
    ).scalar() or 0
    
    # Get total customers (unique clients with Stripe customer ID)
    total_customers = db.query(func.count(func.distinct(Client.id))).filter(
        Client.stripe_customer_id.isnot(None)
    ).scalar() or 0
    
    # Get recent subscriptions, invoices, customers, payments for backward compatibility
    recent_subscriptions = db.query(StripeSubscription).order_by(
        desc(StripeSubscription.created_at)
    ).limit(10).all()
    
    recent_payments = db.query(StripePayment).filter(
        StripePayment.status == "succeeded"
    ).order_by(desc(StripePayment.created_at)).limit(10).all()
    
    # Format for response (backward compatibility)
    subscriptions_list = []
    for sub in recent_subscriptions:
        subscriptions_list.append({
            "id": str(sub.id),
            "status": sub.status,
            "amount": int(sub.mrr * 100),  # Convert to cents
            "current_period_start": int(sub.current_period_start.timestamp()) if sub.current_period_start else None,
            "current_period_end": int(sub.current_period_end.timestamp()) if sub.current_period_end else None,
            "customer_id": sub.client_id
        })
    
    payments_list = []
    for payment in recent_payments:
        payments_list.append({
            "id": str(payment.id),
            "amount": payment.amount_cents,
            "status": payment.status,
            "created_at": int(payment.created_at.timestamp())
        })
    
    return StripeSummaryResponse(
        total_mrr=current_mrr,
        total_arr=arr,
        mrr_change=mrr_change,
        mrr_change_percent=mrr_change_percent,
        new_subscriptions=new_subscriptions,
        churned_subscriptions=churned_subscriptions,
        failed_payments=failed_payments,
        last_30_days_revenue=revenue,
        active_subscriptions=active_subscriptions,
        total_customers=total_customers,
        subscriptions=subscriptions_list,
        invoices=[],  # Empty for now - can be populated from invoices if needed
        customers=[],  # Empty for now - can be populated from clients if needed
        payments=payments_list
    )


@router.get("/kpis", response_model=StripeKPIsResponse)
def get_stripe_kpis(
    range_days: int = Query(30, alias="range", ge=1, le=365),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get top-line KPI cards with time-range selection"""
    if not check_stripe_connected(db):
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
    """Get daily/weekly revenue timeline chart data"""
    if not check_stripe_connected(db):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Stripe not connected."
        )
    
    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=range_days)
    
    # Query successful payments grouped by day/week
    if group_by == "day":
        # Group by day
        revenue_data = db.query(
            func.date(StripePayment.created_at).label("date"),
            func.sum(StripePayment.amount_cents).label("revenue_cents")
        ).filter(
            and_(
                StripePayment.status == "succeeded",
                StripePayment.created_at >= start_date,
                StripePayment.created_at <= end_date
            )
        ).group_by(func.date(StripePayment.created_at)).order_by("date").all()
    else:
        # Group by week
        revenue_data = db.query(
            func.date_trunc("week", StripePayment.created_at).label("date"),
            func.sum(StripePayment.amount_cents).label("revenue_cents")
        ).filter(
            and_(
                StripePayment.status == "succeeded",
                StripePayment.created_at >= start_date,
                StripePayment.created_at <= end_date
            )
        ).group_by(func.date_trunc("week", StripePayment.created_at)).order_by("date").all()
    
    timeline = [
        {
            "date": str(row.date),
            "revenue": float(row.revenue_cents or 0) / 100.0
        }
        for row in revenue_data
    ]
    
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
    if not check_stripe_connected(db):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Stripe not connected."
        )
    
    query = db.query(StripeSubscription).join(Client, StripeSubscription.client_id == Client.id, isouter=True)
    
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
            id=sub.id,
            stripe_subscription_id=sub.stripe_subscription_id,
            client_id=sub.client_id,
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
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get payments table with filters"""
    if not check_stripe_connected(db):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Stripe not connected."
        )
    
    query = db.query(StripePayment).join(Client, StripePayment.client_id == Client.id, isouter=True)
    
    if status_filter:
        query = query.filter(StripePayment.status == status_filter)
    
    total = query.count()
    payments = query.order_by(desc(StripePayment.created_at)).offset(
        (page - 1) * page_size
    ).limit(page_size).all()
    
    result = []
    for payment in payments:
        client = db.query(Client).filter(Client.id == payment.client_id).first() if payment.client_id else None
        result.append(StripePaymentResponse(
            id=payment.id,
            stripe_id=payment.stripe_id,
            client_id=payment.client_id,
            client_name=f"{client.first_name or ''} {client.last_name or ''}".strip() if client else None,
            client_email=client.email if client else None,
            amount_cents=payment.amount_cents,
            currency=payment.currency,
            status=payment.status,
            subscription_id=payment.subscription_id,
            receipt_url=payment.receipt_url,
            created_at=payment.created_at
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
    if not check_stripe_connected(db):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Stripe not connected."
        )
    
    query = db.query(StripePayment).filter(
        or_(
            StripePayment.status == "failed",
            StripePayment.status == "past_due"
        )
    ).join(Client, StripePayment.client_id == Client.id, isouter=True)
    
    total = query.count()
    payments = query.order_by(desc(StripePayment.created_at)).offset(
        (page - 1) * page_size
    ).limit(page_size).all()
    
    result = []
    for payment in payments:
        client = db.query(Client).filter(Client.id == payment.client_id).first() if payment.client_id else None
        # Check if recovery recommendation exists
        recovery = db.query(Recommendation).filter(
            and_(
                Recommendation.client_id == payment.client_id,
                Recommendation.type == "payment_recovery",
                Recommendation.status == "PENDING"
            )
        ).first()
        
        result.append(StripeFailedPaymentResponse(
            id=payment.id,
            stripe_id=payment.stripe_id,
            client_id=payment.client_id,
            client_name=f"{client.first_name or ''} {client.last_name or ''}".strip() if client else None,
            client_email=client.email if client else None,
            amount_cents=payment.amount_cents,
            currency=payment.currency,
            status=payment.status,
            subscription_id=payment.subscription_id,
            receipt_url=payment.receipt_url,
            created_at=payment.created_at,
            has_recovery_recommendation=recovery is not None,
            recovery_recommendation_id=recovery.id if recovery else None
        ))
    
    return result


@router.get("/client/{client_id}/revenue", response_model=StripeClientRevenueResponse)
def get_client_revenue(
    client_id: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get single-client revenue panel"""
    if not check_stripe_connected(db):
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
    if not check_stripe_connected(db):
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
        
        # Count canceled subscriptions in this month
        canceled = db.query(func.count(StripeSubscription.id)).filter(
            and_(
                StripeSubscription.status == "canceled",
                StripeSubscription.updated_at >= month_start,
                StripeSubscription.updated_at <= month_end
            )
        ).scalar() or 0
        
        # Count active subscriptions at start of month
        active = db.query(func.count(StripeSubscription.id)).filter(
            and_(
                StripeSubscription.status == "active",
                StripeSubscription.created_at < month_end
            )
        ).scalar() or 0
        
        # Count new subscriptions in this month
        new_subs = db.query(func.count(StripeSubscription.id)).filter(
            and_(
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
            "churned": canceled
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
    if not check_stripe_connected(db):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Stripe not connected."
        )
    
    start_date = datetime.utcnow() - timedelta(days=days)
    
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
    
    # Recent refunds
    refunds = db.query(StripePayment).filter(
        and_(
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


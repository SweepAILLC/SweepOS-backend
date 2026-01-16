from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import List, Optional
import httpx
from datetime import datetime, timedelta
from decimal import Decimal

from app.db.session import get_db
from app.api.deps import get_current_user
from app.models.user import User
from app.models.oauth_token import OAuthToken, OAuthProvider
# Note: For Stripe Apps OAuth, we use the app owner's secret key (STRIPE_SECRET_KEY)
# The OAuth token is stored to verify connection, but API calls use the secret key
from app.schemas.stripe import (
    StripeSummaryResponse,
    StripeCustomer,
    StripeSubscription,
    StripeInvoice,
    StripePayment,
)

# Note: For Stripe Apps OAuth, the access token is actually the account's API key
# We need to use the account's secret key, not the OAuth token
# Let me check the Stripe OAuth flow - actually, for Stripe Apps, we use the app owner's secret key

router = APIRouter()


@router.get("/status")
def get_stripe_status(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Check if Stripe is connected"""
    is_connected = check_stripe_connected(db)
    return {"connected": is_connected}


def check_stripe_connected(db: Session) -> bool:
    """Check if Stripe is connected via OAuth"""
    oauth_token = db.query(OAuthToken).filter(
        OAuthToken.provider == OAuthProvider.STRIPE
    ).first()
    
    if not oauth_token:
        return False
    
    # Check if token is expired
    if oauth_token.expires_at and oauth_token.expires_at < datetime.utcnow():
        return False
    
    return True


def get_stripe_api_key(db: Session) -> str:
    """
    Get Stripe API key for making API calls.
    For Stripe Apps OAuth, we use the app owner's secret key from settings.
    The OAuth token is stored to verify connection, but API calls use the secret key.
    """
    from app.core.config import settings
    
    # Verify Stripe is connected by checking for OAuth token
    if not check_stripe_connected(db):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Stripe not connected. Please connect Stripe first."
        )
    
    # For Stripe Apps, use the app owner's secret key
    if not settings.STRIPE_SECRET_KEY:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Stripe secret key not configured. Set STRIPE_SECRET_KEY in environment."
        )
    
    return settings.STRIPE_SECRET_KEY


def make_stripe_request(
    api_key: str,
    endpoint: str,
    params: Optional[dict] = None
) -> dict:
    """Make an authenticated request to Stripe API"""
    url = f"https://api.stripe.com/v1/{endpoint}"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/x-www-form-urlencoded"
    }
    
    try:
        response = httpx.get(url, headers=headers, params=params or {}, timeout=30.0)
        response.raise_for_status()
        return response.json()
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 401:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Stripe API authentication failed. Please check your API key."
            )
        error_text = e.response.text
        try:
            error_json = e.response.json()
            error_text = error_json.get("error", {}).get("message", error_text)
        except:
            pass
        # Log the error for debugging
        import logging
        logger = logging.getLogger(__name__)
        logger.error(f"Stripe API error: {error_text} (Status: {e.response.status_code})")
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Stripe API error: {error_text}"
        )
    except httpx.RequestError as e:
        import logging
        logger = logging.getLogger(__name__)
        logger.error(f"Stripe API request error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Failed to connect to Stripe API: {str(e)}"
        )


def calculate_mrr_from_subscriptions(subscriptions: List[dict]) -> Decimal:
    """Calculate Monthly Recurring Revenue from active subscriptions"""
    mrr = Decimal(0)
    
    for sub in subscriptions:
        # Only count active subscriptions
        if sub.get("status") not in ["active", "trialing"]:
            continue
        
        # Get the recurring amount
        items = sub.get("items", {}).get("data", [])
        for item in items:
            price = item.get("price", {})
            amount = Decimal(price.get("unit_amount", 0) or 0)
            quantity = Decimal(item.get("quantity", 1) or 1)
            
            # Convert to monthly if needed
            interval = price.get("recurring", {}).get("interval", "month")
            if interval == "year":
                amount = amount / Decimal(12)
            elif interval == "week":
                amount = amount * Decimal(4.33)  # Average weeks per month
            elif interval == "day":
                amount = amount * Decimal(30)  # Average days per month
            
            mrr += amount * quantity / Decimal(100)  # Convert from cents
    
    return mrr


@router.get("/summary", response_model=StripeSummaryResponse)
def get_stripe_summary(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Get Stripe financial summary including MRR, ARR, subscriptions, customers, and invoices.
    Returns 404 if Stripe is not connected.
    """
    # Check if Stripe is connected first
    if not check_stripe_connected(db):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Stripe not connected. Please connect Stripe via OAuth first."
        )
    
    api_key = get_stripe_api_key(db)
    
    # Fetch subscriptions (active and trialing)
    subscriptions_data = make_stripe_request(
        api_key,
        "subscriptions",
        {"status": "all", "limit": 100}  # Get all subscriptions
    )
    subscriptions = subscriptions_data.get("data", [])
    
    # Calculate MRR and ARR
    mrr = calculate_mrr_from_subscriptions(subscriptions)
    arr = mrr * Decimal(12)
    
    # Count active subscriptions
    active_subscriptions = len([
        s for s in subscriptions 
        if s.get("status") in ["active", "trialing"]
    ])
    
    # Fetch customers
    customers_data = make_stripe_request(
        api_key,
        "customers",
        {"limit": 100}
    )
    customers = customers_data.get("data", [])
    total_customers = customers_data.get("total_count", len(customers))
    
    # Fetch recent invoices (last 30 days)
    # Note: Stripe API uses Unix timestamps for date filtering
    # We'll fetch all invoices and filter in Python since Stripe's date filtering syntax is complex
    thirty_days_ago = int((datetime.utcnow() - timedelta(days=30)).timestamp())
    invoices_data = make_stripe_request(
        api_key,
        "invoices",
        {"limit": 100}
    )
    all_invoices = invoices_data.get("data", [])
    # Filter to last 30 days
    invoices = [
        inv for inv in all_invoices 
        if inv.get("created", 0) >= thirty_days_ago
    ]
    
    # Calculate revenue from paid invoices in last 30 days
    last_30_days_revenue = Decimal(0)
    for invoice in invoices:
        if invoice.get("status") == "paid":
            amount = Decimal(invoice.get("amount_paid", 0) or 0)
            last_30_days_revenue += amount / Decimal(100)  # Convert from cents
    
    # Format subscriptions for response
    formatted_subscriptions = []
    for sub in subscriptions[:10]:  # Limit to 10 most recent
        items = sub.get("items", {}).get("data", [])
        total_amount = Decimal(0)
        for item in items:
            price = item.get("price", {})
            amount = Decimal(price.get("unit_amount", 0) or 0)
            quantity = Decimal(item.get("quantity", 1) or 1)
            total_amount += amount * quantity
        
        formatted_subscriptions.append({
            "id": sub.get("id"),
            "status": sub.get("status"),
            "amount": int(total_amount),
            "current_period_start": sub.get("current_period_start"),
            "current_period_end": sub.get("current_period_end"),
            "customer_id": sub.get("customer"),
        })
    
    # Format invoices for response
    formatted_invoices = []
    for invoice in invoices[:10]:  # Limit to 10 most recent
        formatted_invoices.append({
            "id": invoice.get("id"),
            "amount": invoice.get("amount_paid", invoice.get("amount_due", 0)),
            "status": invoice.get("status"),
            "created_at": invoice.get("created"),
            "customer_id": invoice.get("customer"),
        })
    
    # Format customers for response
    formatted_customers = []
    for customer in customers[:10]:  # Limit to 10 most recent
        formatted_customers.append({
            "id": customer.get("id"),
            "email": customer.get("email"),
            "name": customer.get("name"),
            "created_at": customer.get("created"),
        })
    
    # Get recent payments (from charges)
    charges_data = make_stripe_request(
        api_key,
        "charges",
        {"limit": 10}
    )
    charges = charges_data.get("data", [])
    
    formatted_payments = []
    for charge in charges:
        formatted_payments.append({
            "id": charge.get("id"),
            "amount": charge.get("amount", 0),
            "status": charge.get("status"),
            "created_at": charge.get("created"),
        })
    
    return {
        "total_mrr": float(mrr),
        "total_arr": float(arr),
        "active_subscriptions": active_subscriptions,
        "total_customers": total_customers,
        "last_30_days_revenue": float(last_30_days_revenue),
        "subscriptions": formatted_subscriptions,
        "invoices": formatted_invoices,
        "customers": formatted_customers,
        "payments": formatted_payments,
    }


@router.get("/customers", response_model=List[StripeCustomer])
def get_stripe_customers(
    limit: int = 100,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get list of Stripe customers"""
    api_key = get_stripe_api_key(db)
    
    customers_data = make_stripe_request(
        api_key,
        "customers",
        {"limit": min(limit, 100)}
    )
    
    customers = []
    for customer in customers_data.get("data", []):
        customers.append({
            "id": customer.get("id"),
            "email": customer.get("email"),
            "name": customer.get("name"),
            "created_at": customer.get("created"),
        })
    
    return customers


@router.get("/subscriptions", response_model=List[StripeSubscription])
def get_stripe_subscriptions(
    status: Optional[str] = None,
    limit: int = 100,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get list of Stripe subscriptions"""
    api_key = get_stripe_api_key(db)
    
    params = {"limit": min(limit, 100)}
    if status:
        params["status"] = status
    
    subscriptions_data = make_stripe_request(
        api_key,
        "subscriptions",
        params
    )
    
    subscriptions = []
    for sub in subscriptions_data.get("data", []):
        items = sub.get("items", {}).get("data", [])
        total_amount = Decimal(0)
        for item in items:
            price = item.get("price", {})
            amount = Decimal(price.get("unit_amount", 0) or 0)
            quantity = Decimal(item.get("quantity", 1) or 1)
            total_amount += amount * quantity
        
        subscriptions.append({
            "id": sub.get("id"),
            "status": sub.get("status"),
            "amount": int(total_amount),
            "current_period_start": sub.get("current_period_start"),
            "current_period_end": sub.get("current_period_end"),
            "customer_id": sub.get("customer"),
        })
    
    return subscriptions


@router.get("/invoices", response_model=List[StripeInvoice])
def get_stripe_invoices(
    limit: int = 100,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get list of Stripe invoices"""
    api_key = get_stripe_api_key(db)
    
    invoices_data = make_stripe_request(
        api_key,
        "invoices",
        {"limit": min(limit, 100)}
    )
    
    invoices = []
    for invoice in invoices_data.get("data", []):
        invoices.append({
            "id": invoice.get("id"),
            "amount": invoice.get("amount_paid", invoice.get("amount_due", 0)),
            "status": invoice.get("status"),
            "created_at": invoice.get("created"),
            "customer_id": invoice.get("customer"),
        })
    
    return invoices


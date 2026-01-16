from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime


class StripeCustomer(BaseModel):
    id: str
    email: Optional[str] = None
    name: Optional[str] = None
    created_at: int  # Unix timestamp

    class Config:
        from_attributes = True


class StripeSubscription(BaseModel):
    id: str
    status: str
    amount: int  # Amount in cents
    current_period_start: int  # Unix timestamp
    current_period_end: int  # Unix timestamp
    customer_id: str

    class Config:
        from_attributes = True


class StripeInvoice(BaseModel):
    id: str
    amount: int  # Amount in cents
    status: str
    created_at: int  # Unix timestamp
    customer_id: str

    class Config:
        from_attributes = True


class StripePayment(BaseModel):
    id: str
    amount: int  # Amount in cents
    status: str
    created_at: int  # Unix timestamp

    class Config:
        from_attributes = True


class StripeSummaryResponse(BaseModel):
    total_mrr: float  # Monthly Recurring Revenue
    total_arr: float  # Annual Recurring Revenue
    mrr_change: float  # Change vs previous period
    mrr_change_percent: float  # Percent change
    new_subscriptions: int  # New subscriptions in period
    churned_subscriptions: int  # Churned subscriptions in period
    failed_payments: int  # Failed payments in period
    active_subscriptions: int
    total_customers: int
    last_30_days_revenue: float
    average_client_ltv: float  # Average Lifetime Value (average total spend of all customers)
    subscriptions: List[StripeSubscription]
    invoices: List[StripeInvoice]
    customers: List[StripeCustomer]
    payments: List[StripePayment]

    class Config:
        from_attributes = True


class StripeConnectionStatus(BaseModel):
    connected: bool
    message: Optional[str] = None
    account_id: Optional[str] = None

    class Config:
        from_attributes = True


class StripeKPIsResponse(BaseModel):
    mrr: float
    mrr_change: float
    mrr_change_percent: float
    new_subscriptions: int
    churned_subscriptions: int
    failed_payments: int
    revenue: float

    class Config:
        from_attributes = True


class RevenueTimelinePoint(BaseModel):
    date: str
    revenue: float


class StripeRevenueTimelineResponse(BaseModel):
    timeline: List[RevenueTimelinePoint]
    group_by: str  # "day" or "week"

    class Config:
        from_attributes = True


class StripeSubscriptionResponse(BaseModel):
    id: str  # UUID
    stripe_subscription_id: str
    client_id: Optional[str] = None
    client_name: Optional[str] = None
    client_email: Optional[str] = None
    status: str
    plan_id: Optional[str] = None
    mrr: float
    start_date: datetime
    current_period_end: Optional[datetime] = None
    estimated_lifetime_value: Optional[float] = None

    class Config:
        from_attributes = True


class StripePaymentResponse(BaseModel):
    id: str  # UUID
    stripe_id: str
    client_id: Optional[str] = None
    client_name: Optional[str] = None
    client_email: Optional[str] = None
    amount_cents: int
    currency: str
    status: str
    subscription_id: Optional[str] = None
    receipt_url: Optional[str] = None
    created_at: int  # Unix timestamp

    class Config:
        from_attributes = True


class StripeFailedPaymentResponse(StripePaymentResponse):
    has_recovery_recommendation: bool
    recovery_recommendation_id: Optional[str] = None
    attempt_count: int = 1  # Number of failed attempts for this subscription/client
    first_attempt_at: int  # Unix timestamp of first failed attempt
    latest_attempt_at: int  # Unix timestamp of most recent failed attempt

    class Config:
        from_attributes = True


class StripeClientRevenueResponse(BaseModel):
    client_id: str
    client_name: str
    client_email: Optional[str] = None
    lifetime_revenue_cents: int
    current_subscription_id: Optional[str] = None
    current_mrr: float
    next_invoice_date: Optional[datetime] = None
    payment_history: List[dict]  # List of payment objects

    class Config:
        from_attributes = True


class ChurnMonthData(BaseModel):
    month: str  # YYYY-MM
    churn_rate: float
    canceled: int
    active: int


class CohortMonthData(BaseModel):
    month: str  # YYYY-MM
    new_subscriptions: int
    churned: int


class StripeChurnResponse(BaseModel):
    churn_by_month: List[ChurnMonthData]
    cohort_snapshot: List[CohortMonthData]

    class Config:
        from_attributes = True


class TopCustomer(BaseModel):
    client_id: str
    name: str
    email: Optional[str] = None
    revenue_cents: int


class RecentRefund(BaseModel):
    id: str
    stripe_id: str
    amount_cents: int
    created_at: datetime
    client_id: Optional[str] = None


class StripeTopCustomersResponse(BaseModel):
    top_customers: List[TopCustomer]
    recent_refunds: List[RecentRefund]

    class Config:
        from_attributes = True


class MRRTrendPoint(BaseModel):
    date: str  # YYYY-MM-DD
    mrr: float
    subscriptions_count: int

    class Config:
        from_attributes = True


class MRRTrendResponse(BaseModel):
    trend_data: List[MRRTrendPoint]
    current_mrr: float
    previous_mrr: float
    growth_percent: float

    class Config:
        from_attributes = True


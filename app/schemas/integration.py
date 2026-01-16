from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime


class Payment(BaseModel):
    id: str
    amount: float
    currency: str
    status: str
    created_at: datetime


class Subscription(BaseModel):
    id: str
    customer_id: str
    status: str
    current_period_end: datetime
    amount: float


class StripeSummary(BaseModel):
    total_mrr: float
    last_30_days_revenue: float
    active_subscriptions: int
    payments: List[Payment]
    subscriptions: List[Subscription]


class BrevoStatus(BaseModel):
    connected: bool
    account_email: Optional[str] = None
    account_name: Optional[str] = None
    message: Optional[str] = None


from pydantic import BaseModel
from typing import List, Optional, Dict, Any
from datetime import datetime
from uuid import UUID


class FunnelStepConversion(BaseModel):
    step_order: int
    label: Optional[str] = None
    event_name: str
    count: int
    conversion_rate: Optional[float] = None  # % from previous step


class FunnelConversionMetric(BaseModel):
    funnel_id: UUID
    funnel_name: str
    total_visitors: int
    total_conversions: int
    overall_conversion_rate: float
    step_counts: List[FunnelStepConversion] = []


class GlobalHealthResponse(BaseModel):
    """Platform-wide metrics for owner/admin Health tab (impact & growth)."""

    total_organizations: int = 0
    organizations_created_last_30_days: int = 0

    total_users: int = 0
    users_created_last_30_days: int = 0

    total_clients: int = 0
    clients_created_last_30_days: int = 0

    total_funnels: int = 0
    total_events: int = 0
    total_events_last_30_days: int = 0

    total_payments: int = 0
    total_subscriptions: int = 0
    active_subscriptions: int = 0

    total_mrr_usd: float = 0.0
    total_revenue_stripe_succeeded_usd: float = 0.0
    last_30_days_revenue_stripe_usd: float = 0.0
    treasury_posted_last_30_days_usd: float = 0.0

    funnel_first_step_views_all_time: int = 0
    funnel_first_step_views_last_30_days: int = 0
    unique_visitors_all_time: int = 0
    unique_visitors_last_30_days: int = 0

    orgs_with_stripe_connected: int = 0
    orgs_with_brevo_connected: int = 0
    pending_invitations: int = 0


class OrganizationFunnelCreate(BaseModel):
    """Schema for creating a funnel in an organization (admin only)"""
    name: str
    client_id: Optional[UUID] = None
    slug: Optional[str] = None
    domain: Optional[str] = None
    env: Optional[str] = None


class OrganizationFunnelUpdate(BaseModel):
    """Schema for updating a funnel in an organization (admin only)"""
    name: Optional[str] = None
    client_id: Optional[UUID] = None
    slug: Optional[str] = None
    domain: Optional[str] = None
    env: Optional[str] = None


class OrganizationDashboardSummary(BaseModel):
    """Summary of an organization's dashboard data"""
    organization_id: UUID
    organization_name: str

    # User seats (for system owner to limit org)
    total_users: int = 0
    max_user_seats: Optional[int] = None  # null = unlimited

    # Client stats
    total_clients: int
    clients_by_status: Dict[str, int]  # e.g., {"lead": 5, "active": 10}
    
    # Funnel stats
    total_funnels: int
    active_funnels: int  # Funnels with recent events
    total_events: int
    total_visitors: int
    
    # Stripe stats
    total_mrr: float
    total_arr: float
    active_subscriptions: int
    total_payments: int
    last_30_days_revenue: float
    
    # Brevo stats
    brevo_connected: bool

    # Funnel conversion metrics (per-funnel analytics for last 30 days)
    funnel_conversion_metrics: List[FunnelConversionMetric] = []

    # All funnels for the org (for management UI)
    recent_funnels: List[Dict[str, Any]]


from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from datetime import datetime
from uuid import UUID


class HealthTrendPeriod(BaseModel):
    """One 30-day bucket for owner health trends (oldest-first in the list)."""

    period_label: str
    period_start: str  # ISO 8601
    period_end: str
    show_up_rate_pct: Optional[float] = None
    close_rate_pct: Optional[float] = None
    stripe_revenue_usd: float = 0.0
    calls_booked_count: int = 0
    cumulative_total_clients: int = 0
    active_clients_cohort: int = 0


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
    treasury_posted_all_time_usd: float = 0.0
    """Posted positive Treasury amounts (platform-wide, all time)."""
    cash_collected_all_time_combined_usd: float = 0.0
    """Stripe succeeded (all time) + Treasury posted (all time); rails are usually distinct per org."""
    manual_cash_all_time_usd: float = 0.0
    """User-entered manual payments (all time), not from Stripe."""
    total_processor_revenue_all_time_usd: float = 0.0
    """Stripe + Treasury combined all-time cash plus manual cash entered in-app."""

    funnel_first_step_views_all_time: int = 0
    funnel_first_step_views_last_30_days: int = 0
    unique_visitors_all_time: int = 0
    unique_visitors_last_30_days: int = 0

    orgs_with_stripe_connected: int = 0
    orgs_with_brevo_connected: int = 0
    pending_invitations: int = 0

    # Owner health — product & coaching signals
    stripe_revenue_post_onboarding_usd: float = 0.0
    """Succeeded Stripe revenue (all time) only for charges on or after each org's onboarding (org created_at)."""

    invitation_emails_sent_last_30d: int = 0
    invitation_emails_sent_previous_30d: int = 0
    """Org/user invitation emails initiated (invitation rows created); proxy for app-sent email volume."""

    calls_booked_last_30d: int = 0
    calls_booked_previous_30d: int = 0
    """Calendar check-ins (booked meetings) with start time in each window."""

    lifecycle_active_clients_current: int = 0
    lifecycle_active_clients_previous_30d_cohort: int = 0
    """Clients currently in lifecycle `active` whose record was created before the prior 30d window start (rough prior-period cohort)."""

    show_up_rate_last_30d_pct: Optional[float] = None
    close_rate_last_30d_pct: Optional[float] = None

    health_trend_periods: List[HealthTrendPeriod] = Field(default_factory=list)


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

    # Platform onboarding & coaching trends (owner org modal)
    organization_onboarded_at: Optional[str] = None  # ISO 8601 — org.created_at
    cash_collected_since_onboarding_usd: float = 0.0
    """Stripe payments (or Treasury posted cash if org uses Treasury), only after onboarding."""
    cash_collected_all_time_usd: float = 0.0
    """Canonical all-time cash for this org (Treasury if used, else succeeded Stripe)."""
    manual_cash_all_time_usd: float = 0.0
    """Manual payments entered in-app for this org (not Stripe)."""
    total_processor_revenue_all_time_usd: float = 0.0
    """Treasury-or-Stripe all-time plus manual payments for this org."""
    monthly_health_since_onboarding: List[HealthTrendPeriod] = Field(default_factory=list)
    """Calendar months from onboarding through now: show-up %, close %, cash collected."""


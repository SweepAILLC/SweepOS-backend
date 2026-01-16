from pydantic import BaseModel
from typing import List, Optional, Dict, Any
from datetime import datetime
from uuid import UUID


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
    
    # Recent activity
    recent_clients: List[Dict[str, Any]]
    recent_funnels: List[Dict[str, Any]]  # All funnels for the org


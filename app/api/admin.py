"""
Admin API endpoints for managing organizations, permissions, and global settings.
Only accessible to admin/owner users.
"""
from fastapi import APIRouter, Depends, HTTPException, Request, status
from sqlalchemy.orm import Session
from sqlalchemy import func, desc, asc, and_
from typing import List, Optional
from uuid import UUID

from app.db.session import get_db
from app.api.deps import get_current_user, require_admin, MAIN_ORG_ID
from app.models.user import User
from app.models.organization import Organization
from app.models.client import Client
from app.models.funnel import Funnel, FunnelStep
from app.models.event import Event
from app.models.stripe_payment import StripePayment
from app.models.stripe_subscription import StripeSubscription
from app.models.stripe_treasury_transaction import StripeTreasuryTransaction, TreasuryTransactionStatus
from app.models.oauth_token import OAuthToken, OAuthProvider
from app.models.client import LifecycleState
from app.schemas.organization import (
    Organization as OrganizationSchema,
    OrganizationUpdate,
    OrganizationWithStats,
)
from app.schemas.invitation import InviteOrgAdminRequest, InvitationResponse
from app.models.organization_invitation import OrganizationInvitation
from app.schemas.admin import (
    OrganizationDashboardSummary,
    OrganizationFunnelCreate,
    OrganizationFunnelUpdate,
    FunnelConversionMetric,
    FunnelStepConversion,
)
from app.schemas.funnel import Funnel as FunnelSchema
from app.schemas.permission import (
    OrganizationTabPermission as OrgTabPermissionSchema,
    OrganizationTabPermissionCreate,
    OrganizationTabPermissionUpdate
)
from app.models.organization_tab_permission import OrganizationTabPermission
from app.core.config import settings
from app.core.rate_limit import rate_limit
from datetime import datetime
import uuid
from datetime import datetime, timedelta

router = APIRouter()


# Organizations Management
@router.get("/organizations", response_model=List[OrganizationWithStats])
def list_organizations(
    db: Session = Depends(get_db),
    admin_user: User = Depends(require_admin)
):
    """List all organizations with stats"""
    orgs = db.query(Organization).order_by(Organization.created_at.desc()).all()
    
    result = []
    for org in orgs:
        user_count = db.query(func.count(User.id)).filter(
            User.org_id == org.id
        ).scalar() or 0
        
        client_count = db.query(func.count(Client.id)).filter(
            Client.org_id == org.id
        ).scalar() or 0
        
        funnel_count = db.query(func.count(Funnel.id)).filter(
            Funnel.org_id == org.id
        ).scalar() or 0
        
        org_dict = OrganizationWithStats.model_validate(org, from_attributes=True)
        org_dict.user_count = user_count
        org_dict.client_count = client_count
        org_dict.funnel_count = funnel_count
        result.append(org_dict)
    
    return result


# Invitation-based org onboarding (only way to create new orgs; uses BREVO_API_KEY)
@router.post("/organizations/invite", response_model=dict, status_code=status.HTTP_201_CREATED)
@rate_limit(max_requests=10, window_seconds=900)  # 10 org invites per 15 min per admin
def invite_organization(
    body: InviteOrgAdminRequest,
    request: Request,
    db: Session = Depends(get_db),
    admin_user: User = Depends(require_admin),
):
    """
    Create a new organization and send an invitation email to the org admin.
    They set their own password via the invitation link. No user is created until they accept.
    """
    import secrets
    from datetime import datetime, timedelta
    from app.services.onboarding_email import send_org_admin_invitation_email, INVITATION_EXPIRES_DAYS

    name = (body.name or "").strip()
    admin_email = (body.admin_email or "").strip().lower()
    if not name:
        raise HTTPException(status_code=400, detail="Organization name is required")
    if not admin_email:
        raise HTTPException(status_code=400, detail="Admin email is required")

    org = Organization(name=name)
    db.add(org)
    db.flush()

    token = secrets.token_urlsafe(32)
    expires_at = datetime.utcnow() + timedelta(days=INVITATION_EXPIRES_DAYS)
    inv = OrganizationInvitation(
        org_id=org.id,
        invitee_email=admin_email,
        invitation_type="ORG_ADMIN",
        role="admin",
        token=token,
        expires_at=expires_at,
        created_by=admin_user.id,
    )
    db.add(inv)
    db.commit()
    db.refresh(org)
    db.refresh(inv)

    frontend_url = getattr(settings, "FRONTEND_URL", "") or "http://localhost:3002"
    link = f"{frontend_url.rstrip('/')}/invite/accept?token={token}"
    send_org_admin_invitation_email(to_email=admin_email, org_name=org.name, invitation_link=link)

    return {
        "organization": OrganizationSchema.model_validate(org),
        "invitation": InvitationResponse(
            id=inv.id,
            org_id=inv.org_id,
            invitee_email=inv.invitee_email,
            invitation_type=inv.invitation_type,
            role=inv.role,
            expires_at=inv.expires_at,
            used_at=inv.used_at,
            created_at=inv.created_at,
        ),
    }


@router.get("/organizations/invitations", response_model=List[InvitationResponse])
def list_all_invitations(
    db: Session = Depends(get_db),
    admin_user: User = Depends(require_admin),
):
    """List all pending invitations across all organizations (system owner only)."""
    invs = (
        db.query(OrganizationInvitation)
        .filter(
            OrganizationInvitation.used_at.is_(None),
            OrganizationInvitation.expires_at > datetime.utcnow(),
        )
        .order_by(OrganizationInvitation.created_at.desc())
        .all()
    )
    return [
        InvitationResponse(
            id=i.id,
            org_id=i.org_id,
            invitee_email=i.invitee_email,
            invitation_type=i.invitation_type,
            role=i.role,
            expires_at=i.expires_at,
            used_at=i.used_at,
            created_at=i.created_at,
        )
        for i in invs
    ]


@router.get("/organizations/{org_id}", response_model=OrganizationWithStats)
def get_organization(
    org_id: UUID,
    db: Session = Depends(get_db),
    admin_user: User = Depends(require_admin)
):
    """Get organization details with stats"""
    org = db.query(Organization).filter(Organization.id == org_id).first()
    if not org:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Organization not found"
        )
    
    user_count = db.query(func.count(User.id)).filter(
        User.org_id == org.id
    ).scalar() or 0
    
    client_count = db.query(func.count(Client.id)).filter(
        Client.org_id == org.id
    ).scalar() or 0
    
    funnel_count = db.query(func.count(Funnel.id)).filter(
        Funnel.org_id == org.id
    ).scalar() or 0
    
    org_dict = OrganizationWithStats.model_validate(org, from_attributes=True)
    org_dict.user_count = user_count
    org_dict.client_count = client_count
    org_dict.funnel_count = funnel_count
    return org_dict


@router.patch("/organizations/{org_id}", response_model=OrganizationSchema)
def update_organization(
    org_id: UUID,
    org_data: OrganizationUpdate,
    db: Session = Depends(get_db),
    admin_user: User = Depends(require_admin)
):
    """Update organization"""
    org = db.query(Organization).filter(Organization.id == org_id).first()
    if not org:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Organization not found"
        )
    
    if org_data.name is not None:
        org.name = org_data.name
    if org_data.max_user_seats is not None:
        if org_data.max_user_seats < 0:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="max_user_seats must be 0 or greater",
            )
        org.max_user_seats = org_data.max_user_seats

    db.commit()
    db.refresh(org)
    return org


@router.delete("/organizations/{org_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_organization(
    org_id: UUID,
    db: Session = Depends(get_db),
    admin_user: User = Depends(require_admin)
):
    """
    Delete organization and all related data.
    This will cascade delete all users, clients, funnels, events, payments, etc.
    """
    from app.models.funnel import FunnelStep
    from app.models.session import Session as SessionModel
    from app.models.event_error import EventError
    from app.models.campaign import Campaign
    from app.models.recommendation import Recommendation
    from app.models.stripe_event import StripeEvent
    from app.models.feature import Feature
    
    org = db.query(Organization).filter(Organization.id == org_id).first()
    if not org:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Organization not found"
        )
    
    # Prevent deleting the main org (safety check)
    if str(org_id) == str(MAIN_ORG_ID):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot delete the main organization"
        )
    
    try:
        # Delete in order to respect foreign key constraints
        # Start with child tables that reference other child tables
        
        # Delete funnel steps (references funnels)
        db.query(FunnelStep).filter(FunnelStep.org_id == org_id).delete()
        
        # Delete funnels (references clients, but we'll delete clients after)
        db.query(Funnel).filter(Funnel.org_id == org_id).delete()
        
        # Delete events (references funnels, clients, sessions)
        db.query(Event).filter(Event.org_id == org_id).delete()
        
        # Delete sessions
        db.query(SessionModel).filter(SessionModel.org_id == org_id).delete()
        
        # Delete event errors that reference this org's funnels
        # Since EventError doesn't have org_id, we need to find events that reference
        # funnels from this org, then delete corresponding errors
        # For simplicity, we'll delete all event errors (they're just logs anyway)
        # Or we can skip this if event_errors are meant to be kept for debugging
        # For now, let's skip deleting event_errors since they don't have org_id
        # and are just error logs that might be useful for debugging
        
        # Delete Stripe-related data
        db.query(StripePayment).filter(StripePayment.org_id == org_id).delete()
        db.query(StripeSubscription).filter(StripeSubscription.org_id == org_id).delete()
        db.query(StripeEvent).filter(StripeEvent.org_id == org_id).delete()
        
        # Delete OAuth tokens
        db.query(OAuthToken).filter(OAuthToken.org_id == org_id).delete()
        
        # Delete campaigns
        db.query(Campaign).filter(Campaign.org_id == org_id).delete()
        
        # Delete recommendations
        db.query(Recommendation).filter(Recommendation.org_id == org_id).delete()
        
        # Delete features
        db.query(Feature).filter(Feature.org_id == org_id).delete()
        
        # Delete clients (references users, but users reference org, so delete clients first)
        db.query(Client).filter(Client.org_id == org_id).delete()
        
        # Delete users (must be last as other tables might reference users)
        db.query(User).filter(User.org_id == org_id).delete()
        
        # Finally, delete the organization
        db.delete(org)
        db.commit()
        
        return None
        
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to delete organization: {str(e)}"
        )


# Global Health Stats
@router.get("/health")
def get_global_health(
    db: Session = Depends(get_db),
    admin_user: User = Depends(require_admin)
):
    """Get global system health stats"""
    total_orgs = db.query(func.count(Organization.id)).scalar() or 0
    total_users = db.query(func.count(User.id)).scalar() or 0
    total_clients = db.query(func.count(Client.id)).scalar() or 0
    total_funnels = db.query(func.count(Funnel.id)).scalar() or 0
    total_events = db.query(func.count(Event.id)).scalar() or 0
    total_payments = db.query(func.count(StripePayment.id)).scalar() or 0
    total_subscriptions = db.query(func.count(StripeSubscription.id)).scalar() or 0
    
    return {
        "total_organizations": total_orgs,
        "total_users": total_users,
        "total_clients": total_clients,
        "total_funnels": total_funnels,
        "total_events": total_events,
        "total_payments": total_payments,
        "total_subscriptions": total_subscriptions
    }


# Global Settings (read-only for now, can be extended)
@router.get("/settings")
def get_global_settings(
    admin_user: User = Depends(require_admin)
):
    """Get global system settings"""
    return {
        "sudo_admin_email": settings.SUDO_ADMIN_EMAIL,
        "frontend_url": settings.FRONTEND_URL,
        "stripe_configured": bool(settings.STRIPE_CLIENT_ID),
        "brevo_configured": bool(settings.BREVO_CLIENT_ID),
    }


# Organization Dashboard View
@router.get("/organizations/{org_id}/dashboard", response_model=OrganizationDashboardSummary)
def get_organization_dashboard(
    org_id: UUID,
    db: Session = Depends(get_db),
    admin_user: User = Depends(require_admin)
):
    """
    Get dashboard summary for a specific organization.
    Only accessible to admins in the main org.
    """
    # Verify organization exists
    org = db.query(Organization).filter(Organization.id == org_id).first()
    if not org:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Organization not found"
        )

    total_users = db.query(func.count(User.id)).filter(User.org_id == org_id).scalar() or 0

    # Client stats: group by normalized email (same as frontend merge); count one per group; status = priority state in group
    import re
    all_org_clients = db.query(Client).filter(Client.org_id == org_id).all()
    # Group by normalized email (empty email -> each client its own group by id)
    email_to_clients: dict = {}
    for c in all_org_clients:
        if c.email:
            normalized = re.sub(r"\s+", "", (c.email or "").lower().strip())
            key = normalized or str(c.id)
        else:
            key = str(c.id)
        if key not in email_to_clients:
            email_to_clients[key] = []
        email_to_clients[key].append(c)
    # Priority for merged state (match frontend: active > warm_lead > cold_lead > offboarding > dead)
    state_priority = {
        LifecycleState.ACTIVE: 5,
        LifecycleState.WARM_LEAD: 4,
        LifecycleState.COLD_LEAD: 3,
        LifecycleState.OFFBOARDING: 2,
        LifecycleState.DEAD: 1,
    }
    total_clients = len(email_to_clients)
    clients_by_status = {s.value: 0 for s in LifecycleState}
    for group in email_to_clients.values():
        merged_state = max(
            (c.lifecycle_state for c in group if c.lifecycle_state is not None),
            key=lambda s: state_priority.get(s, 0),
            default=LifecycleState.COLD_LEAD,
        )
        st = merged_state.value
        if st in clients_by_status:
            clients_by_status[st] += 1
        else:
            clients_by_status[st] = 1
    
    # Funnel stats
    total_funnels = db.query(func.count(Funnel.id)).filter(
        Funnel.org_id == org_id
    ).scalar() or 0
    
    # Active funnels (have events in last 7 days)
    seven_days_ago = datetime.utcnow() - timedelta(days=7)
    active_funnels = db.query(func.count(func.distinct(Event.funnel_id))).filter(
        Event.org_id == org_id,
        Event.funnel_id.isnot(None),
        Event.received_at >= seven_days_ago
    ).scalar() or 0
    
    total_events = db.query(func.count(Event.id)).filter(
        Event.org_id == org_id
    ).scalar() or 0
    
    total_visitors = db.query(func.count(func.distinct(Event.visitor_id))).filter(
        Event.org_id == org_id,
        Event.visitor_id.isnot(None)
    ).scalar() or 0
    
    # Stripe stats (align with Stripe dashboard: active + trialing for MRR/subs; revenue from Treasury or Payment)
    thirty_days_ago = datetime.utcnow() - timedelta(days=30)
    total_mrr_result = db.query(func.sum(StripeSubscription.mrr)).filter(
        StripeSubscription.org_id == org_id,
        StripeSubscription.status.in_(["active", "trialing"])
    ).scalar()
    total_mrr = float(total_mrr_result) if total_mrr_result else 0.0
    total_arr = total_mrr * 12
    active_subscriptions = db.query(func.count(StripeSubscription.id)).filter(
        StripeSubscription.org_id == org_id,
        StripeSubscription.status.in_(["active", "trialing"])
    ).scalar() or 0
    total_payments = db.query(func.count(StripePayment.id)).filter(
        StripePayment.org_id == org_id
    ).scalar() or 0
    # Last 30 days revenue: use Treasury if org uses it, else StripePayment (match Stripe dashboard)
    treasury_count = db.query(StripeTreasuryTransaction.id).filter(
        StripeTreasuryTransaction.org_id == org_id
    ).limit(1).first()
    if treasury_count:
        last_30_days_revenue = db.query(func.sum(StripeTreasuryTransaction.amount)).filter(
            StripeTreasuryTransaction.org_id == org_id,
            StripeTreasuryTransaction.status == TreasuryTransactionStatus.POSTED,
            StripeTreasuryTransaction.amount > 0,
            StripeTreasuryTransaction.posted_at >= thirty_days_ago,
            StripeTreasuryTransaction.posted_at <= datetime.utcnow()
        ).scalar() or 0
        last_30_days_revenue = float(last_30_days_revenue) / 100.0  # cents to dollars
    else:
        rev_result = db.query(func.sum(StripePayment.amount_cents)).filter(
            StripePayment.org_id == org_id,
            StripePayment.status == "succeeded",
            StripePayment.created_at >= thirty_days_ago
        ).scalar() or 0
        last_30_days_revenue = float(rev_result) / 100.0  # cents to dollars
    
    # Brevo connection status
    brevo_token = db.query(OAuthToken).filter(
        OAuthToken.org_id == org_id,
        OAuthToken.provider == OAuthProvider.BREVO
    ).first()
    brevo_connected = brevo_token is not None
    
    # Get all funnels for the organization
    all_funnels = db.query(Funnel).filter(
        Funnel.org_id == org_id
    ).order_by(desc(Funnel.created_at)).all()
    all_funnels_data = [
        {
            "id": str(f.id),
            "name": f.name,
            "domain": f.domain,
            "slug": f.slug,
            "created_at": f.created_at.isoformat() if f.created_at else None
        }
        for f in all_funnels
    ]

    # Funnel conversion metrics (last 30 days, same logic as funnel analytics)
    funnel_conversion_metrics: List[FunnelConversionMetric] = []
    range_days = 30
    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=range_days)
    for funnel in all_funnels:
        steps = db.query(FunnelStep).filter(
            FunnelStep.funnel_id == funnel.id,
            FunnelStep.org_id == org_id
        ).order_by(asc(FunnelStep.step_order)).all()
        if not steps:
            funnel_conversion_metrics.append(FunnelConversionMetric(
                funnel_id=funnel.id,
                funnel_name=funnel.name or "Unnamed",
                total_visitors=0,
                total_conversions=0,
                overall_conversion_rate=0.0,
                step_counts=[]
            ))
            continue
        step_counts_list: List[FunnelStepConversion] = []
        previous_count: Optional[int] = None
        for step in steps:
            count = db.query(func.count(Event.id)).filter(
                Event.funnel_id == funnel.id,
                Event.org_id == org_id,
                Event.event_name == step.event_name,
                Event.occurred_at >= start_date,
                Event.occurred_at <= end_date
            ).scalar() or 0
            conversion_rate = None
            if previous_count is not None and previous_count > 0:
                conversion_rate = (count / previous_count) * 100.0
            step_counts_list.append(FunnelStepConversion(
                step_order=step.step_order,
                label=step.label,
                event_name=step.event_name,
                count=count,
                conversion_rate=conversion_rate
            ))
            previous_count = count
        first_step = steps[0]
        total_visitors = db.query(func.count(func.distinct(Event.visitor_id))).filter(
            Event.funnel_id == funnel.id,
            Event.org_id == org_id,
            Event.event_name == first_step.event_name,
            Event.occurred_at >= start_date,
            Event.occurred_at <= end_date,
            Event.visitor_id.isnot(None)
        ).scalar() or 0
        last_step = steps[-1]
        total_conversions = db.query(func.count(func.distinct(Event.visitor_id))).filter(
            Event.funnel_id == funnel.id,
            Event.org_id == org_id,
            Event.event_name == last_step.event_name,
            Event.occurred_at >= start_date,
            Event.occurred_at <= end_date,
            Event.visitor_id.isnot(None)
        ).scalar() or 0
        overall_conversion_rate = (total_conversions / total_visitors * 100.0) if total_visitors > 0 else 0.0
        funnel_conversion_metrics.append(FunnelConversionMetric(
            funnel_id=funnel.id,
            funnel_name=funnel.name or "Unnamed",
            total_visitors=total_visitors,
            total_conversions=total_conversions,
            overall_conversion_rate=overall_conversion_rate,
            step_counts=step_counts_list
        ))

    return OrganizationDashboardSummary(
        organization_id=org_id,
        organization_name=org.name,
        total_users=total_users,
        max_user_seats=getattr(org, "max_user_seats", None),
        total_clients=total_clients,
        clients_by_status=clients_by_status,
        total_funnels=total_funnels,
        active_funnels=active_funnels,
        total_events=total_events,
        total_visitors=total_visitors,
        total_mrr=total_mrr,
        total_arr=total_arr,
        active_subscriptions=active_subscriptions,
        total_payments=total_payments,
        last_30_days_revenue=last_30_days_revenue,
        brevo_connected=brevo_connected,
        funnel_conversion_metrics=funnel_conversion_metrics,
        recent_funnels=all_funnels_data
    )


# Admin endpoints for managing funnels in any organization
@router.get("/organizations/{org_id}/funnels", response_model=List[FunnelSchema])
def list_organization_funnels(
    org_id: UUID,
    db: Session = Depends(get_db),
    admin_user: User = Depends(require_admin)
):
    """List all funnels for a specific organization"""
    # Verify organization exists
    org = db.query(Organization).filter(Organization.id == org_id).first()
    if not org:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Organization not found"
        )
    
    funnels = db.query(Funnel).filter(
        Funnel.org_id == org_id
    ).order_by(desc(Funnel.created_at)).all()
    
    return [FunnelSchema.model_validate(f, from_attributes=True) for f in funnels]


@router.post("/organizations/{org_id}/funnels", response_model=FunnelSchema, status_code=status.HTTP_201_CREATED)
def create_organization_funnel(
    org_id: UUID,
    funnel_data: OrganizationFunnelCreate,
    db: Session = Depends(get_db),
    admin_user: User = Depends(require_admin)
):
    """Create a funnel for a specific organization"""
    # Verify organization exists
    org = db.query(Organization).filter(Organization.id == org_id).first()
    if not org:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Organization not found"
        )
    
    # Verify client_id belongs to org if provided
    if funnel_data.client_id:
        client = db.query(Client).filter(
            Client.id == funnel_data.client_id,
            Client.org_id == org_id
        ).first()
        if not client:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Client not found in this organization"
            )
    
    # Create funnel with org_id
    funnel = Funnel(
        org_id=org_id,
        name=funnel_data.name,
        client_id=funnel_data.client_id,
        slug=funnel_data.slug,
        domain=funnel_data.domain,
        env=funnel_data.env
    )
    db.add(funnel)
    db.commit()
    db.refresh(funnel)
    
    return funnel


@router.get("/organizations/{org_id}/funnels/{funnel_id}", response_model=FunnelSchema)
def get_organization_funnel(
    org_id: UUID,
    funnel_id: UUID,
    db: Session = Depends(get_db),
    admin_user: User = Depends(require_admin)
):
    """Get a specific funnel for an organization"""
    funnel = db.query(Funnel).filter(
        Funnel.id == funnel_id,
        Funnel.org_id == org_id
    ).first()
    
    if not funnel:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Funnel not found"
        )
    
    return funnel


@router.patch("/organizations/{org_id}/funnels/{funnel_id}", response_model=FunnelSchema)
def update_organization_funnel(
    org_id: UUID,
    funnel_id: UUID,
    funnel_data: OrganizationFunnelUpdate,
    db: Session = Depends(get_db),
    admin_user: User = Depends(require_admin)
):
    """Update a funnel for an organization"""
    funnel = db.query(Funnel).filter(
        Funnel.id == funnel_id,
        Funnel.org_id == org_id
    ).first()
    
    if not funnel:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Funnel not found"
        )
    
    # Verify client_id belongs to org if provided
    if funnel_data.client_id is not None:
        client = db.query(Client).filter(
            Client.id == funnel_data.client_id,
            Client.org_id == org_id
        ).first()
        if not client:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Client not found in this organization"
            )
    
    if funnel_data.name is not None:
        funnel.name = funnel_data.name
    if funnel_data.client_id is not None:
        funnel.client_id = funnel_data.client_id
    if funnel_data.slug is not None:
        funnel.slug = funnel_data.slug
    if funnel_data.domain is not None:
        funnel.domain = funnel_data.domain
    if funnel_data.env is not None:
        funnel.env = funnel_data.env
    
    db.commit()
    db.refresh(funnel)
    return funnel


@router.delete("/organizations/{org_id}/funnels/{funnel_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_organization_funnel(
    org_id: UUID,
    funnel_id: UUID,
    db: Session = Depends(get_db),
    admin_user: User = Depends(require_admin)
):
    """Delete a funnel for an organization"""
    funnel = db.query(Funnel).filter(
        Funnel.id == funnel_id,
        Funnel.org_id == org_id
    ).first()
    
    if not funnel:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Funnel not found"
        )
    
    db.delete(funnel)
    db.commit()
    return None


# Admin: Organization Tab Permissions Management
@router.get("/organizations/{org_id}/tabs", response_model=List[OrgTabPermissionSchema])
def list_organization_tab_permissions(
    org_id: UUID,
    db: Session = Depends(get_db),
    admin_user: User = Depends(require_admin)
):
    """List all tab permissions for an organization"""
    from app.api.users import AVAILABLE_TABS
    
    # Verify organization exists
    org = db.query(Organization).filter(Organization.id == org_id).first()
    if not org:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Organization not found"
        )
    
    # Get existing permissions
    existing_permissions = db.query(OrganizationTabPermission).filter(
        OrganizationTabPermission.org_id == org_id
    ).all()
    
    # Create a map of existing permissions
    permission_map = {p.tab_name: p for p in existing_permissions}
    
    # Return all tabs with their permissions (create defaults if missing)
    result = []
    for tab in AVAILABLE_TABS:
        if tab in permission_map:
            result.append(OrgTabPermissionSchema.model_validate(permission_map[tab], from_attributes=True))
        else:
            # Return default (enabled) for tabs without explicit permissions
            result.append(OrgTabPermissionSchema(
                id=uuid.uuid4(),  # Placeholder
                org_id=org_id,
                tab_name=tab,
                enabled=True,  # Default enabled
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow()
            ))
    
    return result


@router.post("/organizations/{org_id}/tabs", response_model=OrgTabPermissionSchema, status_code=status.HTTP_201_CREATED)
def create_organization_tab_permission(
    org_id: UUID,
    permission_data: OrganizationTabPermissionCreate,
    db: Session = Depends(get_db),
    admin_user: User = Depends(require_admin)
):
    """Create or update tab permission for an organization"""
    from app.schemas.permission import (
        OrganizationTabPermission as OrgTabPermissionSchema,
        OrganizationTabPermissionCreate
    )
    
    # Verify organization exists
    org = db.query(Organization).filter(Organization.id == org_id).first()
    if not org:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Organization not found"
        )
    
    # Check if permission already exists
    existing = db.query(OrganizationTabPermission).filter(
        OrganizationTabPermission.org_id == org_id,
        OrganizationTabPermission.tab_name == permission_data.tab_name
    ).first()
    
    if existing:
        existing.enabled = permission_data.enabled
        db.commit()
        db.refresh(existing)
        return OrgTabPermissionSchema.model_validate(existing, from_attributes=True)
    
    # Create new permission
    permission = OrganizationTabPermission(
        org_id=org_id,
        tab_name=permission_data.tab_name,
        enabled=permission_data.enabled
    )
    db.add(permission)
    db.commit()
    db.refresh(permission)
    return OrgTabPermissionSchema.model_validate(permission, from_attributes=True)


@router.patch("/organizations/{org_id}/tabs/{tab_name}", response_model=OrgTabPermissionSchema)
def update_organization_tab_permission(
    org_id: UUID,
    tab_name: str,
    permission_update: OrganizationTabPermissionUpdate,
    db: Session = Depends(get_db),
    admin_user: User = Depends(require_admin)
):
    """Update tab permission for an organization (creates if doesn't exist)"""
    from app.schemas.permission import (
        OrganizationTabPermission as OrgTabPermissionSchema,
        OrganizationTabPermissionUpdate
    )
    
    # Verify organization exists
    org = db.query(Organization).filter(Organization.id == org_id).first()
    if not org:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Organization not found"
        )
    
    permission = db.query(OrganizationTabPermission).filter(
        OrganizationTabPermission.org_id == org_id,
        OrganizationTabPermission.tab_name == tab_name
    ).first()
    
    # If permission doesn't exist, create it
    if not permission:
        permission = OrganizationTabPermission(
            org_id=org_id,
            tab_name=tab_name,
            enabled=permission_update.enabled if permission_update.enabled is not None else True
        )
        db.add(permission)
    else:
        # Update existing permission
        if permission_update.enabled is not None:
            permission.enabled = permission_update.enabled
    
    db.commit()
    db.refresh(permission)
    return OrgTabPermissionSchema.model_validate(permission, from_attributes=True)


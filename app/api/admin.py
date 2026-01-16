"""
Admin API endpoints for managing organizations, permissions, and global settings.
Only accessible to admin/owner users.
"""
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from sqlalchemy import func, desc
from typing import List
from uuid import UUID

from app.db.session import get_db
from app.api.deps import get_current_user, require_admin, MAIN_ORG_ID
from app.models.user import User
from app.models.organization import Organization
from app.models.client import Client
from app.models.funnel import Funnel
from app.models.event import Event
from app.models.stripe_payment import StripePayment
from app.models.stripe_subscription import StripeSubscription
from app.models.oauth_token import OAuthToken, OAuthProvider
from app.models.client import LifecycleState
from app.schemas.organization import (
    Organization as OrganizationSchema,
    OrganizationCreate,
    OrganizationUpdate,
    OrganizationWithStats,
    OrganizationCreateResponse
)
from app.schemas.admin import (
    OrganizationDashboardSummary,
    OrganizationFunnelCreate,
    OrganizationFunnelUpdate
)
from app.schemas.funnel import Funnel as FunnelSchema
from app.schemas.permission import (
    OrganizationTabPermission as OrgTabPermissionSchema,
    OrganizationTabPermissionCreate,
    OrganizationTabPermissionUpdate
)
from app.models.organization_tab_permission import OrganizationTabPermission
from app.core.config import settings
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


@router.post("/organizations", response_model=OrganizationCreateResponse, status_code=status.HTTP_201_CREATED)
def create_organization(
    org_data: OrganizationCreate,
    db: Session = Depends(get_db),
    admin_user: User = Depends(require_admin)
):
    """
    Create a new organization and automatically create an admin user for it.
    The user will have ADMIN role and be scoped only to this organization.
    """
    from app.models.user import User, UserRole
    from app.core.security import get_password_hash
    import secrets
    import string
    
    # Create the organization
    org = Organization(name=org_data.name)
    db.add(org)
    db.flush()  # Flush to get the org.id without committing
    
    # Generate default email and password if not provided
    if org_data.admin_email:
        admin_email = org_data.admin_email
    else:
        # Generate email based on org name: lowercase, replace spaces with dots, add @org.local
        org_slug = org_data.name.lower().replace(' ', '.').replace('_', '.').replace('-', '.')
        # Remove special characters
        org_slug = ''.join(c for c in org_slug if c.isalnum() or c == '.')
        admin_email = f"admin@{org_slug}.local"
    
    if org_data.admin_password:
        admin_password = org_data.admin_password
    else:
        # Generate a random password: 12 characters, alphanumeric + special
        alphabet = string.ascii_letters + string.digits + "!@#$%^&*"
        admin_password = ''.join(secrets.choice(alphabet) for _ in range(12))
    
    # Check if email already exists in this org (shouldn't, but safety check)
    existing_user = db.query(User).filter(
        User.email == admin_email,
        User.org_id == org.id
    ).first()
    
    if existing_user:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"User with email {admin_email} already exists in this organization"
        )
    
    # Create admin user for the new organization
    admin_user_for_org = User(
        org_id=org.id,
        email=admin_email,
        hashed_password=get_password_hash(admin_password),
        role=UserRole.ADMIN,  # Regular admin, not owner
        is_admin=True
    )
    db.add(admin_user_for_org)
    
    # Commit both org and user
    db.commit()
    db.refresh(org)
    
    # Return organization with admin user credentials
    # Note: In production, you might want to send these via email or secure channel
    org_dict = OrganizationSchema.model_validate(org, from_attributes=True).model_dump()
    return OrganizationCreateResponse(
        **org_dict,
        admin_email=admin_email,
        admin_password=admin_password
    )


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
    
    # Client stats
    total_clients = db.query(func.count(Client.id)).filter(
        Client.org_id == org_id
    ).scalar() or 0
    
    clients_by_status = {}
    for state in LifecycleState:
        count = db.query(func.count(Client.id)).filter(
            Client.org_id == org_id,
            Client.lifecycle_state == state
        ).scalar() or 0
        clients_by_status[state.value] = count
    
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
    
    # Stripe stats
    total_mrr = db.query(func.sum(StripeSubscription.mrr)).filter(
        StripeSubscription.org_id == org_id,
        StripeSubscription.status == 'active'
    ).scalar() or 0
    if total_mrr:
        total_mrr = float(total_mrr) / 100.0  # Convert cents to dollars
    
    total_arr = total_mrr * 12
    
    active_subscriptions = db.query(func.count(StripeSubscription.id)).filter(
        StripeSubscription.org_id == org_id,
        StripeSubscription.status == 'active'
    ).scalar() or 0
    
    total_payments = db.query(func.count(StripePayment.id)).filter(
        StripePayment.org_id == org_id
    ).scalar() or 0
    
    thirty_days_ago = datetime.utcnow() - timedelta(days=30)
    last_30_days_revenue = db.query(func.sum(StripePayment.amount_cents)).filter(
        StripePayment.org_id == org_id,
        StripePayment.status == 'succeeded',
        StripePayment.created_at >= thirty_days_ago
    ).scalar() or 0
    if last_30_days_revenue:
        last_30_days_revenue = float(last_30_days_revenue) / 100.0  # Convert cents to dollars
    
    # Brevo connection status
    brevo_token = db.query(OAuthToken).filter(
        OAuthToken.org_id == org_id,
        OAuthToken.provider == OAuthProvider.BREVO
    ).first()
    brevo_connected = brevo_token is not None
    
    # Recent clients (last 5)
    recent_clients = db.query(Client).filter(
        Client.org_id == org_id
    ).order_by(desc(Client.created_at)).limit(5).all()
    
    recent_clients_data = [
        {
            "id": str(c.id),
            "name": f"{c.first_name or ''} {c.last_name or ''}".strip() or "Unnamed",
            "email": c.email,
            "status": c.lifecycle_state.value if c.lifecycle_state else "unknown",
            "created_at": c.created_at.isoformat() if c.created_at else None
        }
        for c in recent_clients
    ]
    
    # Recent funnels (last 5)
    recent_funnels = db.query(Funnel).filter(
        Funnel.org_id == org_id
    ).order_by(desc(Funnel.created_at)).limit(5).all()
    
    recent_funnels_data = [
        {
            "id": str(f.id),
            "name": f.name,
            "domain": f.domain,
            "created_at": f.created_at.isoformat() if f.created_at else None
        }
        for f in recent_funnels
    ]
    
    # Get all funnels for the organization (not just recent)
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
    
    return OrganizationDashboardSummary(
        organization_id=org_id,
        organization_name=org.name,
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
        recent_clients=recent_clients_data,
        recent_funnels=all_funnels_data  # Return all funnels, not just recent
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
    """Update tab permission for an organization"""
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
    
    if not permission:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Permission not found"
        )
    
    if permission_update.enabled is not None:
        permission.enabled = permission_update.enabled
    
    db.commit()
    db.refresh(permission)
    return OrgTabPermissionSchema.model_validate(permission, from_attributes=True)


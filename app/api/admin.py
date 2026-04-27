"""
Admin API endpoints for managing organizations, permissions, and global settings.
Only accessible to admin/owner users.
"""
from fastapi import APIRouter, Depends, HTTPException, Request, status
from sqlalchemy.orm import Session
from sqlalchemy import func, desc, asc, and_, text, exists
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
from app.models.client_checkin import ClientCheckIn
from app.models.manual_payment import ManualPayment
from app.schemas.admin import (
    OrganizationDashboardSummary,
    OrganizationFunnelCreate,
    OrganizationFunnelUpdate,
    FunnelConversionMetric,
    FunnelStepConversion,
    GlobalHealthResponse,
    HealthTrendPeriod,
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
from datetime import datetime, timedelta, timezone

router = APIRouter()


def _utc_naive(dt_aware: datetime) -> datetime:
    return dt_aware.astimezone(timezone.utc).replace(tzinfo=None)


def _global_show_up_rate_pct(
    db: Session,
    period_start: datetime,
    period_end: datetime,
    now_utc: datetime,
) -> Optional[float]:
    """Past Cal.com/Calendly check-ins in window: attended / scheduled (non-cancelled)."""
    q = db.query(ClientCheckIn).filter(
        ClientCheckIn.cancelled == False,
        ClientCheckIn.provider.in_(["calcom", "calendly"]),
        ClientCheckIn.start_time >= period_start,
        ClientCheckIn.start_time < period_end,
        ClientCheckIn.start_time < now_utc,
    )
    rows = q.all()
    if not rows:
        return None
    attended = sum(1 for c in rows if c.completed and not c.no_show)
    total = len(rows)
    return round((attended / total) * 100.0, 1) if total else None


def _global_close_rate_pct(
    db: Session,
    period_start: datetime,
    period_end: datetime,
    now_utc: datetime,
) -> Optional[float]:
    """Sales calls: share with client having at least one succeeded Stripe payment (matches org calendar endpoint)."""
    has_succeeded_payment = exists().where(
        and_(
            StripePayment.client_id == ClientCheckIn.client_id,
            StripePayment.org_id == ClientCheckIn.org_id,
            StripePayment.status == "succeeded",
        )
    )
    base = (
        db.query(func.count(ClientCheckIn.id))
        .filter(
            ClientCheckIn.is_sales_call == True,
            ClientCheckIn.cancelled == False,
            ClientCheckIn.provider.in_(["calcom", "calendly"]),
            ClientCheckIn.start_time >= period_start,
            ClientCheckIn.start_time < period_end,
            ClientCheckIn.start_time < now_utc,
        )
    )
    total = base.scalar() or 0
    if not total:
        return None
    closed = (
        db.query(func.count(ClientCheckIn.id))
        .filter(
            ClientCheckIn.is_sales_call == True,
            ClientCheckIn.cancelled == False,
            ClientCheckIn.provider.in_(["calcom", "calendly"]),
            ClientCheckIn.start_time >= period_start,
            ClientCheckIn.start_time < period_end,
            ClientCheckIn.start_time < now_utc,
        )
        .filter(has_succeeded_payment)
        .scalar()
        or 0
    )
    return round((closed / total) * 100.0, 1)


def _utc_month_start(dt: datetime) -> datetime:
    """UTC-aware instant at start of calendar month for dt (naive assumed UTC)."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt = dt.astimezone(timezone.utc)
    return dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def _add_one_calendar_month_first(dt_start: datetime) -> datetime:
    """dt_start must be UTC-aware first-of-month."""
    y, m = dt_start.year, dt_start.month
    if m == 12:
        return dt_start.replace(year=y + 1, month=1)
    return dt_start.replace(month=m + 1)


def _org_show_up_rate_pct(
    db: Session,
    org_id: UUID,
    period_start: datetime,
    period_end: datetime,
    now_utc: datetime,
) -> Optional[float]:
    q = db.query(ClientCheckIn).filter(
        ClientCheckIn.org_id == org_id,
        ClientCheckIn.cancelled == False,
        ClientCheckIn.provider.in_(["calcom", "calendly"]),
        ClientCheckIn.start_time >= period_start,
        ClientCheckIn.start_time < period_end,
        ClientCheckIn.start_time < now_utc,
    )
    rows = q.all()
    if not rows:
        return None
    attended = sum(1 for c in rows if c.completed and not c.no_show)
    total = len(rows)
    return round((attended / total) * 100.0, 1) if total else None


def _org_close_rate_pct(
    db: Session,
    org_id: UUID,
    period_start: datetime,
    period_end: datetime,
    now_utc: datetime,
) -> Optional[float]:
    has_succeeded_payment = exists().where(
        and_(
            StripePayment.client_id == ClientCheckIn.client_id,
            StripePayment.org_id == org_id,
            StripePayment.status == "succeeded",
        )
    )
    base = (
        db.query(func.count(ClientCheckIn.id))
        .filter(
            ClientCheckIn.org_id == org_id,
            ClientCheckIn.is_sales_call == True,
            ClientCheckIn.cancelled == False,
            ClientCheckIn.provider.in_(["calcom", "calendly"]),
            ClientCheckIn.start_time >= period_start,
            ClientCheckIn.start_time < period_end,
            ClientCheckIn.start_time < now_utc,
        )
    )
    total = base.scalar() or 0
    if not total:
        return None
    closed = (
        db.query(func.count(ClientCheckIn.id))
        .filter(
            ClientCheckIn.org_id == org_id,
            ClientCheckIn.is_sales_call == True,
            ClientCheckIn.cancelled == False,
            ClientCheckIn.provider.in_(["calcom", "calendly"]),
            ClientCheckIn.start_time >= period_start,
            ClientCheckIn.start_time < period_end,
            ClientCheckIn.start_time < now_utc,
        )
        .filter(has_succeeded_payment)
        .scalar()
        or 0
    )
    return round((closed / total) * 100.0, 1)


def _org_cash_usd_window(
    db: Session,
    org_id: UUID,
    org_created_naive: datetime,
    ps_naive: datetime,
    pe_exclusive_naive: datetime,
    uses_treasury: bool,
    now_naive: datetime,
) -> float:
    """Cash in [ps, pe); clips start to onboarding. Treasury or Stripe-only (matches single-org 30d logic)."""
    effective_start = max(ps_naive.replace(tzinfo=None) if ps_naive.tzinfo else ps_naive, org_created_naive)
    if effective_start >= pe_exclusive_naive:
        return 0.0
    ce = min(pe_exclusive_naive.replace(tzinfo=None) if pe_exclusive_naive.tzinfo else pe_exclusive_naive, now_naive)
    if effective_start >= ce:
        return 0.0
    if uses_treasury:
        tsum = db.query(func.coalesce(func.sum(StripeTreasuryTransaction.amount), 0)).filter(
            StripeTreasuryTransaction.org_id == org_id,
            StripeTreasuryTransaction.status == TreasuryTransactionStatus.POSTED,
            StripeTreasuryTransaction.amount > 0,
            StripeTreasuryTransaction.posted_at >= effective_start,
            StripeTreasuryTransaction.posted_at < ce,
            StripeTreasuryTransaction.posted_at >= org_created_naive,
        ).scalar()
        return float(tsum or 0) / 100.0
    cents = db.query(func.coalesce(func.sum(StripePayment.amount_cents), 0)).filter(
        StripePayment.org_id == org_id,
        StripePayment.status == "succeeded",
        StripePayment.created_at >= effective_start,
        StripePayment.created_at < ce,
        StripePayment.created_at >= org_created_naive,
    ).scalar()
    return float(cents or 0) / 100.0


def _org_cash_total_since_onboarding(
    db: Session,
    org_id: UUID,
    org_created_naive: datetime,
    uses_treasury: bool,
    now_naive: datetime,
) -> float:
    if uses_treasury:
        tsum = db.query(func.coalesce(func.sum(StripeTreasuryTransaction.amount), 0)).filter(
            StripeTreasuryTransaction.org_id == org_id,
            StripeTreasuryTransaction.status == TreasuryTransactionStatus.POSTED,
            StripeTreasuryTransaction.amount > 0,
            StripeTreasuryTransaction.posted_at >= org_created_naive,
            StripeTreasuryTransaction.posted_at <= now_naive,
        ).scalar()
        return float(tsum or 0) / 100.0
    cents = db.query(func.coalesce(func.sum(StripePayment.amount_cents), 0)).filter(
        StripePayment.org_id == org_id,
        StripePayment.status == "succeeded",
        StripePayment.created_at >= org_created_naive,
        StripePayment.created_at <= now_naive,
    ).scalar()
    return float(cents or 0) / 100.0


def _org_cash_all_time(
    db: Session,
    org_id: UUID,
    uses_treasury: bool,
    now_naive: datetime,
) -> float:
    """Canonical all-time cash for one org (Treasury if present, else Stripe), through now."""
    if uses_treasury:
        tsum = db.query(func.coalesce(func.sum(StripeTreasuryTransaction.amount), 0)).filter(
            StripeTreasuryTransaction.org_id == org_id,
            StripeTreasuryTransaction.status == TreasuryTransactionStatus.POSTED,
            StripeTreasuryTransaction.amount > 0,
            StripeTreasuryTransaction.posted_at <= now_naive,
        ).scalar()
        return float(tsum or 0) / 100.0
    cents = db.query(func.coalesce(func.sum(StripePayment.amount_cents), 0)).filter(
        StripePayment.org_id == org_id,
        StripePayment.status == "succeeded",
        StripePayment.created_at <= now_naive,
    ).scalar()
    return float(cents or 0) / 100.0


def _org_manual_cash_all_time(db: Session, org_id: UUID) -> float:
    cents = db.query(func.coalesce(func.sum(ManualPayment.amount_cents), 0)).filter(
        ManualPayment.org_id == org_id,
    ).scalar()
    return float(cents or 0) / 100.0


def _global_stripe_rev_month_post_onboarding(
    db: Session,
    ps_naive: datetime,
    pe_exclusive_naive: datetime,
) -> float:
    """Succeeded Stripe volume in window, only for payments at/after each organization's created_at."""
    cents = (
        db.query(func.coalesce(func.sum(StripePayment.amount_cents), 0))
        .join(Organization, Organization.id == StripePayment.org_id)
        .filter(
            StripePayment.status == "succeeded",
            StripePayment.created_at >= ps_naive,
            StripePayment.created_at < pe_exclusive_naive,
            StripePayment.created_at >= Organization.created_at,
        )
        .scalar()
        or 0
    )
    return float(cents) / 100.0


def _first_of_month_n_months_ago(now_utc: datetime, n: int) -> datetime:
    cur = _utc_month_start(now_utc)
    for _ in range(n):
        y, m = cur.year, cur.month
        if m == 1:
            cur = cur.replace(year=y - 1, month=12)
        else:
            cur = cur.replace(month=m - 1)
    return cur


def _month_series_global_start(db: Session, now_utc: datetime, max_months: int) -> datetime:
    """UTC-aware first-of-month for trend grid (earliest org vs capped lookback)."""
    earliest_month = _utc_month_start(now_utc)
    row = db.query(func.min(Organization.created_at)).scalar()
    if row is not None:
        anchor = row.replace(tzinfo=timezone.utc) if row.tzinfo is None else row.astimezone(timezone.utc)
        earliest_month = _utc_month_start(anchor)
    capped = _first_of_month_n_months_ago(now_utc, max(0, max_months - 1))
    return max(earliest_month, capped)


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
@router.get("/health", response_model=GlobalHealthResponse)
def get_global_health(
    db: Session = Depends(get_db),
    admin_user: User = Depends(require_admin)
):
    """Platform-wide metrics: revenue, scale, funnel engagement, growth (30d), integrations."""
    now = datetime.utcnow()
    thirty_days_ago = now - timedelta(days=30)

    total_orgs = db.query(func.count(Organization.id)).scalar() or 0
    orgs_created_30d = db.query(func.count(Organization.id)).filter(
        Organization.created_at >= thirty_days_ago
    ).scalar() or 0

    total_users = db.query(func.count(User.id)).scalar() or 0
    users_created_30d = db.query(func.count(User.id)).filter(
        User.created_at >= thirty_days_ago
    ).scalar() or 0

    total_clients = db.query(func.count(Client.id)).scalar() or 0
    clients_created_30d = db.query(func.count(Client.id)).filter(
        Client.created_at >= thirty_days_ago
    ).scalar() or 0

    total_funnels = db.query(func.count(Funnel.id)).scalar() or 0
    total_events = db.query(func.count(Event.id)).scalar() or 0
    total_events_30d = db.query(func.count(Event.id)).filter(
        Event.occurred_at >= thirty_days_ago
    ).scalar() or 0

    total_payments = db.query(func.count(StripePayment.id)).scalar() or 0
    total_subscriptions = db.query(func.count(StripeSubscription.id)).scalar() or 0
    active_subscriptions = db.query(func.count(StripeSubscription.id)).filter(
        StripeSubscription.status.in_(["active", "trialing"])
    ).scalar() or 0

    mrr_sum = db.query(func.sum(StripeSubscription.mrr)).filter(
        StripeSubscription.status.in_(["active", "trialing"])
    ).scalar()
    total_mrr_usd = float(mrr_sum) if mrr_sum is not None else 0.0

    rev_all = db.query(func.sum(StripePayment.amount_cents)).filter(
        StripePayment.status == "succeeded"
    ).scalar() or 0
    total_revenue_stripe = float(rev_all) / 100.0

    rev_30 = db.query(func.sum(StripePayment.amount_cents)).filter(
        StripePayment.status == "succeeded",
        StripePayment.created_at >= thirty_days_ago,
    ).scalar() or 0
    last_30_stripe = float(rev_30) / 100.0

    treasury_30 = 0.0
    try:
        tsum = db.query(func.sum(StripeTreasuryTransaction.amount)).filter(
            StripeTreasuryTransaction.status == TreasuryTransactionStatus.POSTED,
            StripeTreasuryTransaction.amount > 0,
            StripeTreasuryTransaction.posted_at >= thirty_days_ago,
            StripeTreasuryTransaction.posted_at <= now,
        ).scalar()
        treasury_30 = float(tsum) / 100.0 if tsum else 0.0
    except Exception:
        # Failed SQL poisons the transaction; must rollback before further queries
        db.rollback()
        treasury_30 = 0.0

    treasury_all_time_usd = 0.0
    try:
        tsum_all = db.query(func.coalesce(func.sum(StripeTreasuryTransaction.amount), 0)).filter(
            StripeTreasuryTransaction.status == TreasuryTransactionStatus.POSTED,
            StripeTreasuryTransaction.amount > 0,
        ).scalar()
        treasury_all_time_usd = float(tsum_all or 0) / 100.0
    except Exception:
        db.rollback()
        treasury_all_time_usd = 0.0

    cash_collected_all_time_combined_usd = total_revenue_stripe + treasury_all_time_usd

    manual_cash_all_time_usd = 0.0
    try:
        msum = db.query(func.coalesce(func.sum(ManualPayment.amount_cents), 0)).scalar() or 0
        manual_cash_all_time_usd = float(msum) / 100.0
    except Exception:
        db.rollback()
        manual_cash_all_time_usd = 0.0

    total_processor_revenue_all_time_usd = (
        cash_collected_all_time_combined_usd + manual_cash_all_time_usd
    )

    # First funnel step = event rows matching each funnel's lowest step_order (PostgreSQL DISTINCT ON)
    funnel_first_all = 0
    funnel_first_30d = 0
    try:
        row_all = db.execute(
            text("""
                WITH first_steps AS (
                    SELECT DISTINCT ON (funnel_id) funnel_id, event_name
                    FROM funnel_steps
                    ORDER BY funnel_id, step_order ASC
                )
                SELECT COUNT(e.id)::bigint AS cnt
                FROM events e
                INNER JOIN first_steps fs ON e.funnel_id = fs.funnel_id AND e.event_name = fs.event_name
            """)
        ).fetchone()
        funnel_first_all = int(row_all[0] or 0) if row_all else 0

        row_30 = db.execute(
            text("""
                WITH first_steps AS (
                    SELECT DISTINCT ON (funnel_id) funnel_id, event_name
                    FROM funnel_steps
                    ORDER BY funnel_id, step_order ASC
                )
                SELECT COUNT(e.id)::bigint AS cnt
                FROM events e
                INNER JOIN first_steps fs ON e.funnel_id = fs.funnel_id AND e.event_name = fs.event_name
                WHERE e.occurred_at >= :thirty
            """),
            {"thirty": thirty_days_ago},
        ).fetchone()
        funnel_first_30d = int(row_30[0] or 0) if row_30 else 0
    except Exception:
        db.rollback()
        funnel_first_all = 0
        funnel_first_30d = 0

    unique_visitors_all = db.query(func.count(func.distinct(Event.visitor_id))).filter(
        Event.visitor_id.isnot(None)
    ).scalar() or 0
    unique_visitors_30d = db.query(func.count(func.distinct(Event.visitor_id))).filter(
        Event.visitor_id.isnot(None),
        Event.occurred_at >= thirty_days_ago,
    ).scalar() or 0

    orgs_stripe = db.query(func.count(func.distinct(OAuthToken.org_id))).filter(
        OAuthToken.provider == OAuthProvider.STRIPE
    ).scalar() or 0
    orgs_brevo = db.query(func.count(func.distinct(OAuthToken.org_id))).filter(
        OAuthToken.provider == OAuthProvider.BREVO
    ).scalar() or 0

    pending_inv = db.query(func.count(OrganizationInvitation.id)).filter(
        OrganizationInvitation.used_at.is_(None),
        OrganizationInvitation.expires_at > now,
    ).scalar() or 0

    sixty_days_ago = now - timedelta(days=30) * 2

    # Succeeded Stripe (all time) at/after each org's platform onboarding
    stripe_post_onboarding_cents = (
        db.query(func.coalesce(func.sum(StripePayment.amount_cents), 0))
        .join(Organization, Organization.id == StripePayment.org_id)
        .filter(
            StripePayment.status == "succeeded",
            StripePayment.created_at >= Organization.created_at,
            StripePayment.created_at <= now,
        )
        .scalar()
        or 0
    )
    stripe_revenue_post_onboarding_usd = float(stripe_post_onboarding_cents) / 100.0

    invitation_emails_sent_last_30d = (
        db.query(func.count(OrganizationInvitation.id))
        .filter(OrganizationInvitation.created_at >= thirty_days_ago)
        .scalar()
        or 0
    )
    invitation_emails_sent_previous_30d = (
        db.query(func.count(OrganizationInvitation.id))
        .filter(
            OrganizationInvitation.created_at >= sixty_days_ago,
            OrganizationInvitation.created_at < thirty_days_ago,
        )
        .scalar()
        or 0
    )

    now_utc = datetime.now(timezone.utc)
    thirty_utc = now_utc - timedelta(days=30)
    sixty_utc = now_utc - timedelta(days=60)

    calls_booked_last_30d = (
        db.query(func.count(ClientCheckIn.id))
        .filter(
            ClientCheckIn.start_time >= thirty_utc,
            ClientCheckIn.start_time < now_utc,
        )
        .scalar()
        or 0
    )
    calls_booked_previous_30d = (
        db.query(func.count(ClientCheckIn.id))
        .filter(
            ClientCheckIn.start_time >= sixty_utc,
            ClientCheckIn.start_time < thirty_utc,
        )
        .scalar()
        or 0
    )

    lifecycle_active_clients_current = (
        db.query(func.count(Client.id))
        .filter(Client.lifecycle_state == LifecycleState.ACTIVE)
        .scalar()
        or 0
    )
    lifecycle_active_clients_previous_30d_cohort = (
        db.query(func.count(Client.id))
        .filter(
            Client.lifecycle_state == LifecycleState.ACTIVE,
            Client.created_at < thirty_days_ago,
        )
        .scalar()
        or 0
    )

    show_up_rate_last_30d_pct = _global_show_up_rate_pct(db, thirty_utc, now_utc, now_utc)
    close_rate_last_30d_pct = _global_close_rate_pct(db, thirty_utc, now_utc, now_utc)

    # Calendar months since first org onboarding (cap 36 months): monthly show-up, close rate, Stripe post-onboarding
    health_trend_periods: List[HealthTrendPeriod] = []
    MAX_HEALTH_MONTHS = 36
    grid_start = _month_series_global_start(db, now_utc, MAX_HEALTH_MONTHS)
    month_cursor = grid_start
    while month_cursor <= now_utc:
        month_end_exclusive = min(_add_one_calendar_month_first(month_cursor), now_utc)
        ps_naive = _utc_naive(month_cursor)
        pe_naive_exclusive = _utc_naive(month_end_exclusive)

        period_label = month_cursor.strftime("%b %Y")

        stripe_rev = _global_stripe_rev_month_post_onboarding(db, ps_naive, pe_naive_exclusive)

        calls_ct = (
            db.query(func.count(ClientCheckIn.id))
            .filter(
                ClientCheckIn.start_time >= month_cursor,
                ClientCheckIn.start_time < month_end_exclusive,
            )
            .scalar()
            or 0
        )

        cum_clients = (
            db.query(func.count(Client.id)).filter(Client.created_at < pe_naive_exclusive).scalar() or 0
        )
        active_cohort = (
            db.query(func.count(Client.id))
            .filter(
                Client.lifecycle_state == LifecycleState.ACTIVE,
                Client.created_at < pe_naive_exclusive,
            )
            .scalar()
            or 0
        )

        sup = _global_show_up_rate_pct(db, month_cursor, month_end_exclusive, now_utc)
        cr = _global_close_rate_pct(db, month_cursor, month_end_exclusive, now_utc)

        health_trend_periods.append(
            HealthTrendPeriod(
                period_label=period_label,
                period_start=month_cursor.isoformat(),
                period_end=month_end_exclusive.isoformat(),
                show_up_rate_pct=sup,
                close_rate_pct=cr,
                stripe_revenue_usd=stripe_rev,
                calls_booked_count=calls_ct,
                cumulative_total_clients=cum_clients,
                active_clients_cohort=active_cohort,
            )
        )

        if month_end_exclusive >= now_utc:
            break
        month_cursor = _add_one_calendar_month_first(month_cursor)

    return GlobalHealthResponse(
        total_organizations=total_orgs,
        organizations_created_last_30_days=orgs_created_30d,
        total_users=total_users,
        users_created_last_30_days=users_created_30d,
        total_clients=total_clients,
        clients_created_last_30_days=clients_created_30d,
        total_funnels=total_funnels,
        total_events=total_events,
        total_events_last_30_days=total_events_30d,
        total_payments=total_payments,
        total_subscriptions=total_subscriptions,
        active_subscriptions=active_subscriptions,
        total_mrr_usd=total_mrr_usd,
        total_revenue_stripe_succeeded_usd=total_revenue_stripe,
        last_30_days_revenue_stripe_usd=last_30_stripe,
        treasury_posted_last_30_days_usd=treasury_30,
        treasury_posted_all_time_usd=treasury_all_time_usd,
        cash_collected_all_time_combined_usd=cash_collected_all_time_combined_usd,
        manual_cash_all_time_usd=manual_cash_all_time_usd,
        total_processor_revenue_all_time_usd=total_processor_revenue_all_time_usd,
        funnel_first_step_views_all_time=funnel_first_all,
        funnel_first_step_views_last_30_days=funnel_first_30d,
        unique_visitors_all_time=unique_visitors_all,
        unique_visitors_last_30_days=unique_visitors_30d,
        orgs_with_stripe_connected=orgs_stripe,
        orgs_with_brevo_connected=orgs_brevo,
        pending_invitations=pending_inv,
        stripe_revenue_post_onboarding_usd=stripe_revenue_post_onboarding_usd,
        invitation_emails_sent_last_30d=invitation_emails_sent_last_30d,
        invitation_emails_sent_previous_30d=invitation_emails_sent_previous_30d,
        calls_booked_last_30d=calls_booked_last_30d,
        calls_booked_previous_30d=calls_booked_previous_30d,
        lifecycle_active_clients_current=lifecycle_active_clients_current,
        lifecycle_active_clients_previous_30d_cohort=lifecycle_active_clients_previous_30d_cohort,
        show_up_rate_last_30d_pct=show_up_rate_last_30d_pct,
        close_rate_last_30d_pct=close_rate_last_30d_pct,
        health_trend_periods=health_trend_periods,
    )


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

    # Owner modal: cash + monthly coaching metrics since org onboarding (calendar months)
    uses_treasury = treasury_count is not None
    org_created_naive = org.created_at.replace(tzinfo=None) if org.created_at else datetime.utcnow()
    now_naive = datetime.utcnow()
    now_utc_dash = datetime.now(timezone.utc)
    cash_collected_since_onboarding_usd = _org_cash_total_since_onboarding(
        db, org_id, org_created_naive, uses_treasury, now_naive
    )
    cash_collected_all_time_usd = _org_cash_all_time(db, org_id, uses_treasury, now_naive)
    manual_cash_all_time_usd = _org_manual_cash_all_time(db, org_id)
    total_processor_revenue_all_time_usd = cash_collected_all_time_usd + manual_cash_all_time_usd

    monthly_health_since_onboarding: List[HealthTrendPeriod] = []
    if org.created_at:
        oc_anchor = (
            org.created_at.replace(tzinfo=timezone.utc)
            if org.created_at.tzinfo is None
            else org.created_at.astimezone(timezone.utc)
        )
    else:
        oc_anchor = now_utc_dash
    month_cursor = _utc_month_start(oc_anchor)
    cap_month = _first_of_month_n_months_ago(now_utc_dash, 35)
    if month_cursor < cap_month:
        month_cursor = cap_month

    while month_cursor <= now_utc_dash:
        month_end_exclusive = min(_add_one_calendar_month_first(month_cursor), now_utc_dash)
        ps_naive = _utc_naive(month_cursor)
        pe_naive_exclusive = _utc_naive(month_end_exclusive)

        cash_m = _org_cash_usd_window(
            db,
            org_id,
            org_created_naive,
            ps_naive,
            pe_naive_exclusive,
            uses_treasury,
            now_naive,
        )

        calls_ct = (
            db.query(func.count(ClientCheckIn.id))
            .filter(
                ClientCheckIn.org_id == org_id,
                ClientCheckIn.start_time >= month_cursor,
                ClientCheckIn.start_time < month_end_exclusive,
            )
            .scalar()
            or 0
        )

        cum_clients = (
            db.query(func.count(Client.id))
            .filter(Client.org_id == org_id, Client.created_at < pe_naive_exclusive)
            .scalar()
            or 0
        )
        active_cohort = (
            db.query(func.count(Client.id))
            .filter(
                Client.org_id == org_id,
                Client.lifecycle_state == LifecycleState.ACTIVE,
                Client.created_at < pe_naive_exclusive,
            )
            .scalar()
            or 0
        )

        sup = _org_show_up_rate_pct(db, org_id, month_cursor, month_end_exclusive, now_utc_dash)
        cr = _org_close_rate_pct(db, org_id, month_cursor, month_end_exclusive, now_utc_dash)

        monthly_health_since_onboarding.append(
            HealthTrendPeriod(
                period_label=month_cursor.strftime("%b %Y"),
                period_start=month_cursor.isoformat(),
                period_end=month_end_exclusive.isoformat(),
                show_up_rate_pct=sup,
                close_rate_pct=cr,
                stripe_revenue_usd=cash_m,
                calls_booked_count=calls_ct,
                cumulative_total_clients=cum_clients,
                active_clients_cohort=active_cohort,
            )
        )

        if month_end_exclusive >= now_utc_dash:
            break
        month_cursor = _add_one_calendar_month_first(month_cursor)

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
        recent_funnels=all_funnels_data,
        organization_onboarded_at=org.created_at.isoformat() if org.created_at else None,
        cash_collected_since_onboarding_usd=cash_collected_since_onboarding_usd,
        cash_collected_all_time_usd=cash_collected_all_time_usd,
        manual_cash_all_time_usd=manual_cash_all_time_usd,
        total_processor_revenue_all_time_usd=total_processor_revenue_all_time_usd,
        monthly_health_since_onboarding=monthly_health_since_onboarding,
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


"""
Funnel Analytics API
Provides CRUD for funnels, steps, event ingestion, analytics, and health monitoring.
"""
from fastapi import APIRouter, Depends, HTTPException, status, Query
from fastapi import Request
from sqlalchemy.orm import Session
from sqlalchemy import func, and_, or_, desc, asc
from typing import List, Optional, Union
from datetime import datetime, timedelta, timezone
from uuid import UUID
import uuid
import re

from app.db.session import get_db
from app.api.deps import get_current_user
from app.models.user import User
from app.models.funnel import Funnel, FunnelStep
from app.models.event import Event
from app.models.session import Session
from app.models.event_error import EventError
from app.models.client import Client
from app.schemas.funnel import (
    Funnel as FunnelSchema,
    FunnelCreate,
    FunnelUpdate,
    FunnelWithSteps,
    FunnelStep as FunnelStepSchema,
    FunnelStepCreate,
    FunnelStepUpdate,
    EventIn,
    EventResponse,
    FunnelHealth,
    FunnelAnalytics,
    StepCount,
    UTMSourceStats,
    ReferrerStats,
)

router = APIRouter()


# Funnel CRUD
@router.get("", response_model=List[FunnelSchema])
def list_funnels(
    client_id: Optional[UUID] = Query(None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """List all funnels for the current organization"""
    # Get selected org_id from user object (set by get_current_user)
    org_id = getattr(current_user, 'selected_org_id', current_user.org_id)
    
    query = db.query(Funnel).filter(Funnel.org_id == org_id)
    
    if client_id:
        query = query.filter(Funnel.client_id == client_id)
    
    funnels = query.order_by(desc(Funnel.created_at)).all()
    return funnels


@router.post("", response_model=FunnelSchema, status_code=status.HTTP_201_CREATED)
def create_funnel(
    funnel_data: FunnelCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Create a new funnel"""
    # Get selected org_id from user object (set by get_current_user)
    org_id = getattr(current_user, 'selected_org_id', current_user.org_id)
    
    # CRITICAL: Set org_id from selected org (token)
    funnel_dict = funnel_data.model_dump()
    funnel_dict['org_id'] = org_id
    
    # Verify client_id belongs to org if provided
    if funnel_dict.get('client_id'):
        client = db.query(Client).filter(
            Client.id == funnel_dict['client_id'],
            Client.org_id == org_id
        ).first()
        if not client:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Client not found"
            )
    
    funnel = Funnel(**funnel_dict)
    db.add(funnel)
    db.commit()
    db.refresh(funnel)
    return funnel


@router.get("/{funnel_id}", response_model=FunnelWithSteps)
def get_funnel(
    funnel_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get funnel details with steps"""
    # Get selected org_id from user object (set by get_current_user)
    org_id = getattr(current_user, 'selected_org_id', current_user.org_id)
    
    funnel = db.query(Funnel).filter(
        Funnel.id == funnel_id,
        Funnel.org_id == org_id
    ).first()
    
    if not funnel:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Funnel not found"
        )
    
    # Load steps
    steps = db.query(FunnelStep).filter(
        FunnelStep.funnel_id == funnel_id,
        FunnelStep.org_id == org_id
    ).order_by(asc(FunnelStep.step_order)).all()
    
    funnel_dict = {
        **FunnelSchema.model_validate(funnel, from_attributes=True).model_dump(),
        "steps": [FunnelStepSchema.model_validate(step, from_attributes=True) for step in steps]
    }
    
    return FunnelWithSteps(**funnel_dict)


@router.patch("/{funnel_id}", response_model=FunnelSchema)
def update_funnel(
    funnel_id: UUID,
    funnel_update: FunnelUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Update a funnel"""
    # Get selected org_id from user object (set by get_current_user)
    org_id = getattr(current_user, 'selected_org_id', current_user.org_id)
    
    funnel = db.query(Funnel).filter(
        Funnel.id == funnel_id,
        Funnel.org_id == org_id
    ).first()
    
    if not funnel:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Funnel not found"
        )
    
    update_data = funnel_update.model_dump(exclude_unset=True)
    
    # Verify client_id belongs to org if provided
    if 'client_id' in update_data and update_data['client_id']:
        client = db.query(Client).filter(
            Client.id == update_data['client_id'],
            Client.org_id == org_id
        ).first()
        if not client:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Client not found"
            )
    
    for field, value in update_data.items():
        setattr(funnel, field, value)
    
    funnel.updated_at = datetime.utcnow()
    db.commit()
    db.refresh(funnel)
    return funnel


@router.delete("/{funnel_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_funnel(
    funnel_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Delete a funnel (cascades to steps)"""
    # Get selected org_id from user object (set by get_current_user)
    org_id = getattr(current_user, 'selected_org_id', current_user.org_id)
    
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


# Funnel Steps CRUD
@router.post("/{funnel_id}/steps", response_model=FunnelStepSchema, status_code=status.HTTP_201_CREATED)
def create_funnel_step(
    funnel_id: UUID,
    step_data: FunnelStepCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Add a step to a funnel"""
    # Get selected org_id from user object (set by get_current_user)
    org_id = getattr(current_user, 'selected_org_id', current_user.org_id)
    
    # Verify funnel exists and belongs to org
    funnel = db.query(Funnel).filter(
        Funnel.id == funnel_id,
        Funnel.org_id == org_id
    ).first()
    
    if not funnel:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Funnel not found"
        )
    
    step_dict = step_data.model_dump()
    step_dict['org_id'] = org_id
    step_dict['funnel_id'] = funnel_id
    
    step = FunnelStep(**step_dict)
    db.add(step)
    db.commit()
    db.refresh(step)
    return step


@router.patch("/{funnel_id}/steps/{step_id}", response_model=FunnelStepSchema)
def update_funnel_step(
    funnel_id: UUID,
    step_id: UUID,
    step_update: FunnelStepUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Update a funnel step"""
    # Get selected org_id from user object (set by get_current_user)
    org_id = getattr(current_user, 'selected_org_id', current_user.org_id)
    
    step = db.query(FunnelStep).filter(
        FunnelStep.id == step_id,
        FunnelStep.funnel_id == funnel_id,
        FunnelStep.org_id == org_id
    ).first()
    
    if not step:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Step not found"
        )
    
    update_data = step_update.model_dump(exclude_unset=True)
    for field, value in update_data.items():
        setattr(step, field, value)
    
    step.updated_at = datetime.utcnow()
    db.commit()
    db.refresh(step)
    return step


@router.delete("/{funnel_id}/steps/{step_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_funnel_step(
    funnel_id: UUID,
    step_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Delete a funnel step"""
    # Get selected org_id from user object (set by get_current_user)
    org_id = getattr(current_user, 'selected_org_id', current_user.org_id)
    
    step = db.query(FunnelStep).filter(
        FunnelStep.id == step_id,
        FunnelStep.funnel_id == funnel_id,
        FunnelStep.org_id == org_id
    ).first()
    
    if not step:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Step not found"
        )
    
    db.delete(step)
    db.commit()
    return None


@router.post("/{funnel_id}/steps/reorder", response_model=List[FunnelStepSchema])
def reorder_funnel_steps(
    funnel_id: UUID,
    step_orders: List[dict],  # [{"step_id": "uuid", "step_order": 1}, ...]
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Reorder funnel steps by updating step_order"""
    # Get selected org_id from user object (set by get_current_user)
    org_id = getattr(current_user, 'selected_org_id', current_user.org_id)
    
    # Verify funnel exists
    funnel = db.query(Funnel).filter(
        Funnel.id == funnel_id,
        Funnel.org_id == org_id
    ).first()
    
    if not funnel:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Funnel not found"
        )
    
    # Update step orders
    for item in step_orders:
        step_id = UUID(item['step_id'])
        new_order = item['step_order']
        
        step = db.query(FunnelStep).filter(
            FunnelStep.id == step_id,
            FunnelStep.funnel_id == funnel_id,
            FunnelStep.org_id == org_id
        ).first()
        
        if step:
            step.step_order = new_order
            step.updated_at = datetime.utcnow()
    
    db.commit()
    
    # Return updated steps
    steps = db.query(FunnelStep).filter(
        FunnelStep.funnel_id == funnel_id,
        FunnelStep.org_id == org_id
    ).order_by(asc(FunnelStep.step_order)).all()
    
    return [FunnelStepSchema.model_validate(step, from_attributes=True) for step in steps]


# Event Ingestion - Public endpoint for client-side tracking
@router.post("/events", response_model=EventResponse, status_code=status.HTTP_202_ACCEPTED)
def ingest_event(
    event_data: EventIn,
    db: Session = Depends(get_db)
):
    """
    Ingest a funnel event (public endpoint for client-side tracking).
    Accepts event payload and stores it, then processes synchronously.
    No authentication required - org_id is determined from funnel_id.
    """
    try:
        # Funnel ID is required for public endpoint
        if not event_data.funnel_id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="funnel_id is required"
            )
        
        # Validate funnel exists and get org_id
        funnel = db.query(Funnel).filter(
            Funnel.id == event_data.funnel_id
        ).first()
        
        if not funnel:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Funnel not found"
            )
        
        org_id = funnel.org_id
        
        # Validate client_id if provided (must belong to same org)
        if event_data.client_id:
            client = db.query(Client).filter(
                Client.id == event_data.client_id,
                Client.org_id == org_id
            ).first()
            if not client:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Client not found"
                )
        
        # Check idempotency if key provided
        if event_data.idempotency_key:
            existing = db.query(Event).filter(
                Event.org_id == org_id,
                Event.event_metadata.contains({"idempotency_key": event_data.idempotency_key})
            ).first()
            if existing:
                return EventResponse(event_id=existing.id, status="duplicate")
        
        # Create event
        event_timestamp = event_data.event_timestamp or datetime.utcnow()
        
        event = Event(
            org_id=org_id,
            funnel_id=event_data.funnel_id,
            client_id=event_data.client_id,
            type="funnel_event",
            event_name=event_data.event_name,
            visitor_id=event_data.visitor_id,
            session_id=event_data.session_id,
            event_metadata=event_data.metadata or {},
            payload=event_data.metadata,  # Store metadata in payload for backward compatibility
            occurred_at=event_timestamp,
            received_at=datetime.utcnow()
        )
        
        # Add idempotency key to metadata if provided
        if event_data.idempotency_key:
            event.event_metadata = event.event_metadata or {}
            event.event_metadata['idempotency_key'] = event_data.idempotency_key
        
        db.add(event)
        db.commit()
        db.refresh(event)
        
        # Process event synchronously (enrichment and session tracking)
        _enrich_and_process_event(db, event, org_id)
        
        return EventResponse(event_id=event.id, status="accepted")
        
    except HTTPException:
        raise
    except Exception as e:
        # Log error
        error = EventError(
            payload=event_data.model_dump(),
            reason=str(e)
        )
        db.add(error)
        db.commit()
        
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error processing event: {str(e)}"
        )


def _enrich_and_process_event(db: Session, event: Event, org_id: UUID):
    """Enrich event and update session tracking"""
    # Update or create session
    if event.visitor_id or event.session_id:
        session = db.query(Session).filter(
            Session.org_id == org_id,
            or_(
                Session.session_id == event.session_id,
                Session.visitor_id == event.visitor_id
            )
        ).first()
        
        if session:
            session.last_seen = datetime.utcnow()
            # Update UTM if present in metadata
            if event.event_metadata and 'utm' in event.event_metadata:
                session.utm = event.event_metadata['utm']
            # Update referrer if present in metadata (only on first event of session)
            if event.event_metadata and 'referrer' in event.event_metadata and not session.referrer:
                session.referrer = event.event_metadata['referrer']
        else:
            # Create new session
            utm = event.event_metadata.get('utm') if event.event_metadata else None
            referrer = event.event_metadata.get('referrer') if event.event_metadata else None
            session = Session(
                org_id=org_id,
                visitor_id=event.visitor_id,
                session_id=event.session_id,
                first_seen=event.occurred_at,
                last_seen=event.occurred_at,
                utm=utm,
                referrer=referrer,
                session_metadata=event.event_metadata
            )
            db.add(session)
        
        db.commit()
    
    # Try to infer funnel_id from metadata if not provided
    if not event.funnel_id and event.event_metadata:
        page_url = event.event_metadata.get('page_url') or event.event_metadata.get('url')
        if page_url:
            # Try to match by domain
            try:
                from urllib.parse import urlparse
                parsed = urlparse(page_url)
                domain = parsed.netloc
                
                funnel = db.query(Funnel).filter(
                    Funnel.org_id == org_id,
                    Funnel.domain == domain
                ).first()
                
                if funnel:
                    event.funnel_id = funnel.id
                    db.commit()
            except Exception:
                # URL parsing failed, skip
                pass


# Analytics & Health
@router.get("/{funnel_id}/health", response_model=FunnelHealth)
def get_funnel_health(
    funnel_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get funnel health metrics"""
    # Get selected org_id from user object (set by get_current_user)
    org_id = getattr(current_user, 'selected_org_id', current_user.org_id)
    
    # Verify funnel exists
    funnel = db.query(Funnel).filter(
        Funnel.id == funnel_id,
        Funnel.org_id == org_id
    ).first()
    
    if not funnel:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Funnel not found"
        )
    
    # Get last event time - use received_at (server timestamp) for accuracy
    # This is more reliable than occurred_at which depends on client clock
    last_event = db.query(Event).filter(
        Event.funnel_id == funnel_id,
        Event.org_id == org_id
    ).order_by(desc(Event.received_at)).first()
    
    # Use received_at for last event time (when we actually received it)
    # Fall back to occurred_at if received_at is not set (backward compatibility)
    # Ensure timezone-aware datetime for proper serialization
    if last_event:
        if last_event.received_at:
            last_event_at = last_event.received_at
        elif last_event.occurred_at:
            last_event_at = last_event.occurred_at
        else:
            last_event_at = None
        
        # Ensure timezone-aware (UTC) for proper JSON serialization
        # FastAPI/Pydantic will serialize timezone-aware datetimes correctly
        if last_event_at and last_event_at.tzinfo is None:
            # If naive datetime, assume it's UTC and make it timezone-aware
            last_event_at = last_event_at.replace(tzinfo=timezone.utc)
    else:
        last_event_at = None
    
    # Calculate events per minute (last 60 minutes)
    # Use received_at for accurate server-side timing
    now = datetime.utcnow()
    one_hour_ago = now - timedelta(hours=1)
    
    # Get events in the last hour using received_at
    events_in_last_hour = db.query(Event).filter(
        Event.funnel_id == funnel_id,
        Event.org_id == org_id,
        Event.received_at >= one_hour_ago
    ).all()
    
    events_count = len(events_in_last_hour)
    
    # Calculate events per minute based on actual time span
    if events_count > 0:
        # Get the oldest and newest event times in the last hour
        event_times = [
            evt.received_at if evt.received_at else evt.occurred_at 
            for evt in events_in_last_hour
            if evt.received_at or evt.occurred_at
        ]
        
        if event_times:
            oldest_event_time = min(event_times)
            newest_event_time = max(event_times)
            
            # Calculate time difference in minutes
            time_diff_seconds = (newest_event_time - oldest_event_time).total_seconds()
            time_diff_minutes = time_diff_seconds / 60.0
            
            # If all events happened in less than 1 minute, calculate rate based on time since oldest event
            # Otherwise use the actual time span between oldest and newest
            if time_diff_minutes < 1.0:
                # Use time since oldest event to now, but cap at 60 minutes
                time_since_oldest = (now - oldest_event_time).total_seconds() / 60.0
                time_since_oldest = min(time_since_oldest, 60.0)  # Cap at 60 minutes
                if time_since_oldest > 0:
                    events_per_minute = events_count / time_since_oldest
                else:
                    events_per_minute = float(events_count)  # All events in same instant
            else:
                # Use actual time span between events
                events_per_minute = events_count / time_diff_minutes
        else:
            events_per_minute = 0.0
    else:
        events_per_minute = 0.0
    
    # Error count last 24 hours - filter by org_id for multi-tenant isolation
    one_day_ago = datetime.utcnow() - timedelta(days=1)
    # Note: EventError doesn't have org_id, but we can filter by funnel_id if we track it
    # For now, we'll get all errors and filter by checking if the payload contains the funnel_id
    # This is a limitation - ideally EventError should have org_id and funnel_id
    all_errors = db.query(EventError).filter(
        EventError.created_at >= one_day_ago
    ).all()
    
    # Filter errors that belong to this funnel (check payload for funnel_id)
    error_count = 0
    funnel_id_str = str(funnel_id)
    for error in all_errors:
        if error.payload and isinstance(error.payload, dict):
            # Check if error payload contains this funnel_id
            payload_funnel_id = error.payload.get('funnel_id')
            if payload_funnel_id and str(payload_funnel_id) == funnel_id_str:
                error_count += 1
    
    # Total events - count all events for this funnel
    total_events = db.query(func.count(Event.id)).filter(
        Event.funnel_id == funnel_id,
        Event.org_id == org_id
    ).scalar() or 0
    
    return FunnelHealth(
        funnel_id=funnel_id,
        last_event_at=last_event_at,
        events_per_minute=events_per_minute,
        error_count_last_24h=error_count,
        total_events=total_events
    )


@router.get("/{funnel_id}/analytics", response_model=FunnelAnalytics)
def get_funnel_analytics(
    funnel_id: UUID,
    range_days: int = Query(30, alias="range", ge=1, le=365),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get funnel analytics: step counts, conversion rates, bookings, revenue"""
    # Get selected org_id from user object (set by get_current_user)
    org_id = getattr(current_user, 'selected_org_id', current_user.org_id)
    
    # Verify funnel exists
    funnel = db.query(Funnel).filter(
        Funnel.id == funnel_id,
        Funnel.org_id == org_id
    ).first()
    
    if not funnel:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Funnel not found"
        )
    
    # Get steps
    steps = db.query(FunnelStep).filter(
        FunnelStep.funnel_id == funnel_id,
        FunnelStep.org_id == org_id
    ).order_by(asc(FunnelStep.step_order)).all()
    
    if not steps:
        return FunnelAnalytics(
            funnel_id=funnel_id,
            range_days=range_days,
            step_counts=[],
            total_visitors=0,
            total_conversions=0,
            overall_conversion_rate=0.0,
            bookings=0,
            revenue_cents=0,
            top_utm_sources=[],
            top_referrers=[]
        )
    
    # Calculate date range
    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=range_days)
    
    # Get step counts
    step_counts = []
    previous_count = None
    
    for step in steps:
        count = db.query(func.count(Event.id)).filter(
            Event.funnel_id == funnel_id,
            Event.org_id == org_id,
            Event.event_name == step.event_name,
            Event.occurred_at >= start_date,
            Event.occurred_at <= end_date
        ).scalar() or 0
        
        # Calculate conversion rate from previous step
        conversion_rate = None
        if previous_count is not None and previous_count > 0:
            conversion_rate = (count / previous_count) * 100.0
        
        step_counts.append(StepCount(
            step_order=step.step_order,
            label=step.label,
            event_name=step.event_name,
            count=count,
            conversion_rate=conversion_rate
        ))
        
        previous_count = count
    
    # Get total unique visitors (visitors who triggered the first step)
    first_step = steps[0] if steps else None
    total_visitors = 0
    if first_step:
        total_visitors = db.query(func.count(func.distinct(Event.visitor_id))).filter(
            Event.funnel_id == funnel_id,
            Event.org_id == org_id,
            Event.event_name == first_step.event_name,
            Event.occurred_at >= start_date,
            Event.occurred_at <= end_date,
            Event.visitor_id.isnot(None)
        ).scalar() or 0
    
    # Get conversions (unique visitors who triggered the last step)
    if steps:
        last_step = steps[-1]
        # Count unique visitors who converted (triggered last step)
        total_conversions = db.query(func.count(func.distinct(Event.visitor_id))).filter(
            Event.funnel_id == funnel_id,
            Event.org_id == org_id,
            Event.event_name == last_step.event_name,
            Event.occurred_at >= start_date,
            Event.occurred_at <= end_date,
            Event.visitor_id.isnot(None)
        ).scalar() or 0
    else:
        total_conversions = 0
    
    # Calculate overall conversion rate based on unique visitors
    overall_conversion_rate = 0.0
    if total_visitors > 0:
        overall_conversion_rate = (total_conversions / total_visitors) * 100.0
    
    # Get bookings and revenue from events with payment_succeeded
    bookings = db.query(func.count(Event.id)).filter(
        Event.funnel_id == funnel_id,
        Event.org_id == org_id,
        Event.event_name == "payment_succeeded",
        Event.occurred_at >= start_date,
        Event.occurred_at <= end_date
    ).scalar() or 0
    
    # Revenue from metadata (if stored)
    revenue_events = db.query(Event).filter(
        Event.funnel_id == funnel_id,
        Event.org_id == org_id,
        Event.event_name == "payment_succeeded",
        Event.occurred_at >= start_date,
        Event.occurred_at <= end_date
    ).all()
    
    revenue_cents = 0
    for evt in revenue_events:
        if evt.event_metadata and 'amount_cents' in evt.event_metadata:
            revenue_cents += int(evt.event_metadata['amount_cents'])
    
    # Aggregate UTM sources
    # Get all events in range with sessions
    events_with_sessions = db.query(Event, Session).join(
        Session,
        and_(
            Event.session_id == Session.session_id,
            Event.org_id == Session.org_id
        )
    ).filter(
        Event.funnel_id == funnel_id,
        Event.org_id == org_id,
        Event.occurred_at >= start_date,
        Event.occurred_at <= end_date,
        Session.utm.isnot(None)
    ).all()
    
    # Aggregate UTM sources
    # Track unique visitors per source for conversion calculation
    utm_source_visitors = {}  # {source: set(visitor_ids)}
    utm_source_converted_visitors = {}  # {source: set(visitor_ids)}
    utm_source_stats = {}
    
    for event, session in events_with_sessions:
        if session.utm and 'source' in session.utm and event.visitor_id:
            source = session.utm['source']
            if source not in utm_source_stats:
                utm_source_stats[source] = {'count': 0, 'conversions': 0, 'revenue_cents': 0}
                utm_source_visitors[source] = set()
                utm_source_converted_visitors[source] = set()
            
            utm_source_stats[source]['count'] += 1
            utm_source_visitors[source].add(event.visitor_id)
            
            # Check if this is a conversion (last step) - track unique visitors
            if steps and event.event_name == steps[-1].event_name:
                utm_source_converted_visitors[source].add(event.visitor_id)
            
            # Add revenue if payment succeeded
            if event.event_name == "payment_succeeded" and event.event_metadata and 'amount_cents' in event.event_metadata:
                utm_source_stats[source]['revenue_cents'] += int(event.event_metadata['amount_cents'])
    
    # Update conversions to use unique visitors and add unique visitor count
    for source in utm_source_stats:
        utm_source_stats[source]['conversions'] = len(utm_source_converted_visitors.get(source, set()))
        utm_source_stats[source]['unique_visitors'] = len(utm_source_visitors.get(source, set()))
    
    # Convert to list and sort by unique visitors
    top_utm_sources = [
        UTMSourceStats(
            source=source,
            count=stats['count'],
            unique_visitors=stats['unique_visitors'],
            conversions=stats['conversions'],
            revenue_cents=stats['revenue_cents']
        )
        for source, stats in sorted(utm_source_stats.items(), key=lambda x: x[1]['unique_visitors'], reverse=True)[:10]
    ]
    
    # Aggregate referrers
    events_with_referrers = db.query(Event, Session).join(
        Session,
        and_(
            Event.session_id == Session.session_id,
            Event.org_id == Session.org_id
        )
    ).filter(
        Event.funnel_id == funnel_id,
        Event.org_id == org_id,
        Event.occurred_at >= start_date,
        Event.occurred_at <= end_date,
        Session.referrer.isnot(None)
    ).all()
    
    # Track unique visitors per referrer for conversion calculation
    referrer_visitors = {}  # {referrer: set(visitor_ids)}
    referrer_converted_visitors = {}  # {referrer: set(visitor_ids)}
    referrer_stats = {}
    
    for event, session in events_with_referrers:
        if session.referrer and event.visitor_id:
            # Normalize referrer (remove protocol, trailing slash)
            referrer = re.sub(r'^https?://', '', session.referrer)
            referrer = referrer.rstrip('/')
            if not referrer:
                referrer = "Direct"
            
            if referrer not in referrer_stats:
                referrer_stats[referrer] = {'count': 0, 'conversions': 0, 'revenue_cents': 0}
                referrer_visitors[referrer] = set()
                referrer_converted_visitors[referrer] = set()
            
            referrer_stats[referrer]['count'] += 1
            referrer_visitors[referrer].add(event.visitor_id)
            
            # Check if this is a conversion (last step) - track unique visitors
            if steps and event.event_name == steps[-1].event_name:
                referrer_converted_visitors[referrer].add(event.visitor_id)
            
            # Add revenue if payment succeeded
            if event.event_name == "payment_succeeded" and event.event_metadata and 'amount_cents' in event.event_metadata:
                referrer_stats[referrer]['revenue_cents'] += int(event.event_metadata['amount_cents'])
    
    # Update conversions to use unique visitors and add unique visitor count
    for referrer in referrer_stats:
        referrer_stats[referrer]['conversions'] = len(referrer_converted_visitors.get(referrer, set()))
        referrer_stats[referrer]['unique_visitors'] = len(referrer_visitors.get(referrer, set()))
    
    # Convert to list and sort by unique visitors
    top_referrers = [
        ReferrerStats(
            referrer=ref,
            count=stats['count'],
            unique_visitors=stats['unique_visitors'],
            conversions=stats['conversions'],
            revenue_cents=stats['revenue_cents']
        )
        for ref, stats in sorted(referrer_stats.items(), key=lambda x: x[1]['unique_visitors'], reverse=True)[:10]
    ]
    
    return FunnelAnalytics(
        funnel_id=funnel_id,
        range_days=range_days,
        step_counts=step_counts,
        total_visitors=total_visitors,
        total_conversions=total_conversions,
        overall_conversion_rate=overall_conversion_rate,
        bookings=bookings,
        revenue_cents=revenue_cents,
        top_utm_sources=top_utm_sources,
        top_referrers=top_referrers
    )


# Event Explorer
@router.get("/events")
def explore_events(
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Explore events with filters"""
    try:
        # Manually parse query parameters to avoid FastAPI validation issues
        query_params = request.query_params
        
        funnel_id = query_params.get("funnel_id")
        event_name = query_params.get("event_name")
        visitor_id = query_params.get("visitor_id")
        start_date = query_params.get("start_date")
        end_date = query_params.get("end_date")
        limit_str = query_params.get("limit", "50")
        offset_str = query_params.get("offset", "0")
        
        # Convert limit and offset to int with defaults
        try:
            limit_int = int(limit_str) if limit_str else 50
            if limit_int < 1 or limit_int > 200:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"limit must be between 1 and 200, got {limit_int}"
                )
        except (ValueError, TypeError):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid limit format: {limit_str}. Must be an integer."
            )
        
        try:
            offset_int = int(offset_str) if offset_str else 0
            if offset_int < 0:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"offset must be >= 0, got {offset_int}"
                )
        except (ValueError, TypeError):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid offset format: {offset_str}. Must be an integer."
            )
        
        # Get selected org_id from user object (set by get_current_user)
        org_id = getattr(current_user, 'selected_org_id', current_user.org_id)
        
        # Build query
        query = db.query(Event).filter(Event.org_id == org_id)
        
        # Convert funnel_id string to UUID if provided
        funnel_uuid = None
        if funnel_id:
            try:
                funnel_uuid = UUID(funnel_id)
                query = query.filter(Event.funnel_id == funnel_uuid)
            except (ValueError, TypeError):
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Invalid funnel_id format: {funnel_id}"
                )
        
        if event_name:
            query = query.filter(Event.event_name == event_name)
        
        if visitor_id:
            query = query.filter(Event.visitor_id == visitor_id)
        
        # Parse datetime strings if provided
        if start_date:
            try:
                # Try parsing ISO format datetime string
                if isinstance(start_date, str):
                    start_datetime = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
                else:
                    start_datetime = start_date
                query = query.filter(Event.occurred_at >= start_datetime)
            except (ValueError, AttributeError) as e:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Invalid start_date format: {start_date}. Use ISO 8601 format. Error: {str(e)}"
                )
        
        if end_date:
            try:
                # Try parsing ISO format datetime string
                if isinstance(end_date, str):
                    end_datetime = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
                else:
                    end_datetime = end_date
                query = query.filter(Event.occurred_at <= end_datetime)
            except (ValueError, AttributeError) as e:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Invalid end_date format: {end_date}. Use ISO 8601 format. Error: {str(e)}"
                )
        
        events = query.order_by(desc(Event.occurred_at)).offset(offset_int).limit(limit_int).all()
        
        # Convert to dict for JSON serialization
        result = []
        for event in events:
            # Get session data for UTM/referrer if available
            utm_data = None
            referrer_data = None
            if event.session_id:
                session = db.query(Session).filter(
                    Session.org_id == current_user.org_id,
                    Session.session_id == event.session_id
                ).first()
                if session:
                    utm_data = session.utm
                    referrer_data = session.referrer
            
            result.append({
                "id": str(event.id),
                "funnel_id": str(event.funnel_id) if event.funnel_id else None,
                "client_id": str(event.client_id) if event.client_id else None,
                "event_name": event.event_name,
                "visitor_id": event.visitor_id,
                "session_id": event.session_id,
                "metadata": event.event_metadata,
                "utm": utm_data,  # UTM parameters from session
                "referrer": referrer_data,  # Referrer from session
                "occurred_at": event.occurred_at.isoformat() if event.occurred_at else None,
                "received_at": event.received_at.isoformat() if event.received_at else None
            })
        
        return result
    except HTTPException:
        raise
    except Exception as e:
        # Log the error for debugging
        import traceback
        print(f"Error in explore_events: {str(e)}")
        print(traceback.format_exc())
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Internal server error: {str(e)}"
        )


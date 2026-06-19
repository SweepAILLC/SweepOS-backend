"""Clients API — crud routes."""
from __future__ import annotations

import logging
import re
import uuid
from datetime import datetime, timedelta, timezone
from threading import Lock as ThreadingLock
from typing import List, Optional, Tuple
from uuid import UUID

import httpx
from fastapi import APIRouter, BackgroundTasks, Body, Depends, HTTPException, Query, Request, status
from fastapi.security import HTTPAuthorizationCredentials
from sqlalchemy import and_, desc, func, or_
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, defer
from sqlalchemy.orm.attributes import flag_modified
from starlette.concurrency import run_in_threadpool

from app.api.deps import get_current_user, security
from app.api.clients.helpers import (
    LOG,
    WHOP_PAID_STATUSES,
    effective_org_id,
    merge_client_meta_from_duplicates,
    normalize_email,
    client_created_sort_key,
    load_whop_payments,
    org_checkin_sync_lock,
    refresh_call_insights_after_checkin_sync,
    scope_org_id,
    parse_client_uuid,
    sync_check_ins_in_worker,
    user_pipeline_priorities,
    brevo_merged_stats_for_client,
    fetch_brevo_email_stats,
    merge_brevo_stats,
)
from app.core.config import settings
from app.core.rate_limit import check_sliding_window
from app.db.session import get_db, SessionLocal
from app.long_jobs import schedule_background_work
from app.models.calendar_booking_sales import CalendarBookingSales
from app.models.client import Client, LifecycleState
from app.models.client_checkin import ClientCheckIn
from app.models.manual_payment import ManualPayment
from app.models.organization import Organization
from app.models.stripe_payment import StripePayment
from app.models.stripe_subscription import StripeSubscription
from app.models.stripe_treasury_transaction import StripeTreasuryTransaction, TreasuryTransactionStatus
from app.models.user import User
from app.models.whop_payment import WhopPayment
from app.utils.stripe_helpers import extract_email_from_payment_raw
from app.utils.stripe_ids import normalize_stripe_id_for_dedup

router = APIRouter()


from app.schemas.client import Client as ClientSchema, ClientCreate, ClientUpdate, MergeClientsRequest
from app.services.client_automation import (
    apply_manual_lifecycle_change,
    resolve_lifecycle_state,
    update_client_progress,
    update_client_lifecycle_state,
    run_pipeline_lifecycle_for_org,
)
from app.services.health_score_cache_service import invalidate_health_score_cache
from app.services.call_insight_service import (
    on_client_became_dead,
    reconcile_call_insights_for_client_merge,
    refresh_insight_summary_from_latest_stored_insight,
    refresh_latest_call_insight_background,
)
from app.services.client_delete_service import purge_client_dependencies

def list_clients(
    lifecycle_state: Optional[LifecycleState] = Query(None),
    limit: int = Query(200, ge=1, le=1000),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    try:
        # Same org scope as create/delete (JWT selected org, UUID-normalized)
        org_id = scope_org_id(current_user)

        # CRITICAL: Filter by org_id for multi-tenant isolation (use selected org from token)
        query = db.query(Client).filter(Client.org_id == org_id)
        if lifecycle_state:
            query = query.filter(Client.lifecycle_state == lifecycle_state)
        # Order by most recently updated/created and cap result size to keep response fast
        clients = (
            query.order_by(Client.updated_at.desc(), Client.created_at.desc())
            .limit(limit)
            .all()
        )

        # Pipeline lifecycle rules run on calendar sync, payments, and worker jobs — not on
        # every board read (that reverted manual column moves and doubled query cost).

        # Convert each client with proper error handling
        result = []
        for client in clients:
            try:
                # Use Pydantic's model_validate with from_attributes to handle Decimal conversion
                validated = ClientSchema.model_validate(client, from_attributes=True)
                result.append(validated)
            except Exception as client_error:
                from pydantic import ValidationError
                import traceback
                print(f"ERROR validating client {client.id}: {str(client_error)}")
                print(f"Client data: estimated_mrr={client.estimated_mrr} (type: {type(client.estimated_mrr)}), email={client.email}")
                if isinstance(client_error, ValidationError):
                    print(f"Validation errors: {client_error.errors()}")
                    for error in client_error.errors():
                        print(f"  - Field: {error.get('loc')}, Error: {error.get('msg')}, Input: {error.get('input')}")
                print(traceback.format_exc())
                # Skip invalid clients for now
                continue
        
        return result
    except Exception as e:
        import traceback
        print(f"ERROR in list_clients: {str(e)}")
        print(traceback.format_exc())
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error loading clients: {str(e)}"
        )


def create_client(
    client_data: ClientCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    # Selected org from JWT — same scope as list/delete (UUID-normalized)
    org_id = scope_org_id(current_user)
    
    # Indexed-friendly duplicate check (case-insensitive); JSON `emails` overlap is rare on manual create
    if client_data.email:
        trimmed = client_data.email.strip()
        existing_client = db.query(Client).filter(
            Client.org_id == org_id,
            func.lower(Client.email) == trimmed.lower(),
        ).first()
        if existing_client:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Client with email {trimmed} already exists (ID: {existing_client.id})",
            )
    
    # CRITICAL: Set org_id from selected org (token)
    client_dict = client_data.model_dump()
    client_dict['org_id'] = org_id
    client = Client(**client_dict)
    
    # Update program dates if program fields are set
    # Handle end_date -> duration calculation
    if client.program_start_date and client.program_end_date:
        # Calculate duration from start and end dates
        duration = (client.program_end_date - client.program_start_date).days
        if duration > 0:
            client.program_duration_days = duration
        else:
            # Invalid: end date before start date
            client.program_end_date = None
            client.program_duration_days = None
    
    # Update program dates (handles duration -> end_date if needed)
    if client.program_start_date or client.program_duration_days or client.program_end_date:
        client.update_program_dates()
        # Calculate initial progress
        client.program_progress_percent = client.calculate_progress()
    
    db.add(client)
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Could not create client due to a conflict with existing data (e.g. duplicate email).",
        )
    db.refresh(client)
    from app.services.fathom_client_link import relink_fathom_for_client_and_queue

    relink_fathom_for_client_and_queue(db, org_id, client)
    return client


@router.get("/{client_id}", response_model=ClientSchema)
def get_client(
    client_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get client details"""
    client_uuid = parse_client_uuid(client_id)
    org_id = scope_org_id(current_user)

    client = db.query(Client).filter(
        Client.id == client_uuid,
        Client.org_id == org_id
    ).first()
    if not client:
        raise HTTPException(status_code=404, detail="Client not found")
    
    # Update progress if program is set
    if client.program_start_date and client.program_duration_days:
        old_state = client.lifecycle_state.value if hasattr(client.lifecycle_state, 'value') else str(client.lifecycle_state)
        progress_updated = update_client_progress(db, client)
        state_updated = update_client_lifecycle_state(db, client)
        if progress_updated or state_updated:
            db.commit()
            # Refresh the client object to get updated state
            db.refresh(client)
            new_state = client.lifecycle_state.value if hasattr(client.lifecycle_state, 'value') else str(client.lifecycle_state)
            print(f"[CLIENT_API] Client {client.id} ({client.email}) updated: progress={client.program_progress_percent}%, state={old_state} → {new_state}")
            if old_state != new_state:
                print(f"[CLIENT_API] ✅ State change confirmed: {old_state} → {new_state}")
    
    return client

@router.patch("/{client_id}", response_model=ClientSchema)
def update_client(
    client_id: str,
    client_update: ClientUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    client_uuid = parse_client_uuid(client_id)
    org_id = scope_org_id(current_user)
    client = db.query(Client).filter(
        Client.id == client_uuid,
        Client.org_id == org_id
    ).first()
    if not client:
        raise HTTPException(status_code=404, detail="Client not found")

    try:
        update_data = client_update.model_dump(exclude_unset=True)

        # Manual kanban / drawer column moves: persist to DB and reset follow-up clock
        # so the next calendar sync does not auto-revert the card.
        entered_dead = False
        if "lifecycle_state" in update_data:
            raw_state = update_data.pop("lifecycle_state")
            try:
                new_state = resolve_lifecycle_state(raw_state)
            except ValueError:
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail=f"Invalid lifecycle_state: {raw_state}",
                )
            prev_lc = (
                client.lifecycle_state.value
                if hasattr(client.lifecycle_state, "value")
                else str(client.lifecycle_state or "")
            )
            entered_dead = new_state == LifecycleState.DEAD and prev_lc != LifecycleState.DEAD.value
            apply_manual_lifecycle_change(client, new_state)

        # JSON columns need flag_modified so SQLAlchemy persists nested dict updates.
        if "offer_enrollment" in update_data:
            client.offer_enrollment = update_data.pop("offer_enrollment")
            flag_modified(client, "offer_enrollment")
        if "meta" in update_data:
            client.meta = update_data.pop("meta")
            flag_modified(client, "meta")

        # Apply remaining scalar updates
        for field, value in update_data.items():
            setattr(client, field, value)
        
        # Handle program fields updates
        # If program fields are being cleared (set to None), clear all program-related fields
        if 'program_start_date' in update_data and update_data['program_start_date'] is None:
            # Clearing program - also clear related fields
            client.program_start_date = None
            client.program_duration_days = None
            client.program_end_date = None
            client.program_progress_percent = None
            print(f"[UPDATE_CLIENT] Cleared program fields for client {client.id}")
        elif 'program_end_date' in update_data and update_data['program_end_date'] is None:
            # Clearing program end date - also clear related fields
            client.program_end_date = None
            client.program_duration_days = None
            client.program_progress_percent = None
            print(f"[UPDATE_CLIENT] Cleared program end date for client {client.id}")
        elif 'program_duration_days' in update_data and update_data['program_duration_days'] is None:
            # Clearing program duration - also clear related fields
            client.program_duration_days = None
            client.program_end_date = None
            client.program_progress_percent = None
            print(f"[UPDATE_CLIENT] Cleared program duration for client {client.id}")
        elif 'program_start_date' in update_data or 'program_end_date' in update_data or 'program_duration_days' in update_data:
            # Program fields are being set/updated
            try:
                # If end_date is provided, calculate duration from start and end
                if 'program_end_date' in update_data and update_data['program_end_date']:
                    end_date = update_data['program_end_date']
                    if isinstance(end_date, str):
                        end_date = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
                    client.program_end_date = end_date
                    
                    # Calculate duration if start date exists
                    if client.program_start_date:
                        duration = (client.program_end_date - client.program_start_date).days
                        if duration > 0:
                            client.program_duration_days = duration
                        else:
                            print(f"[UPDATE_CLIENT] Warning: End date is before start date for client {client.id}")
                            client.program_end_date = None
                            client.program_duration_days = None
                
                # Update program dates (this handles duration -> end_date calculation if needed)
                client.update_program_dates()
                
                # Recalculate progress
                progress = client.calculate_progress()
                client.program_progress_percent = progress
            except Exception as e:
                import traceback
                print(f"[UPDATE_CLIENT] Error updating program dates/progress: {e}")
                traceback.print_exc()
                # Don't fail the update if progress calculation fails, but log it
                # Set progress to None if calculation fails
                if not client.program_start_date or not client.program_duration_days:
                    client.program_progress_percent = None
        
        # Handle explicit program_progress_percent updates
        if 'program_progress_percent' in update_data:
            client.program_progress_percent = update_data['program_progress_percent']

        # Keep program progress in sync with timeline (cleared dates must not leave 100% → auto-dead).
        if not client.program_start_date or not client.program_duration_days:
            client.program_progress_percent = None
            if not client.program_start_date:
                client.program_end_date = None
                client.program_duration_days = None
        
        schedule_dead_llm_refresh = False
        if entered_dead:
            try:
                schedule_dead_llm_refresh = on_client_became_dead(db, org_id, client)
            except Exception as dead_hook_err:
                print(f"[UPDATE_CLIENT] Dead lifecycle hook failed for {client.id}: {dead_hook_err}")

        db.commit()
        db.refresh(client)
        invalidate_health_score_cache(db, client.id, org_id)
        if schedule_dead_llm_refresh:
            schedule_background_work(
                refresh_latest_call_insight_background,
                None,
                str(org_id),
                str(client.id),
            )
        if "email" in update_data or "emails" in update_data:
            from app.services.fathom_client_link import relink_fathom_for_client_and_queue

            relink_fathom_for_client_and_queue(db, org_id, client)
        return ClientSchema.model_validate(client, from_attributes=True)
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        import traceback
        traceback.print_exc()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error updating client: {str(e)}"
        )


@router.post("/merge", response_model=ClientSchema)
def merge_clients(
    body: MergeClientsRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Merge multiple client records into one. Keeps the oldest client (by created_at),
    merges fields from the others, reassigns all related records to the kept client, then deletes the others.
    Call this to persist a single client profile per person (e.g. same email) instead of merging in memory on each load.
    """
    org_id = getattr(current_user, "selected_org_id", current_user.org_id)
    if len(body.client_ids) < 2:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="At least 2 client IDs required to merge")

    # Load all clients, same org only
    clients = db.query(Client).filter(
        Client.id.in_(body.client_ids),
        Client.org_id == org_id,
    ).order_by(Client.created_at.asc()).all()

    if len(clients) < 2:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Fewer than 2 clients found for the given IDs in this organization",
        )

    keep = clients[0]
    to_remove = clients[1:]
    keep_id = keep.id
    remove_ids = [c.id for c in to_remove]

    # Merge fields into keep (prefer non-empty / best value)
    state_priority = {
        LifecycleState.ACTIVE: 7,
        LifecycleState.OFFBOARDING: 6,
        LifecycleState.BOOKED: 5,
        LifecycleState.QUALIFIED: 4,
        LifecycleState.NURTURING: 3,
        LifecycleState.COLD_LEAD: 2,
        LifecycleState.DEAD: 1,
    }
    best_state = keep
    for c in to_remove:
        if state_priority.get(c.lifecycle_state, 0) > state_priority.get(best_state.lifecycle_state, 0):
            best_state = c
    keep.lifecycle_state = best_state.lifecycle_state

    for c in to_remove:
        if c.first_name and (not keep.first_name or not keep.first_name.strip()):
            keep.first_name = c.first_name
        if c.last_name and (not keep.last_name or not keep.last_name.strip()):
            keep.last_name = c.last_name
        if c.phone and (not keep.phone or not keep.phone.strip()):
            keep.phone = c.phone
        if c.instagram and (not keep.instagram or not keep.instagram.strip()):
            keep.instagram = c.instagram
        if c.stripe_customer_id and (not keep.stripe_customer_id or not keep.stripe_customer_id.strip()):
            keep.stripe_customer_id = c.stripe_customer_id
        keep.estimated_mrr = max((keep.estimated_mrr or 0), (c.estimated_mrr or 0))
        keep.lifetime_revenue_cents = max((keep.lifetime_revenue_cents or 0), (c.lifetime_revenue_cents or 0))
        if c.notes and c.notes.strip():
            keep.notes = (keep.notes or "").rstrip() + "\n" + c.notes.strip() if keep.notes else c.notes.strip()
    # Merge emails: collect all emails from keep + to_remove, dedupe, set primary and emails list
    all_emails_set = keep.get_all_emails_normalized()
    for c in to_remove:
        all_emails_set |= c.get_all_emails_normalized()
    if all_emails_set:
        emails_list = sorted(all_emails_set)
        keep.email = emails_list[0] if emails_list else keep.email
        keep.emails = emails_list[1:] if len(emails_list) > 1 else (keep.emails or [])
    # Program: prefer client with highest progress among keep + to_remove
    all_for_program = [keep] + to_remove
    best_program = max(all_for_program, key=lambda x: (x.program_progress_percent or 0))
    if best_program.program_progress_percent is not None:
        keep.program_start_date = best_program.program_start_date
        keep.program_duration_days = best_program.program_duration_days
        keep.program_end_date = best_program.program_end_date
        keep.program_progress_percent = best_program.program_progress_percent

    merge_client_meta_from_duplicates(keep, to_remove)
    reconcile_call_insights_for_client_merge(db, org_id, keep_id, remove_ids)

    # Reassign related records from to_remove to keep
    from app.models.stripe_subscription import StripeSubscription
    from app.models.event import Event
    from app.models.funnel import Funnel
    from app.models.recommendation import Recommendation

    for rid in remove_ids:
        db.query(StripePayment).filter(
            StripePayment.client_id == rid,
            StripePayment.org_id == org_id,
        ).update({StripePayment.client_id: keep_id}, synchronize_session=False)
        db.query(StripeSubscription).filter(
            StripeSubscription.client_id == rid,
            StripeSubscription.org_id == org_id,
        ).update({StripeSubscription.client_id: keep_id}, synchronize_session=False)
        db.query(Event).filter(
            Event.client_id == rid,
            Event.org_id == org_id,
        ).update({Event.client_id: keep_id}, synchronize_session=False)
        db.query(Funnel).filter(
            Funnel.client_id == rid,
            Funnel.org_id == org_id,
        ).update({Funnel.client_id: keep_id}, synchronize_session=False)
        db.query(Recommendation).filter(
            Recommendation.client_id == rid,
            Recommendation.org_id == org_id,
        ).update({Recommendation.client_id: keep_id}, synchronize_session=False)
        db.query(ManualPayment).filter(
            ManualPayment.client_id == rid,
            ManualPayment.org_id == org_id,
        ).update({ManualPayment.client_id: keep_id}, synchronize_session=False)
        db.query(WhopPayment).filter(
            WhopPayment.client_id == rid,
            WhopPayment.org_id == org_id,
        ).update({WhopPayment.client_id: keep_id}, synchronize_session=False)
        db.query(ClientCheckIn).filter(
            ClientCheckIn.client_id == rid,
            ClientCheckIn.org_id == org_id,
        ).update({ClientCheckIn.client_id: keep_id}, synchronize_session=False)
        db.query(StripeTreasuryTransaction).filter(
            StripeTreasuryTransaction.client_id == rid,
            StripeTreasuryTransaction.org_id == org_id,
        ).update({StripeTreasuryTransaction.client_id: keep_id}, synchronize_session=False)
        try:
            from app.models.fathom_call_record import FathomCallRecord

            db.query(FathomCallRecord).filter(
                FathomCallRecord.client_id == rid,
                FathomCallRecord.org_id == org_id,
            ).update({FathomCallRecord.client_id: keep_id}, synchronize_session=False)
        except Exception:
            pass

    for rid in remove_ids:
        purge_client_dependencies(db, org_id, rid)

    db.query(Client).filter(
        Client.id.in_(remove_ids),
        Client.org_id == org_id,
    ).delete(synchronize_session=False)

    refresh_insight_summary_from_latest_stored_insight(db, org_id, keep_id)
    try:
        db.commit()
        db.refresh(keep)
    except IntegrityError as e:
        db.rollback()
        LOG.exception("merge_clients integrity error org=%s keep=%s remove=%s", org_id, keep_id, remove_ids)
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Could not merge clients due to conflicting related records. Try again or contact support.",
        ) from e

    invalidate_health_score_cache(db, keep_id, org_id, do_commit=True)
    try:
        from app.services.fathom_client_link import relink_fathom_for_client_and_queue

        relink_fathom_for_client_and_queue(db, org_id, keep)
    except Exception:
        LOG.exception("merge_clients fathom relink failed org=%s client=%s", org_id, keep_id)
    return ClientSchema.model_validate(keep, from_attributes=True)


@router.delete("/{client_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_client(
    client_id: str,
    delete_merged: bool = Query(False, description="If true and client is merged, delete all merged clients"),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Delete a client. If delete_merged is True and client has same email as others, delete all clients with that email.
    
    Before deleting, sets client_id to NULL in all related records (payments, subscriptions, events, etc.)
    to avoid foreign key constraint violations.
    """
    client_uuid = parse_client_uuid(client_id)
    org_id = scope_org_id(current_user)
    # CRITICAL: Filter by org_id for multi-tenant isolation (selected org from token)
    client = db.query(Client).filter(
        Client.id == client_uuid,
        Client.org_id == org_id
    ).first()
    if not client:
        # Exists in another org → help user fix org context; truly missing → 404
        other = db.query(Client).filter(Client.id == client_uuid).first()
        if other:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=(
                    "This client belongs to another organization. "
                    "Open Settings → Accounts and switch to the correct organization, then try again."
                ),
            )
        raise HTTPException(status_code=404, detail="Client not found")

    client_ids_to_delete = [str(client_uuid)]
    
    # If delete_merged is True and client has an email, find and delete all clients with same email
    if delete_merged and client.email:
        import re
        normalized_email = re.sub(r'\s+', '', client.email.lower().strip())
        
        # Find all clients with the same email
        all_clients_with_email = db.query(Client).filter(
            and_(
                Client.org_id == org_id,
                Client.email.isnot(None)
            )
        ).all()
        
        # Filter clients with matching normalized email
        clients_with_same_email = [
            c for c in all_clients_with_email
            if c.email and re.sub(r'\s+', '', c.email.lower().strip()) == normalized_email
        ]
        
        if len(clients_with_same_email) > 1:
            client_ids_to_delete = [str(c.id) for c in clients_with_same_email]
            print(f"[DELETE_CLIENT] Deleting {len(clients_with_same_email)} clients with email '{normalized_email}': {client_ids_to_delete}")
    
    deleted_count = 0
    for cid in client_ids_to_delete:
        try:
            client_uuid = UUID(cid)
        except ValueError:
            print(f"[DELETE_CLIENT] Invalid client ID: {cid}")
            continue

        client_to_delete = db.query(Client).filter(
            Client.id == client_uuid,
            Client.org_id == org_id,
        ).first()
        if not client_to_delete:
            continue

        try:
            purge_client_dependencies(db, org_id, client_uuid)
            db.delete(client_to_delete)
            deleted_count += 1
            print(f"[DELETE_CLIENT] Deleted client {cid} and dependencies")
        except IntegrityError as e:
            db.rollback()
            print(f"[DELETE_CLIENT] Integrity error deleting client {cid}: {e}")
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=(
                    "Could not delete client because related records still reference it. "
                    "Try again; if this persists, contact support."
                ),
            ) from e
        except Exception as e:
            db.rollback()
            print(f"[DELETE_CLIENT] Error deleting client {cid}: {e}")
            import traceback

            traceback.print_exc()
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Error deleting client: {str(e)}",
            ) from e

    if deleted_count == 0:
        raise HTTPException(status_code=404, detail="Client not found")

    db.commit()
    print(f"[DELETE_CLIENT] Successfully deleted {deleted_count} client(s)")

    return None


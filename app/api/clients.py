from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.orm import Session
from sqlalchemy import desc, func, or_, and_
from typing import List, Optional
from uuid import UUID
from app.db.session import get_db
from app.models.client import Client, LifecycleState
from app.models.stripe_payment import StripePayment
from app.schemas.client import Client as ClientSchema, ClientCreate, ClientUpdate
from app.api.deps import get_current_user
from app.models.user import User
from app.services.client_automation import update_client_progress, update_client_lifecycle_state, process_client_automation

router = APIRouter()


@router.get("", response_model=List[ClientSchema])
def list_clients(
    lifecycle_state: Optional[LifecycleState] = Query(None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    try:
        # CRITICAL: Filter by org_id for multi-tenant isolation
        query = db.query(Client).filter(Client.org_id == current_user.org_id)
        if lifecycle_state:
            query = query.filter(Client.lifecycle_state == lifecycle_state)
        clients = query.all()
        
        # Update progress for all clients with programs before returning
        for client in clients:
            if client.program_start_date and client.program_duration_days:
                update_client_progress(db, client)
                update_client_lifecycle_state(db, client)
        db.commit()
        
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


@router.post("", response_model=ClientSchema, status_code=status.HTTP_201_CREATED)
def create_client(
    client_data: ClientCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    # CRITICAL: Set org_id from current user
    client_dict = client_data.model_dump()
    client_dict['org_id'] = current_user.org_id
    client = Client(**client_dict)
    
    # Update program dates if program fields are set
    if client.program_start_date or client.program_duration_days:
        client.update_program_dates()
        # Calculate initial progress
        client.program_progress_percent = client.calculate_progress()
    
    db.add(client)
    db.commit()
    db.refresh(client)
    return client


@router.get("/{client_id}", response_model=ClientSchema)
def get_client(
    client_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get client details"""
    # CRITICAL: Filter by org_id for multi-tenant isolation
    client = db.query(Client).filter(
        Client.id == client_id,
        Client.org_id == current_user.org_id
    ).first()
    if not client:
        raise HTTPException(status_code=404, detail="Client not found")
    
    # Update progress if program is set
    if client.program_start_date and client.program_duration_days:
        update_client_progress(db, client)
        update_client_lifecycle_state(db, client)
        db.commit()
    
    return client


@router.get("/{client_id}/payments")
def get_client_payments(
    client_id: str,
    limit: int = Query(50, ge=1, le=100),
    merged_client_ids: Optional[str] = Query(None, description="Comma-separated list of merged client IDs"),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get payments for a specific client, including merged clients if specified"""
    # CRITICAL: Filter by org_id for multi-tenant isolation
    client = db.query(Client).filter(
        Client.id == client_id,
        Client.org_id == current_user.org_id
    ).first()
    if not client:
        raise HTTPException(status_code=404, detail="Client not found")
    
    # Determine which client IDs to fetch payments for
    client_ids_to_fetch = [client_id]
    
    # If merged_client_ids is provided, use those
    if merged_client_ids:
        client_ids_to_fetch = [cid.strip() for cid in merged_client_ids.split(',') if cid.strip()]
        print(f"[CLIENT_PAYMENTS] Using provided merged_client_ids: {client_ids_to_fetch}")
    # Otherwise, automatically check if client has same email as other clients (for merged clients)
    elif client.email:
        # Normalize email for comparison (lowercase, trim, remove all whitespace)
        import re
        normalized_email = re.sub(r'\s+', '', client.email.lower().strip())
        
        # Find all clients with the same email (fetch all and filter in Python for better normalization)
        all_clients_with_email = db.query(Client).filter(
            and_(
                Client.org_id == current_user.org_id,
                Client.email.isnot(None)
            )
        ).all()
        
        # Filter clients with matching normalized email
        clients_with_same_email = [
            c for c in all_clients_with_email
            if c.email and re.sub(r'\s+', '', c.email.lower().strip()) == normalized_email
        ]
        
        if len(clients_with_same_email) > 1:
            # Multiple clients with same email - fetch payments from all
            client_ids_to_fetch = [str(c.id) for c in clients_with_same_email]
            print(f"[CLIENT_PAYMENTS] Auto-detected {len(clients_with_same_email)} clients with email '{normalized_email}', fetching payments from all: {client_ids_to_fetch}")
        else:
            print(f"[CLIENT_PAYMENTS] Only 1 client found with email '{normalized_email}', using single client")
    
    # Fetch payments from all relevant client IDs
    # Convert client_ids to UUIDs for query
    client_uuids = []
    for cid in client_ids_to_fetch:
        try:
            client_uuids.append(UUID(cid))
        except ValueError:
            print(f"[CLIENT_PAYMENTS] Invalid client ID: {cid}")
    
    if not client_uuids:
        client_uuids = [UUID(client_id)]
    
    # Fetch all payments from merged clients (no limit yet, we'll deduplicate first)
    all_payments = db.query(StripePayment).filter(
        StripePayment.client_id.in_(client_uuids),
        StripePayment.org_id == current_user.org_id
    ).order_by(desc(StripePayment.created_at)).all()
    
    # Apply the same deduplication logic as the recent payments table
    def deduplicate_payments(payments_list):
        """Deduplicate payments using same logic as recent payments table"""
        seen = set()
        deduplicated = []
        
        # Sort: prefer charge over payment_intent over invoice, then by created_at (most recent first)
        # Also prefer payments with subscription_id over those without (for same invoice_id)
        payments_list.sort(key=lambda p: (
            0 if p.type == 'charge' else (1 if p.type == 'payment_intent' else 2),  # Charges first, then payment_intents, then invoices
            0 if p.subscription_id else 1,  # Payments with subscription_id first
            -(p.created_at.timestamp() if p.created_at else 0)  # Most recent first
        ))
        
        # Track invoice_ids that have been seen with subscription_id
        invoice_ids_with_sub = set()
        
        # Track payments without invoice/subscription by (amount, client_id, time_window)
        # to catch payment_intent/charge duplicates
        standalone_payments_seen = {}  # key: (amount_cents, client_id, time_bucket) -> stripe_id
        
        for payment in payments_list:
            # Create deduplication key - same logic as recent payments
            if payment.subscription_id and payment.invoice_id:
                key = (payment.subscription_id, payment.invoice_id)
                # Mark this invoice_id as having a subscription_id
                invoice_ids_with_sub.add(payment.invoice_id)
            elif payment.invoice_id:
                # If this invoice_id was already seen with a subscription_id, skip this one
                if payment.invoice_id in invoice_ids_with_sub:
                    print(f"[CLIENT_PAYMENTS] Skipping payment {payment.stripe_id} with invoice_id {payment.invoice_id} (already have one with subscription_id)")
                    continue
                key = (None, payment.invoice_id)
            else:
                # No subscription or invoice - need to check for payment_intent/charge duplicates
                # Group by amount, client_id, and time window (within 30 seconds)
                if payment.created_at:
                    # Round to nearest 30 seconds to group nearby payments
                    time_bucket = int(payment.created_at.timestamp() / 30) * 30
                    standalone_key = (payment.amount_cents, payment.client_id, time_bucket)
                    
                    # Check if we've seen a payment with same amount, client, and time window
                    if standalone_key in standalone_payments_seen:
                        existing_id = standalone_payments_seen[standalone_key]
                        print(f"[CLIENT_PAYMENTS] Skipping duplicate standalone payment {payment.stripe_id} (type: {payment.type}) - matches {existing_id}")
                        continue
                    
                    # Mark this payment as seen
                    standalone_payments_seen[standalone_key] = payment.stripe_id
                
                # Use stripe_id as key for payments without invoice/subscription
                key = payment.stripe_id
            
            if key not in seen:
                seen.add(key)
                deduplicated.append(payment)
            else:
                print(f"[CLIENT_PAYMENTS] Skipping duplicate payment {payment.stripe_id} with key {key}")
        
        return deduplicated
    
    # Deduplicate payments
    deduplicated_payments = deduplicate_payments(all_payments)
    
    # Apply limit after deduplication
    payments = deduplicated_payments[:limit]
    
    # Calculate total revenue from deduplicated payments (succeeded only)
    succeeded_deduplicated = [p for p in deduplicated_payments if p.status == "succeeded"]
    total_revenue_cents = sum(p.amount_cents for p in succeeded_deduplicated)
    
    print(f"[CLIENT_PAYMENTS] Fetched {len(all_payments)} payments, {len(deduplicated_payments)} after deduplication, showing {len(payments)} (total revenue from succeeded: ${total_revenue_cents/100:.2f})")
    
    return {
        "client_id": client_id,
        "total_amount_paid_cents": total_revenue_cents,
        "total_amount_paid": total_revenue_cents / 100.0,
        "payments": [
            {
                "id": str(payment.id),
                "stripe_id": payment.stripe_id,
                "amount_cents": payment.amount_cents or 0,
                "amount": (payment.amount_cents or 0) / 100.0,
                "currency": payment.currency or "usd",
                "status": payment.status,
                "created_at": payment.created_at.isoformat() if payment.created_at else None,
                "receipt_url": payment.receipt_url,
                "subscription_id": payment.subscription_id,
            }
            for payment in payments
        ]
    }


@router.patch("/{client_id}", response_model=ClientSchema)
def update_client(
    client_id: str,
    client_update: ClientUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    # CRITICAL: Filter by org_id for multi-tenant isolation
    client = db.query(Client).filter(
        Client.id == client_id,
        Client.org_id == current_user.org_id
    ).first()
    if not client:
        raise HTTPException(status_code=404, detail="Client not found")
    
    try:
        update_data = client_update.model_dump(exclude_unset=True)
        
        # Apply updates
        for field, value in update_data.items():
            setattr(client, field, value)
        
        # Update program dates if program fields changed
        if 'program_start_date' in update_data or 'program_duration_days' in update_data:
            try:
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
        
        db.commit()
        db.refresh(client)
        return client
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
    # CRITICAL: Filter by org_id for multi-tenant isolation
    client = db.query(Client).filter(
        Client.id == client_id,
        Client.org_id == current_user.org_id
    ).first()
    if not client:
        raise HTTPException(status_code=404, detail="Client not found")
    
    client_ids_to_delete = [client_id]
    
    # If delete_merged is True and client has an email, find and delete all clients with same email
    if delete_merged and client.email:
        import re
        normalized_email = re.sub(r'\s+', '', client.email.lower().strip())
        
        # Find all clients with the same email
        all_clients_with_email = db.query(Client).filter(
            and_(
                Client.org_id == current_user.org_id,
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
    
    # Before deleting, set client_id to NULL in all related records to avoid foreign key violations
    from app.models.stripe_subscription import StripeSubscription
    from app.models.event import Event
    from app.models.funnel import Funnel
    from app.models.recommendation import Recommendation
    
    deleted_count = 0
    for cid in client_ids_to_delete:
        try:
            client_uuid = UUID(cid)
            client_to_delete = db.query(Client).filter(
                Client.id == client_uuid,
                Client.org_id == current_user.org_id
            ).first()
            
            if not client_to_delete:
                continue
            
            # Set client_id to NULL in related tables
            # Stripe Payments
            db.query(StripePayment).filter(
                StripePayment.client_id == client_uuid,
                StripePayment.org_id == current_user.org_id
            ).update({StripePayment.client_id: None}, synchronize_session=False)
            
            # Stripe Subscriptions
            db.query(StripeSubscription).filter(
                StripeSubscription.client_id == client_uuid,
                StripeSubscription.org_id == current_user.org_id
            ).update({StripeSubscription.client_id: None}, synchronize_session=False)
            
            # Events
            db.query(Event).filter(
                Event.client_id == client_uuid,
                Event.org_id == current_user.org_id
            ).update({Event.client_id: None}, synchronize_session=False)
            
            # Funnels
            db.query(Funnel).filter(
                Funnel.client_id == client_uuid,
                Funnel.org_id == current_user.org_id
            ).update({Funnel.client_id: None}, synchronize_session=False)
            
            # Recommendations
            db.query(Recommendation).filter(
                Recommendation.client_id == client_uuid,
                Recommendation.org_id == current_user.org_id
            ).update({Recommendation.client_id: None}, synchronize_session=False)
            
            # Now delete the client
            db.delete(client_to_delete)
            deleted_count += 1
            print(f"[DELETE_CLIENT] Set client_id to NULL in related records and deleted client {cid}")
            
        except ValueError:
            print(f"[DELETE_CLIENT] Invalid client ID: {cid}")
        except Exception as e:
            print(f"[DELETE_CLIENT] Error deleting client {cid}: {str(e)}")
            import traceback
            traceback.print_exc()
            db.rollback()
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Error deleting client: {str(e)}"
            )
    
    db.commit()
    print(f"[DELETE_CLIENT] Successfully deleted {deleted_count} client(s)")
    
    return None


@router.post("/automation/process", status_code=status.HTTP_200_OK)
def process_client_automation_endpoint(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Manually trigger client automation processing.
    Updates progress and lifecycle states for all clients with programs.
    This can also be called via a scheduled task/cron job.
    """
    try:
        result = process_client_automation(db, org_id=current_user.org_id)
        return {
            "success": True,
            "message": "Client automation processed successfully",
            **result
        }
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error processing automation: {str(e)}"
        )


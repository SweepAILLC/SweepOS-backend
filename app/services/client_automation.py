"""
Client lifecycle automation service.

Handles automatic state transitions based on program progress and payment events.
"""
from sqlalchemy.orm import Session
from sqlalchemy import and_
from datetime import datetime
from app.models.client import Client, LifecycleState
from decimal import Decimal
import uuid


def update_client_progress(db: Session, client: Client) -> bool:
    """
    Calculate and update client's program progress.
    Returns True if progress was updated, False otherwise.
    """
    if not client.program_start_date or not client.program_duration_days:
        # No program set, clear progress
        if client.program_progress_percent is not None:
            client.program_progress_percent = None
            return True
        return False
    
    # Calculate current progress
    new_progress = client.calculate_progress()
    
    # Update if changed
    if client.program_progress_percent != new_progress:
        client.program_progress_percent = new_progress
        return True
    
    return False


def update_client_lifecycle_state(db: Session, client: Client, force: bool = False) -> bool:
    """
    Update client lifecycle state based on program progress and expiration.
    
    Rules:
    - If no program set: keep current state (unless manually changed)
    - At 75% progress: move from 'active' to 'offboarding'
    - When program expires (100%): move from 'offboarding' to 'dead'
    - If client receives payment: move back to 'active' (handled in webhook)
    
    Returns True if state was changed, False otherwise.
    """
    # If no program is set, don't auto-update state
    if not client.program_start_date or not client.program_duration_days:
        return False
    
    # Calculate current progress
    progress = client.calculate_progress()
    if progress is None:
        return False
    
    # Determine target state based on progress
    current_state = client.lifecycle_state
    target_state = None
    
    # Get current state as string for comparison (handles both enum and string)
    current_state_str = current_state.value if hasattr(current_state, 'value') else str(current_state)
    
    # Debug logging
    print(f"[CLIENT_AUTOMATION] Client {client.id} ({client.email}): progress={progress:.2f}%, current_state={current_state_str}")
    
    if progress >= 100.0:
        # Program expired - move to dead (unless already there)
        if current_state_str != 'dead':
            target_state = LifecycleState.DEAD
            print(f"[CLIENT_AUTOMATION] Client {client.id} reached 100% - moving to DEAD")
    elif progress >= 75.0:
        # 75% complete - move to offboarding (from any state except dead or already offboarding)
        # This handles clients in cold_lead, warm_lead, or active states
        if current_state_str not in ['offboarding', 'dead']:
            target_state = LifecycleState.OFFBOARDING
            print(f"[CLIENT_AUTOMATION] Client {client.id} reached 75% ({progress:.2f}%) - moving from {current_state_str} to OFFBOARDING")
        else:
            print(f"[CLIENT_AUTOMATION] Client {client.id} at 75% but already in {current_state_str}, skipping")
    # For progress < 75%, keep current state (unless manually moved)
    
    # Update state if needed
    if target_state:
        target_state_str = target_state.value if hasattr(target_state, 'value') else str(target_state)
        
        # Always update if target_state is set and different from current
        # The force parameter is for manual overrides, but we should always update based on progress
        if current_state_str != target_state_str:
            print(f"[CLIENT_AUTOMATION] ✅ Updating client {client.id} ({client.email}) from {current_state_str} to {target_state_str} (progress: {progress:.1f}%)")
            client.lifecycle_state = target_state
            # Force flush to ensure state is saved immediately
            db.flush()
            # Verify the update took effect
            db.refresh(client)
            updated_state_str = client.lifecycle_state.value if hasattr(client.lifecycle_state, 'value') else str(client.lifecycle_state)
            if updated_state_str != target_state_str:
                print(f"[CLIENT_AUTOMATION] ⚠️  WARNING: State update may have failed! Expected {target_state_str}, got {updated_state_str}")
            else:
                print(f"[CLIENT_AUTOMATION] ✅ Verified: Client state successfully updated to {updated_state_str}")
            return True
        else:
            print(f"[CLIENT_AUTOMATION] ⚠️  Would update client {client.id} to {target_state_str}, but already in that state (force={force})")
    else:
        print(f"[CLIENT_AUTOMATION] No state change needed for client {client.id} (progress: {progress:.1f}%, state: {current_state_str})")
    
    return False


def process_client_automation(db: Session, org_id: uuid.UUID = None):
    """
    Process automation for all clients (or clients in a specific org).
    
    This should be called periodically (e.g., via cron or scheduled task).
    Updates progress and lifecycle states for all clients with programs.
    """
    query = db.query(Client)
    if org_id:
        query = query.filter(Client.org_id == org_id)
    
    # Only process clients with programs set
    query = query.filter(
        Client.program_start_date.isnot(None),
        Client.program_duration_days.isnot(None)
    )
    
    clients = query.all()
    updated_count = 0
    state_changed_count = 0
    
    for client in clients:
        # Update progress
        if update_client_progress(db, client):
            updated_count += 1
        
        # Update lifecycle state based on progress
        if update_client_lifecycle_state(db, client):
            state_changed_count += 1
    
    db.commit()
    
    print(f"[CLIENT_AUTOMATION] Processed {len(clients)} clients: {updated_count} progress updates, {state_changed_count} state changes")
    
    return {
        "clients_processed": len(clients),
        "progress_updates": updated_count,
        "state_changes": state_changed_count
    }


def move_client_to_active_on_payment(db: Session, client: Client):
    """
    Move client back to 'active' when they receive a new payment.
    This is called from the Stripe webhook processor.
    
    Rules:
    - Only move if currently in 'offboarding' or 'dead'
    - Reset program if needed (optional - could extend program instead)
    """
    if client.lifecycle_state in [LifecycleState.OFFBOARDING, LifecycleState.DEAD]:
        print(f"[CLIENT_AUTOMATION] Moving client {client.id} back to ACTIVE due to new payment (was {client.lifecycle_state.value})")
        client.lifecycle_state = LifecycleState.ACTIVE
        # Optionally reset program or extend it - for now, just move to active
        # The program progress will continue from where it was
        return True
    return False


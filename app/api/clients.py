from fastapi import APIRouter, Depends, HTTPException, status, Query, Body
from sqlalchemy.orm import Session
import re
from sqlalchemy import desc, func, or_, and_
from typing import List, Optional
from uuid import UUID
import uuid
from datetime import datetime, timezone, timedelta
from app.db.session import get_db
from app.models.client import Client, LifecycleState
from app.models.stripe_payment import StripePayment
from app.models.stripe_subscription import StripeSubscription
from app.models.stripe_treasury_transaction import StripeTreasuryTransaction, TreasuryTransactionStatus
from app.models.manual_payment import ManualPayment
from app.models.client_checkin import ClientCheckIn
from app.schemas.client import (
    Client as ClientSchema,
    ClientCreate,
    ClientUpdate,
    MergeClientsRequest,
    TerminalSummaryResponse,
    TerminalCashCollected,
    TerminalMRR,
    TerminalTopContributor,
)
from app.api.deps import get_current_user, get_selected_org_id
from app.models.user import User
from app.utils.stripe_ids import normalize_stripe_id_for_dedup
from app.services.client_automation import update_client_progress, update_client_lifecycle_state, process_client_automation

router = APIRouter()


@router.get("", response_model=List[ClientSchema])
def list_clients(
    lifecycle_state: Optional[LifecycleState] = Query(None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    try:
        # Get selected org_id from user object (set by get_current_user)
        org_id = getattr(current_user, 'selected_org_id', current_user.org_id)
        
        # CRITICAL: Filter by org_id for multi-tenant isolation (use selected org from token)
        query = db.query(Client).filter(Client.org_id == org_id)
        if lifecycle_state:
            query = query.filter(Client.lifecycle_state == lifecycle_state)
        clients = query.all()

        # Trigger client automation in a background thread to avoid blocking the API request
        import threading
        def run_client_automation_in_background():
            from app.db.session import SessionLocal
            bg_db = SessionLocal()
            try:
                print(f"[CLIENT_API] Starting background client automation for org {org_id}...")
                result = process_client_automation(bg_db, org_id=org_id)
                print(f"[CLIENT_API] Background client automation complete: {result.get('progress_updates', 0)} progress updates, {result.get('state_changes', 0)} state changes")
            except Exception as e:
                import traceback
                print(f"[CLIENT_API] ❌ Background client automation failed: {str(e)}")
                traceback.print_exc()
            finally:
                bg_db.close()

        automation_thread = threading.Thread(target=run_client_automation_in_background, daemon=True)
        automation_thread.start()
        
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
    # Get selected org_id from user object (set by get_current_user)
    org_id = getattr(current_user, 'selected_org_id', current_user.org_id)
    
    # Check for duplicate client by email
    if client_data.email:
        existing_client = db.query(Client).filter(
            Client.email == client_data.email,
            Client.org_id == org_id
        ).first()
        
        if existing_client:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Client with email {client_data.email} already exists (ID: {existing_client.id})"
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
    # Get selected org_id from user object (set by get_current_user)
    org_id = getattr(current_user, 'selected_org_id', current_user.org_id)
    
    # CRITICAL: Filter by org_id for multi-tenant isolation (use selected org from token)
    client = db.query(Client).filter(
        Client.id == client_id,
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


def _normalize_email(email: str | None) -> str | None:
    if not email:
        return None
    return re.sub(r"\s+", "", email.lower().strip()) or None


@router.get("/terminal-summary", response_model=TerminalSummaryResponse)
def get_terminal_summary(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Precomputed terminal dashboard summary: cash collected (today/7d/30d), MRR/ARR,
    and top 5 revenue contributors for 30d and 90d. One query instead of N+1.
    """
    org_id = getattr(current_user, "selected_org_id", current_user.org_id)

    now = datetime.utcnow()
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    seven_days_ago = today_start - timedelta(days=7)
    thirty_days_ago = today_start - timedelta(days=30)

    # --- Cash collected: Stripe (succeeded, dedupe by stripe_id) + Manual ---
    stripe_payments = (
        db.query(StripePayment)
        .filter(
            StripePayment.org_id == org_id,
            StripePayment.status == "succeeded",
        )
        .all()
    )
    seen_stripe_ids = set()
    today_cash = 0.0
    last_7_cash = 0.0
    last_30_cash = 0.0
    for p in stripe_payments:
        if p.stripe_id and p.stripe_id in seen_stripe_ids:
            continue
        if p.stripe_id:
            seen_stripe_ids.add(p.stripe_id)
        ts = p.created_at
        if not ts:
            continue
        amount = (p.amount_cents or 0) / 100.0
        if ts >= today_start:
            today_cash += amount
        if ts >= seven_days_ago:
            last_7_cash += amount
        if ts >= thirty_days_ago:
            last_30_cash += amount

    manual_payments = (
        db.query(ManualPayment)
        .filter(ManualPayment.org_id == org_id)
        .all()
    )
    for p in manual_payments:
        ts = p.payment_date or p.created_at
        if not ts:
            continue
        if getattr(ts, "tzinfo", None):
            ts = ts.astimezone(timezone.utc).replace(tzinfo=None)
        amount = (p.amount_cents or 0) / 100.0
        if ts >= today_start:
            today_cash += amount
        if ts >= seven_days_ago:
            last_7_cash += amount
        if ts >= thirty_days_ago:
            last_30_cash += amount

    cash_collected = TerminalCashCollected(
        today=today_cash,
        last_7_days=last_7_cash,
        last_30_days=last_30_cash,
    )

    # --- MRR/ARR: from Stripe subscriptions (active/trialing) or fallback to client estimated_mrr ---
    current_mrr = 0.0
    mrr_result = (
        db.query(func.coalesce(func.sum(StripeSubscription.mrr), 0))
        .filter(
            StripeSubscription.org_id == org_id,
            StripeSubscription.status.in_(["active", "trialing"]),
        )
        .scalar()
    )
    if mrr_result is not None:
        try:
            current_mrr = float(mrr_result)
        except (TypeError, ValueError):
            pass
    if current_mrr == 0.0:
        clients = db.query(Client).filter(Client.org_id == org_id).all()
        grouped = {}
        processed = set()
        for c in clients:
            if c.id in processed:
                continue
            key = _normalize_email(c.email) if c.email else (f"stripe:{c.stripe_customer_id}" if c.stripe_customer_id else str(c.id))
            if key not in grouped:
                grouped[key] = []
            same = [x for x in clients if (x.id not in processed and (
                (_normalize_email(x.email) == _normalize_email(c.email) and c.email) or
                (x.stripe_customer_id == c.stripe_customer_id and c.stripe_customer_id and not c.email) or
                (x.id == c.id)
            ))]
            for x in same:
                grouped[key].append(x)
                processed.add(x.id)
        for group in grouped.values():
            max_mrr = max((float(c.estimated_mrr or 0) for c in group), default=0)
            current_mrr += max_mrr
    mrr = TerminalMRR(current_mrr=current_mrr, arr=current_mrr * 12)

    # --- Top contributors: revenue by client (then merge by email), 30d and 90d ---
    ninety_days_ago = today_start - timedelta(days=90)

    def _revenue_by_client(since: datetime):
        rev = {}
        stripe_q = (
            db.query(StripePayment.client_id, func.sum(StripePayment.amount_cents).label("total"))
            .filter(
                StripePayment.org_id == org_id,
                StripePayment.status == "succeeded",
                StripePayment.client_id.isnot(None),
                StripePayment.created_at >= since,
            )
            .group_by(StripePayment.client_id)
        )
        for row in stripe_q.all():
            cid = str(row.client_id)
            rev[cid] = rev.get(cid, 0) + (row.total or 0)
        manual_q = (
            db.query(ManualPayment.client_id, func.sum(ManualPayment.amount_cents).label("total"))
            .filter(
                ManualPayment.org_id == org_id,
                ManualPayment.payment_date >= since,
            )
            .group_by(ManualPayment.client_id)
        )
        for row in manual_q.all():
            cid = str(row.client_id)
            rev[cid] = rev.get(cid, 0) + (row.total or 0)
        return rev

    rev_30 = _revenue_by_client(thirty_days_ago)
    rev_90 = _revenue_by_client(ninety_days_ago)

    all_clients = db.query(Client).filter(Client.org_id == org_id).all()
    all_client_ids = [c.id for c in all_clients]

    last_stripe = (
        db.query(StripePayment.client_id, func.max(StripePayment.created_at).label("last_at"))
        .filter(
            StripePayment.org_id == org_id,
            StripePayment.client_id.in_(all_client_ids),
            StripePayment.status == "succeeded",
        )
        .group_by(StripePayment.client_id)
    ).all()
    last_manual = (
        db.query(ManualPayment.client_id, func.max(ManualPayment.payment_date).label("last_at"))
        .filter(ManualPayment.org_id == org_id, ManualPayment.client_id.in_(all_client_ids))
        .group_by(ManualPayment.client_id)
    ).all()
    last_payment_by_client = {}
    for row in last_stripe:
        last_payment_by_client[str(row.client_id)] = row.last_at
    for row in last_manual:
        k = str(row.client_id)
        dt = row.last_at
        if dt and getattr(dt, "replace", None):
            dt = dt.replace(tzinfo=None) if dt.tzinfo else dt
        if k not in last_payment_by_client or (dt and (not last_payment_by_client[k] or dt > last_payment_by_client[k])):
            last_payment_by_client[k] = dt

    def _build_top(rev_by_client, limit=5):
        grouped = {}
        processed = set()
        for c in all_clients:
            if str(c.id) in processed:
                continue
            norm = _normalize_email(c.email)
            if norm:
                key = f"email:{norm}"
                same = [x for x in all_clients if str(x.id) not in processed and _normalize_email(x.email) == norm]
            elif c.stripe_customer_id:
                key = f"stripe:{c.stripe_customer_id}"
                same = [x for x in all_clients if str(x.id) not in processed and x.stripe_customer_id == c.stripe_customer_id]
            else:
                key = str(c.id)
                same = [c]
            for x in same:
                processed.add(str(x.id))
            if key not in grouped:
                grouped[key] = []
            grouped[key].extend(same)
        contributors = []
        for group in grouped.values():
            total_revenue_cents = sum(rev_by_client.get(str(c.id), 0) for c in group)
            if total_revenue_cents <= 0:
                continue
            primary = min(group, key=lambda c: (c.created_at or datetime.min).timestamp())
            names = set()
            for c in group:
                n = " ".join(filter(None, [c.first_name, c.last_name])).strip()
                if n:
                    names.add(n)
            display_name = " / ".join(sorted(names)) if names else (primary.email or "Unknown")
            last_dates = [last_payment_by_client.get(str(c.id)) for c in group if last_payment_by_client.get(str(c.id))]
            last_payment = max(last_dates, key=lambda d: d or datetime.min) if last_dates else None
            contributors.append({
                "client_id": str(primary.id),
                "display_name": display_name,
                "revenue": total_revenue_cents / 100.0,
                "last_payment_date": last_payment.isoformat() if last_payment else None,
                "merged_client_ids": [str(c.id) for c in group] if len(group) > 1 else None,
            })
        contributors.sort(key=lambda x: -x["revenue"])
        return [TerminalTopContributor(**c) for c in contributors[:limit]]

    top_30 = _build_top(rev_30)
    top_90 = _build_top(rev_90)

    return TerminalSummaryResponse(
        cash_collected=cash_collected,
        mrr=mrr,
        top_contributors_30d=top_30,
        top_contributors_90d=top_90,
    )


@router.get("/{client_id}/payments")
def get_client_payments(
    client_id: str,
    limit: int = Query(50, ge=1, le=100),
    merged_client_ids: Optional[str] = Query(None, description="Comma-separated list of merged client IDs"),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get payments for a specific client, including merged clients if specified"""
    # Get selected org_id from user object (set by get_current_user)
    org_id = getattr(current_user, 'selected_org_id', current_user.org_id)
    
    # CRITICAL: Filter by org_id for multi-tenant isolation (use selected org from token)
    client = db.query(Client).filter(
        Client.id == client_id,
        Client.org_id == org_id
    ).first()
    if not client:
        raise HTTPException(status_code=404, detail="Client not found")
    
    # Determine which client IDs to fetch payments for
    client_ids_to_fetch = [client_id]
    merged_clients_list = [client]  # Track all merged clients for email collection
    
    # If merged_client_ids is provided, use those
    if merged_client_ids:
        client_ids_to_fetch = [cid.strip() for cid in merged_client_ids.split(',') if cid.strip()]
        # Fetch all merged clients to get their emails
        merged_clients_list = db.query(Client).filter(
            Client.id.in_([UUID(cid) for cid in client_ids_to_fetch if cid]),
            Client.org_id == current_user.org_id
        ).all()
        print(f"[CLIENT_PAYMENTS] Using provided merged_client_ids: {client_ids_to_fetch}")
    # Check if client has merged_client_ids in meta field (from frontend)
    elif client.meta and isinstance(client.meta, dict) and client.meta.get('merged_client_ids'):
        merged_ids_from_meta = client.meta.get('merged_client_ids', [])
        if isinstance(merged_ids_from_meta, list) and len(merged_ids_from_meta) > 0:
            # Convert to UUIDs and fetch all merged clients
            try:
                merged_uuids = [UUID(cid) for cid in merged_ids_from_meta if cid]
                merged_clients_list = db.query(Client).filter(
                    Client.id.in_(merged_uuids),
                    Client.org_id == current_user.org_id
                ).all()
                client_ids_to_fetch = [str(c.id) for c in merged_clients_list]
                print(f"[CLIENT_PAYMENTS] Found merged_client_ids in meta: {client_ids_to_fetch}")
            except (ValueError, TypeError) as e:
                print(f"[CLIENT_PAYMENTS] Error parsing merged_client_ids from meta: {e}")
                merged_clients_list = [client]
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
            merged_clients_list = clients_with_same_email
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
    # Also fetch payments by email matching if client has an email
    all_payments = []
    
    # 1. Fetch payments by client_id (directly linked)
    payments_by_client_id = db.query(StripePayment).filter(
        StripePayment.client_id.in_(client_uuids),
        StripePayment.org_id == org_id
    ).order_by(desc(StripePayment.created_at)).all()
    all_payments.extend(payments_by_client_id)
    
    # 2. Fetch payments by email matching (for payments not yet linked to client)
    # Collect all emails from all merged clients
    import re
    all_merged_emails = set()
    for merged_client in merged_clients_list:
        if merged_client.email:
            normalized_email = re.sub(r'\s+', '', merged_client.email.lower().strip())
            all_merged_emails.add(normalized_email)
    
    if all_merged_emails:
        print(f"[CLIENT_PAYMENTS] Checking payments for {len(all_merged_emails)} unique emails from merged clients: {list(all_merged_emails)}")
        
        # Fetch ALL StripePayments (not just unlinked ones) to check for email matches
        # This handles cases where payment is linked to wrong client but email matches merged clients
        all_stripe_payments = db.query(StripePayment).filter(
            StripePayment.org_id == current_user.org_id
        ).all()
        
        # Track which payments we've already added (by ID) to avoid duplicates
        added_payment_ids = {p.id for p in all_payments}
        
        for payment in all_stripe_payments:
            # Skip if already added (from client_id matching)
            if payment.id in added_payment_ids:
                continue
            
            # Check if customer email matches in raw_event
            customer_email = None
            if payment.raw_event:
                if isinstance(payment.raw_event, dict):
                    # Check various paths where email might be (including receipt_email for invoices)
                    customer_email = (
                        payment.raw_event.get('customer_email') or
                        payment.raw_event.get('receipt_email') or  # Common for invoices
                        (payment.raw_event.get('customer', {}).get('email') if isinstance(payment.raw_event.get('customer'), dict) else None) or
                        (payment.raw_event.get('data', {}).get('object', {}).get('customer_email') if isinstance(payment.raw_event.get('data'), dict) else None) or
                        (payment.raw_event.get('data', {}).get('object', {}).get('receipt_email') if isinstance(payment.raw_event.get('data'), dict) else None) or  # For invoice events
                        (payment.raw_event.get('data', {}).get('object', {}).get('customer', {}).get('email') if isinstance(payment.raw_event.get('data', {}).get('object', {}).get('customer'), dict) else None)
                    )
            
            # Also check if payment is linked to a client with matching email
            if not customer_email and payment.client_id:
                linked_client = db.query(Client).filter(Client.id == payment.client_id).first()
                if linked_client and linked_client.email:
                    customer_email = linked_client.email
            
            if customer_email:
                normalized_payment_email = re.sub(r'\s+', '', customer_email.lower().strip())
                if normalized_payment_email in all_merged_emails:
                    all_payments.append(payment)
                    added_payment_ids.add(payment.id)
                    payment_type = getattr(payment, 'type', 'unknown')
                    invoice_id = getattr(payment, 'invoice_id', None)
                    print(f"[CLIENT_PAYMENTS] Found payment {payment.stripe_id} (type: {payment_type}, invoice_id: {invoice_id}) by email match: {customer_email} (linked to client_id: {payment.client_id})")
                else:
                    # Log when email doesn't match (for debugging)
                    payment_type = getattr(payment, 'type', 'unknown')
                    if payment_type == 'invoice':
                        print(f"[CLIENT_PAYMENTS] Invoice payment {payment.stripe_id} has email {customer_email} but doesn't match merged client emails: {list(all_merged_emails)}")
        
        # 3. Fetch from Treasury Transactions by email
        treasury_transactions = db.query(StripeTreasuryTransaction).filter(
            StripeTreasuryTransaction.org_id == org_id,
            StripeTreasuryTransaction.customer_email.isnot(None)
        ).all()
        
        for transaction in treasury_transactions:
            if transaction.customer_email:
                normalized_transaction_email = re.sub(r'\s+', '', transaction.customer_email.lower().strip())
                if normalized_transaction_email in all_merged_emails:
                    # Convert Treasury Transaction to payment-like object for deduplication
                    # Create a simple object that mimics StripePayment structure
                    class TreasuryPayment:
                        def __init__(self, transaction):
                            self.id = transaction.id
                            self.stripe_id = transaction.stripe_transaction_id
                            self.client_id = transaction.client_id
                            self.amount_cents = abs(transaction.amount) if transaction.amount > 0 else 0
                            self.currency = transaction.currency or 'usd'
                            self.status = 'succeeded' if transaction.status == TreasuryTransactionStatus.POSTED else 'pending'
                            self.type = 'treasury_transaction'
                            self.subscription_id = None
                            self.invoice_id = None
                            self.receipt_url = None
                            self.created_at = transaction.created or transaction.posted_at
                            self.raw_event = transaction.raw_data
                    
                    all_payments.append(TreasuryPayment(transaction))
    
    # 4. Fetch manual payments for all merged clients
    manual_payments = db.query(ManualPayment).filter(
        ManualPayment.client_id.in_(client_uuids),
        ManualPayment.org_id == org_id
    ).order_by(desc(ManualPayment.payment_date)).all()
    
    # Convert manual payments to payment-like objects
    class ManualPaymentWrapper:
        def __init__(self, manual_payment):
            self.id = manual_payment.id
            self.stripe_id = None  # Manual payments don't have stripe_id
            self.client_id = manual_payment.client_id
            self.amount_cents = manual_payment.amount_cents
            self.currency = manual_payment.currency or 'usd'
            self.status = 'succeeded'  # Manual payments are always succeeded
            self.type = 'manual_payment'
            self.subscription_id = None
            self.invoice_id = None
            self.receipt_url = manual_payment.receipt_url
            self.created_at = manual_payment.payment_date
            self.description = manual_payment.description
            self.payment_method = manual_payment.payment_method
    
    for manual_payment in manual_payments:
        all_payments.append(ManualPaymentWrapper(manual_payment))
    
    # Apply the EXACT same deduplication logic as the recent payments table
    def deduplicate_payments(payments_list):
        """Deduplicate payments using EXACT same logic as recent payments table (/api/stripe/payments)"""
        seen_stripe_ids = set()  # Track exact duplicates by stripe_id
        deduplicated = []
        payment_map = {}  # Track best payment for each key
        
        # Sort: prefer charge over payment_intent over invoice, then by created_at (most recent first)
        # This matches the recent payments table sorting exactly
        payments_list.sort(key=lambda p: (
            {'charge': 0, 'payment_intent': 1, 'invoice': 2, 'manual_payment': 3}.get(getattr(p, 'type', None), 4),
            -(getattr(p, 'created_at', None).timestamp() if getattr(p, 'created_at', None) else 0)
        ))
        
        for payment in payments_list:
            stripe_id = getattr(payment, 'stripe_id', None)
            payment_id = getattr(payment, 'id', None)
            payment_type = getattr(payment, 'type', None)
            payment_status = getattr(payment, 'status', None)
            
            # For manual payments (no stripe_id), use payment id
            if payment_type == 'manual_payment':
                if payment_id and str(payment_id) not in seen_stripe_ids:
                    seen_stripe_ids.add(str(payment_id))
                    deduplicated.append(payment)
                continue
            
            norm_stripe = normalize_stripe_id_for_dedup(stripe_id) if stripe_id else None
            if norm_stripe and norm_stripe in seen_stripe_ids:
                print(f"[CLIENT_PAYMENTS] Skipping exact duplicate payment {stripe_id} (same normalized id)")
                continue
            if norm_stripe:
                seen_stripe_ids.add(norm_stripe)
            
            if payment_status != 'succeeded':
                deduplicated.append(payment)
                continue
            
            subscription_id = getattr(payment, 'subscription_id', None)
            invoice_id = getattr(payment, 'invoice_id', None)
            
            if subscription_id and invoice_id:
                key = ('subscription_invoice', normalize_stripe_id_for_dedup(subscription_id), normalize_stripe_id_for_dedup(invoice_id))
            elif invoice_id:
                key = ('invoice', normalize_stripe_id_for_dedup(invoice_id))
            elif subscription_id:
                key = ('subscription', normalize_stripe_id_for_dedup(subscription_id))
            else:
                # Standalone payment (no subscription or invoice) - keep all (no deduplication)
                deduplicated.append(payment)
                continue
            
            # Check if we've seen this key before (matches recent payments logic)
            if key not in payment_map:
                # First payment with this key - keep it (it's the best one due to sorting)
                payment_map[key] = payment
                deduplicated.append(payment)
            else:
                # Duplicate key - we already have a better payment (due to sorting)
                existing_payment = payment_map[key]
                print(f"[CLIENT_PAYMENTS] Skipping duplicate payment {stripe_id} (type: {payment_type}) - keeping {getattr(existing_payment, 'stripe_id', None)} (type: {getattr(existing_payment, 'type', None)}) for {key[0]} {key[1]}")
        
        return deduplicated
    
    # Deduplicate payments
    deduplicated_payments = deduplicate_payments(all_payments)
    
    # Apply limit after deduplication
    payments = deduplicated_payments[:limit]
    
    # Calculate total revenue from deduplicated payments (succeeded only)
    # Manual payments are always succeeded, Stripe payments need to check status
    succeeded_deduplicated = [
        p for p in deduplicated_payments 
        if getattr(p, 'status', None) == "succeeded" or getattr(p, 'type', None) == "manual_payment"
    ]
    total_revenue_cents = sum(getattr(p, 'amount_cents', 0) for p in succeeded_deduplicated)
    
    print(f"[CLIENT_PAYMENTS] Fetched {len(all_payments)} payments (by client_id: {len(payments_by_client_id)}, by email: {len(all_payments) - len(payments_by_client_id)}), {len(deduplicated_payments)} after deduplication, showing {len(payments)} (total revenue from succeeded: ${total_revenue_cents/100:.2f})")
    
    # Convert payments to response format
    payment_responses = []
    for payment in payments:
        # Handle both StripePayment objects and TreasuryPayment objects
        payment_id = getattr(payment, 'id', None)
        stripe_id = getattr(payment, 'stripe_id', None)
        amount_cents = getattr(payment, 'amount_cents', 0)
        currency = getattr(payment, 'currency', 'usd')
        payment_status = getattr(payment, 'status', 'unknown')
        created_at = getattr(payment, 'created_at', None)
        receipt_url = getattr(payment, 'receipt_url', None)
        subscription_id = getattr(payment, 'subscription_id', None)
        invoice_id = getattr(payment, 'invoice_id', None)
        payment_type = getattr(payment, 'type', None)
        description = getattr(payment, 'description', None)
        payment_method = getattr(payment, 'payment_method', None)
        
        # Manual payments may not have stripe_id, so check payment_id instead
        if payment_id:
            # Format created_at
            created_at_str = None
            if created_at:
                if hasattr(created_at, 'isoformat'):
                    created_at_str = created_at.isoformat()
                elif hasattr(created_at, 'strftime'):
                    created_at_str = created_at.strftime('%Y-%m-%dT%H:%M:%S')
                else:
                    created_at_str = str(created_at)
            
            payment_responses.append({
                "id": str(payment_id),
                "stripe_id": stripe_id,  # Will be None for manual payments
                "amount_cents": amount_cents or 0,
                "amount": (amount_cents or 0) / 100.0,
                "currency": currency,
                "status": payment_status,
                "created_at": created_at_str,
                "receipt_url": receipt_url,
                "subscription_id": subscription_id,
                "invoice_id": invoice_id,
                "type": payment_type,
                "description": description,  # For manual payments
                "payment_method": payment_method,  # For manual payments
            })
    
    return {
        "client_id": client_id,
        "total_amount_paid_cents": total_revenue_cents,
        "total_amount_paid": total_revenue_cents / 100.0,
        "payments": payment_responses
    }


@router.post("/{client_id}/manual-payment", status_code=status.HTTP_201_CREATED)
def create_manual_payment(
    client_id: str,
    amount: float = Query(..., ge=0.01, description="Payment amount in dollars"),
    payment_date: Optional[str] = Query(None, description="Payment date (ISO format). Defaults to now."),
    description: Optional[str] = Query(None, description="Payment description/notes"),
    payment_method: Optional[str] = Query(None, description="Payment method (e.g., cash, check, bank_transfer)"),
    receipt_url: Optional[str] = Query(None, description="Optional receipt/document URL"),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Create a manual payment transaction for a client.
    Manual payments affect cash collected totals and revenue contributors,
    but do NOT appear in Stripe dashboard or failed payment queues.
    """
    from datetime import datetime, timezone
    
    # Get selected org_id from user object (set by get_current_user)
    org_id = getattr(current_user, 'selected_org_id', current_user.org_id)
    
    try:
        client_uuid = UUID(client_id)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid client ID format"
        )
    
    # Verify client exists and belongs to user's org (use selected org from token)
    client = db.query(Client).filter(
        Client.id == client_uuid,
        Client.org_id == org_id
    ).first()
    
    if not client:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Client not found"
        )
    
    # Parse payment date
    # The frontend sends ISO strings with timezone info (from toISOString())
    # We preserve the timezone-aware datetime to maintain the user's local date
    if payment_date:
        try:
            # Handle ISO format strings (may include 'Z' or timezone offset)
            if payment_date.endswith('Z'):
                payment_date = payment_date.replace('Z', '+00:00')
            payment_datetime = datetime.fromisoformat(payment_date)
            # If no timezone info, assume it's already in UTC (shouldn't happen with toISOString())
            if payment_datetime.tzinfo is None:
                payment_datetime = payment_datetime.replace(tzinfo=timezone.utc)
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid payment_date format: {str(e)}"
            )
    else:
        payment_datetime = datetime.now(timezone.utc)
    
    # Convert amount to cents
    amount_cents = int(round(amount * 100))
    
    # Create manual payment
    manual_payment = ManualPayment(
        org_id=org_id,
        client_id=client_uuid,
        amount_cents=amount_cents,
        currency="usd",
        payment_date=payment_datetime,
        description=description,
        payment_method=payment_method,
        receipt_url=receipt_url,
        created_by=current_user.id
    )
    
    db.add(manual_payment)
    db.commit()
    db.refresh(manual_payment)
    
    return {
        "id": str(manual_payment.id),
        "client_id": client_id,
        "amount_cents": manual_payment.amount_cents,
        "amount": manual_payment.amount_cents / 100.0,
        "currency": manual_payment.currency,
        "payment_date": manual_payment.payment_date.isoformat(),
        "description": manual_payment.description,
        "payment_method": manual_payment.payment_method,
        "receipt_url": manual_payment.receipt_url,
        "status": "succeeded",
        "type": "manual_payment"
    }


@router.delete("/{client_id}/manual-payment/{payment_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_manual_payment(
    client_id: str,
    payment_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Delete a manual payment transaction.
    Only manual payments can be deleted (not Stripe payments).
    """
    # Get selected org_id from user object (set by get_current_user)
    org_id = getattr(current_user, 'selected_org_id', current_user.org_id)
    
    try:
        client_uuid = UUID(client_id)
        payment_uuid = UUID(payment_id)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid client ID or payment ID format"
        )
    
    # Verify client exists and belongs to user's org (use selected org from token)
    client = db.query(Client).filter(
        Client.id == client_uuid,
        Client.org_id == org_id
    ).first()
    
    if not client:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Client not found"
        )
    
    # Find and delete the manual payment
    manual_payment = db.query(ManualPayment).filter(
        ManualPayment.id == payment_uuid,
        ManualPayment.client_id == client_uuid,
        ManualPayment.org_id == org_id
    ).first()
    
    if not manual_payment:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Manual payment not found"
        )
    
    db.delete(manual_payment)
    db.commit()
    
    return None


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
        LifecycleState.ACTIVE: 5,
        LifecycleState.WARM_LEAD: 4,
        LifecycleState.COLD_LEAD: 3,
        LifecycleState.OFFBOARDING: 2,
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
    # Program: prefer client with highest progress among keep + to_remove
    all_for_program = [keep] + to_remove
    best_program = max(all_for_program, key=lambda x: (x.program_progress_percent or 0))
    if best_program.program_progress_percent is not None:
        keep.program_start_date = best_program.program_start_date
        keep.program_duration_days = best_program.program_duration_days
        keep.program_end_date = best_program.program_end_date
        keep.program_progress_percent = best_program.program_progress_percent

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
        db.query(ClientCheckIn).filter(
            ClientCheckIn.client_id == rid,
            ClientCheckIn.org_id == org_id,
        ).update({ClientCheckIn.client_id: keep_id}, synchronize_session=False)
        db.query(StripeTreasuryTransaction).filter(
            StripeTreasuryTransaction.client_id == rid,
            StripeTreasuryTransaction.org_id == org_id,
        ).update({StripeTreasuryTransaction.client_id: keep_id}, synchronize_session=False)

    for c in to_remove:
        db.delete(c)

    db.commit()
    db.refresh(keep)
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


# Check-in endpoints - MUST be before /{client_id}/check-ins to avoid route conflicts
@router.post("/check-ins/sync")
def sync_check_ins(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Sync calendar events (Cal.com/Calendly) with clients by matching attendee emails.
    Creates or updates check-in records for matching clients.
    """
    print(f"[CHECKIN SYNC API] ===== ENDPOINT CALLED =====")
    print(f"[CHECKIN SYNC API] User: {current_user.id}, Org: {current_user.org_id}")
    
    try:
        from app.services.checkin_sync import sync_all_checkins
        print(f"[CHECKIN SYNC API] ✅ Successfully imported sync_all_checkins")
    except ImportError as e:
        import traceback
        error_trace = traceback.format_exc()
        print(f"[CHECKIN SYNC API] ❌ Import error: {str(e)}")
        print(f"[CHECKIN SYNC API] Traceback:\n{error_trace}")
        traceback.print_exc()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to import checkin_sync service: {str(e)}"
        )
    
    try:
        print(f"[CHECKIN SYNC API] Calling sync_all_checkins...")
        results = sync_all_checkins(db, current_user.org_id, current_user.id)
        print(f"[CHECKIN SYNC API] ✅ Sync completed successfully")
        return {
            "success": True,
            "message": f"Synced {results['total']} check-ins",
            "calcom": results["calcom"],
            "calendly": results["calendly"],
            "total": results["total"]
        }
    except Exception as e:
        import traceback
        error_trace = traceback.format_exc()
        print(f"[CHECKIN SYNC API] ❌ Error: {str(e)}")
        print(f"[CHECKIN SYNC API] Traceback:\n{error_trace}")
        traceback.print_exc()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error syncing check-ins: {str(e)}"
        )


@router.get("/{client_id}/check-ins")
def get_client_check_ins(
    client_id: str,
    limit: int = Query(50, ge=1, le=100),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Get check-in history for a client, ordered by start_time (most recent first).
    """
    try:
        client_uuid = UUID(client_id)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid client ID format"
        )
    
    # Verify client exists and belongs to user's org
    client = db.query(Client).filter(
        Client.id == client_uuid,
        Client.org_id == current_user.org_id
    ).first()
    
    if not client:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Client not found"
        )
    
    # Get check-ins for this client
    check_ins = db.query(ClientCheckIn).filter(
        ClientCheckIn.client_id == client_uuid,
        ClientCheckIn.org_id == current_user.org_id
    ).order_by(desc(ClientCheckIn.start_time)).limit(limit).all()
    
    return [
        {
            "id": str(checkin.id),
            "event_id": checkin.event_id,
            "event_uri": checkin.event_uri,
            "provider": checkin.provider,
            "title": checkin.title,
            "start_time": checkin.start_time.isoformat() if checkin.start_time else None,
            "end_time": checkin.end_time.isoformat() if checkin.end_time else None,
            "location": checkin.location,
            "meeting_url": checkin.meeting_url,
            "attendee_email": checkin.attendee_email,
            "attendee_name": checkin.attendee_name,
            "completed": checkin.completed,
            "cancelled": checkin.cancelled,
            "created_at": checkin.created_at.isoformat() if checkin.created_at else None,
        }
        for checkin in check_ins
    ]


@router.patch("/check-ins/{check_in_id}")
def update_check_in(
    check_in_id: str,
    completed: Optional[bool] = Body(None),
    cancelled: Optional[bool] = Body(None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Update a check-in (mark as completed/cancelled or update other fields).
    Accepts JSON body with optional 'completed' and 'cancelled' boolean fields.
    """
    try:
        check_in_uuid = UUID(check_in_id)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid check-in ID format"
        )
    
    # Get check-in and verify it belongs to user's org
    check_in = db.query(ClientCheckIn).filter(
        and_(
            ClientCheckIn.id == check_in_uuid,
            ClientCheckIn.org_id == current_user.org_id
        )
    ).first()
    
    if not check_in:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Check-in not found"
        )
    
    # Update fields
    if completed is not None:
        check_in.completed = completed
    if cancelled is not None:
        check_in.cancelled = cancelled
    
    check_in.updated_at = datetime.now(timezone.utc)
    
    try:
        db.commit()
        db.refresh(check_in)
        
        return {
            "id": str(check_in.id),
            "event_id": check_in.event_id,
            "event_uri": check_in.event_uri,
            "provider": check_in.provider,
            "title": check_in.title,
            "start_time": check_in.start_time.isoformat() if check_in.start_time else None,
            "end_time": check_in.end_time.isoformat() if check_in.end_time else None,
            "location": check_in.location,
            "meeting_url": check_in.meeting_url,
            "attendee_email": check_in.attendee_email,
            "attendee_name": check_in.attendee_name,
            "completed": check_in.completed,
            "cancelled": check_in.cancelled,
            "created_at": check_in.created_at.isoformat() if check_in.created_at else None,
        }
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to update check-in: {str(e)}"
        )


@router.delete("/check-ins/{check_in_id}")
def delete_check_in(
    check_in_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Delete a check-in.
    """
    try:
        check_in_uuid = UUID(check_in_id)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid check-in ID format"
        )
    
    # Get check-in and verify it belongs to user's org
    check_in = db.query(ClientCheckIn).filter(
        and_(
            ClientCheckIn.id == check_in_uuid,
            ClientCheckIn.org_id == current_user.org_id
        )
    ).first()
    
    if not check_in:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Check-in not found"
        )
    
    try:
        db.delete(check_in)
        db.commit()
        return {"success": True, "message": "Check-in deleted successfully"}
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to delete check-in: {str(e)}"
        )


@router.post("/{client_id}/check-ins")
def create_manual_check_in(
    client_id: str,
    title: str = Body(...),
    start_time: str = Body(...),
    end_time: Optional[str] = Body(None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Create a manual check-in for a client (for days without calendar bookings).
    """
    try:
        client_uuid = UUID(client_id)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid client ID format"
        )
    
    # Verify client exists and belongs to user's org
    client = db.query(Client).filter(
        Client.id == client_uuid,
        Client.org_id == current_user.org_id
    ).first()
    
    if not client:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Client not found"
        )
    
    # Parse datetime strings
    try:
        start_datetime = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
        end_datetime = datetime.fromisoformat(end_time.replace('Z', '+00:00')) if end_time else None
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid datetime format: {str(e)}"
        )
    
    # Create manual check-in
    manual_check_in = ClientCheckIn(
        org_id=current_user.org_id,
        client_id=client_uuid,
        event_id=f"manual_{uuid.uuid4()}",  # Generate unique ID for manual check-ins
        provider="manual",
        title=title,
        start_time=start_datetime,
        end_time=end_datetime,
        attendee_email=client.email or "",
        attendee_name=f"{client.first_name} {client.last_name}".strip(),
        completed=False,
        cancelled=False
    )
    
    try:
        db.add(manual_check_in)
        db.commit()
        db.refresh(manual_check_in)
        
        return {
            "id": str(manual_check_in.id),
            "event_id": manual_check_in.event_id,
            "event_uri": manual_check_in.event_uri,
            "provider": manual_check_in.provider,
            "title": manual_check_in.title,
            "start_time": manual_check_in.start_time.isoformat() if manual_check_in.start_time else None,
            "end_time": manual_check_in.end_time.isoformat() if manual_check_in.end_time else None,
            "location": manual_check_in.location,
            "meeting_url": manual_check_in.meeting_url,
            "attendee_email": manual_check_in.attendee_email,
            "attendee_name": manual_check_in.attendee_name,
            "completed": manual_check_in.completed,
            "cancelled": manual_check_in.cancelled,
            "created_at": manual_check_in.created_at.isoformat() if manual_check_in.created_at else None,
        }
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create check-in: {str(e)}"
        )


@router.get("/{client_id}/check-ins/next")
def get_next_check_in(
    client_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Get the next upcoming check-in for a client.
    Returns None if no upcoming check-ins.
    """
    try:
        client_uuid = UUID(client_id)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid client ID format"
        )
    
    # Verify client exists and belongs to user's org
    client = db.query(Client).filter(
        Client.id == client_uuid,
        Client.org_id == current_user.org_id
    ).first()
    
    if not client:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Client not found"
        )
    
    # Get next upcoming check-in (not completed, not cancelled, in the future)
    now = datetime.now(timezone.utc)
    next_checkin = db.query(ClientCheckIn).filter(
        ClientCheckIn.client_id == client_uuid,
        ClientCheckIn.org_id == current_user.org_id,
        ClientCheckIn.completed == False,
        ClientCheckIn.cancelled == False,
        ClientCheckIn.start_time > now
    ).order_by(ClientCheckIn.start_time).first()
    
    if not next_checkin:
        return None
    
    return {
        "id": str(next_checkin.id),
        "event_id": next_checkin.event_id,
        "event_uri": next_checkin.event_uri,
        "provider": next_checkin.provider,
        "title": next_checkin.title,
        "start_time": next_checkin.start_time.isoformat() if next_checkin.start_time else None,
        "end_time": next_checkin.end_time.isoformat() if next_checkin.end_time else None,
        "location": next_checkin.location,
        "meeting_url": next_checkin.meeting_url,
        "attendee_email": next_checkin.attendee_email,
        "attendee_name": next_checkin.attendee_name,
    }


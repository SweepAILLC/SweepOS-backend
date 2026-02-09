"""
Stripe Treasury Transactions sync service.
Uses Treasury Transactions API as the source of truth for payments and client generation.
"""
# Dynamic import for stripe (optional dependency)
try:
    import stripe
    STRIPE_AVAILABLE = True
except ImportError:
    STRIPE_AVAILABLE = False
    stripe = None

from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert
from typing import Optional, Dict, Any
import uuid
import json
import re

from app.core.encryption import decrypt_token


def _check_stripe_available():
    """Check if stripe library is available"""
    if not STRIPE_AVAILABLE:
        raise ImportError("stripe library is not installed. Install it with: pip install stripe")
from app.models.oauth_token import OAuthToken, OAuthProvider
from app.models.stripe_treasury_transaction import (
    StripeTreasuryTransaction,
    TreasuryTransactionStatus,
    TreasuryTransactionFlowType
)
from app.models.client import Client


def get_stripe_api_key(db: Session, org_id: uuid.UUID) -> str:
    """Get and decrypt Stripe API key for org"""
    oauth_token = db.query(OAuthToken).filter(
        OAuthToken.provider == OAuthProvider.STRIPE,
        OAuthToken.org_id == org_id
    ).first()
    
    if not oauth_token:
        raise ValueError(f"Stripe not connected for org {org_id}")
    
    return decrypt_token(oauth_token.access_token)


def extract_customer_email_from_description(description: Optional[str]) -> Optional[str]:
    """
    Extract customer email from transaction description.
    Stripe Treasury descriptions often contain customer info like:
    "Jane Austen (6789) | Outbound transfer | transfer"
    "Payment from customer@example.com"
    """
    if not description:
        return None
    
    # Try to extract email pattern
    email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
    emails = re.findall(email_pattern, description)
    if emails:
        return emails[0].lower()
    
    return None


def extract_customer_info_from_flow(flow_data: Optional[Dict[str, Any]], api_key: Optional[str] = None) -> tuple:
    """
    Extract customer email and ID from flow data if available.
    Returns (customer_email, customer_id)
    """
    _check_stripe_available()
    if not flow_data:
        return None, None
    
    customer_email = None
    customer_id = None
    
    # Try to get customer info from flow
    if isinstance(flow_data, dict):
        customer_id = flow_data.get("customer") or flow_data.get("customer_id")
        
        # Some flows have customer details
        customer_details = flow_data.get("customer_details") or flow_data.get("customer")
        if isinstance(customer_details, dict):
            customer_email = customer_details.get("email")
            if not customer_id:
                customer_id = customer_details.get("id")
    
    return customer_email, customer_id


def upsert_client_from_treasury_transaction(
    db: Session,
    transaction: Dict[str, Any],
    org_id: uuid.UUID,
    api_key: Optional[str] = None
) -> Optional[Client]:
    """
    Create or update client from Treasury Transaction data.
    Extracts customer email from description or flow data.
    If flow is just an ID, fetches it to get customer details.
    If customer_id is available but no email, fetches customer from Stripe.
    """
    # Extract customer email from description
    description = transaction.get("description", "")
    customer_email = extract_customer_email_from_description(description)
    
    # Try to get customer info from flow
    flow_data = transaction.get("flow")
    flow_email = None
    flow_customer_id = None
    
    if isinstance(flow_data, str) and api_key:
        # Flow is just an ID, fetch it to get customer info
        try:
            # Set API key for this request
            original_key = stripe.api_key
            stripe.api_key = api_key
            
            flow_obj = None
            if flow_data.startswith("obt_"):
                flow_obj = stripe.treasury.OutboundTransfer.retrieve(flow_data)
            elif flow_data.startswith("ic_"):
                flow_obj = stripe.treasury.InboundTransfer.retrieve(flow_data)
            elif flow_data.startswith("tr_"):
                flow_obj = stripe.treasury.ReceivedCredit.retrieve(flow_data)
            elif flow_data.startswith("txn_"):
                # This is a transaction ID, not a flow - skip
                pass
            
            if flow_obj:
                flow_dict = flow_obj.to_dict() if hasattr(flow_obj, 'to_dict') else dict(flow_obj)
                flow_email, flow_customer_id = extract_customer_info_from_flow(flow_dict)
            
            # Restore original API key
            stripe.api_key = original_key
        except Exception as e:
            print(f"[TREASURY SYNC] Error fetching flow {flow_data}: {str(e)}")
            if api_key:
                stripe.api_key = api_key  # Restore on error
    elif isinstance(flow_data, dict):
        flow_email, flow_customer_id = extract_customer_info_from_flow(flow_data)
    
    if flow_email:
        customer_email = flow_email
    
    # If we have customer_id but no email, fetch customer from Stripe
    if not customer_email and flow_customer_id and api_key:
        try:
            # Set API key for this request
            original_key = stripe.api_key
            stripe.api_key = api_key
            
            customer_obj = stripe.Customer.retrieve(flow_customer_id)
            if customer_obj:
                customer_dict = customer_obj.to_dict() if hasattr(customer_obj, 'to_dict') else dict(customer_obj)
                customer_email = customer_dict.get("email")
                if customer_email:
                    print(f"[TREASURY SYNC] Fetched customer email from Stripe Customer {flow_customer_id}: {customer_email}")
            
            # Restore original API key
            stripe.api_key = original_key
        except Exception as e:
            print(f"[TREASURY SYNC] Error fetching customer {flow_customer_id}: {str(e)}")
            if api_key:
                stripe.api_key = api_key  # Restore on error
    
    if not customer_email:
        return None
    
    # Try to find existing client by email
    client = db.query(Client).filter(
        Client.email == customer_email.lower(),
        Client.org_id == org_id
    ).first()
    
    if client:
        # Update existing client
        if not client.stripe_customer_id and flow_data and isinstance(flow_data, dict):
            customer_id = flow_data.get("customer") or flow_data.get("customer_id")
            if customer_id:
                client.stripe_customer_id = customer_id
        client.updated_at = datetime.utcnow()
        return client
    
    # Create new client
    # Try to extract name from description (e.g., "Jane Austen (6789)")
    name_match = re.match(r'^([^(]+)', description) if description else None
    name = name_match.group(1).strip() if name_match else None
    
    first_name = None
    last_name = None
    if name:
        name_parts = name.split()
        first_name = name_parts[0] if name_parts else None
        last_name = ' '.join(name_parts[1:]) if len(name_parts) > 1 else None
    
    client = Client(
        org_id=org_id,
        email=customer_email.lower(),
        first_name=first_name,
        last_name=last_name,
        lifecycle_state='active',
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow()
    )
    
    # Set stripe_customer_id if available
    if flow_data and isinstance(flow_data, dict):
        customer_id = flow_data.get("customer") or flow_data.get("customer_id")
        if customer_id:
            client.stripe_customer_id = customer_id
    
    db.add(client)
    db.flush()
    
    print(f"[TREASURY SYNC] Created client {client.id} from transaction {transaction.get('id')} (email: {customer_email})")
    return client


def upsert_treasury_transaction(
    db: Session,
    transaction_data: Dict[str, Any],
    org_id: uuid.UUID,
    api_key: Optional[str] = None
) -> StripeTreasuryTransaction:
    """
    Idempotently upsert a Treasury Transaction.
    """
    transaction_id = transaction_data.get("id")
    if not transaction_id:
        raise ValueError("Transaction data missing 'id' field")
    
    # Parse status
    status_str = transaction_data.get("status", "open")
    try:
        status = TreasuryTransactionStatus(status_str)
    except ValueError:
        status = TreasuryTransactionStatus.OPEN
    
    # Parse flow type
    flow_type_str = transaction_data.get("flow_type")
    flow_type = None
    if flow_type_str:
        try:
            flow_type = TreasuryTransactionFlowType(flow_type_str)
        except ValueError:
            pass
    
    # Parse timestamps
    created_ts = transaction_data.get("created")
    created = datetime.fromtimestamp(created_ts) if created_ts else datetime.utcnow()
    
    posted_at_ts = transaction_data.get("status_transitions", {}).get("posted_at")
    posted_at = datetime.fromtimestamp(posted_at_ts) if posted_at_ts else None
    
    void_at_ts = transaction_data.get("status_transitions", {}).get("void_at")
    void_at = datetime.fromtimestamp(void_at_ts) if void_at_ts else None
    
    # Extract balance impact
    balance_impact = transaction_data.get("balance_impact", {})
    
    # Extract customer email and create/update client
    description = transaction_data.get("description", "")
    customer_email = extract_customer_email_from_description(description)
    
    flow_data = transaction_data.get("flow")
    customer_id = None
    
    # Extract customer_id from flow (even if it's a string ID)
    customer_id = None
    if isinstance(flow_data, dict):
        _, customer_id = extract_customer_info_from_flow(flow_data)
    
    # Try to create/update client from transaction (this will fetch flow and customer if needed)
    client = upsert_client_from_treasury_transaction(db, transaction_data, org_id, api_key)
    
    # If no client was created but we have a customer_id, try to find existing client
    if not client and customer_id:
        # Even without email, try to find/create client by customer_id
        # This handles cases where customer exists but email isn't in transaction
        existing_client = db.query(Client).filter(
            Client.stripe_customer_id == customer_id,
            Client.org_id == org_id
        ).first()
        if existing_client:
            client = existing_client
            print(f"[TREASURY SYNC] Linked transaction to existing client {client.id} by customer_id {customer_id}")
    
    # Use ON CONFLICT for idempotent upsert
    try:
        stmt = insert(StripeTreasuryTransaction).values(
            org_id=org_id,
            stripe_transaction_id=transaction_id,
            financial_account_id=transaction_data.get("financial_account"),
            flow_id=transaction_data.get("flow") if isinstance(transaction_data.get("flow"), str) else None,
            flow_type=flow_type,
            amount=transaction_data.get("amount", 0),
            currency=transaction_data.get("currency", "usd"),
            status=status,
            balance_impact_cash=balance_impact.get("cash"),
            balance_impact_inbound_pending=balance_impact.get("inbound_pending"),
            balance_impact_outbound_pending=balance_impact.get("outbound_pending"),
            created=created,
            posted_at=posted_at,
            void_at=void_at,
            description=description,
            customer_email=customer_email,
            customer_id=customer_id,
            client_id=client.id if client else None,
            raw_data=transaction_data,
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow()
        )
        
        stmt = stmt.on_conflict_do_update(
            index_elements=['stripe_transaction_id'],
            set_=dict(
                status=stmt.excluded.status,
                amount=stmt.excluded.amount,
                balance_impact_cash=stmt.excluded.balance_impact_cash,
                balance_impact_inbound_pending=stmt.excluded.balance_impact_inbound_pending,
                balance_impact_outbound_pending=stmt.excluded.balance_impact_outbound_pending,
                posted_at=stmt.excluded.posted_at,
                void_at=stmt.excluded.void_at,
                description=stmt.excluded.description,
                customer_email=stmt.excluded.customer_email,
                customer_id=stmt.excluded.customer_id,
                client_id=stmt.excluded.client_id,
                raw_data=stmt.excluded.raw_data,
                updated_at=datetime.utcnow()
            )
        )
        
        db.execute(stmt)
    except Exception as e:
        # Fallback: manual upsert if constraint doesn't exist
        print(f"[TREASURY SYNC] ON CONFLICT failed, using manual upsert: {str(e)}")
        existing = db.query(StripeTreasuryTransaction).filter(
            StripeTreasuryTransaction.stripe_transaction_id == transaction_id
        ).first()
        
        if existing:
            # Update existing
            existing.status = status
            existing.amount = transaction_data.get("amount", 0)
            existing.balance_impact_cash = balance_impact.get("cash")
            existing.balance_impact_inbound_pending = balance_impact.get("inbound_pending")
            existing.balance_impact_outbound_pending = balance_impact.get("outbound_pending")
            existing.posted_at = posted_at
            existing.void_at = void_at
            existing.description = description
            existing.customer_email = customer_email
            existing.customer_id = customer_id
            existing.client_id = client.id if client else None
            existing.raw_data = transaction_data
            existing.updated_at = datetime.utcnow()
        else:
            # Create new
            transaction = StripeTreasuryTransaction(
                org_id=org_id,
                stripe_transaction_id=transaction_id,
                financial_account_id=transaction_data.get("financial_account"),
                flow_id=transaction_data.get("flow") if isinstance(transaction_data.get("flow"), str) else None,
                flow_type=flow_type,
                amount=transaction_data.get("amount", 0),
                currency=transaction_data.get("currency", "usd"),
                status=status,
                balance_impact_cash=balance_impact.get("cash"),
                balance_impact_inbound_pending=balance_impact.get("inbound_pending"),
                balance_impact_outbound_pending=balance_impact.get("outbound_pending"),
                created=created,
                posted_at=posted_at,
                void_at=void_at,
                description=description,
                customer_email=customer_email,
                customer_id=customer_id,
                client_id=client.id if client else None,
                raw_data=transaction_data,
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow()
            )
            db.add(transaction)
        
        db.flush()
    
    # Get the transaction record
    transaction = db.query(StripeTreasuryTransaction).filter(
        StripeTreasuryTransaction.stripe_transaction_id == transaction_id
    ).first()
    
    if not transaction:
        raise Exception(f"Failed to retrieve transaction {transaction_id} after upsert")
    
    return transaction


def sync_treasury_transactions(
    db: Session,
    org_id: uuid.UUID,
    financial_account_id: Optional[str] = None,
    limit: int = 100,
    created_since: Optional[datetime] = None
) -> Dict[str, Any]:
    """
    Sync Treasury Transactions from Stripe API.
    
    Args:
        db: Database session
        org_id: Organization ID
        financial_account_id: Optional financial account ID to filter by
        limit: Maximum number of transactions to fetch per page
        created_since: Only fetch transactions created after this date
    
    Returns:
        dict with sync results
    """
    _check_stripe_available()
    api_key = get_stripe_api_key(db, org_id)
    stripe.api_key = api_key
    
    results = {
        "transactions_synced": 0,
        "transactions_updated": 0,
        "clients_created": 0,
        "clients_updated": 0,
        "errors": []
    }
    
    try:
        # Build query parameters
        params = {"limit": limit, "expand": ["data.flow"]}  # Expand flow to get customer details
        
        if financial_account_id:
            params["financial_account"] = financial_account_id
        
        if created_since:
            params["created"] = {"gte": int(created_since.timestamp())}
        
        print(f"[TREASURY SYNC] Fetching Treasury Transactions with params: {params}")
        
        # Fetch transactions
        transactions = stripe.treasury.Transaction.list(**params)
        
        for transaction in transactions.auto_paging_iter():
            try:
                # Convert Stripe object to dict
                transaction_dict = transaction.to_dict() if hasattr(transaction, 'to_dict') else dict(transaction)
                
                # Check if transaction already exists
                existing = db.query(StripeTreasuryTransaction).filter(
                    StripeTreasuryTransaction.stripe_transaction_id == transaction_dict.get("id")
                ).first()
                
                was_new = existing is None
                
                # Upsert transaction (this also creates/updates clients)
                treasury_transaction = upsert_treasury_transaction(db, transaction_dict, org_id, api_key)
                
                if was_new:
                    results["transactions_synced"] += 1
                else:
                    results["transactions_updated"] += 1
                
                # Track client creation/updates
                if treasury_transaction.client_id:
                    client = db.query(Client).filter(Client.id == treasury_transaction.client_id).first()
                    if client and client.created_at and (datetime.utcnow() - client.created_at).total_seconds() < 60:
                        results["clients_created"] += 1
                    elif client:
                        results["clients_updated"] += 1
                
            except Exception as e:
                error_msg = f"Error processing transaction {transaction.get('id', 'unknown')}: {str(e)}"
                print(f"[TREASURY SYNC] {error_msg}")
                results["errors"].append(error_msg)
                import traceback
                traceback.print_exc()
                continue
        
        db.commit()
        print(f"[TREASURY SYNC] ✅ Sync complete: {results}")
        return results
        
    except Exception as e:
        db.rollback()
        import traceback
        error_msg = f"Treasury sync failed: {str(e)}"
        print(f"[TREASURY SYNC] ❌ {error_msg}")
        print(traceback.format_exc())
        results["errors"].append(error_msg)
        return results


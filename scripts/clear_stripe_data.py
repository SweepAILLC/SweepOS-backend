#!/usr/bin/env python3
"""
Clear all Stripe-related data from the database.
This removes payments, subscriptions, events, and resets client revenue/MRR.

Usage:
    python scripts/clear_stripe_data.py [org_id]
    
    If org_id is provided, only clears data for that organization.
    If not provided, clears all Stripe data (use with caution).
"""
import sys
import os
import uuid

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.db.session import SessionLocal
from app.models.stripe_payment import StripePayment
from app.models.stripe_subscription import StripeSubscription
from app.models.stripe_event import StripeEvent
from app.models.client import Client
from sqlalchemy import and_

def clear_stripe_data(org_id: uuid.UUID = None):
    """
    Clear all Stripe data from database.
    
    Args:
        org_id: Optional organization ID to filter by. If None, clears all data.
    """
    db = SessionLocal()
    
    try:
        if org_id:
            print(f"🗑️  Clearing Stripe data for org {org_id}...")
            org_filter = lambda q: q.filter(StripePayment.org_id == org_id)
            sub_filter = lambda q: q.filter(StripeSubscription.org_id == org_id)
            event_filter = lambda q: q.filter(StripeEvent.org_id == org_id)
            client_filter = lambda q: q.filter(and_(Client.org_id == org_id, Client.stripe_customer_id.isnot(None)))
        else:
            print("🗑️  Clearing ALL Stripe data from database...")
            print("⚠️  WARNING: This will clear data for ALL organizations!")
            org_filter = lambda q: q
            sub_filter = lambda q: q
            event_filter = lambda q: q
            client_filter = lambda q: q.filter(Client.stripe_customer_id.isnot(None))
        
        # Count before deletion
        payments_count = org_filter(db.query(StripePayment)).count()
        subscriptions_count = sub_filter(db.query(StripeSubscription)).count()
        events_count = event_filter(db.query(StripeEvent)).count()
        clients_with_stripe = client_filter(db.query(Client)).count()
        
        # Delete all Stripe data
        org_filter(db.query(StripePayment)).delete(synchronize_session=False)
        sub_filter(db.query(StripeSubscription)).delete(synchronize_session=False)
        event_filter(db.query(StripeEvent)).delete(synchronize_session=False)
        
        # Reset client revenue and MRR for clients linked to Stripe
        if org_id:
            db.query(Client).filter(
                and_(
                    Client.org_id == org_id,
                    Client.stripe_customer_id.isnot(None)
                )
            ).update({
                'lifetime_revenue_cents': 0,
                'estimated_mrr': 0
            }, synchronize_session=False)
        else:
            db.query(Client).filter(Client.stripe_customer_id.isnot(None)).update({
                'lifetime_revenue_cents': 0,
                'estimated_mrr': 0
            }, synchronize_session=False)
        
        db.commit()
        
        print(f"✅ Cleared:")
        print(f"   - {payments_count} payments")
        print(f"   - {subscriptions_count} subscriptions")
        print(f"   - {events_count} events")
        print(f"   - Reset revenue/MRR for {clients_with_stripe} clients")
        print("\n💡 You can now reconnect Stripe and run sync to repopulate data")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        db.rollback()
    finally:
        db.close()

if __name__ == "__main__":
    # Parse org_id from command line if provided
    org_id = None
    if len(sys.argv) > 1:
        try:
            org_id = uuid.UUID(sys.argv[1])
        except ValueError:
            print(f"❌ Invalid org_id format: {sys.argv[1]}")
            print("Usage: python scripts/clear_stripe_data.py [org_id]")
            sys.exit(1)
    
    clear_stripe_data(org_id)


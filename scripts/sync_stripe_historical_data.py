#!/usr/bin/env python3
"""
Sync historical data from Stripe API to populate the database.
This fetches all subscriptions, payments, and customers from Stripe and stores them.
"""
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.db.session import SessionLocal
from app.models.oauth_token import OAuthToken, OAuthProvider

def sync_historical_data():
    """Fetch and store historical data from Stripe (CLI script wrapper)"""
    from app.services.stripe_sync import sync_stripe_historical_data
    
    db = SessionLocal()
    
    try:
        print("🔄 Syncing historical data from Stripe...")
        
        # Check connection
        oauth_token = db.query(OAuthToken).filter(
            OAuthToken.provider == OAuthProvider.STRIPE
        ).first()
        
        if not oauth_token:
            print("❌ Stripe not connected via OAuth. Please connect first.")
            return
        
        print(f"✅ Connected to Stripe account: {oauth_token.account_id}")
        
        # Use the service function
        result = sync_stripe_historical_data(db, background=False)
        
        if result.get("error"):
            print(f"❌ Error: {result.get('error')}")
            return
        
        print(f"✅ Historical data sync complete!")
        print(f"   - Customers: {result.get('customers_synced', 0)} new")
        print(f"   - Subscriptions: {result.get('subscriptions_synced', 0)} new, {result.get('subscriptions_updated', 0)} updated")
        print(f"   - Payments: {result.get('payments_synced', 0)} new, {result.get('payments_updated', 0)} updated")
        print(f"   - Clients MRR: {result.get('clients_updated', 0)} updated")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        db.close()

if __name__ == "__main__":
    sync_historical_data()


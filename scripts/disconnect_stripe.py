#!/usr/bin/env python3
"""
Script to disconnect Stripe by removing OAuth tokens from the database.
Run this before reconnecting with live mode.

Usage:
    python scripts/disconnect_stripe.py [org_id]
    
    If org_id is provided, only disconnects Stripe for that organization.
    If not provided, disconnects all Stripe connections (use with caution).
"""
import sys
import os
import uuid

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.db.session import SessionLocal
from app.models.oauth_token import OAuthToken, OAuthProvider

def disconnect_stripe(org_id: uuid.UUID = None):
    """
    Remove Stripe OAuth token from database.
    
    Args:
        org_id: Optional organization ID to filter by. If None, disconnects all.
    """
    db = SessionLocal()
    try:
        # Find Stripe OAuth token(s)
        query = db.query(OAuthToken).filter(
            OAuthToken.provider == OAuthProvider.STRIPE
        )
        
        if org_id:
            query = query.filter(OAuthToken.org_id == org_id)
            print(f"🔌 Disconnecting Stripe for org {org_id}...")
        else:
            print("🔌 Disconnecting ALL Stripe connections...")
            print("⚠️  WARNING: This will disconnect Stripe for ALL organizations!")
        
        stripe_tokens = query.all()
        
        if stripe_tokens:
            for stripe_token in stripe_tokens:
                account_id = stripe_token.account_id
                token_org_id = stripe_token.org_id
                print(f"Found Stripe connection: account_id={account_id}, org_id={token_org_id}")
                print("Deleting OAuth token...")
                db.delete(stripe_token)
            
            db.commit()
            print(f"✅ Stripe disconnected successfully! ({len(stripe_tokens)} connection(s) removed)")
            print("You can now reconnect Stripe.")
        else:
            print("ℹ️  No Stripe connection found in database.")
            if org_id:
                print(f"   (for org {org_id})")
            print("Stripe is already disconnected.")
        
    except Exception as e:
        db.rollback()
        print(f"❌ Error disconnecting Stripe: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
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
            print("Usage: python scripts/disconnect_stripe.py [org_id]")
            sys.exit(1)
    
    disconnect_stripe(org_id)


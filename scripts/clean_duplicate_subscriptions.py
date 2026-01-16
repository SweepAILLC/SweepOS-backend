#!/usr/bin/env python3
"""
Clean up duplicate subscriptions in the database.
Removes duplicates based on (stripe_subscription_id, org_id), keeping the most recent one.
"""
import sys
import os
import uuid

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.db.session import SessionLocal
from app.models.stripe_subscription import StripeSubscription
from sqlalchemy import func

def clean_duplicate_subscriptions(org_id: uuid.UUID = None):
    """
    Clean duplicate subscriptions.
    
    Args:
        org_id: Optional organization ID to filter by. If None, cleans all orgs.
    """
    db = SessionLocal()
    
    try:
        if org_id:
            print(f"🧹 Cleaning duplicate subscriptions for org {org_id}...")
        else:
            print("🧹 Cleaning duplicate subscriptions for all orgs...")
        
        # Find duplicates: subscriptions with same stripe_subscription_id and org_id
        if org_id:
            duplicates_query = db.query(
                StripeSubscription.stripe_subscription_id,
                StripeSubscription.org_id,
                func.count(StripeSubscription.id).label('count')
            ).filter(
                StripeSubscription.org_id == org_id
            ).group_by(
                StripeSubscription.stripe_subscription_id,
                StripeSubscription.org_id
            ).having(func.count(StripeSubscription.id) > 1)
        else:
            duplicates_query = db.query(
                StripeSubscription.stripe_subscription_id,
                StripeSubscription.org_id,
                func.count(StripeSubscription.id).label('count')
            ).group_by(
                StripeSubscription.stripe_subscription_id,
                StripeSubscription.org_id
            ).having(func.count(StripeSubscription.id) > 1)
        
        duplicates = duplicates_query.all()
        
        if not duplicates:
            print("✅ No duplicate subscriptions found.")
            return
        
        total_deleted = 0
        for dup in duplicates:
            sub_id = dup.stripe_subscription_id
            dup_org_id = dup.org_id
            count = dup.count
            
            print(f"Found {count} duplicates for subscription {sub_id} in org {dup_org_id}")
            
            # Get all duplicates, ordered by updated_at (most recent first)
            subs = db.query(StripeSubscription).filter(
                StripeSubscription.stripe_subscription_id == sub_id,
                StripeSubscription.org_id == dup_org_id
            ).order_by(
                StripeSubscription.updated_at.desc()
            ).all()
            
            # Keep the first (most recent), delete the rest
            to_keep = subs[0]
            to_delete = subs[1:]
            
            print(f"  Keeping: {to_keep.id} (updated: {to_keep.updated_at}, status: {to_keep.status}, mrr: {to_keep.mrr})")
            
            for sub in to_delete:
                print(f"  Deleting: {sub.id} (updated: {sub.updated_at}, status: {sub.status}, mrr: {sub.mrr})")
                db.delete(sub)
                total_deleted += 1
        
        db.commit()
        print(f"✅ Cleaned {total_deleted} duplicate subscriptions.")
        
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
            print("Usage: python scripts/clean_duplicate_subscriptions.py [org_id]")
            sys.exit(1)
    
    clean_duplicate_subscriptions(org_id)


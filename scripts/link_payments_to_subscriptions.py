#!/usr/bin/env python3
"""
Link existing payments to subscriptions based on client and active subscriptions.
This fixes payments that were created without subscription_id.
"""
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.db.session import SessionLocal
from app.models.stripe_payment import StripePayment
from app.models.stripe_subscription import StripeSubscription
from app.models.client import Client
from sqlalchemy import and_

def link_payments_to_subscriptions():
    db = SessionLocal()
    try:
        # Find all payments without subscription_id
        payments_without_sub = db.query(StripePayment).filter(
            StripePayment.subscription_id.is_(None)
        ).all()
        
        print(f"Found {len(payments_without_sub)} payments without subscription_id")
        
        linked_count = 0
        for payment in payments_without_sub:
            if not payment.client_id:
                continue
            
            # Find active subscription for this client
            active_sub = db.query(StripeSubscription).filter(
                and_(
                    StripeSubscription.client_id == payment.client_id,
                    StripeSubscription.status == "active"
                )
            ).first()
            
            if active_sub:
                payment.subscription_id = active_sub.stripe_subscription_id
                linked_count += 1
                print(f"Linked payment {payment.stripe_id[:20]}... to subscription {active_sub.stripe_subscription_id[:20]}...")
        
        if linked_count > 0:
            db.commit()
            print(f"\n✅ Successfully linked {linked_count} payments to subscriptions")
        else:
            print("\n⚠️  No payments were linked (no active subscriptions found for clients)")
        
    except Exception as e:
        db.rollback()
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        db.close()

if __name__ == "__main__":
    link_payments_to_subscriptions()


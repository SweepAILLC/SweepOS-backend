"""
Script to clear all Stripe-related data from the database.
WARNING: This will delete ALL Stripe data for ALL organizations.
Use with caution!
"""
import sys
from sqlalchemy import text
from app.db.session import SessionLocal
from app.models.stripe_payment import StripePayment
from app.models.stripe_subscription import StripeSubscription
from app.models.stripe_event import StripeEvent
from app.models.stripe_treasury_transaction import StripeTreasuryTransaction

def clear_stripe_data(org_id: str = None):
    """
    Clear all Stripe-related data from the database.
    
    Args:
        org_id: Optional UUID string. If provided, only clears data for that organization.
                If None, clears data for ALL organizations.
    """
    db = SessionLocal()
    try:
        print("🗑️  Starting Stripe data cleanup...")
        
        if org_id:
            print(f"   Filtering by org_id: {org_id}")
            org_filter = {"org_id": org_id}
        else:
            print("   Clearing data for ALL organizations")
            org_filter = {}
        
        # Count records before deletion
        payment_count = db.query(StripePayment).filter_by(**org_filter).count() if org_filter else db.query(StripePayment).count()
        subscription_count = db.query(StripeSubscription).filter_by(**org_filter).count() if org_filter else db.query(StripeSubscription).count()
        event_count = db.query(StripeEvent).filter_by(**org_filter).count() if org_filter else db.query(StripeEvent).count()
        treasury_count = db.query(StripeTreasuryTransaction).filter_by(**org_filter).count() if org_filter else db.query(StripeTreasuryTransaction).count()
        
        print(f"\n📊 Records to delete:")
        print(f"   - Stripe Payments: {payment_count}")
        print(f"   - Stripe Subscriptions: {subscription_count}")
        print(f"   - Stripe Events: {event_count}")
        print(f"   - Treasury Transactions: {treasury_count}")
        print(f"   - Total: {payment_count + subscription_count + event_count + treasury_count}")
        
        # Confirm deletion
        if org_id:
            confirm = input(f"\n⚠️  Are you sure you want to delete all Stripe data for org {org_id}? (yes/no): ")
        else:
            confirm = input("\n⚠️  Are you sure you want to delete ALL Stripe data for ALL organizations? (yes/no): ")
        
        if confirm.lower() != 'yes':
            print("❌ Deletion cancelled.")
            return
        
        # Delete in order (respecting foreign key constraints)
        print("\n🗑️  Deleting records...")
        
        if org_filter:
            deleted_payments = db.query(StripePayment).filter_by(**org_filter).delete(synchronize_session=False)
            deleted_subscriptions = db.query(StripeSubscription).filter_by(**org_filter).delete(synchronize_session=False)
            deleted_events = db.query(StripeEvent).filter_by(**org_filter).delete(synchronize_session=False)
            deleted_treasury = db.query(StripeTreasuryTransaction).filter_by(**org_filter).delete(synchronize_session=False)
        else:
            deleted_payments = db.query(StripePayment).delete(synchronize_session=False)
            deleted_subscriptions = db.query(StripeSubscription).delete(synchronize_session=False)
            deleted_events = db.query(StripeEvent).delete(synchronize_session=False)
            deleted_treasury = db.query(StripeTreasuryTransaction).delete(synchronize_session=False)
        
        db.commit()
        
        print(f"\n✅ Deletion complete!")
        print(f"   - Deleted {deleted_payments} Stripe Payments")
        print(f"   - Deleted {deleted_subscriptions} Stripe Subscriptions")
        print(f"   - Deleted {deleted_events} Stripe Events")
        print(f"   - Deleted {deleted_treasury} Treasury Transactions")
        print(f"   - Total deleted: {deleted_payments + deleted_subscriptions + deleted_events + deleted_treasury}")
        
    except Exception as e:
        db.rollback()
        print(f"\n❌ Error during deletion: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        db.close()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Clear Stripe-related data from the database")
    parser.add_argument(
        "--org-id",
        type=str,
        help="Optional: Only clear data for a specific organization UUID"
    )
    
    args = parser.parse_args()
    clear_stripe_data(org_id=args.org_id)



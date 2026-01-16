#!/usr/bin/env python3
"""
Clean up duplicate payments in the database.
Removes duplicates based on (subscription_id, invoice_id) or (invoice_id) for the same org,
keeping the charge record over invoice record, and the most recent one.
"""
import sys
import os
import uuid

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.db.session import SessionLocal
from app.models.stripe_payment import StripePayment
from sqlalchemy import func, and_, or_

def clean_duplicate_payments(org_id: uuid.UUID = None):
    """
    Clean duplicate payments.
    
    Args:
        org_id: Optional organization ID to filter by. If None, cleans all orgs.
    """
    db = SessionLocal()
    
    try:
        if org_id:
            print(f"🧹 Cleaning duplicate payments for org {org_id}...")
        else:
            print("🧹 Cleaning duplicate payments for all orgs...")
        
        # Strategy 1: Find duplicates by (subscription_id, invoice_id, org_id)
        # For payments with the same subscription+invoice, keep the charge (not invoice) and most recent
        base_query = db.query(StripePayment)
        if org_id:
            base_query = base_query.filter(StripePayment.org_id == org_id)
        
        # Find payments with same subscription_id + invoice_id
        duplicates_query = base_query.filter(
            and_(
                StripePayment.subscription_id.isnot(None),
                StripePayment.invoice_id.isnot(None)
            )
        ).all()
        
        # Group by (subscription_id, invoice_id, org_id)
        grouped = {}
        for payment in duplicates_query:
            key = (payment.subscription_id, payment.invoice_id, str(payment.org_id))
            if key not in grouped:
                grouped[key] = []
            grouped[key].append(payment)
        
        total_deleted = 0
        
        # Process each group
        for key, payments in grouped.items():
            if len(payments) > 1:
                sub_id, inv_id, org = key
                print(f"Found {len(payments)} duplicates for subscription {sub_id}, invoice {inv_id}, org {org}")
                
                # Sort: prefer charge over invoice, then by updated_at (most recent first)
                payments.sort(key=lambda p: (
                    0 if p.type == 'charge' else 1,  # Charges first
                    -(p.updated_at.timestamp() if p.updated_at else 0)  # Most recent first
                ))
                
                to_keep = payments[0]
                to_delete = payments[1:]
                
                print(f"  Keeping: {to_keep.stripe_id} (type: {to_keep.type}, updated: {to_keep.updated_at}, amount: {to_keep.amount_cents})")
                
                for payment in to_delete:
                    print(f"  Deleting: {payment.stripe_id} (type: {payment.type}, updated: {payment.updated_at}, amount: {payment.amount_cents})")
                    db.delete(payment)
                    total_deleted += 1
        
        # Strategy 2: Find duplicates by (invoice_id, org_id) where subscription_id is NULL
        # For payments with same invoice but no subscription, prefer charge over invoice
        invoice_duplicates = base_query.filter(
            and_(
                StripePayment.invoice_id.isnot(None),
                StripePayment.subscription_id.is_(None)
            )
        ).all()
        
        # Group by (invoice_id, org_id)
        invoice_grouped = {}
        for payment in invoice_duplicates:
            key = (payment.invoice_id, str(payment.org_id))
            if key not in invoice_grouped:
                invoice_grouped[key] = []
            invoice_grouped[key].append(payment)
        
        # Process each group
        for key, payments in invoice_grouped.items():
            if len(payments) > 1:
                inv_id, org = key
                print(f"Found {len(payments)} duplicates for invoice {inv_id}, org {org} (no subscription)")
                
                # Sort: prefer charge over invoice, then by updated_at
                payments.sort(key=lambda p: (
                    0 if p.type == 'charge' else 1,
                    -(p.updated_at.timestamp() if p.updated_at else 0)
                ))
                
                to_keep = payments[0]
                to_delete = payments[1:]
                
                print(f"  Keeping: {to_keep.stripe_id} (type: {to_keep.type}, updated: {to_keep.updated_at})")
                
                for payment in to_delete:
                    print(f"  Deleting: {payment.stripe_id} (type: {payment.type}, updated: {payment.updated_at})")
                    db.delete(payment)
                    total_deleted += 1
        
        db.commit()
        print(f"✅ Cleaned {total_deleted} duplicate payments.")
        
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
            print("Usage: python scripts/clean_duplicate_payments.py [org_id]")
            sys.exit(1)
    
    clean_duplicate_payments(org_id)


#!/usr/bin/env python3
"""
Retroactively create subscriptions from existing invoice payments.
This fixes the issue where payments were created but subscriptions weren't.
Since test events don't include subscription IDs, we create one subscription per client
based on their invoice payments.
"""
import os
import sys

# Add parent directory to path to import app modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.db.session import SessionLocal
from app.models.stripe_payment import StripePayment
from app.models.stripe_subscription import StripeSubscription
from app.models.client import Client
from datetime import datetime, timedelta
from decimal import Decimal
from collections import defaultdict

db = SessionLocal()

print("=" * 60)
print("Fix Missing Subscriptions")
print("=" * 60)
print()

# Find all succeeded invoice payments (those starting with 'in_')
invoice_payments = db.query(StripePayment).filter(
    StripePayment.status == "succeeded",
    StripePayment.stripe_id.like("in_%")  # Invoice payments
).all()

print(f"Found {len(invoice_payments)} invoice payments")
print()

# Group payments by client to create one subscription per client
# Use the average payment amount as MRR
client_payments = defaultdict(list)
for payment in invoice_payments:
    if payment.client_id:
        client_payments[payment.client_id].append(payment)

print(f"Found {len(client_payments)} clients with invoice payments")
print()

created_count = 0
updated_count = 0

for client_id, payments in client_payments.items():
    client = db.query(Client).filter(Client.id == client_id).first()
    if not client:
        continue
    
    # Calculate average payment amount as MRR (assume monthly)
    total_amount = sum(p.amount_cents for p in payments)
    avg_amount = total_amount / len(payments)
    invoice_mrr = Decimal(avg_amount) / Decimal(100)
    
    # Use the most recent payment date
    latest_payment = max(payments, key=lambda p: p.created_at)
    
    # Check if client already has a subscription
    existing_sub = db.query(StripeSubscription).filter(
        StripeSubscription.client_id == client_id,
        StripeSubscription.status == "active"
    ).first()
    
    if existing_sub:
        # Update MRR if it's 0 or None
        if existing_sub.mrr == 0 or existing_sub.mrr is None:
            existing_sub.mrr = invoice_mrr
            existing_sub.status = "active"
            updated_count += 1
            print(f"✅ Updated subscription for client {client.email or client_id}: MRR: ${float(invoice_mrr):.2f}")
        continue
    
    # Create a synthetic subscription ID from client ID
    subscription_id = f"sub_synthetic_{client_id}"
    
    # Check if this synthetic subscription already exists
    existing_synthetic = db.query(StripeSubscription).filter(
        StripeSubscription.stripe_subscription_id == subscription_id
    ).first()
    
    if existing_synthetic:
        if existing_synthetic.mrr == 0 or existing_synthetic.mrr is None:
            existing_synthetic.mrr = invoice_mrr
            existing_synthetic.status = "active"
            updated_count += 1
            print(f"✅ Updated synthetic subscription for client {client.email or client_id}: MRR: ${float(invoice_mrr):.2f}")
        continue
    
    # Create new subscription
    subscription = StripeSubscription(
        stripe_subscription_id=subscription_id,
        client_id=client_id,
        status="active",
        current_period_start=latest_payment.created_at,
        current_period_end=latest_payment.created_at + timedelta(days=30),
        mrr=invoice_mrr,
        raw={"created_from_payments": [p.stripe_id for p in payments], "synthetic": True},
        created_at=latest_payment.created_at
    )
    db.add(subscription)
    created_count += 1
    print(f"✅ Created subscription for client {client.email or client_id}: MRR: ${float(invoice_mrr):.2f} (from {len(payments)} payments)")

try:
    db.commit()
    print()
    print(f"✅ Successfully created {created_count} subscriptions")
    print(f"✅ Successfully updated {updated_count} subscriptions")
except Exception as e:
    print(f"❌ Error committing: {e}")
    import traceback
    traceback.print_exc()
    db.rollback()

print()
print("=" * 60)


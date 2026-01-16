#!/usr/bin/env python3
"""
Check if Stripe events are being processed and stored correctly.
Run this to verify webhook processing is working.
"""
import os
import sys

# Add parent directory to path to import app modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.db.session import SessionLocal
from app.models.stripe_event import StripeEvent
from app.models.stripe_payment import StripePayment
from app.models.stripe_subscription import StripeSubscription
from app.models.client import Client

db = SessionLocal()

print("=" * 60)
print("Stripe Data Check")
print("=" * 60)
print()

# Check events
events = db.query(StripeEvent).order_by(StripeEvent.received_at.desc()).limit(10).all()
print(f"Recent Events: {len(events)}")
for event in events:
    print(f"  - {event.type} (ID: {event.stripe_event_id[:20]}...) - Processed: {event.processed}")
print()

# Check payments
payments = db.query(StripePayment).order_by(StripePayment.created_at.desc()).limit(10).all()
print(f"Payments: {len(payments)}")
for payment in payments:
    print(f"  - {payment.status}: ${payment.amount_cents/100:.2f} (created: {payment.created_at}) - ID: {payment.stripe_id[:20]}...")
print()

# Check succeeded payments in last 30 days
from datetime import datetime, timedelta
thirty_days_ago = datetime.utcnow() - timedelta(days=30)
succeeded_payments = db.query(StripePayment).filter(
    StripePayment.status == "succeeded",
    StripePayment.created_at >= thirty_days_ago
).all()
print(f"Succeeded Payments (last 30 days): {len(succeeded_payments)}")
total_revenue = sum(p.amount_cents for p in succeeded_payments)
print(f"Total Revenue (last 30 days): ${total_revenue/100:.2f}")
for payment in succeeded_payments:
    print(f"  - ${payment.amount_cents/100:.2f} on {payment.created_at}")
print()

# Check subscriptions
subscriptions = db.query(StripeSubscription).order_by(StripeSubscription.created_at.desc()).limit(10).all()
print(f"Subscriptions: {len(subscriptions)}")
for sub in subscriptions:
    print(f"  - {sub.status}: ${float(sub.mrr):.2f}/mo (MRR type: {type(sub.mrr)}, value: {sub.mrr}) - ID: {sub.stripe_subscription_id[:20]}...")
    print(f"    Created: {sub.created_at}, Updated: {sub.updated_at}")
print()

# Check active subscriptions specifically
active_subs = db.query(StripeSubscription).filter(StripeSubscription.status == "active").all()
print(f"Active Subscriptions: {len(active_subs)}")
total_mrr = sum(float(sub.mrr) for sub in active_subs)
print(f"Total MRR from active subs: ${total_mrr:.2f}")
for sub in active_subs:
    print(f"  - MRR: ${float(sub.mrr):.2f} (raw: {sub.mrr})")
print()

# Check clients with revenue
clients_with_revenue = db.query(Client).filter(Client.lifetime_revenue_cents > 0).all()
print(f"Clients with Revenue: {len(clients_with_revenue)}")
for client in clients_with_revenue:
    print(f"  - {client.email or 'No email'}: ${client.lifetime_revenue_cents/100:.2f}")
print()

# Check unprocessed events
unprocessed_events = db.query(StripeEvent).filter(StripeEvent.processed == False).all()
print(f"Unprocessed Events: {len(unprocessed_events)}")
if unprocessed_events:
    print("  ⚠️  Some events failed to process. Check backend logs.")
    for event in unprocessed_events[:5]:  # Show first 5
        print(f"    - {event.type} (ID: {event.stripe_event_id[:20]}...)")
print()

# Check payments with subscriptions that don't exist
payments_with_subscriptions = db.query(StripePayment).filter(
    StripePayment.subscription_id.isnot(None),
    StripePayment.status == "succeeded"
).all()
print(f"Payments with subscription_id: {len(payments_with_subscriptions)}")
missing_subs = []
for payment in payments_with_subscriptions:
    if payment.subscription_id:
        sub_exists = db.query(StripeSubscription).filter(
            StripeSubscription.stripe_subscription_id == payment.subscription_id
        ).first()
        if not sub_exists:
            missing_subs.append(payment)
            print(f"  - Payment {payment.stripe_id[:20]}... has subscription_id {payment.subscription_id[:20]}... but subscription doesn't exist")
print(f"Missing subscriptions: {len(missing_subs)}")
print()

print("=" * 60)


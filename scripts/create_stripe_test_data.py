#!/usr/bin/env python3
"""
Create test data in a Stripe test account.
Run this AFTER connecting to your test account via OAuth.

This script uses the OAuth token to create test customers and subscriptions.
"""
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.db.session import SessionLocal
from app.core.config import settings
from app.core.encryption import decrypt_token
from app.models.oauth_token import OAuthToken, OAuthProvider
import stripe

def create_test_data():
    """Create test customers and subscriptions in Stripe"""
    db = SessionLocal()
    
    try:
        # Get OAuth token
        oauth_token = db.query(OAuthToken).filter(
            OAuthToken.provider == OAuthProvider.STRIPE
        ).first()
        
        if not oauth_token:
            print("❌ No Stripe OAuth token found. Please connect Stripe first via the dashboard.")
            return
        
        # Use the OAuth token
        decrypted_token = decrypt_token(oauth_token.access_token)
        stripe.api_key = decrypted_token
        
        print(f"✅ Using Stripe account: {oauth_token.account_id}")
        print(f"🔄 Creating test data...\n")
        
        # Create test customers
        customers = []
        customer_data = [
            {"email": "test1@example.com", "name": "Test Customer 1"},
            {"email": "test2@example.com", "name": "Test Customer 2"},
            {"email": "test3@example.com", "name": "Test Customer 3"},
        ]
        
        for data in customer_data:
            try:
                customer = stripe.Customer.create(
                    email=data["email"],
                    name=data["name"]
                )
                customers.append(customer)
                print(f"✅ Created customer: {customer.id} ({data['email']})")
            except Exception as e:
                print(f"⚠️  Failed to create customer {data['email']}: {e}")
        
        if not customers:
            print("❌ No customers created. Cannot create subscriptions.")
            return
        
        # Create a test product and price
        try:
            product = stripe.Product.create(
                name="Test Subscription Plan",
                description="Monthly test subscription"
            )
            print(f"✅ Created product: {product.id}")
            
            price = stripe.Price.create(
                product=product.id,
                unit_amount=2999,  # $29.99
                currency="usd",
                recurring={"interval": "month"}
            )
            print(f"✅ Created price: {price.id} ($29.99/month)")
        except Exception as e:
            print(f"❌ Failed to create product/price: {e}")
            return
        
        # Create subscriptions for customers
        subscriptions = []
        for customer in customers:
            try:
                subscription = stripe.Subscription.create(
                    customer=customer.id,
                    items=[{"price": price.id}],
                    collection_method="charge_automatically"
                )
                subscriptions.append(subscription)
                print(f"✅ Created subscription: {subscription.id} for customer {customer.email} (status: {subscription.status})")
            except Exception as e:
                print(f"⚠️  Failed to create subscription for {customer.email}: {e}")
        
        print(f"\n✅ Test data creation complete!")
        print(f"   - Customers: {len(customers)}")
        print(f"   - Subscriptions: {len(subscriptions)}")
        print(f"\n💡 Now run 'make sync-stripe-historical' to sync this data to your database.")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        db.close()

if __name__ == "__main__":
    create_test_data()


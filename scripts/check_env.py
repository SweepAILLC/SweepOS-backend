#!/usr/bin/env python3
"""
Debug script to check if environment variables are loaded correctly.
Run this inside the backend container to verify .env file is being read.

Note: Docker Compose doesn't copy .env into the container - it reads it and
injects variables as environment variables. So we check the actual env vars.
"""
import os
import sys

# Add parent directory to path to import app modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.core.config import settings

print("=" * 60)
print("Environment Variables Check")
print("=" * 60)
print()
print("Note: Docker Compose reads .env from project root and injects as env vars")
print("The .env file itself is not copied into the container.")
print()

# Get raw environment variables (what Docker Compose injected)
raw_stripe_client_id = os.environ.get('STRIPE_OAUTH_CLIENT_ID', '')
raw_stripe_redirect = os.environ.get('STRIPE_REDIRECT_URI', '')
raw_stripe_secret = os.environ.get('STRIPE_SECRET_KEY', '')

# Check Stripe OAuth variables
print("Stripe OAuth Configuration:")
print(f"  STRIPE_OAUTH_CLIENT_ID (raw env): '{raw_stripe_client_id}'")
print(f"  STRIPE_OAUTH_CLIENT_ID (settings): {settings.STRIPE_OAUTH_CLIENT_ID or '(not set)'}")
print(f"  STRIPE_REDIRECT_URI: {settings.STRIPE_REDIRECT_URI}")
print(f"  STRIPE_SECRET_KEY: {'***' + settings.STRIPE_SECRET_KEY[-4:] if settings.STRIPE_SECRET_KEY else '(not set)'}")
print()

# Check Brevo OAuth variables
raw_brevo_client_id = os.environ.get('BREVO_CLIENT_ID', '')
raw_brevo_secret = os.environ.get('BREVO_CLIENT_SECRET', '')
print("Brevo OAuth Configuration:")
print(f"  BREVO_CLIENT_ID (raw env): '{raw_brevo_client_id}'")
print(f"  BREVO_CLIENT_ID (settings): {settings.BREVO_CLIENT_ID or '(not set)'}")
print(f"  BREVO_REDIRECT_URI: {settings.BREVO_REDIRECT_URI}")
print(f"  BREVO_CLIENT_SECRET: {'***' + settings.BREVO_CLIENT_SECRET[-4:] if settings.BREVO_CLIENT_SECRET else '(not set)'}")
print()

# Detailed diagnostics
print("Diagnostics:")
print(f"  STRIPE_OAUTH_CLIENT_ID length: {len(raw_stripe_client_id)}")
print(f"  STRIPE_OAUTH_CLIENT_ID is empty string: {raw_stripe_client_id == ''}")
print(f"  STRIPE_OAUTH_CLIENT_ID is None: {raw_stripe_client_id is None}")
print(f"  STRIPE_OAUTH_CLIENT_ID stripped: '{raw_stripe_client_id.strip()}'")
print()

# Check if values are empty strings
if not raw_stripe_client_id or raw_stripe_client_id.strip() == "":
    print("⚠️  WARNING: STRIPE_OAUTH_CLIENT_ID is empty or not set")
    print("   This means either:")
    print("   1. The .env file has STRIPE_OAUTH_CLIENT_ID= with no value")
    print("   2. The .env file doesn't have STRIPE_OAUTH_CLIENT_ID at all")
    print("   3. Docker Compose didn't load the variable (check docker-compose.yml)")
    print("   4. The backend container needs to be restarted after updating .env")
elif settings.STRIPE_OAUTH_CLIENT_ID and settings.STRIPE_OAUTH_CLIENT_ID.strip():
    print("✅ STRIPE_OAUTH_CLIENT_ID is set correctly")
    print(f"   Value starts with: {settings.STRIPE_OAUTH_CLIENT_ID[:10]}...")
else:
    print("⚠️  WARNING: STRIPE_OAUTH_CLIENT_ID exists in env but settings can't read it")

print("=" * 60)


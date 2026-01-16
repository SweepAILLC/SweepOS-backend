#!/usr/bin/env python3
"""
Script to check Stripe OAuth configuration and show what URL will be generated.
"""
import sys
import os
from urllib.parse import urlencode

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.core.config import settings

def check_oauth_config():
    """Check OAuth configuration and show what URL will be generated"""
    print("🔍 Checking Stripe OAuth Configuration...\n")
    
    # Check API key
    if settings.STRIPE_SECRET_KEY:
        key_type = "LIVE" if settings.STRIPE_SECRET_KEY.startswith("sk_live_") else "TEST"
        print(f"✅ STRIPE_SECRET_KEY: {key_type} mode ({settings.STRIPE_SECRET_KEY[:20]}...)")
    else:
        print("❌ STRIPE_SECRET_KEY: Not set")
    
    # Check OAuth Client ID
    if settings.STRIPE_OAUTH_CLIENT_ID:
        print(f"✅ STRIPE_OAUTH_CLIENT_ID: {settings.STRIPE_OAUTH_CLIENT_ID}")
    else:
        print("❌ STRIPE_OAUTH_CLIENT_ID: Not set")
    
    # Check Test OAuth URL
    if settings.STRIPE_TEST_OAUTH_URL:
        print(f"⚠️  STRIPE_TEST_OAUTH_URL: Set (will be used instead of Client ID)")
        print(f"   {settings.STRIPE_TEST_OAUTH_URL[:80]}...")
    else:
        print("✅ STRIPE_TEST_OAUTH_URL: Not set (will use Client ID)")
    
    # Check Redirect URI
    if settings.STRIPE_REDIRECT_URI:
        print(f"✅ STRIPE_REDIRECT_URI: {settings.STRIPE_REDIRECT_URI}")
    else:
        print("❌ STRIPE_REDIRECT_URI: Not set")
    
    print("\n" + "="*60)
    print("OAuth URL that will be generated:")
    print("="*60)
    
    # Show what URL will be generated
    if settings.STRIPE_TEST_OAUTH_URL:
        print(f"\n{settings.STRIPE_TEST_OAUTH_URL}")
        print("\n⚠️  Using test OAuth URL directly (from STRIPE_TEST_OAUTH_URL)")
    elif settings.STRIPE_OAUTH_CLIENT_ID and settings.STRIPE_REDIRECT_URI:
        import secrets
        state = secrets.token_urlsafe(32)
        params = {
            "client_id": settings.STRIPE_OAUTH_CLIENT_ID,
            "redirect_uri": settings.STRIPE_REDIRECT_URI,
            "state": state,
        }
        oauth_url = f"https://marketplace.stripe.com/oauth/v2/authorize?{urlencode(params)}"
        print(f"\n{oauth_url}")
        print("\n✅ Using Client ID to generate OAuth URL")
    else:
        print("\n❌ Cannot generate OAuth URL - missing required configuration")
        if not settings.STRIPE_OAUTH_CLIENT_ID:
            print("   - STRIPE_OAUTH_CLIENT_ID is required")
        if not settings.STRIPE_REDIRECT_URI:
            print("   - STRIPE_REDIRECT_URI is required")
    
    print("\n" + "="*60)
    print("Important Checks:")
    print("="*60)
    
    # Check redirect URI format
    if settings.STRIPE_REDIRECT_URI:
        if settings.STRIPE_REDIRECT_URI.startswith("http://localhost"):
            print("⚠️  Redirect URI uses HTTP localhost - Stripe requires HTTPS for live mode")
            print("   Consider using ngrok or the manual callback endpoint")
        elif settings.STRIPE_REDIRECT_URI.startswith("https://"):
            print("✅ Redirect URI uses HTTPS")
        else:
            print("⚠️  Redirect URI should use HTTPS for live mode")
    
    # Check if redirect URI matches common patterns
    if settings.STRIPE_REDIRECT_URI:
        if "/api/oauth/stripe/callback" not in settings.STRIPE_REDIRECT_URI:
            print("⚠️  Redirect URI should end with /api/oauth/stripe/callback")
    
    print("\n💡 Tip: Make sure the redirect URI matches what's configured in your Stripe App settings!")

if __name__ == "__main__":
    check_oauth_config()


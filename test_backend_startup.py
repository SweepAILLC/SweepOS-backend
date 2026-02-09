#!/usr/bin/env python3
"""
Test script to verify backend can start without errors.
This will catch import errors and basic startup issues.
"""
import sys
import os

# Add the backend directory to the path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

print("Testing backend imports...")

try:
    print("1. Testing core imports...")
    from app.core.config import settings
    print("   ✅ Config imported")
    
    from app.db.session import engine, SessionLocal
    print("   ✅ Database session imported")
    
    from app.core.security import create_access_token, verify_password
    print("   ✅ Security imported")
    
    from app.core.encryption import encrypt_token, decrypt_token
    print("   ✅ Encryption imported")
    
    print("\n2. Testing API imports...")
    from app.api import auth, clients, events, oauth, integrations, stripe, webhooks, funnels, admin, users, encryption, email_ingestion
    print("   ✅ All API modules imported")
    
    print("\n3. Testing FastAPI app creation...")
    from app.main import app
    print("   ✅ FastAPI app created")
    
    print("\n4. Testing database connection...")
    from app.db.session import get_db
    db_gen = get_db()
    db = next(db_gen)
    db.execute("SELECT 1")
    db.close()
    print("   ✅ Database connection works")
    
    print("\n✅ All tests passed! Backend should start correctly.")
    sys.exit(0)
    
except ImportError as e:
    print(f"\n❌ Import error: {str(e)}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
except Exception as e:
    print(f"\n❌ Error: {str(e)}")
    import traceback
    traceback.print_exc()
    sys.exit(1)



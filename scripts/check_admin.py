#!/usr/bin/env python3
"""Check or reset admin user"""
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from sqlalchemy.orm import Session
from app.db.session import SessionLocal
from app.models.user import User
from app.core.security import get_password_hash, verify_password
from app.core.config import settings


def check_admin():
    db: Session = SessionLocal()
    try:
        admin = db.query(User).filter(User.email == settings.SUDO_ADMIN_EMAIL).first()
        if admin:
            print(f"Admin user found: {admin.email}")
            print(f"Is admin: {admin.is_admin}")
            print(f"Created at: {admin.created_at}")
            
            # Test password
            test_password = settings.SUDO_ADMIN_PASSWORD
            if verify_password(test_password, admin.hashed_password):
                print(f"✓ Password verification successful")
                print(f"Login with:")
                print(f"  Email: {admin.email}")
                print(f"  Password: {test_password}")
            else:
                print(f"✗ Password verification failed")
                print(f"Expected password from .env: {test_password}")
                print("\nTo reset password, run:")
                print(f"  python scripts/reset_admin_password.py")
        else:
            print(f"No admin user found with email: {settings.SUDO_ADMIN_EMAIL}")
            print("Run: make seed-admin")
    except Exception as e:
        print(f"Error: {e}")
        raise
    finally:
        db.close()


if __name__ == "__main__":
    check_admin()


#!/usr/bin/env python3
"""Reset admin user password to match .env"""
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from sqlalchemy.orm import Session
from app.db.session import SessionLocal
from app.models.user import User
from app.core.security import get_password_hash
from app.core.config import settings


def reset_admin_password():
    db: Session = SessionLocal()
    try:
        admin = db.query(User).filter(User.email == settings.SUDO_ADMIN_EMAIL).first()
        if not admin:
            print(f"No admin user found with email: {settings.SUDO_ADMIN_EMAIL}")
            print("Run: make seed-admin")
            return
        
        # Update password
        admin.hashed_password = get_password_hash(settings.SUDO_ADMIN_PASSWORD)
        db.commit()
        
        print(f"✓ Admin password reset successfully")
        print(f"Login with:")
        print(f"  Email: {admin.email}")
        print(f"  Password: {settings.SUDO_ADMIN_PASSWORD}")
    except Exception as e:
        db.rollback()
        print(f"Error resetting password: {e}")
        raise
    finally:
        db.close()


if __name__ == "__main__":
    reset_admin_password()


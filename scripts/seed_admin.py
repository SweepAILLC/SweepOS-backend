#!/usr/bin/env python3
"""Seed script to create initial admin user"""
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from sqlalchemy.orm import Session
from app.db.session import SessionLocal
from app.models.user import User
from app.core.security import get_password_hash
from app.core.config import settings


def seed_admin():
    db: Session = SessionLocal()
    try:
        # Check if admin already exists
        admin = db.query(User).filter(User.email == settings.SUDO_ADMIN_EMAIL).first()
        if admin:
            print(f"Admin user {settings.SUDO_ADMIN_EMAIL} already exists")
            return
        
        # Create admin user
        admin = User(
            email=settings.SUDO_ADMIN_EMAIL,
            hashed_password=get_password_hash(settings.SUDO_ADMIN_PASSWORD),
            is_admin=True
        )
        db.add(admin)
        db.commit()
        print(f"Admin user created: {settings.SUDO_ADMIN_EMAIL}")
        print(f"Password: {settings.SUDO_ADMIN_PASSWORD}")
    except Exception as e:
        db.rollback()
        print(f"Error creating admin user: {e}")
        raise
    finally:
        db.close()


if __name__ == "__main__":
    seed_admin()


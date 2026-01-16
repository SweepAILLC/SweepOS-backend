#!/usr/bin/env python3
"""Check organizations and their admin users"""
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from sqlalchemy.orm import Session
from app.db.session import SessionLocal
from app.models.user import User
from app.models.organization import Organization
from app.core.security import verify_password


def check_org_users():
    db: Session = SessionLocal()
    try:
        # Get all organizations
        orgs = db.query(Organization).order_by(Organization.created_at.desc()).all()
        
        print("=" * 60)
        print("Organizations and Users")
        print("=" * 60)
        print()
        
        for org in orgs:
            print(f"Organization: {org.name}")
            print(f"  ID: {org.id}")
            print(f"  Created: {org.created_at}")
            
            # Get users for this org
            users = db.query(User).filter(User.org_id == org.id).all()
            print(f"  Users ({len(users)}):")
            
            for user in users:
                print(f"    - Email: {user.email}")
                print(f"      Role: {user.role.value if hasattr(user.role, 'value') else user.role}")
                print(f"      Is Admin: {user.is_admin}")
                print(f"      Created: {user.created_at}")
                print()
            
            print("-" * 60)
            print()
        
        # Specifically look for test orgs
        test_orgs = db.query(Organization).filter(
            Organization.name.ilike('%test%')
        ).all()
        
        if test_orgs:
            print("=" * 60)
            print("Test Organizations Found:")
            print("=" * 60)
            for org in test_orgs:
                print(f"\n{org.name} (ID: {org.id})")
                users = db.query(User).filter(User.org_id == org.id).all()
                for user in users:
                    print(f"  Email: {user.email}")
                    print(f"  Role: {user.role.value if hasattr(user.role, 'value') else user.role}")
                    print(f"  Note: Password is hashed in DB - check admin panel creation response")
                    print()
        else:
            print("No test organizations found.")
            
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        db.close()


if __name__ == "__main__":
    check_org_users()


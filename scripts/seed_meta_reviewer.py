#!/usr/bin/env python3
"""Seed an isolated demo org + login for Meta App Review (and personal use).

Idempotent: safe to run on every deploy. Creates:
- A dedicated "Meta Reviewer Demo" organization (not a real client org)
- A single OWNER user in that org, with onboarding gates pre-cleared so the
  reviewer lands straight on the dashboard instead of an onboarding flow.

Credentials come from env (META_REVIEWER_EMAIL / META_REVIEWER_PASSWORD),
falling back to a default email/password only for local dev convenience.
"""
import sys
import os
import uuid
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from sqlalchemy.orm import Session
from app.db.session import SessionLocal
from app.models.user import User, UserRole
from app.models.organization import Organization
from app.models.user_organization import UserOrganization
from app.core.security import get_password_hash

# Fixed UUID so re-running this script is idempotent.
REVIEWER_ORG_ID = uuid.UUID("00000000-0000-0000-0000-000000000002")
REVIEWER_EMAIL = os.environ.get("META_REVIEWER_EMAIL", "test@gmail.com")
REVIEWER_PASSWORD = os.environ.get("META_REVIEWER_PASSWORD", "password123")
REVIEWER_ORG_NAME = "Meta Reviewer Demo"


def seed_meta_reviewer():
    db: Session = SessionLocal()
    try:
        org = db.query(Organization).filter(Organization.id == REVIEWER_ORG_ID).first()
        if not org:
            org = Organization(id=REVIEWER_ORG_ID, name=REVIEWER_ORG_NAME)
            db.add(org)
            db.flush()
            print(f"Created org: {REVIEWER_ORG_NAME} ({REVIEWER_ORG_ID})")

        user = db.query(User).filter(User.email == REVIEWER_EMAIL).first()
        now = datetime.utcnow()
        if user:
            print(f"Reviewer user {REVIEWER_EMAIL} already exists, updating password/org/onboarding")
            user.hashed_password = get_password_hash(REVIEWER_PASSWORD)
            user.org_id = REVIEWER_ORG_ID
            user.role = UserRole.OWNER
            user.is_admin = False
        else:
            user = User(
                email=REVIEWER_EMAIL,
                hashed_password=get_password_hash(REVIEWER_PASSWORD),
                org_id=REVIEWER_ORG_ID,
                role=UserRole.OWNER,
                is_admin=False,
            )
            db.add(user)
            print(f"Created reviewer user: {REVIEWER_EMAIL}")

        # Pre-clear mandatory onboarding gates so the reviewer lands on the dashboard.
        user.onboarding_csa_completed_at = user.onboarding_csa_completed_at or now
        user.onboarding_intake_completed_at = user.onboarding_intake_completed_at or now
        user.onboarding_call_booked_at = user.onboarding_call_booked_at or now
        user.onboarding_tour_completed_at = user.onboarding_tour_completed_at or now
        db.flush()

        link = (
            db.query(UserOrganization)
            .filter(UserOrganization.user_id == user.id, UserOrganization.org_id == REVIEWER_ORG_ID)
            .first()
        )
        if not link:
            db.add(UserOrganization(user_id=user.id, org_id=REVIEWER_ORG_ID, is_primary=True))

        db.commit()
        print(f"Reviewer login ready: {REVIEWER_EMAIL} / org {REVIEWER_ORG_ID}")
    except Exception as e:
        db.rollback()
        print(f"Error seeding Meta reviewer account: {e}")
        raise
    finally:
        db.close()


if __name__ == "__main__":
    seed_meta_reviewer()

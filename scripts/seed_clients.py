#!/usr/bin/env python3
"""Seed script to create sample clients for demo"""
import sys
import os
from datetime import datetime, timedelta

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from sqlalchemy.orm import Session
from app.db.session import SessionLocal
from app.models.client import Client, LifecycleState


def seed_clients():
    db: Session = SessionLocal()
    try:
        # Check if clients already exist
        existing = db.query(Client).count()
        if existing > 0:
            print(f"{existing} clients already exist. Skipping seed.")
            return
        
        sample_clients = [
            Client(
                first_name="John",
                last_name="Doe",
                email="john.doe@example.com",
                phone="+1-555-0101",
                lifecycle_state=LifecycleState.COLD_LEAD,
                estimated_mrr=0.0,
                last_activity_at=datetime.utcnow() - timedelta(days=5)
            ),
            Client(
                first_name="Jane",
                last_name="Smith",
                email="jane.smith@example.com",
                phone="+1-555-0102",
                lifecycle_state=LifecycleState.WARM_LEAD,
                estimated_mrr=0.0,
                last_activity_at=datetime.utcnow() - timedelta(days=2)
            ),
            Client(
                first_name="Bob",
                last_name="Johnson",
                email="bob.johnson@example.com",
                phone="+1-555-0103",
                lifecycle_state=LifecycleState.ACTIVE,
                stripe_customer_id="cus_mock_001",
                estimated_mrr=99.0,
                last_activity_at=datetime.utcnow() - timedelta(hours=12)
            ),
            Client(
                first_name="Alice",
                last_name="Williams",
                email="alice.williams@example.com",
                phone="+1-555-0104",
                lifecycle_state=LifecycleState.ACTIVE,
                stripe_customer_id="cus_mock_002",
                estimated_mrr=199.0,
                last_activity_at=datetime.utcnow() - timedelta(hours=6)
            ),
            Client(
                first_name="Charlie",
                last_name="Brown",
                email="charlie.brown@example.com",
                phone="+1-555-0105",
                lifecycle_state=LifecycleState.OFFBOARDING,
                stripe_customer_id="cus_mock_003",
                estimated_mrr=49.0,
                last_activity_at=datetime.utcnow() - timedelta(days=30)
            ),
            Client(
                first_name="Diana",
                last_name="Davis",
                email="diana.davis@example.com",
                phone="+1-555-0106",
                lifecycle_state=LifecycleState.DEAD,
                estimated_mrr=0.0,
                last_activity_at=datetime.utcnow() - timedelta(days=90)
            ),
        ]
        
        for client in sample_clients:
            db.add(client)
        
        db.commit()
        print(f"Created {len(sample_clients)} sample clients")
    except Exception as e:
        db.rollback()
        print(f"Error creating sample clients: {e}")
        raise
    finally:
        db.close()


if __name__ == "__main__":
    seed_clients()


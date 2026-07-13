#!/usr/bin/env python3
"""
Smoke test for first-payment onboarding automation enqueue logic.

Run from repo root:
  cd backend && python scripts/test_first_payment_automation.py

Uses DATABASE_URL from the environment (or .env via app.core.config).
Creates ephemeral org/client/payment rows and rolls back at the end.
"""
from __future__ import annotations

import sys
import uuid
from datetime import datetime, timedelta

from sqlalchemy.orm import Session

from app.db.session import SessionLocal
from app.models.automation import AutomationEmailJob, AutomationRule, Playbook
from app.models.client import Client, LifecycleState
from app.models.organization import Organization
from app.models.stripe_payment import StripePayment
from app.services.automation_engine import on_payment_received, seed_default_rules


def _assert(cond: bool, msg: str) -> None:
    if not cond:
        raise AssertionError(msg)


def run() -> None:
    db: Session = SessionLocal()
    org_id = uuid.uuid4()
    client_id = uuid.uuid4()
    payment_id = f"ch_test_{uuid.uuid4().hex[:12]}"

    try:
        org = Organization(id=org_id, name=f"Automation Test {org_id.hex[:8]}")
        db.add(org)
        db.flush()
        client = Client(
            id=client_id,
            org_id=org_id,
            email=f"automation-test-{client_id.hex[:8]}@example.com",
            lifecycle_state=LifecycleState.BOOKED,
            lifetime_revenue_cents=0,
        )
        db.add(client)
        db.flush()

        seed_default_rules(db, org_id)
        rule = (
            db.query(AutomationRule)
            .filter(
                AutomationRule.org_id == org_id,
                AutomationRule.playbook == Playbook.FIRST_PAYMENT_ONBOARDING.value,
            )
            .first()
        )
        _assert(rule is not None, "seed_default_rules should create onboarding rule")
        rule.enabled = True
        db.flush()

        # Simulate webhook path: payment row committed, lifetime already incremented.
        payment = StripePayment(
            org_id=org_id,
            stripe_id=payment_id,
            client_id=client_id,
            amount_cents=10_000,
            currency="usd",
            status="succeeded",
            type="charge",
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow(),
        )
        db.add(payment)
        client.lifetime_revenue_cents = 10_000
        db.commit()

        job_ids = on_payment_received(
            db,
            org_id=org_id,
            client_id=client_id,
            payment_source="stripe",
            payment_external_id=payment_id,
            amount_cents=10_000,
            paid_at=datetime.utcnow(),
        )
        db.commit()

        _assert(len(job_ids) == 1, f"expected 1 onboarding job, got {job_ids}")
        job = db.query(AutomationEmailJob).filter(AutomationEmailJob.id == job_ids[0]).first()
        _assert(job is not None, "job row should exist")
        _assert(job.playbook == Playbook.FIRST_PAYMENT_ONBOARDING.value, "wrong playbook")
        _assert(job.client_id == client_id, "wrong client on job")

        db.refresh(client)
        _assert(
            client.lifecycle_state == LifecycleState.ACTIVE,
            f"client should be ACTIVE after first payment, got {client.lifecycle_state}",
        )

        # Idempotent replay should not create a second job.
        replay_ids = on_payment_received(
            db,
            org_id=org_id,
            client_id=client_id,
            payment_source="stripe",
            payment_external_id=payment_id,
            amount_cents=10_000,
            paid_at=datetime.utcnow(),
        )
        db.commit()
        _assert(replay_ids == [], f"replay should not enqueue duplicate jobs, got {replay_ids}")

        # Sync-then-webhook: lifetime already includes this payment amount.
        client2_id = uuid.uuid4()
        payment2_id = f"ch_test_{uuid.uuid4().hex[:12]}"
        client2 = Client(
            id=client2_id,
            org_id=org_id,
            email=f"automation-test-2-{client2_id.hex[:8]}@example.com",
            lifecycle_state=LifecycleState.QUALIFIED,
            lifetime_revenue_cents=10_000,
        )
        db.add(client2)
        db.add(
            StripePayment(
                org_id=org_id,
                stripe_id=payment2_id,
                client_id=client2_id,
                amount_cents=10_000,
                currency="usd",
                status="succeeded",
                type="charge",
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow(),
            )
        )
        db.commit()

        job_ids2 = on_payment_received(
            db,
            org_id=org_id,
            client_id=client2_id,
            payment_source="stripe",
            payment_external_id=payment2_id,
            amount_cents=10_000,
            paid_at=datetime.utcnow(),
        )
        db.commit()
        _assert(
            len(job_ids2) == 1,
            "sync-then-webhook lifetime (already includes payment) should still enqueue onboarding",
        )

        # Stale backfill should be rejected by recency gate.
        client3_id = uuid.uuid4()
        payment3_id = f"ch_test_{uuid.uuid4().hex[:12]}"
        client3 = Client(
            id=client3_id,
            org_id=org_id,
            email=f"automation-test-3-{client3_id.hex[:8]}@example.com",
            lifecycle_state=LifecycleState.BOOKED,
            lifetime_revenue_cents=5_000,
        )
        db.add(client3)
        db.add(
            StripePayment(
                org_id=org_id,
                stripe_id=payment3_id,
                client_id=client3_id,
                amount_cents=5_000,
                currency="usd",
                status="succeeded",
                type="charge",
                created_at=datetime.utcnow() - timedelta(days=3),
                updated_at=datetime.utcnow(),
            )
        )
        db.commit()

        stale_ids = on_payment_received(
            db,
            org_id=org_id,
            client_id=client3_id,
            payment_source="stripe",
            payment_external_id=payment3_id,
            amount_cents=5_000,
            paid_at=datetime.utcnow() - timedelta(days=3),
        )
        db.commit()
        _assert(stale_ids == [], f"stale payment should not enqueue, got {stale_ids}")

        print("✅ All first-payment automation tests passed.")
    except Exception:
        db.rollback()
        raise
    finally:
        db.rollback()
        db.close()


if __name__ == "__main__":
    try:
        run()
    except Exception as exc:
        print(f"❌ Test failed: {exc}", file=sys.stderr)
        raise

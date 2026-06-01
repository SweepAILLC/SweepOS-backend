"""Tests for pipeline lifecycle automation rules."""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta
from types import SimpleNamespace

from app.models.client import LifecycleState
from app.services.client_automation import (
    is_follow_up_expired,
    move_client_to_active_on_payment,
    update_booked_to_nurturing,
    update_expired_follow_ups_to_cold_lead,
)


def _client(*, lifecycle=LifecycleState.QUALIFIED, meta=None, created_at=None):
    return SimpleNamespace(
        id=uuid.uuid4(),
        org_id=uuid.uuid4(),
        email="lead@example.com",
        lifecycle_state=lifecycle,
        meta=meta or {},
        last_activity_at=None,
        created_at=created_at or datetime.utcnow(),
        updated_at=datetime.utcnow(),
    )


def test_move_to_active_from_booked_on_payment():
    client = _client(lifecycle=LifecycleState.BOOKED)
    assert move_client_to_active_on_payment(None, client) is True
    assert client.lifecycle_state == LifecycleState.ACTIVE


def test_move_to_active_skips_already_active():
    client = _client(lifecycle=LifecycleState.ACTIVE)
    assert move_client_to_active_on_payment(None, client) is False


def test_expired_follow_up_moves_qualified_to_cold():
    old = datetime.utcnow() - timedelta(days=20)
    client = _client(lifecycle=LifecycleState.QUALIFIED, created_at=old)
    assert is_follow_up_expired(client)
    assert update_expired_follow_ups_to_cold_lead(client) is True
    assert client.lifecycle_state == LifecycleState.COLD_LEAD


def test_booked_to_nurturing_requires_unclosed_past_sales_call():
    client = _client(lifecycle=LifecycleState.BOOKED)
    db = SimpleNamespace(flush=lambda: None)

    class Q:
        def filter(self, *args, **kwargs):
            return self

        def order_by(self, *args, **kwargs):
            return self

        def all(self):
            past = datetime.utcnow() - timedelta(hours=2)
            return [
                SimpleNamespace(
                    start_time=past,
                    end_time=past + timedelta(hours=1),
                    sale_closed=False,
                )
            ]

    db.query = lambda *args, **kwargs: Q()

    import app.services.client_automation as mod

    orig_sale = mod.client_has_recorded_sale
    mod.client_has_recorded_sale = lambda *a, **k: False
    try:
        assert update_booked_to_nurturing(db, client) is True
        assert client.lifecycle_state == LifecycleState.NURTURING
    finally:
        mod.client_has_recorded_sale = orig_sale

"""update_expired_follow_ups: qualified steps to nurturing (timer reset), nurturing/booked to cold_lead."""
from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import patch

from app.models.client import LifecycleState
from app.services.client_automation import (
    get_follow_up_due_at,
    is_follow_up_expired,
    update_expired_follow_ups,
)


def _client(state, *, days_since_activity=20, meta=None):
    now = datetime.utcnow()
    return SimpleNamespace(
        id="client-1",
        org_id="org-1",
        email="lead@example.com",
        lifecycle_state=state,
        last_activity_at=now - timedelta(days=days_since_activity),
        created_at=now - timedelta(days=60),
        updated_at=now,
        meta=meta or {},
    )


def _run(client, *, paid=False, upcoming=False):
    with patch(
        "app.services.client_automation.client_has_recorded_payment", return_value=paid
    ), patch(
        "app.services.client_automation._has_upcoming_sales_call", return_value=upcoming
    ):
        return update_expired_follow_ups(None, client)


def test_qualified_at_100_percent_moves_to_nurturing_and_resets_timer():
    client = _client("qualified")
    assert _run(client) is True
    assert client.lifecycle_state == LifecycleState.NURTURING
    assert "follow_up_anchor_at" in client.meta
    due = get_follow_up_due_at(client)
    assert due is not None and due > datetime.utcnow() + timedelta(days=13)
    # Fresh window: a second run must not cascade straight to cold_lead.
    assert is_follow_up_expired(client) is False
    assert _run(client) is False
    assert client.lifecycle_state == LifecycleState.NURTURING


def test_nurturing_at_100_percent_moves_to_cold_lead():
    client = _client("nurturing")
    assert _run(client) is True
    assert client.lifecycle_state == LifecycleState.COLD_LEAD


def test_booked_at_100_percent_still_moves_to_cold_lead():
    client = _client("booked")
    assert _run(client) is True
    assert client.lifecycle_state == LifecycleState.COLD_LEAD


def test_under_100_percent_does_not_move():
    client = _client("qualified", days_since_activity=5)
    assert _run(client) is False
    assert client.lifecycle_state == "qualified"


def test_paid_or_upcoming_call_blocks_move():
    assert _run(_client("qualified"), paid=True) is False
    assert _run(_client("qualified"), upcoming=True) is False


def test_other_columns_untouched():
    for state in ("cold_lead", "active", "offboarding", "dead"):
        assert _run(_client(state)) is False


def test_sweep_skips_manually_protected_and_not_due_cards():
    from unittest.mock import MagicMock

    from app.services.client_automation import sweep_expired_follow_ups_all_orgs

    due = _client("qualified")
    not_due = _client("qualified", days_since_activity=3)
    locked = _client(
        "nurturing",
        meta={"lifecycle_manual_at": datetime.utcnow().isoformat() + "Z"},
    )
    db = MagicMock()
    db.query.return_value.filter.return_value.all.return_value = [due, not_due, locked]
    with patch(
        "app.services.client_automation.client_has_recorded_payment", return_value=False
    ), patch(
        "app.services.client_automation._has_upcoming_sales_call", return_value=False
    ):
        assert sweep_expired_follow_ups_all_orgs(db) == 1
    assert due.lifecycle_state == LifecycleState.NURTURING
    assert not_due.lifecycle_state == "qualified"
    assert locked.lifecycle_state == "nurturing"
    db.commit.assert_called_once()

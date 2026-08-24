"""Fast tests for flow-test diagnostics, sales booking gate, and trigger wiring."""

from __future__ import annotations

import uuid
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from app.services.automation_engine import on_booking_created_pre_sale
from app.services.automation_flow_test import (
    assert_trigger_wiring,
    diagnose_flow,
    run_flow_test,
)


def test_trigger_call_sites_are_wired():
    missing = assert_trigger_wiring()
    assert missing == {}, f"Missing automation trigger imports: {missing}"


@patch("app.services.automation_engine._enqueue_flow_steps")
@patch("app.services.automation_engine._has_no_recorded_sale", return_value=True)
@patch("app.services.automation_engine._booking_matches_trigger_config", return_value=True)
@patch("app.services.automation_engine._list_trigger_steps")
def test_booking_trigger_skips_non_sales_calls(mock_list, _match, _no_sale, mock_enqueue):
    rule = SimpleNamespace(
        playbook="pre_sale_post_booking",
        trigger_config={"provider": "any", "match_all_events": True, "event_type_ids": []},
        step_index=0,
    )
    mock_list.return_value = [rule]
    client = SimpleNamespace(id=uuid.uuid4())
    db = MagicMock()
    db.query.return_value.filter.return_value.first.return_value = client

    with patch("app.services.checkin_sync.get_sales_call_flags", return_value=(False, None)) as flags:
        out = on_booking_created_pre_sale(
            db,
            org_id=uuid.uuid4(),
            client_id=client.id,
            provider="calcom",
            external_booking_id="evt_1",
            event_type_id="42",
        )
        flags.assert_called_once()
    assert out == []
    mock_enqueue.assert_not_called()


@patch("app.services.automation_engine._enqueue_flow_steps", return_value=[uuid.uuid4()])
@patch("app.services.automation_engine._has_no_recorded_sale", return_value=True)
@patch("app.services.automation_engine._booking_matches_trigger_config", return_value=True)
@patch("app.services.automation_engine._list_trigger_steps")
def test_booking_trigger_fires_for_sales_calls(mock_list, _match, _no_sale, mock_enqueue):
    rule = SimpleNamespace(
        playbook="pre_sale_post_booking",
        trigger_config={"provider": "any", "match_all_events": True, "event_type_ids": []},
        step_index=0,
    )
    mock_list.return_value = [rule]
    client = SimpleNamespace(id=uuid.uuid4())
    db = MagicMock()
    db.query.return_value.filter.return_value.first.return_value = client

    with patch("app.services.checkin_sync.get_sales_call_flags", return_value=(True, None)):
        out = on_booking_created_pre_sale(
            db,
            org_id=uuid.uuid4(),
            client_id=client.id,
            provider="calendly",
            external_booking_id="evt_sales",
            event_type_id="https://api.calendly.com/event_types/AAAA",
        )
    assert len(out) == 1
    mock_enqueue.assert_called_once()


@patch("app.services.automation_flow_test.seed_default_rules")
@patch("app.services.automation_flow_test.read_dispatcher_health")
@patch("app.services.automation_flow_test.get_brevo_auth_headers")
def test_diagnose_post_booking_flags_empty_trigger(mock_brevo, mock_health, _seed):
    mock_health.return_value = {"healthy": True, "queue_depth": 0}
    mock_brevo.return_value = {"api-key": "x"}

    rule = SimpleNamespace(
        enabled=True,
        node_kind="action",
        playbook="pre_sale_post_booking",
        require_approval=False,
        trigger_config={"provider": "any", "event_type_ids": [], "match_all_events": False},
        step_index=0,
        created_at=None,
    )
    db = MagicMock()
    db.query.return_value.filter.return_value.order_by.return_value.all.return_value = [rule]

    out = diagnose_flow(db, uuid.uuid4(), "post_booking")
    assert out["booking_trigger_ready"] is False
    assert out["ready_for_live_sends"] is False
    assert any("booking trigger" in b.lower() for b in out["blockers"])


@patch("app.services.automation_flow_test.seed_default_rules")
@patch("app.services.automation_flow_test.read_dispatcher_health")
@patch("app.services.automation_flow_test.get_brevo_auth_headers")
@patch("app.services.automation_flow_test.resolve_sender_for_org")
@patch("app.services.automation_flow_test.send_email")
@patch("app.services.automation_flow_test.diagnose_flow")
def test_run_flow_test_sends_enabled_actions_only(
    mock_diag, mock_send, mock_sender, mock_headers, mock_health, _seed
):
    mock_diag.return_value = {
        "flow": "onboarding",
        "dispatcher_healthy": True,
        "dispatcher": {},
        "brevo_connected": True,
        "brevo_note": None,
        "step_count": 2,
        "enabled_action_count": 1,
        "enabled_wait_count": 1,
        "enabled_playbooks": ["first_payment_onboarding"],
        "approval_gated_playbooks": [],
        "booking_trigger_ready": True,
        "booking_trigger_note": None,
        "blockers": [],
        "ready_for_live_sends": True,
    }
    mock_headers.return_value = {"api-key": "x"}
    mock_sender.return_value = {"email": "coach@example.com", "name": "Coach"}
    mock_send.return_value = {"messageId": "msg-1"}

    wait = SimpleNamespace(
        id=uuid.uuid4(),
        playbook="w1",
        enabled=True,
        node_kind="wait",
        trigger_kind="payment",
        step_index=0,
        subject_template=None,
        schedule_mode="after_previous",
        created_at=None,
    )
    action = SimpleNamespace(
        id=uuid.uuid4(),
        playbook="first_payment_onboarding",
        enabled=True,
        node_kind="action",
        trigger_kind="payment",
        step_index=1,
        subject_template="Welcome, {{first_name}}",
        schedule_mode="after_previous",
        created_at=None,
    )
    disabled = SimpleNamespace(
        id=uuid.uuid4(),
        playbook="off",
        enabled=False,
        node_kind="action",
        trigger_kind="payment",
        step_index=2,
        subject_template="Nope",
        schedule_mode="after_previous",
        created_at=None,
    )

    client = SimpleNamespace(
        id=uuid.uuid4(),
        first_name="Alex",
        last_name="Roe",
        email="alex@example.com",
    )

    db = MagicMock()
    db.query.return_value.filter.return_value.first.return_value = client
    db.query.return_value.filter.return_value.order_by.return_value.all.return_value = [
        wait,
        action,
        disabled,
    ]

    out = run_flow_test(
        db,
        org_id=uuid.uuid4(),
        flow="onboarding",
        email="test@example.com",
        client_id=client.id,
    )

    assert out["ok"] is True
    assert out["sent_count"] == 1
    assert mock_send.call_count == 1
    subject = mock_send.call_args.kwargs["subject"]
    assert subject.startswith("[TEST]")
    assert "Alex" in subject
    statuses = {r["playbook"]: r["status"] for r in out["results"]}
    assert statuses["w1"] == "skipped"
    assert statuses["first_payment_onboarding"] == "sent"
    assert statuses["off"] == "skipped"

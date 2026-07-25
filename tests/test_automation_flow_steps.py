"""Unit tests for automation flow scheduling, wait nodes, and worker skip logic."""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from app.models.automation import NodeKind, ScheduleMode
from app.schemas.automation import AutomationFlowStepCreate
from app.services.automation_dispatcher import _process_one
from app.services.automation_engine import (
    _enqueue_flow_steps,
    _enqueue_job,
    _schedule_for_step,
)


def _rule(**kwargs):
    defaults = dict(
        id=uuid.uuid4(),
        playbook="step_action",
        enabled=True,
        delay_seconds=0,
        schedule_mode=ScheduleMode.AFTER_PREVIOUS.value,
        step_index=0,
        node_kind=NodeKind.ACTION.value,
        audience_filter=None,
        require_approval=False,
    )
    defaults.update(kwargs)
    return SimpleNamespace(**defaults)


def _client():
    return SimpleNamespace(
        id=uuid.uuid4(),
        email="lead@example.com",
        lifecycle_state="booked",
        lifetime_revenue_cents=0,
    )


class TestScheduleForStep:
    def test_after_previous_chains_from_prev(self):
        now = datetime(2026, 7, 25, 12, 0, 0)
        prev = now + timedelta(hours=1)
        rule = _rule(delay_seconds=1800, schedule_mode=ScheduleMode.AFTER_PREVIOUS.value)
        assert _schedule_for_step(rule, now=now, prev_scheduled_at=prev) == prev + timedelta(
            seconds=1800
        )

    def test_after_trigger_ignores_prev(self):
        now = datetime(2026, 7, 25, 12, 0, 0)
        prev = now + timedelta(hours=5)
        rule = _rule(delay_seconds=600, schedule_mode=ScheduleMode.AFTER_TRIGGER.value)
        assert _schedule_for_step(rule, now=now, prev_scheduled_at=prev) == now + timedelta(
            seconds=600
        )

    def test_before_meeting_skips_when_missing_start(self):
        now = datetime(2026, 7, 25, 12, 0, 0)
        rule = _rule(delay_seconds=3600, schedule_mode=ScheduleMode.BEFORE_MEETING.value)
        assert _schedule_for_step(rule, now=now, prev_scheduled_at=now, meeting_start=None) is None

    def test_before_meeting_clamps_to_now_when_late(self):
        now = datetime(2026, 7, 25, 12, 0, 0)
        meeting = now + timedelta(minutes=30)
        rule = _rule(delay_seconds=3600, schedule_mode=ScheduleMode.BEFORE_MEETING.value)
        assert _schedule_for_step(rule, now=now, prev_scheduled_at=now, meeting_start=meeting) == now


class TestEnqueueFlowSteps:
    @patch("app.services.automation_engine.audience_filter_passes", return_value=True)
    @patch("app.services.automation_engine._enqueue_job")
    @patch("app.services.automation_engine._list_trigger_steps")
    @patch("app.services.automation_engine._now")
    def test_wait_alone_creates_no_jobs(self, mock_now, mock_list, mock_enqueue, _aud):
        now = datetime(2026, 7, 25, 12, 0, 0)
        mock_now.return_value = now
        mock_list.return_value = [
            _rule(
                playbook="w1",
                node_kind=NodeKind.WAIT.value,
                delay_seconds=3600,
                schedule_mode=ScheduleMode.AFTER_TRIGGER.value,
            )
        ]
        mock_enqueue.return_value = uuid.uuid4()

        created = _enqueue_flow_steps(
            MagicMock(),
            org_id=uuid.uuid4(),
            client_id=uuid.uuid4(),
            client=_client(),
            flow="post_booking",
            trigger_kind="booking",
            trigger_event="booking.created",
            discriminator_base="booking:abc",
            payload={},
        )

        assert created == []
        mock_enqueue.assert_not_called()

    @patch("app.services.automation_engine.audience_filter_passes", return_value=True)
    @patch("app.services.automation_engine._enqueue_job")
    @patch("app.services.automation_engine._list_trigger_steps")
    @patch("app.services.automation_engine._now")
    def test_action_wait_action_chains_delay(self, mock_now, mock_list, mock_enqueue, _aud):
        now = datetime(2026, 7, 25, 12, 0, 0)
        mock_now.return_value = now
        action1 = _rule(
            playbook="a1",
            node_kind=NodeKind.ACTION.value,
            delay_seconds=0,
            schedule_mode=ScheduleMode.AFTER_TRIGGER.value,
            step_index=0,
        )
        wait = _rule(
            playbook="w1",
            node_kind=NodeKind.WAIT.value,
            delay_seconds=3600,
            schedule_mode=ScheduleMode.AFTER_PREVIOUS.value,
            step_index=1,
        )
        action2 = _rule(
            playbook="a2",
            node_kind=NodeKind.ACTION.value,
            delay_seconds=600,
            schedule_mode=ScheduleMode.AFTER_PREVIOUS.value,
            step_index=2,
        )
        mock_list.return_value = [action1, wait, action2]
        id1, id2 = uuid.uuid4(), uuid.uuid4()
        mock_enqueue.side_effect = [id1, id2]

        created = _enqueue_flow_steps(
            MagicMock(),
            org_id=uuid.uuid4(),
            client_id=uuid.uuid4(),
            client=_client(),
            flow="onboarding",
            trigger_kind="payment",
            trigger_event="payment.received",
            discriminator_base="pay:xyz",
            payload={"payment_id": "ch_1"},
        )

        assert created == [id1, id2]
        assert mock_enqueue.call_count == 2
        first_at = mock_enqueue.call_args_list[0].kwargs["scheduled_at"]
        second_at = mock_enqueue.call_args_list[1].kwargs["scheduled_at"]
        assert first_at == now
        # wait 3600 after first, then action +600
        assert second_at == now + timedelta(seconds=3600 + 600)
        assert mock_enqueue.call_args_list[0].kwargs["rule"].playbook == "a1"
        assert mock_enqueue.call_args_list[1].kwargs["rule"].playbook == "a2"

    @patch("app.services.automation_engine.audience_filter_passes", return_value=True)
    @patch("app.services.automation_engine._enqueue_job")
    @patch("app.services.automation_engine._list_trigger_steps")
    @patch("app.services.automation_engine._now")
    def test_disabled_wait_does_not_shift_chain(self, mock_now, mock_list, mock_enqueue, _aud):
        now = datetime(2026, 7, 25, 12, 0, 0)
        mock_now.return_value = now
        wait = _rule(
            playbook="w1",
            enabled=False,
            node_kind=NodeKind.WAIT.value,
            delay_seconds=7200,
            schedule_mode=ScheduleMode.AFTER_PREVIOUS.value,
        )
        action = _rule(
            playbook="a1",
            node_kind=NodeKind.ACTION.value,
            delay_seconds=300,
            schedule_mode=ScheduleMode.AFTER_PREVIOUS.value,
        )
        mock_list.return_value = [wait, action]
        mock_enqueue.return_value = uuid.uuid4()

        _enqueue_flow_steps(
            MagicMock(),
            org_id=uuid.uuid4(),
            client_id=uuid.uuid4(),
            client=_client(),
            flow="onboarding",
            trigger_kind="payment",
            trigger_event="payment.received",
            discriminator_base="pay:1",
            payload={},
        )

        scheduled = mock_enqueue.call_args.kwargs["scheduled_at"]
        assert scheduled == now + timedelta(seconds=300)

    def test_enqueue_job_refuses_wait_nodes(self):
        rule = _rule(node_kind=NodeKind.WAIT.value, enabled=True)
        assert (
            _enqueue_job(
                MagicMock(),
                org_id=uuid.uuid4(),
                client_id=uuid.uuid4(),
                rule=rule,
                trigger_event="x",
                discriminator="d",
            )
            is None
        )


class TestDispatcherWaitDefense:
    def test_process_one_skips_wait_rule(self):
        job = SimpleNamespace(
            id=uuid.uuid4(),
            rule_id=uuid.uuid4(),
            client_id=uuid.uuid4(),
            org_id=uuid.uuid4(),
            state="sending",
            error_text=None,
            updated_at=None,
            payload_json={},
        )
        rule = _rule(id=job.rule_id, node_kind=NodeKind.WAIT.value, enabled=True)
        db = MagicMock()
        db.query.return_value.filter.return_value.first.return_value = rule

        _process_one(db, job)

        assert job.state == "skipped"
        assert "Wait node" in (job.error_text or "")
        db.commit.assert_called()


class TestAutomationFlowStepSchema:
    def test_accepts_wait_and_action(self):
        wait = AutomationFlowStepCreate(trigger_kind="booking", node_kind="wait")
        action = AutomationFlowStepCreate(trigger_kind="payment", node_kind="action")
        assert wait.node_kind == "wait"
        assert action.node_kind == "action"

    def test_rejects_invalid_node_kind(self):
        with pytest.raises(Exception):
            AutomationFlowStepCreate(trigger_kind="booking", node_kind="sms")

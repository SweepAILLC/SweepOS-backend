"""Tests for pipeline lifecycle promotion on upcoming sales calls."""
from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from app.models.client import LifecycleState
from app.services.client_automation import (
    process_pipeline_lifecycle_for_client,
    update_booked_to_nurturing,
    update_expired_follow_ups_to_cold_lead,
    update_to_booked_on_upcoming_sales_call,
)


def _client(*, lifecycle=LifecycleState.NURTURING, email="lead@example.com"):
    return SimpleNamespace(
        id=uuid.uuid4(),
        org_id=uuid.uuid4(),
        email=email,
        lifecycle_state=lifecycle,
        last_activity_at=None,
        meta={},
    )


@patch("app.services.client_automation._has_upcoming_sales_call", return_value=True)
def test_promotes_nurturing_to_booked_on_upcoming_sales_call(_mock_upcoming):
    client = _client(lifecycle=LifecycleState.NURTURING)
    db = MagicMock()
    assert update_to_booked_on_upcoming_sales_call(db, client) is True
    assert client.lifecycle_state == LifecycleState.BOOKED
    assert client.last_activity_at is not None


@patch("app.services.client_automation._has_upcoming_sales_call", return_value=True)
def test_skips_promotion_when_already_booked(_mock_upcoming):
    client = _client(lifecycle=LifecycleState.BOOKED)
    db = MagicMock()
    assert update_to_booked_on_upcoming_sales_call(db, client) is False


@patch("app.services.client_automation._has_upcoming_sales_call", return_value=False)
def test_no_promotion_without_upcoming_sales_call(_mock_upcoming):
    client = _client(lifecycle=LifecycleState.QUALIFIED)
    db = MagicMock()
    assert update_to_booked_on_upcoming_sales_call(db, client) is False
    assert client.lifecycle_state == LifecycleState.QUALIFIED


@patch("app.services.client_automation._has_upcoming_sales_call", return_value=True)
@patch("app.services.client_automation._has_unclosed_past_sales_call", return_value=True)
@patch("app.services.client_automation.client_has_recorded_sale", return_value=False)
def test_booked_stays_booked_when_another_sales_call_is_upcoming(
    _mock_sale, _mock_past, _mock_upcoming
):
    client = _client(lifecycle=LifecycleState.BOOKED)
    db = MagicMock()
    assert update_booked_to_nurturing(db, client) is False
    assert client.lifecycle_state == LifecycleState.BOOKED


@patch("app.services.client_automation._has_upcoming_sales_call", return_value=False)
@patch("app.services.client_automation._has_unclosed_past_sales_call", return_value=True)
@patch("app.services.client_automation.client_has_recorded_sale", return_value=False)
def test_booked_moves_to_nurturing_after_unclosed_past_call(_mock_sale, _mock_past, _mock_upcoming):
    client = _client(lifecycle=LifecycleState.BOOKED)
    db = MagicMock()
    assert update_booked_to_nurturing(db, client) is True
    assert client.lifecycle_state == LifecycleState.NURTURING


@patch("app.services.client_automation.update_expired_follow_ups_to_cold_lead", return_value=False)
@patch("app.services.client_automation.update_booked_to_nurturing", return_value=False)
@patch("app.services.client_automation.update_to_booked_on_upcoming_sales_call", return_value=True)
def test_process_pipeline_runs_booked_promotion_first(_promote, _nurture, _cold):
    client = _client(lifecycle=LifecycleState.COLD_LEAD)
    db = MagicMock()
    assert process_pipeline_lifecycle_for_client(db, client) is True
    _promote.assert_called_once_with(db, client)


@patch("app.services.client_automation._has_upcoming_sales_call", return_value=True)
@patch("app.services.client_automation.is_follow_up_expired", return_value=True)
def test_follow_up_expiry_skipped_when_sales_call_upcoming(_expired, _upcoming):
    client = _client(lifecycle=LifecycleState.BOOKED)
    db = MagicMock()
    assert update_expired_follow_ups_to_cold_lead(db, client) is False
    assert client.lifecycle_state == LifecycleState.BOOKED

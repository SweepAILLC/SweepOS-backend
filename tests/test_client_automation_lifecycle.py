"""apply_automatic_lifecycle_for_client: manual-lock only gates downgrade rules.

Payment->active and upcoming-call->booked are new external signals, not
automation re-litigating stale state, so neither should be blocked by an
operator's earlier drag-and-drop (the 14-day manual-lock window).
"""
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from app.services.client_automation import apply_automatic_lifecycle_for_client


def _client(**kwargs):
    defaults = dict(
        id="client-1",
        org_id="org-1",
        lifecycle_state="booked",
        program_start_date=None,
        program_duration_days=None,
    )
    defaults.update(kwargs)
    return SimpleNamespace(**defaults)


class TestPaymentBypassesManualLock:
    def test_payment_moves_to_active_even_when_manually_protected(self):
        client = _client(lifecycle_state="booked")
        with patch(
            "app.services.client_automation.client_has_recorded_payment", return_value=True
        ), patch(
            "app.services.client_automation.move_client_to_active_on_payment", return_value=True
        ) as move_active, patch(
            "app.services.client_automation.is_manual_lifecycle_protected", return_value=True
        ) as protected:
            result = apply_automatic_lifecycle_for_client(MagicMock(), client)
        assert result is True
        move_active.assert_called_once()
        protected.assert_not_called()


class TestBookedTransitionBypassesManualLock:
    def test_upcoming_call_moves_to_booked_even_when_manually_protected(self):
        client = _client(lifecycle_state="nurturing")
        with patch(
            "app.services.client_automation.client_has_recorded_payment", return_value=False
        ), patch(
            "app.services.client_automation.update_to_booked_on_upcoming_sales_call",
            return_value=True,
        ) as to_booked, patch(
            "app.services.client_automation.is_manual_lifecycle_protected", return_value=True
        ) as protected:
            result = apply_automatic_lifecycle_for_client(None, client)
        assert result is True
        to_booked.assert_called_once()
        protected.assert_not_called()


class TestDowngradeRulesStillRespectManualLock:
    def test_nurturing_revert_is_blocked_when_manually_protected(self):
        client = _client(lifecycle_state="booked")
        with patch(
            "app.services.client_automation.client_has_recorded_payment", return_value=False
        ), patch(
            "app.services.client_automation.update_to_booked_on_upcoming_sales_call",
            return_value=False,
        ), patch(
            "app.services.client_automation.is_manual_lifecycle_protected", return_value=True
        ) as protected, patch(
            "app.services.client_automation.update_booked_to_nurturing"
        ) as to_nurturing, patch(
            "app.services.client_automation.revert_booked_without_sales_call"
        ) as revert, patch(
            "app.services.client_automation.update_expired_follow_ups"
        ) as to_cold:
            result = apply_automatic_lifecycle_for_client(None, client)
        assert result is False
        protected.assert_called_once()
        to_nurturing.assert_not_called()
        revert.assert_not_called()
        to_cold.assert_not_called()

    def test_downgrade_rules_run_when_not_protected(self):
        client = _client(lifecycle_state="booked")
        with patch(
            "app.services.client_automation.client_has_recorded_payment", return_value=False
        ), patch(
            "app.services.client_automation.update_to_booked_on_upcoming_sales_call",
            return_value=False,
        ), patch(
            "app.services.client_automation.is_manual_lifecycle_protected", return_value=False
        ), patch(
            "app.services.client_automation.update_booked_to_nurturing", return_value=True
        ) as to_nurturing:
            result = apply_automatic_lifecycle_for_client(None, client)
        assert result is True
        to_nurturing.assert_called_once()


class TestForceBypassesManualLockExplicitly:
    def test_force_true_runs_downgrade_rules_even_when_protected(self):
        client = _client(lifecycle_state="booked")
        with patch(
            "app.services.client_automation.client_has_recorded_payment", return_value=False
        ), patch(
            "app.services.client_automation.update_to_booked_on_upcoming_sales_call",
            return_value=False,
        ), patch(
            "app.services.client_automation.is_manual_lifecycle_protected", return_value=True
        ) as protected, patch(
            "app.services.client_automation.update_booked_to_nurturing", return_value=True
        ) as to_nurturing:
            result = apply_automatic_lifecycle_for_client(None, client, force=True)
        assert result is True
        protected.assert_not_called()
        to_nurturing.assert_called_once()

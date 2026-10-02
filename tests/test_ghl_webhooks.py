"""GHL webhook secret verification + early-exit skip paths (mock db, mirrors the
existing style in test_inbound_webhook_reliability.py / calendar_webhooks skip tests)."""
import uuid
from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException

from app.api.ghl_webhooks import _ingest_and_process_ghl, _verify_ghl_shared_secret


class TestVerifyGhlSharedSecret:
    def test_accepts_matching_secret(self):
        _verify_ghl_shared_secret("super-secret", "super-secret")  # no raise

    def test_accepts_when_unset_local_dev_posture(self):
        _verify_ghl_shared_secret(None, "")  # no raise, warn-and-accept

    def test_rejects_mismatched_secret(self):
        with pytest.raises(HTTPException) as exc:
            _verify_ghl_shared_secret("super-secret", "wrong-value")
        assert exc.value.status_code == 403

    def test_rejects_missing_header_when_secret_configured(self):
        with pytest.raises(HTTPException) as exc:
            _verify_ghl_shared_secret("super-secret", "")
        assert exc.value.status_code == 403


class TestIngestAndProcessGhl:
    def test_unrecognized_payload_skipped_without_db_access(self):
        db = MagicMock()
        out = _ingest_and_process_ghl(db, uuid.uuid4(), {"foo": "bar"})
        assert out == {"ok": True, "skipped": True, "reason": "unrecognized_payload"}
        db.query.assert_not_called()

    def test_disabled_calendar_skipped_before_dedup_inbox(self):
        db = MagicMock()
        db.query.return_value.filter.return_value.first.return_value = None
        payload = {
            "type": "AppointmentCreate",
            "appointment": {"id": "appt_1", "calendarId": "cal_not_enabled"},
        }
        out = _ingest_and_process_ghl(db, uuid.uuid4(), payload)
        assert out == {"ok": True, "skipped": True, "reason": "calendar_not_enabled"}
        db.add.assert_not_called()

    def test_already_processed_event_is_deduped(self):
        db = MagicMock()
        setting_row = MagicMock()
        inbox_row = MagicMock(status="done")

        # query().filter().first() resolves the enabled calendar setting; the
        # inbound-webhook-inbox lookup is patched separately below.
        db.query.return_value.filter.return_value.first.return_value = setting_row
        payload = {
            "type": "AppointmentCreate",
            "appointment": {"id": "appt_1", "calendarId": "cal_1"},
        }
        with patch(
            "app.services.inbound_webhook_inbox.record_inbound_event",
            return_value=(inbox_row, False),
        ):
            out = _ingest_and_process_ghl(db, uuid.uuid4(), payload)
        assert out == {"ok": True, "is_new": False, "fired_jobs": [], "deduped": True}


class TestInboxRetry:
    """A failed GHL delivery was recorded under provider "ghl" but the worker's inbox
    flush had no processor for it, so every retry burned an attempt as an unknown
    provider and the booking was never written."""

    def test_flush_routes_ghl_rows_to_the_ghl_processor(self):
        from types import SimpleNamespace

        from app.api.ghl_webhooks import process_ghl_webhook_payload
        from app.services import inbound_webhook_inbox as inbox

        row = SimpleNamespace(provider="ghl")
        with patch.object(inbox, "claim_due_inbound_events", return_value=[row]), patch.object(
            inbox, "process_recorded_event"
        ) as proc, patch.object(inbox, "mark_inbound_retry") as retry:
            inbox.flush_due_inbound_webhooks(MagicMock())
        retry.assert_not_called()
        assert proc.call_args.args[2] is process_ghl_webhook_payload

    def test_retry_processes_enabled_calendar_event(self):
        from app.api import ghl_webhooks as gw

        setting = MagicMock()
        db = MagicMock()
        db.query.return_value.filter.return_value.first.return_value = setting
        payload = {"appointment": {"id": "appt_1", "calendarId": "cal_1"}}
        with patch.object(gw, "_process_ghl_appointment_event") as process:
            gw.process_ghl_webhook_payload(db, uuid.uuid4(), payload)
        assert process.call_args.args[3] is setting

    def test_retry_skips_disabled_calendar_and_junk(self):
        from app.api import ghl_webhooks as gw

        db = MagicMock()
        db.query.return_value.filter.return_value.first.return_value = None
        with patch.object(gw, "_process_ghl_appointment_event") as process:
            gw.process_ghl_webhook_payload(db, uuid.uuid4(), {"appointment": {"id": "a", "calendarId": "c"}})
            gw.process_ghl_webhook_payload(db, uuid.uuid4(), {"nothing": True})
        process.assert_not_called()

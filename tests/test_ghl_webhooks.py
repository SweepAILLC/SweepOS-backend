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

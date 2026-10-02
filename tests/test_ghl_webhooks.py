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


# --- GHL-7: real-time opt-in webhook --------------------------------------------------

from datetime import datetime, timezone  # noqa: E402
from types import SimpleNamespace  # noqa: E402

from fastapi import FastAPI  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

from app.api import ghl_webhooks as gw  # noqa: E402
from app.db.session import get_db  # noqa: E402
from app.models.funnel import Funnel  # noqa: E402
from app.services.ghl_client import normalize_ghl_workflow_opt_in  # noqa: E402

RECEIVED = datetime(2026, 10, 2, 15, 0, tzinfo=timezone.utc)


def _opt_in_body(funnel_id, **extra):
    return {
        "contact_id": "ct_1",
        "first_name": "Ada",
        "last_name": "Lovelace",
        "email": "ada@example.com",
        "phone": "+15550100001",
        "customData": {
            "sweep_event": "opt_in",
            "sweep_funnel_id": str(funnel_id),
            "page_url": "https://go.example.com/vsl-optin?utm_source=facebook&utm_campaign=sept",
            "monthly_revenue": "$10k-$25k",
        },
        **extra,
    }


class TestNormalizeWorkflowOptIn:
    def test_fields_answers_and_utm(self):
        out = normalize_ghl_workflow_opt_in(_opt_in_body(uuid.uuid4()), received_at=RECEIVED)
        assert (out["contact_id"], out["email"], out["first_name"]) == ("ct_1", "ada@example.com", "Ada")
        assert out["page_path"] == "/vsl-optin"
        assert out["utm_raw"] == {"source": "facebook", "campaign": "sept"}
        assert out["answers"] == {"monthly_revenue": "$10k-$25k"}  # sweep_* keys and page_url dropped
        assert out["created_at"] == RECEIVED

    def test_nested_contact_and_attribution_fallback(self):
        body = {
            "contact": {
                "id": "ct_2",
                "email": "g@example.com",
                "firstName": "Grace",
                "attributionSource": {"utmSource": "google", "utmMedium": "cpc", "campaign": "brand"},
            },
            "customData": {"sweep_event": "opt_in"},
        }
        out = normalize_ghl_workflow_opt_in(body, received_at=RECEIVED)
        assert (out["contact_id"], out["email"], out["first_name"]) == ("ct_2", "g@example.com", "Grace")
        assert out["utm_raw"] == {"source": "google", "medium": "cpc", "campaign": "brand"}


def _client_app(secret=None):
    app = FastAPI()
    app.include_router(gw.router, prefix="/webhooks")
    app.dependency_overrides[get_db] = lambda: MagicMock()
    return app


class TestOptInRoute:
    def _post(self, body, *, secret=None, header=None, env="production"):
        org = uuid.uuid4()
        headers = {"x-ghl-webhook-secret": header} if header else {}
        with patch("app.services.ghl_client.resolve_ghl_webhook_secret", return_value=secret), patch.object(
            gw.settings, "ENVIRONMENT", env
        ), patch.object(gw, "_ingest_ghl_opt_in", return_value={"ok": True}) as ingest, patch.object(
            gw, "_ingest_and_process_ghl", return_value={"ok": True, "appointment": True}
        ) as appt:
            r = TestClient(_client_app()).post(f"/webhooks/ghl/{org}", json=body, headers=headers)
        return r, ingest, appt

    def test_opt_in_without_secret_is_refused_outside_dev(self):
        r, ingest, _ = self._post(_opt_in_body(uuid.uuid4()))
        assert r.status_code == 403
        ingest.assert_not_called()

    def test_opt_in_without_secret_is_accepted_in_dev(self):
        r, ingest, _ = self._post(_opt_in_body(uuid.uuid4()), env="development")
        assert r.status_code == 200
        ingest.assert_called_once()

    def test_opt_in_with_matching_secret(self):
        r, ingest, appt = self._post(_opt_in_body(uuid.uuid4()), secret="s3cret", header="s3cret")
        assert r.status_code == 200
        ingest.assert_called_once()
        appt.assert_not_called()

    def test_wrong_secret_is_403(self):
        r, ingest, _ = self._post(_opt_in_body(uuid.uuid4()), secret="s3cret", header="nope")
        assert r.status_code == 403
        ingest.assert_not_called()

    def test_unsupported_sweep_event_is_acked(self):
        body = {"customData": {"sweep_event": "payment"}}
        r, ingest, appt = self._post(body, secret="s", header="s")
        assert r.json()["reason"] == "unsupported_sweep_event"
        ingest.assert_not_called()
        appt.assert_not_called()

    def test_no_sweep_event_keeps_appointment_path(self):
        r, ingest, appt = self._post({"appointment": {"id": "a", "calendarId": "c"}})
        assert r.json() == {"ok": True, "appointment": True}
        ingest.assert_not_called()


class TestIngestOptIn:
    def test_funnel_from_another_org_is_404(self):
        db = MagicMock()
        db.query.return_value.filter.return_value.first.return_value = None
        with pytest.raises(HTTPException) as exc:
            gw._ingest_ghl_opt_in(db, uuid.uuid4(), _opt_in_body(uuid.uuid4()))
        assert exc.value.status_code == 404

    def test_funnel_lookup_is_scoped_to_url_org_and_ghl_source(self):
        db = MagicMock()
        org = uuid.uuid4()
        gw._load_paired_funnel(db, org, str(uuid.uuid4()))
        clauses = [str(c) for c in db.query.return_value.filter.call_args.args]
        assert any("org_id" in c for c in clauses) and any("source" in c for c in clauses)
        assert gw._load_paired_funnel(db, org, "not-a-uuid") is None

    def _ingest(self, *, status="pending"):
        funnel = Funnel(id=uuid.uuid4(), org_id=uuid.uuid4(), name="f", source="ghl", ghl_config={})
        db = MagicMock()
        db.query.return_value.filter.return_value.first.return_value = funnel
        row = SimpleNamespace(status=status, payload=None)

        def record(db, **kw):
            row.payload = kw["payload"]
            row.event_id = kw["event_id"]
            return row, True

        with patch("app.services.inbound_webhook_inbox.record_inbound_event", side_effect=record), patch(
            "app.services.inbound_webhook_inbox.mark_inbound_done"
        ) as done, patch.object(gw, "_process_ghl_opt_in") as process:
            out = gw._ingest_ghl_opt_in(db, funnel.org_id, _opt_in_body(funnel.id))
        return out, row, done, process, funnel

    def test_records_then_processes(self):
        out, row, done, process, funnel = self._ingest()
        assert out == {"ok": True}
        assert row.event_id == f"optin:{funnel.id}:ct_1"
        assert gw.RECEIVED_AT_KEY in row.payload
        process.assert_called_once()
        done.assert_called_once()

    def test_duplicate_delivery_is_deduped(self):
        out, _, done, process, _ = self._ingest(status="done")
        assert out == {"ok": True, "deduped": True}
        process.assert_not_called()

    def test_process_tags_with_original_receipt_time_and_records_health(self):
        funnel = Funnel(id=uuid.uuid4(), org_id=uuid.uuid4(), name="f", source="ghl", ghl_config={"steps": []})
        db = MagicMock()
        db.query.return_value.filter.return_value.first.return_value = funnel
        body = {**_opt_in_body(funnel.id), gw.RECEIVED_AT_KEY: RECEIVED.isoformat()}
        with patch("app.services.ghl_lead_sync.process_submission_payload") as proc:
            gw._process_ghl_opt_in(db, funnel.org_id, body)
        payload = proc.call_args.args[2]
        assert payload["notify"] is True and payload["funnel_id"] == str(funnel.id)
        assert payload["submission"]["created_at"] == RECEIVED.isoformat()
        assert funnel.ghl_config["webhook"]["last_received_at"] == RECEIVED.isoformat()

    def test_inbox_retry_routes_opt_ins(self):
        body = _opt_in_body(uuid.uuid4())
        with patch.object(gw, "_process_ghl_opt_in") as opt_in, patch.object(gw, "_process_ghl_appointment_event") as appt:
            gw.process_ghl_webhook_payload(MagicMock(), uuid.uuid4(), body)
        opt_in.assert_called_once()
        appt.assert_not_called()


class TestWebhookSecretEndpoint:
    def test_rotate_returns_secret_once_with_no_store(self):
        from fastapi import Response

        from app.api import ghl as api

        user = SimpleNamespace(id=uuid.uuid4(), org_id=uuid.uuid4(), selected_org_id=None)
        resp = Response()
        with patch.object(api.gc, "set_ghl_webhook_secret", return_value=object()) as store:
            out = api.rotate_webhook_secret(resp, MagicMock(), user)
        assert len(out.secret) >= 40 and out.header == "x-ghl-webhook-secret"
        assert store.call_args.args[2] == out.secret
        assert resp.headers["cache-control"] == "no-store"

    def test_rotate_requires_connection(self):
        from fastapi import Response

        from app.api import ghl as api

        user = SimpleNamespace(id=uuid.uuid4(), org_id=uuid.uuid4(), selected_org_id=None)
        with patch.object(api.gc, "set_ghl_webhook_secret", return_value=None):
            with pytest.raises(HTTPException) as exc:
                api.rotate_webhook_secret(Response(), MagicMock(), user)
        assert exc.value.status_code == 400

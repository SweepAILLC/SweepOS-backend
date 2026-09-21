"""Signatures, inbox retry backoff, calendar destination URLs, Discord claim."""
from __future__ import annotations

import hashlib
import hmac
import time
import uuid
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

from app.api.calendar_webhooks import verify_calcom_signature, verify_calendly_signature
from app.services.calendar_webhook_onboard import calendar_webhook_destination
from app.services.inbound_webhook_inbox import _backoff_seconds
from app.services.integration_side_effects import (
    ACTION_DISCORD_BOOKING,
    emit_new_booking_discord,
    emit_new_payment_discord,
)


def test_calendly_signature_t_v1_accepted():
    secret = "signing-key-example"
    body = b'{"event":"invitee.created"}'
    ts = str(int(time.time()))
    signed = f"{ts}.".encode("utf-8") + body
    digest = hmac.new(secret.encode(), signed, hashlib.sha256).hexdigest()
    header = f"t={ts},v1={digest}"
    assert verify_calendly_signature(secret, header, body) is True


def test_calendly_signature_tampered_rejected():
    secret = "signing-key-example"
    body = b'{"event":"invitee.created"}'
    ts = str(int(time.time()))
    signed = f"{ts}.".encode("utf-8") + body
    digest = hmac.new(secret.encode(), signed, hashlib.sha256).hexdigest()
    header = f"t={ts},v1={digest}"
    assert verify_calendly_signature(secret, header, b'{"event":"nope"}') is False


def test_calendly_signature_legacy_raw_body_still_accepted():
    secret = "legacy-secret"
    body = b'{"event":"invitee.created"}'
    digest = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    assert verify_calendly_signature(secret, digest, body) is True


def test_calcom_signature_hex_of_raw_body():
    secret = "cal-secret"
    body = b'{"triggerEvent":"BOOKING_CREATED"}'
    digest = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    assert verify_calcom_signature(secret, digest, body) is True
    assert verify_calcom_signature(secret, digest, b"{}") is False


def test_empty_secret_accepts_for_local_dev():
    assert verify_calcom_signature(None, "", b"{}") is True
    assert verify_calendly_signature("", "", b"{}") is True


def test_inbox_backoff_grows_and_caps():
    assert _backoff_seconds(1) == 30
    assert _backoff_seconds(2) == 90
    assert _backoff_seconds(8) == 2 * 60 * 60


def test_calendar_destination_uses_public_url():
    org_id = uuid.UUID("11111111-1111-1111-1111-111111111111")
    with patch("app.services.calendar_webhook_onboard.settings") as settings:
        settings.BACKEND_PUBLIC_URL = "https://api.example.com/"
        assert calendar_webhook_destination(org_id, "calcom") == (
            f"https://api.example.com/webhooks/calcom/{org_id}"
        )
        assert calendar_webhook_destination(org_id, "calendly") == (
            f"https://api.example.com/webhooks/calendly/{org_id}"
        )


def test_calendar_destination_none_without_public_url():
    org_id = uuid.uuid4()
    with patch("app.services.calendar_webhook_onboard.settings") as settings:
        settings.BACKEND_PUBLIC_URL = None
        assert calendar_webhook_destination(org_id, "calcom") is None


def test_emit_booking_skips_historical_when_require_recent():
    db = MagicMock()
    old = datetime.now(timezone.utc) - timedelta(days=14)
    sent = emit_new_booking_discord(
        db,
        org_id=uuid.uuid4(),
        provider="calcom",
        event_id="old-booking",
        start_time=old,
        require_recent=True,
    )
    assert sent is False
    db.add.assert_not_called()


def test_emit_booking_claims_once():
    db = MagicMock()
    db.commit.side_effect = [None]
    org = uuid.uuid4()
    with patch("app.services.discord_notify.send_discord_event_background") as send, patch(
        "app.services.discord_notify.format_org_local_datetime", return_value="19/09/2026 12:00 UTC"
    ):
        ok = emit_new_booking_discord(
            db,
            org_id=org,
            provider="calendly",
            event_id="evt_live",
            attendee_name="Ada",
            attendee_email="ada@example.com",
            event_type_label="Strategy Call",
            start_time=datetime.now(timezone.utc),
        )
    assert ok is True
    send.assert_called_once()
    added = db.add.call_args[0][0]
    assert added.action == ACTION_DISCORD_BOOKING
    assert added.source_id == "evt_live"


def test_emit_payment_second_claim_skipped():
    from sqlalchemy.exc import IntegrityError

    db = MagicMock()
    db.commit.side_effect = IntegrityError("dup", None, None)
    with patch("app.services.discord_notify.send_discord_event_background") as send:
        ok = emit_new_payment_discord(
            db,
            org_id=uuid.uuid4(),
            source="stripe",
            payment_id="ch_1",
            amount_cents=49700,
        )
    assert ok is False
    send.assert_not_called()


def test_calcom_payload_skips_unknown_trigger():
    from app.api.calendar_webhooks import process_calcom_webhook_payload

    db = MagicMock()
    out = process_calcom_webhook_payload(db, uuid.uuid4(), {"triggerEvent": "PING"})
    assert out["skipped"] is True
    db.add.assert_not_called()


def test_calendly_payload_skips_unknown_kind():
    from app.api.calendar_webhooks import process_calendly_webhook_payload

    db = MagicMock()
    out = process_calendly_webhook_payload(db, uuid.uuid4(), {"event": "invitee.updated"})
    assert out["skipped"] is True
    db.add.assert_not_called()

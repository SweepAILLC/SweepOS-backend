"""Whop webhook signature + payload unwrap."""
import base64
import hashlib
import hmac
import time

from app.services.whop_webhook import payment_item_from_webhook_payload, verify_whop_webhook_signature


def _sign_whsec(secret_raw: bytes, webhook_id: str, webhook_ts: str, body: bytes) -> tuple[str, str]:
    secret = "whsec_" + base64.b64encode(secret_raw).decode()
    signed = f"{webhook_id}.{webhook_ts}.{body.decode('utf-8')}"
    sig = base64.b64encode(hmac.new(secret_raw, signed.encode("utf-8"), hashlib.sha256).digest()).decode()
    return secret, f"v1,{sig}"


def _sign_ws_raw(secret: str, webhook_id: str, webhook_ts: str, body: bytes) -> str:
    signed = f"{webhook_id}.{webhook_ts}.{body.decode('utf-8')}".encode("utf-8")
    sig = base64.b64encode(hmac.new(secret.encode("utf-8"), signed, hashlib.sha256).digest()).decode()
    return f"v1,{sig}"


class TestWhopWebhookSignature:
    def test_whsec_accepted(self):
        body = b'{"type":"payment.succeeded","data":{"id":"pay_1"}}'
        webhook_id = "msg_1"
        webhook_ts = str(int(time.time()))
        secret, signature = _sign_whsec(b"super-secret-key-bytes!!", webhook_id, webhook_ts, body)
        headers = {
            "webhook-id": webhook_id,
            "webhook-timestamp": webhook_ts,
            "webhook-signature": signature,
        }
        assert verify_whop_webhook_signature(secret, headers, body) is True

    def test_ws_raw_secret_accepted(self):
        body = b'{"type":"payment.succeeded"}'
        webhook_id = "msg_ws"
        webhook_ts = str(int(time.time()))
        secret = "ws_testsecretvalue"
        signature = _sign_ws_raw(secret, webhook_id, webhook_ts, body)
        headers = {
            "webhook-id": webhook_id,
            "webhook-timestamp": webhook_ts,
            "webhook-signature": signature,
        }
        assert verify_whop_webhook_signature(secret, headers, body) is True

    def test_tampered_body_rejected(self):
        body = b'{"type":"payment.succeeded"}'
        webhook_id = "msg_2"
        webhook_ts = str(int(time.time()))
        secret, signature = _sign_whsec(b"key123456789012345678901234", webhook_id, webhook_ts, body)
        headers = {
            "webhook-id": webhook_id,
            "webhook-timestamp": webhook_ts,
            "webhook-signature": signature,
        }
        assert verify_whop_webhook_signature(secret, headers, b'{"type":"nope"}') is False

    def test_stale_timestamp_rejected(self):
        body = b"{}"
        webhook_id = "msg_old"
        webhook_ts = str(int(time.time()) - 600)
        secret, signature = _sign_whsec(b"key123456789012345678901234", webhook_id, webhook_ts, body)
        headers = {
            "webhook-id": webhook_id,
            "webhook-timestamp": webhook_ts,
            "webhook-signature": signature,
        }
        assert verify_whop_webhook_signature(secret, headers, body) is False


class TestWhopPayerFields:
    def test_payer_name_from_user(self):
        from app.services.whop_sync import _payer_name

        assert _payer_name({"user": {"name": "Ada Lovelace"}}) == "Ada Lovelace"

    def test_payer_email_official_user_field(self):
        from app.services.whop_sync import _payer_email

        assert _payer_email({"user": {"email": "john.doe@example.com"}}) == "john.doe@example.com"

    def test_payer_email_nested(self):
        from app.services.whop_sync import _payer_email

        assert _payer_email({"member": {"user": {"email": "ada@example.com"}}}) == "ada@example.com"

    def test_payer_email_ignores_username(self):
        from app.services.whop_sync import _payer_email

        assert _payer_email({"user": {"username": "johndoe42", "email": None}}) is None

    def test_apply_identity_fills_missing_email_and_name(self):
        from types import SimpleNamespace
        from app.services.whop_sync import apply_whop_identity_to_client

        client = SimpleNamespace(email=None, emails=None, first_name=None, last_name=None, updated_at=None)
        apply_whop_identity_to_client(client, "buyer@example.com", "Kylo Ruhe")
        assert client.email == "buyer@example.com"
        assert client.first_name == "Kylo"
        assert client.last_name == "Ruhe"

    def test_apply_identity_keeps_existing_name_adds_extra_email(self):
        from types import SimpleNamespace
        from app.services.whop_sync import apply_whop_identity_to_client

        client = SimpleNamespace(
            email="old@example.com",
            emails=[],
            first_name="Kylo",
            last_name="Ruhe",
            updated_at=None,
        )
        apply_whop_identity_to_client(client, "buyer@example.com", "Other Name")
        assert client.email == "old@example.com"
        assert client.emails == ["buyer@example.com"]
        assert client.first_name == "Kylo"
        assert client.last_name == "Ruhe"

    def test_split_display_name(self):
        from app.services.whop_sync import _split_display_name

        assert _split_display_name("Kylo Ruhe") == ("Kylo", "Ruhe")
        assert _split_display_name("Kylo") == ("Kylo", None)


class TestWhopAmountMapping:
    def test_settlement_amount_is_customer_charge(self):
        from app.services.whop_sync import _amount_cents, _payment_currency

        item = {
            "settlement_amount": 49.0,
            "settlement_currency": "usd",
            "total": 45.0,
            "usd_total": 45.0,
            "currency": "usd",
        }
        assert _amount_cents(item) == 4900
        assert _payment_currency(item) == "usd"

    def test_docs_example_decimal_dollars(self):
        from app.services.whop_sync import _amount_cents

        assert _amount_cents({"usd_total": 6.9, "currency": "usd"}) == 690

    def test_live_api_money_object(self):
        from app.services.whop_sync import _amount_cents

        item = {
            "currency": "usd",
            "total": {"currency": "usd", "amount": "1295.82", "decimals": 2},
            "usd_total": {"currency": "usd", "amount": "1295.82", "decimals": 2},
        }
        assert _amount_cents(item) == 129582

    def test_customer_email_top_level(self):
        from app.services.whop_sync import _payer_email

        assert _payer_email({"customer_email": "buyer@example.com", "user": {"id": "user_1"}}) == "buyer@example.com"

    def test_zero_settlement_falls_through_to_total(self):
        from app.services.whop_sync import _amount_cents

        assert _amount_cents({"settlement_amount": 0, "total": 49.0, "currency": "usd"}) == 4900

    def test_jpy_is_zero_decimal(self):
        from app.services.whop_sync import _amount_cents, _payment_currency

        item = {"settlement_amount": 1500, "currency": "jpy", "settlement_currency": "jpy"}
        assert _payment_currency(item) == "jpy"
        assert _amount_cents(item) == 1500

    def test_paid_at_unix_timestamp(self):
        from app.services.whop_sync import _parse_created_at

        dt = _parse_created_at({"paid_at": 1701406800})
        assert dt.year == 2023

    def test_hydrate_skips_complete_payload(self):
        from app.services.whop_sync import payment_needs_hydrate

        item = {
            "id": "pay_1",
            "settlement_amount": 10,
            "currency": "usd",
            "customer_email": "a@b.com",
            "user": {"email": "a@b.com", "id": "user_1", "name": "Ada Lovelace"},
        }
        assert payment_needs_hydrate(item) is False

    def test_hydrate_needed_when_email_missing(self):
        from app.services.whop_sync import payment_needs_hydrate

        assert payment_needs_hydrate({"id": "pay_1", "usd_total": 10, "user": {"id": "user_1"}}) is True


class TestWhopWebhookPayload:
    def test_payment_succeeded_data(self):
        item = payment_item_from_webhook_payload(
            {"type": "payment.succeeded", "data": {"id": "pay_abc", "status": "paid", "usd_total": 10}}
        )
        assert item and item["id"] == "pay_abc"

    def test_refund_nested_payment(self):
        item = payment_item_from_webhook_payload(
            {"type": "refund.created", "data": {"id": "ref_1", "payment": {"id": "pay_xyz", "status": "refunded"}}}
        )
        assert item and item["id"] == "pay_xyz"

    def test_unrelated_ignored(self):
        assert payment_item_from_webhook_payload({"type": "membership.activated", "data": {"id": "mem_1"}}) is None

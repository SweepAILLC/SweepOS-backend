"""Security tests for Fathom webhook signature verification."""
import base64
import hashlib
import hmac
import time

from app.api.integrations import _verify_fathom_webhook_signature


def _sign(secret_raw: bytes, webhook_id: str, webhook_ts: str, body: bytes) -> str:
    secret = "whsec_" + base64.b64encode(secret_raw).decode()
    signed = f"{webhook_id}.{webhook_ts}.{body.decode('utf-8')}"
    sec_b64 = secret.split("_", 1)[1]
    sec_bytes = base64.b64decode(sec_b64)
    sig = base64.b64encode(
        hmac.new(sec_bytes, signed.encode("utf-8"), hashlib.sha256).digest()
    ).decode("utf-8")
    return secret, f"v1,{sig}"


class TestFathomWebhookSignature:
    def test_valid_signature_accepted(self):
        body = b'{"recording_id": 42, "title": "Sales call"}'
        webhook_id = "msg_test_1"
        webhook_ts = str(int(time.time()))
        secret, signature = _sign(b"super-secret-key-bytes!!", webhook_id, webhook_ts, body)
        headers = {
            "webhook-id": webhook_id,
            "webhook-timestamp": webhook_ts,
            "webhook-signature": signature,
        }
        assert _verify_fathom_webhook_signature(secret, headers, body) is True

    def test_tampered_body_rejected(self):
        body = b'{"recording_id": 42}'
        webhook_id = "msg_test_2"
        webhook_ts = str(int(time.time()))
        secret, signature = _sign(b"key123456789012345678901234", webhook_id, webhook_ts, body)
        headers = {
            "webhook-id": webhook_id,
            "webhook-timestamp": webhook_ts,
            "webhook-signature": signature,
        }
        assert _verify_fathom_webhook_signature(secret, headers, b'{"recording_id": 99}') is False

    def test_missing_headers_rejected(self):
        assert _verify_fathom_webhook_signature("whsec_x", {}, b"{}") is False

    def test_stale_timestamp_rejected(self):
        body = b"{}"
        webhook_id = "msg_old"
        webhook_ts = str(int(time.time()) - 600)
        secret, signature = _sign(b"key123456789012345678901234", webhook_id, webhook_ts, body)
        headers = {
            "webhook-id": webhook_id,
            "webhook-timestamp": webhook_ts,
            "webhook-signature": signature,
        }
        assert _verify_fathom_webhook_signature(secret, headers, body) is False

    def test_case_insensitive_header_keys(self):
        body = b'{"ok": true}'
        webhook_id = "msg_ci"
        webhook_ts = str(int(time.time()))
        secret, signature = _sign(b"key123456789012345678901234", webhook_id, webhook_ts, body)
        headers = {
            "Webhook-Id": webhook_id,
            "Webhook-Timestamp": webhook_ts,
            "Webhook-Signature": signature,
        }
        assert _verify_fathom_webhook_signature(secret, headers, body) is True

"""Whop webhook signature verify + payload unwrap (Standard Webhooks + ws_ keys)."""
from __future__ import annotations

import base64
import hashlib
import hmac
import time
from typing import Any, Dict, List, Optional


def verify_whop_webhook_signature(secret: str, headers: Dict[str, str], raw_body: bytes) -> bool:
    webhook_id = headers.get("webhook-id") or headers.get("Webhook-Id")
    webhook_ts = headers.get("webhook-timestamp") or headers.get("Webhook-Timestamp")
    webhook_sig = headers.get("webhook-signature") or headers.get("Webhook-Signature")
    if not webhook_id or not webhook_ts or not webhook_sig:
        return False
    try:
        ts = int(str(webhook_ts))
    except Exception:
        return False
    if abs(int(time.time()) - ts) > 300:
        return False

    signed = f"{webhook_id}.{webhook_ts}.{raw_body.decode('utf-8')}".encode("utf-8")
    candidates: List[str] = []
    for chunk in str(webhook_sig).split(" "):
        chunk = chunk.strip()
        if not chunk:
            continue
        parts = chunk.split(",", 1)
        candidates.append(parts[1] if len(parts) > 1 else parts[0])
    if not candidates:
        return False

    for key in _hmac_keys(secret):
        expected = base64.b64encode(hmac.new(key, signed, hashlib.sha256).digest()).decode("utf-8")
        if any(hmac.compare_digest(expected, c) for c in candidates if c):
            return True
    return False


def _hmac_keys(secret: str) -> List[bytes]:
    s = (secret or "").strip()
    keys: List[bytes] = []
    if not s:
        return keys
    keys.append(s.encode("utf-8"))
    for prefix in ("whsec_", "ws_"):
        if not s.startswith(prefix):
            continue
        rest = s[len(prefix) :]
        if rest:
            keys.append(rest.encode("utf-8"))
            try:
                keys.append(base64.b64decode(rest))
            except Exception:
                pass
    return keys


def payment_item_from_webhook_payload(payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Return a Whop payment dict from a webhook envelope, or None if not a payment."""
    data = payload.get("data")
    event_type = str(payload.get("type") or "").strip().lower()
    if isinstance(data, dict) and data.get("id") and str(data.get("id")).startswith("pay_"):
        return data
    if event_type.startswith("payment.") and isinstance(data, dict) and data.get("id"):
        return data
    if event_type.startswith("refund.") and isinstance(data, dict):
        payment = data.get("payment")
        if isinstance(payment, dict) and payment.get("id"):
            return payment
        nested = data.get("payment_id") or data.get("paymentId")
        if isinstance(nested, str) and nested.startswith("pay_"):
            return {"id": nested, "status": "refunded"}
    return None

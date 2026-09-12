"""
HTTP client for Whop REST API (Company API key).
See https://docs.whop.com/developer/api/getting-started
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import httpx

WHOP_API_BASE = "https://api.whop.com/api/v1"


def _headers(api_key: str) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {api_key.strip()}",
        "Accept": "application/json",
    }


def _whop_error_detail(response: httpx.Response) -> str:
    try:
        body = response.json()
        err = body.get("error") if isinstance(body, dict) else None
        if isinstance(err, dict):
            msg = err.get("message") or err.get("code") or str(err)
            param = err.get("param")
            if param:
                return f"{msg} (param={param})"
            return str(msg)
        if isinstance(body, dict) and body.get("message"):
            return str(body["message"])
    except Exception:
        pass
    text = (response.text or "").strip()
    return text[:400] if text else response.reason_phrase


def validate_credentials(api_key: str, company_id: str) -> None:
    """Raise ValueError / HTTPStatusError if key or company_id is invalid."""
    company_id = company_id.strip()
    if not company_id.startswith("biz_"):
        raise ValueError("company_id must look like biz_…")
    with httpx.Client(timeout=30.0) as client:
        r = client.get(
            f"{WHOP_API_BASE}/payments",
            headers=_headers(api_key),
            params={"account_id": company_id, "first": 1},
        )
        if r.is_error:
            raise ValueError(_whop_error_detail(r))
        r.raise_for_status()


def list_payments_page(
    api_key: str,
    company_id: str,
    *,
    first: int = 50,
    after: Optional[str] = None,
    updated_after: Optional[datetime] = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    One page of payments. Returns (data_rows, page_info).
    """
    params: Dict[str, Any] = {"account_id": company_id.strip(), "first": first}
    if after:
        params["after"] = after
    if updated_after:
        params["updated_after"] = updated_after.replace(microsecond=0).isoformat().replace("+00:00", "Z")

    with httpx.Client(timeout=60.0) as client:
        r = client.get(f"{WHOP_API_BASE}/payments", headers=_headers(api_key), params=params)
        r.raise_for_status()
        body = r.json()
    data = body.get("data") or []
    page_info = body.get("page_info") or {}
    if not isinstance(data, list):
        data = []
    if not isinstance(page_info, dict):
        page_info = {}
    return data, page_info


WHOP_PAYMENT_WEBHOOK_EVENTS = [
    "payment.succeeded",
    "payment.created",
    "payment.failed",
    "payment.pending",
    "refund.created",
    "refund.updated",
]


def retrieve_payment(api_key: str, payment_id: str) -> Dict[str, Any]:
    payment_id = (payment_id or "").strip()
    if not payment_id:
        raise ValueError("payment_id required")
    with httpx.Client(timeout=30.0) as client:
        r = client.get(f"{WHOP_API_BASE}/payments/{payment_id}", headers=_headers(api_key))
        r.raise_for_status()
        body = r.json()
    return body if isinstance(body, dict) else {}


def retrieve_member(api_key: str, member_id: str) -> Dict[str, Any]:
    member_id = (member_id or "").strip()
    if not member_id:
        raise ValueError("member_id required")
    with httpx.Client(timeout=30.0) as client:
        r = client.get(f"{WHOP_API_BASE}/members/{member_id}", headers=_headers(api_key))
        r.raise_for_status()
        body = r.json()
    return body if isinstance(body, dict) else {}


def list_webhooks(api_key: str, *, company_id: Optional[str] = None) -> List[Dict[str, Any]]:
    params: Dict[str, Any] = {}
    if company_id:
        params["account_id"] = company_id.strip()
    with httpx.Client(timeout=30.0) as client:
        r = client.get(f"{WHOP_API_BASE}/webhooks", headers=_headers(api_key), params=params or None)
        r.raise_for_status()
        body = r.json()
    data = body.get("data") if isinstance(body, dict) else body
    if isinstance(data, list):
        return [row for row in data if isinstance(row, dict)]
    return []


def create_webhook(
    api_key: str,
    *,
    url: str,
    events: Optional[List[str]] = None,
    resource_id: Optional[str] = None,
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "url": url,
        "events": events or WHOP_PAYMENT_WEBHOOK_EVENTS,
        "api_version": "v1",
        "enabled": True,
    }
    if resource_id:
        payload["resource_id"] = resource_id.strip()
    with httpx.Client(timeout=30.0) as client:
        r = client.post(f"{WHOP_API_BASE}/webhooks", headers=_headers(api_key), json=payload)
        r.raise_for_status()
        body = r.json()
    return body if isinstance(body, dict) else {}


def delete_webhook(api_key: str, webhook_id: str) -> None:
    webhook_id = (webhook_id or "").strip()
    if not webhook_id:
        return
    with httpx.Client(timeout=30.0) as client:
        r = client.delete(f"{WHOP_API_BASE}/webhooks/{webhook_id}", headers=_headers(api_key))
        if r.status_code in (404, 410):
            return
        r.raise_for_status()

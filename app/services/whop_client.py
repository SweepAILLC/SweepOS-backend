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


def validate_credentials(api_key: str, company_id: str) -> None:
    """Raise httpx.HTTPStatusError if key or company_id is invalid."""
    company_id = company_id.strip()
    if not company_id.startswith("biz_"):
        raise ValueError("company_id must look like biz_…")
    with httpx.Client(timeout=30.0) as client:
        r = client.get(
            f"{WHOP_API_BASE}/payments",
            headers=_headers(api_key),
            params={"company_id": company_id, "first": 1},
        )
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
    params: Dict[str, Any] = {"company_id": company_id.strip(), "first": first}
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

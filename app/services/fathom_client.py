"""HTTP client for Fathom External API (https://api.fathom.ai/external/v1)."""
from __future__ import annotations

import json
import uuid
from typing import Any, Dict, List, Optional

import httpx
from sqlalchemy.orm import Session

from app.core.config import settings
from app.models.organization import Organization
from app.models.user import User

BASE = "https://api.fathom.ai/external/v1"


def normalize_fathom_api_key(raw: Optional[str]) -> Optional[str]:
    """Strip whitespace, newlines, wrapping quotes, and accidental Bearer prefix."""
    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        return None
    if (len(s) >= 2 and s[0] == s[-1]) and s[0] in ("'", '"'):
        s = s[1:-1].strip()
    low = s[:7].lower()
    if low == "bearer ":
        s = s[7:].strip()
    s = "".join(s.splitlines()).strip()
    return s or None


def fathom_configured() -> bool:
    """True if global env key is set (legacy / server-wide)."""
    return bool(normalize_fathom_api_key(getattr(settings, "FATHOM_API_KEY", None)))


def resolve_fathom_api_key(
    db: Optional[Session],
    org_id: Optional[uuid.UUID],
    *,
    user: Optional[Any] = None,
) -> Optional[str]:
    """
    Resolve which Fathom API key to use.

    Order:
    1. Organization row for `org_id` (`organizations.fathom_api_key`) — primary, per-org.
    2. Explicit `user` (legacy key on user row).
    3. Any user in the org with `users.fathom_api_key` set (legacy / webhooks).
    4. `FATHOM_API_KEY` in environment.
    """
    if db is not None and org_id is not None:
        org = db.query(Organization).filter(Organization.id == org_id).first()
        if org and getattr(org, "fathom_api_key", None):
            k = normalize_fathom_api_key(org.fathom_api_key)
            if k:
                return k
    if user is not None:
        k = normalize_fathom_api_key(getattr(user, "fathom_api_key", None))
        if k:
            return k
    if db is not None and org_id is not None:
        row = (
            db.query(User)
            .filter(User.org_id == org_id, User.fathom_api_key.isnot(None))
            .first()
        )
        if row and row.fathom_api_key:
            k = normalize_fathom_api_key(row.fathom_api_key)
            if k:
                return k
    env_k = normalize_fathom_api_key(getattr(settings, "FATHOM_API_KEY", None))
    if env_k:
        return env_k
    return None


def fathom_configured_for_org(db: Session, org_id: uuid.UUID) -> bool:
    return bool(resolve_fathom_api_key(db, org_id))


_RETRYABLE_STATUS = frozenset({429, 502, 503})


def _get_with_retries(
    fn,
    *,
    timeout: float,
    max_attempts: int = 3,
) -> Any:
    """
    Small wrapper around httpx.Client.get with limited retries and backoff.

    Fathom's API can sporadically time out or return transient 5xx/429s for large accounts.
    Retrying a couple times on these cases dramatically reduces visible failures
    without pushing undue load.
    """
    last_exc: Exception | None = None
    for attempt in range(max_attempts):
        try:
            with httpx.Client(timeout=timeout) as client:
                r = fn(client)
                if r.status_code in _RETRYABLE_STATUS and attempt < max_attempts - 1:
                    # Simple exponential backoff with upper bound
                    import time as _time

                    _time.sleep(min(0.5 * (2**attempt), 4.0))
                    continue
                r.raise_for_status()
                try:
                    return r.json()
                except json.JSONDecodeError as e:
                    snippet = (r.text or "")[:300]
                    raise RuntimeError(
                        f"Fathom returned non-JSON response (HTTP {r.status_code}): {snippet!r}"
                    ) from e
        except (httpx.TimeoutException, httpx.ConnectError, httpx.HTTPStatusError) as e:
            last_exc = e
            if attempt < max_attempts - 1:
                import time as _time

                _time.sleep(min(0.5 * (2**attempt), 4.0))
                continue
            raise
    raise last_exc or RuntimeError("Fathom request exhausted retries")


def _headers(api_key: str) -> Dict[str, str]:
    if not api_key or not str(api_key).strip():
        raise RuntimeError("Fathom API key not set")
    return {"X-Api-Key": str(api_key).strip(), "Accept": "application/json"}


def list_meetings(
    *,
    cursor: Optional[str] = None,
    created_after: Optional[str] = None,
    include_summary: bool = True,
    include_transcript: bool = True,
    timeout: float = 60.0,
    db: Optional[Session] = None,
    org_id: Optional[uuid.UUID] = None,
    user: Optional[Any] = None,
    api_key: Optional[str] = None,
) -> Dict[str, Any]:
    key = api_key or resolve_fathom_api_key(db, org_id, user=user)
    if not key:
        raise RuntimeError("Fathom not configured")
    params: Dict[str, Any] = {
        "include_summary": include_summary,
        "include_transcript": include_transcript,
    }
    if cursor:
        params["cursor"] = cursor
    if created_after:
        params["created_after"] = created_after
    return _get_with_retries(
        lambda c: c.get(f"{BASE}/meetings", headers=_headers(key), params=params),
        timeout=timeout,
    )


def get_recording_summary(
    recording_id: int,
    timeout: float = 60.0,
    *,
    api_key: Optional[str] = None,
    db: Optional[Session] = None,
    org_id: Optional[uuid.UUID] = None,
) -> Dict[str, Any]:
    key = api_key or resolve_fathom_api_key(db, org_id)
    if not key:
        raise RuntimeError("Fathom not configured")
    return _get_with_retries(
        lambda c: c.get(
            f"{BASE}/recordings/{recording_id}/summary",
            headers=_headers(key),
        ),
        timeout=timeout,
    )


def get_recording_transcript(
    recording_id: int,
    timeout: float = 120.0,
    *,
    api_key: Optional[str] = None,
    db: Optional[Session] = None,
    org_id: Optional[uuid.UUID] = None,
) -> Dict[str, Any]:
    key = api_key or resolve_fathom_api_key(db, org_id)
    if not key:
        raise RuntimeError("Fathom not configured")
    return _get_with_retries(
        lambda c: c.get(
            f"{BASE}/recordings/{recording_id}/transcript",
            headers=_headers(key),
        ),
        timeout=timeout,
    )


def create_webhook(
    destination_url: str,
    *,
    include_transcript: bool = True,
    include_summary: bool = True,
    include_action_items: bool = True,
    triggered_for: Optional[List[str]] = None,
    timeout: float = 30.0,
    db: Optional[Session] = None,
    org_id: Optional[uuid.UUID] = None,
    api_key: Optional[str] = None,
) -> Dict[str, Any]:
    key = api_key or resolve_fathom_api_key(db, org_id)
    if not key:
        raise RuntimeError("Fathom not configured")
    body = {
        "destination_url": destination_url,
        "include_transcript": include_transcript,
        "include_summary": include_summary,
        "include_crm_matches": False,
        "include_action_items": include_action_items,
        "triggered_for": triggered_for
        or [
            "my_recordings",
            "my_shared_with_team_recordings",
            "shared_external_recordings",
        ],
    }
    with httpx.Client(timeout=timeout) as client:
        r = client.post(f"{BASE}/webhooks", headers=_headers(key), json=body)
        r.raise_for_status()
        return r.json()

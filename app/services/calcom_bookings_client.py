"""Fetch and normalize Cal.com v2 bookings for check-in sync."""
from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Set

import httpx

from app.services.calendar_booking_time import format_cal_api_time

# Cal.com accepts ONE status per request (or omit for all). Comma-separated status is invalid.
CALCOM_BOOKING_STATUSES = ("upcoming", "past", "cancelled", "unconfirmed", "recurring")
# Cal.com bookings list requires 2026-05-01 for cursor pagination (see Cal.com API docs).
CALCOM_BOOKINGS_API_VERSION = "2026-05-01"
CALCOM_API_VERSION = CALCOM_BOOKINGS_API_VERSION
DEFAULT_LOOKBACK_DAYS = 365
DEFAULT_LOOKAHEAD_DAYS = 365


def _booking_uid(booking: dict) -> str:
    return str(booking.get("uid") or booking.get("id") or "").strip()


def extract_calcom_attendees(booking: dict) -> List[Dict[str, Any]]:
    """Collect attendee emails from Cal.com list/detail booking payloads."""
    out: List[Dict[str, Any]] = []
    seen: Set[str] = set()

    def add(email: Optional[str], name: Optional[str] = None) -> None:
        if not email or not str(email).strip():
            return
        key = str(email).strip().lower()
        if key in seen:
            return
        seen.add(key)
        out.append({"email": str(email).strip(), "name": name})

    attendees = booking.get("attendees")
    if isinstance(attendees, list):
        for row in attendees:
            if isinstance(row, str):
                add(row)
            elif isinstance(row, dict):
                add(row.get("email") or row.get("displayEmail"), row.get("name"))

    # Singular attendee object (some API shapes)
    single = booking.get("attendee")
    if isinstance(single, dict):
        add(single.get("email") or single.get("displayEmail"), single.get("name"))

    guests = booking.get("guests")
    if isinstance(guests, list):
        for g in guests:
            if isinstance(g, str):
                add(g)
            elif isinstance(g, dict):
                add(g.get("email"), g.get("name"))

    for field in ("bookingFieldsResponses", "responses"):
        responses = booking.get(field)
        if not isinstance(responses, dict):
            continue
        email = responses.get("email") or responses.get("EMAIL")
        name = responses.get("name") or responses.get("NAME")
        if email:
            add(str(email), str(name) if name else None)
        for val in responses.values():
            if isinstance(val, str) and "@" in val and re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", val.strip()):
                add(val.strip())

    return out


def resolve_calcom_participants(
    booking: dict,
    *,
    detail: Optional[dict] = None,
    extra_attendee_rows: Optional[List[Dict[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    """Merge list payload, optional detail payload, and /attendees fallback rows."""
    merged = dict(booking)
    if isinstance(detail, dict):
        for key in ("attendees", "guests", "bookingFieldsResponses", "responses", "attendee"):
            if detail.get(key) is not None:
                merged[key] = detail[key]

    out = extract_calcom_attendees(merged)
    seen = {r["email"].lower() for r in out}
    if extra_attendee_rows:
        for row in extra_attendee_rows:
            if not isinstance(row, dict):
                continue
            email = row.get("email") or row.get("displayEmail")
            if not email:
                continue
            key = str(email).strip().lower()
            if key in seen:
                continue
            seen.add(key)
            out.append({"email": str(email).strip(), "name": row.get("name")})
    return out


def fetch_calcom_booking_detail(
    access_token: str,
    booking_uid: str,
    *,
    timeout: float = 15.0,
) -> Optional[dict]:
    """Fetch full booking (includes form responses / attendees omitted from list API)."""
    if not booking_uid:
        return None
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "cal-api-version": "2026-02-25",
    }
    try:
        with httpx.Client(timeout=httpx.Timeout(timeout, connect=8.0)) as client:
            response = client.get(
                f"https://api.cal.com/v2/bookings/{booking_uid}",
                headers=headers,
            )
        if response.status_code != 200:
            return None
        payload = response.json()
        data = payload.get("data") if isinstance(payload, dict) else None
        if isinstance(data, list):
            return data[0] if data and isinstance(data[0], dict) else None
        return data if isinstance(data, dict) else None
    except Exception as exc:
        print(f"[CALCOM FETCH] detail failed uid={booking_uid}: {exc}")
        return None


def _parse_bookings_payload(api_response: dict) -> tuple[List[dict], bool]:
    if not isinstance(api_response, dict):
        return [], False
    if api_response.get("status") not in (None, "success"):
        return [], False

    bookings_data = api_response.get("data", [])
    if isinstance(bookings_data, list):
        batch = [b for b in bookings_data if isinstance(b, dict)]
    elif isinstance(bookings_data, dict):
        batch = [b for b in bookings_data.get("bookings", []) if isinstance(b, dict)]
    else:
        batch = []

    pagination = api_response.get("pagination") or {}
    has_more = bool(pagination.get("hasMore"))
    next_cursor = pagination.get("nextCursor")
    if next_cursor:
        has_more = True
    # Legacy offset pagination metadata
    if pagination.get("hasNextPage"):
        has_more = True
    return batch, has_more


def _status_date_filters(
    status: Optional[str],
    *,
    now: datetime,
    lookback_days: int,
    lookahead_days: int,
) -> Dict[str, Optional[datetime]]:
    """Per-status date windows aligned with Cal.com v2 status walk semantics."""
    lookback_start = now - timedelta(days=lookback_days)
    future_end = now + timedelta(days=lookahead_days)
    in_progress_start = now - timedelta(hours=2)

    if status in ("upcoming", "unconfirmed", "recurring"):
        return {"after_start": in_progress_start, "before_end": future_end}
    if status in ("past", "cancelled"):
        return {"after_start": lookback_start, "before_end": None}
    return {"after_start": lookback_start, "before_end": future_end}


def _fetch_calcom_status_pages(
    http_client: httpx.Client,
    headers: dict,
    *,
    status: Optional[str],
    after_start: Optional[datetime] = None,
    before_end: Optional[datetime] = None,
    after_updated_at: Optional[datetime] = None,
    max_pages: int = 25,
) -> List[dict]:
    rows: List[dict] = []
    take = 100
    cursor: Optional[str] = None
    skip = 0

    for page in range(max_pages):
        params: Dict[str, Any] = {"take": take}
        if status:
            params["status"] = status
        if after_start is not None:
            params["afterStart"] = format_cal_api_time(after_start)
        if before_end is not None:
            params["beforeEnd"] = format_cal_api_time(before_end)
        if after_updated_at is not None:
            params["afterUpdatedAt"] = format_cal_api_time(after_updated_at)
        if cursor:
            params["cursor"] = cursor
        elif page > 0:
            params["skip"] = skip

        response = http_client.get(
            "https://api.cal.com/v2/bookings",
            headers=headers,
            params=params,
        )
        if response.status_code != 200:
            print(
                f"[CALCOM FETCH] status={status or 'all'} page={page + 1} "
                f"HTTP {response.status_code}: {response.text[:300]}"
            )
            break

        payload = response.json()
        batch, has_more = _parse_bookings_payload(payload)
        if not batch:
            break
        rows.extend(batch)

        pagination = payload.get("pagination") or {}
        cursor = pagination.get("nextCursor")
        if cursor:
            skip = 0
            continue
        if not has_more or len(batch) < take:
            break
        skip += take

    return rows


def fetch_calcom_booking_attendees(
    access_token: str,
    booking_uid: str,
    *,
    timeout: float = 15.0,
) -> List[Dict[str, Any]]:
    """Fallback: list endpoint sometimes omits attendees — fetch by booking uid."""
    if not booking_uid:
        return []
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "cal-api-version": CALCOM_API_VERSION,
    }
    try:
        with httpx.Client(timeout=httpx.Timeout(timeout, connect=8.0)) as client:
            response = client.get(
                f"https://api.cal.com/v2/bookings/{booking_uid}/attendees",
                headers=headers,
            )
        if response.status_code != 200:
            return []
        payload = response.json()
        data = payload.get("data") if isinstance(payload, dict) else None
        if not isinstance(data, list):
            return []
        return [
            {"email": row.get("email") or row.get("displayEmail"), "name": row.get("name")}
            for row in data
            if isinstance(row, dict) and (row.get("email") or row.get("displayEmail"))
        ]
    except Exception as exc:
        print(f"[CALCOM FETCH] attendees fallback failed uid={booking_uid}: {exc}")
        return []


def fetch_all_calcom_bookings(
    access_token: str,
    *,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    lookahead_days: int = DEFAULT_LOOKAHEAD_DAYS,
    after_updated_at: Optional[datetime] = None,
    timeout: float = 25.0,
) -> List[dict]:
    """
    Pull upcoming + past + cancelled Cal.com bookings.
    Issues one request per status (required by Cal.com v2) and dedupes by booking uid.
    """
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "cal-api-version": CALCOM_API_VERSION,
    }
    now = datetime.now(timezone.utc)

    seen: Set[str] = set()
    merged: List[dict] = []

    cal_timeout = httpx.Timeout(timeout, connect=10.0)
    with httpx.Client(timeout=cal_timeout) as http_client:
        for status in CALCOM_BOOKING_STATUSES:
            # Status-specific walks have built-in time bounds; extra date filters can drop rows.
            batch = _fetch_calcom_status_pages(
                http_client,
                headers,
                status=status,
                max_pages=25,
            )
            added = 0
            for booking in batch:
                uid = _booking_uid(booking)
                if not uid or uid in seen:
                    continue
                seen.add(uid)
                merged.append(booking)
                added += 1
            print(f"[CALCOM FETCH] status={status}: +{added} unique (batch={len(batch)})")

        if after_updated_at is not None:
            updated_batch = _fetch_calcom_status_pages(
                http_client,
                headers,
                status=None,
                after_updated_at=after_updated_at,
                max_pages=10,
            )
            added_updated = 0
            for booking in updated_batch:
                uid = _booking_uid(booking)
                if not uid or uid in seen:
                    continue
                seen.add(uid)
                merged.append(booking)
                added_updated += 1
            if added_updated:
                print(f"[CALCOM FETCH] afterUpdatedAt: +{added_updated} additional unique")

        # Unfiltered pass for edge statuses; narrow to sync window.
        lookback_start = now - timedelta(days=lookback_days)
        future_end = now + timedelta(days=lookahead_days)
        extra = _fetch_calcom_status_pages(
            http_client,
            headers,
            status=None,
            after_start=lookback_start,
            before_end=future_end,
            max_pages=15,
        )
        added_extra = 0
        for booking in extra:
            uid = _booking_uid(booking)
            if not uid or uid in seen:
                continue
            seen.add(uid)
            merged.append(booking)
            added_extra += 1
        if added_extra:
            print(f"[CALCOM FETCH] status=all: +{added_extra} additional unique")

    print(
        f"[CALCOM FETCH] Total unique bookings: {len(merged)} "
        f"(lookback={lookback_days}d, lookahead={lookahead_days}d)"
    )
    return merged

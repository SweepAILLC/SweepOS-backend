#!/usr/bin/env python3
"""
Audit calendar sync accuracy: provider APIs vs DB vs synced-bookings endpoint.

Usage (from backend/ with env loaded):
  python scripts/audit_calendar_sync.py --api-base https://api.sweepai.site --email you@org.com --password '...'

Or local:
  python scripts/audit_calendar_sync.py --api-base http://localhost:8000 ...
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set

import httpx

# Allow running from repo root or backend/
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.services.calendar_booking_time import classify_booking_window  # noqa: E402


def _login(base: str, email: str, password: str, org_id: Optional[str] = None) -> str:
    body: Dict[str, Any] = {"email": email, "password": password}
    if org_id:
        body["org_id"] = org_id
    r = httpx.post(f"{base.rstrip('/')}/auth/login", json=body, timeout=30.0)
    r.raise_for_status()
    data = r.json()
    token = data.get("access_token")
    if token:
        return token
    if data.get("requires_org_selection") and data.get("organizations"):
        for org in data["organizations"]:
            try:
                return _login(base, email, password, org_id=org["id"])
            except httpx.HTTPStatusError:
                continue
    raise RuntimeError("login failed: no access_token")


def _headers(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


def _classify_rows(rows: List[dict], now_iso: str) -> Dict[str, int]:
    now = datetime.fromisoformat(now_iso.replace("Z", "+00:00"))
    counts = {"upcoming": 0, "past": 0, "unknown": 0}
    for row in rows:
        bucket = classify_booking_window(row.get("start_time"), row.get("end_time"), now=now)
        if bucket:
            counts[bucket] += 1
        else:
            counts["unknown"] += 1
    return counts


def _overlap_check(api_rows: List[dict], db_upcoming: List[dict], db_past: List[dict]) -> Dict[str, Any]:
    db_by_event: Dict[str, dict] = {}
    for row in db_upcoming + db_past:
        key = f"{row.get('provider')}:{row.get('event_id')}"
        db_by_event[key] = row

    missing_in_db: List[str] = []
    bucket_mismatch: List[dict] = []

    for api in api_rows:
        provider = api.get("provider")
        event_id = api.get("event_id")
        if not provider or not event_id:
            continue
        key = f"{provider}:{event_id}"
        db_row = db_by_event.get(key)
        if not db_row:
            missing_in_db.append(key)
            continue
        api_bucket = api.get("_bucket")
        start = db_row.get("start_time")
        end = db_row.get("end_time")
        db_bucket = classify_booking_window(start, end)
        if api_bucket and db_bucket and api_bucket != db_bucket:
            bucket_mismatch.append(
                {"key": key, "api_bucket": api_bucket, "db_bucket": db_bucket, "start": start, "end": end}
            )

    return {
        "missing_in_db_count": len(missing_in_db),
        "missing_in_db_sample": missing_in_db[:10],
        "bucket_mismatch_count": len(bucket_mismatch),
        "bucket_mismatch_sample": bucket_mismatch[:10],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit calendar sync accuracy")
    parser.add_argument("--api-base", default=os.getenv("NEXT_PUBLIC_API_BASE_URL", "http://localhost:8000"))
    parser.add_argument("--email", default=os.getenv("SUDO_ADMIN_EMAIL"))
    parser.add_argument("--password", default=os.getenv("SUDO_ADMIN_PASSWORD"))
    parser.add_argument("--skip-sync", action="store_true", help="Only read DB endpoint, do not POST sync")
    args = parser.parse_args()

    if not args.email or not args.password:
        print("ERROR: provide --email/--password or set SUDO_ADMIN_EMAIL/SUDO_ADMIN_PASSWORD", file=sys.stderr)
        return 1

    base = args.api_base.rstrip("/")
    token = _login(base, args.email, args.password)
    h = _headers(token)

    calcom = httpx.get(f"{base}/integrations/calcom/status", headers=h, timeout=20.0)
    calendly = httpx.get(f"{base}/integrations/calendly/status", headers=h, timeout=20.0)
    calcom_connected = calcom.json().get("connected") if calcom.status_code == 200 else False
    calendly_connected = calendly.json().get("connected") if calendly.status_code == 200 else False
    print(f"Connected: calcom={calcom_connected} calendly={calendly_connected}")

    if not args.skip_sync:
        print("Running POST /clients/check-ins/sync ...")
        sync_r = httpx.post(f"{base}/clients/check-ins/sync", headers=h, timeout=180.0)
        print(f"Sync status: {sync_r.status_code}")
        if sync_r.status_code == 200:
            print(json.dumps(sync_r.json(), indent=2)[:2000])
        else:
            print(sync_r.text[:500])

    params = {
        "upcoming_limit": 200,
        "past_limit": 200,
        "past_since": (datetime.now(timezone.utc) - __import__("datetime").timedelta(days=365)).isoformat(),
    }
    db_r = httpx.get(f"{base}/integrations/calendar/synced-bookings", headers=h, params=params, timeout=180.0)
    db_r.raise_for_status()
    payload = db_r.json()
    now_iso = payload.get("server_time") or datetime.now(timezone.utc).isoformat()
    upcoming = payload.get("upcoming") or []
    past = payload.get("past") or []
    all_db = upcoming + past

    print(f"\nDB synced-bookings (no provider filter):")
    print(f"  upcoming={len(upcoming)} past={len(past)} total={len(all_db)}")
    print(f"  upcoming classify recount: {_classify_rows(upcoming, now_iso)}")
    print(f"  past classify recount: {_classify_rows(past, now_iso)}")

    # Live Cal.com proxy counts (when connected)
    if calcom_connected:
        for st in ("upcoming", "past", None):
            params = {"take": 100}
            if st:
                params["status"] = st
            pr = httpx.get(f"{base}/integrations/calcom/bookings", headers=h, params=params, timeout=90)
            if pr.status_code == 200:
                pdata = pr.json()
                pb = pdata.get("bookings") or []
                now = datetime.fromisoformat(now_iso.replace("Z", "+00:00"))
                live_u = live_p = 0
                for b in pb:
                    s = b.get("start") or b.get("startTime")
                    e = b.get("end") or b.get("endTime")
                    bucket = classify_booking_window(s, e, now=now)
                    if bucket == "upcoming":
                        live_u += 1
                    elif bucket == "past":
                        live_p += 1
                label = st or "all"
                print(
                    f"  live calcom proxy status={label}: returned={len(pb)} "
                    f"total={pdata.get('total')} time-upcoming={live_u} time-past={live_p}"
                )

    # Mis-bucketed rows: should be zero for 100% accuracy
    mis_upcoming = [
        r for r in upcoming
        if classify_booking_window(r.get("start_time"), r.get("end_time"), now=datetime.fromisoformat(now_iso.replace("Z", "+00:00"))) != "upcoming"
    ]
    mis_past = [
        r for r in past
        if classify_booking_window(r.get("start_time"), r.get("end_time"), now=datetime.fromisoformat(now_iso.replace("Z", "+00:00"))) != "past"
    ]
    print(f"  MIS-BUCKETED upcoming rows: {len(mis_upcoming)}")
    print(f"  MIS-BUCKETED past rows: {len(mis_past)}")
    if mis_upcoming:
        print("  sample mis-upcoming:", json.dumps(mis_upcoming[:3], indent=2))
    if mis_past:
        print("  sample mis-past:", json.dumps(mis_past[:3], indent=2))

    by_provider: Dict[str, Dict[str, int]] = defaultdict(lambda: {"upcoming": 0, "past": 0})
    for r in upcoming:
        by_provider[r.get("provider", "?")]["upcoming"] += 1
    for r in past:
        by_provider[r.get("provider", "?")]["past"] += 1
    print(f"  by_provider: {dict(by_provider)}")

    # Provider-filter regression check
    if calcom_connected:
        filt = httpx.get(
            f"{base}/integrations/calendar/synced-bookings",
            headers=h,
            params={**params, "provider": "calcom"},
            timeout=60.0,
        )
        if filt.status_code == 200:
            fdata = filt.json()
            calcom_only = len(fdata.get("upcoming") or []) + len(fdata.get("past") or [])
            calcom_all = sum(1 for r in all_db if r.get("provider") == "calcom")
            print(f"\nProvider filter regression (calcom): filtered={calcom_only} vs all_calcom={calcom_all}")
            if calcom_all > calcom_only and calendly_connected:
                print("  WARNING: provider=calcom filter hides rows when both providers connected")

    ok = len(mis_upcoming) == 0 and len(mis_past) == 0
    print("\nAUDIT RESULT:", "PASS" if ok else "FAIL")
    return 0 if ok else 2


if __name__ == "__main__":
    raise SystemExit(main())

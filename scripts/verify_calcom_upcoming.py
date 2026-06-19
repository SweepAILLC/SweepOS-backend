#!/usr/bin/env python3
"""Verify Cal.com upcoming fetch includes a booking by title (uses CALCOM_API_KEY from env)."""
from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime, timezone

from app.core.config import settings
from app.services.calcom_bookings_client import fetch_all_calcom_bookings, _booking_uid
from app.services.calendar_booking_time import classify_booking_window


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--title-contains", default="Pro Client Consultation between Sweep and Shai")
    args = parser.parse_args()

    token = (settings.CALCOM_API_KEY or os.environ.get("CALCOM_API_KEY") or "").strip()
    if not token:
        print("ERROR: Set CALCOM_API_KEY in .env or environment")
        return 1

    now = datetime.now(timezone.utc)
    all_rows = fetch_all_calcom_bookings(token)
    upcoming = [
        b
        for b in all_rows
        if classify_booking_window(b.get("start"), b.get("end"), now=now) == "upcoming"
    ]
    matches = [
        b
        for b in all_rows
        if args.title_contains.lower() in (b.get("title") or "").lower()
        and classify_booking_window(b.get("start"), b.get("end"), now=now) == "upcoming"
    ]

    print(f"Fetched {len(all_rows)} unique bookings; {len(upcoming)} time-upcoming")
    for b in upcoming:
        print(f"  upcoming uid={_booking_uid(b)} start={b.get('start')} title={b.get('title')}")

    if not matches:
        print(f"\nFAIL: no time-upcoming booking matching {args.title_contains!r}")
        return 1

    b = matches[0]
    print(f"\nPASS: found uid={_booking_uid(b)} start={b.get('start')} end={b.get('end')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

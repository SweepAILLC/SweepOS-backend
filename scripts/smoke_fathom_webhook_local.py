#!/usr/bin/env python3
"""Local-only signed POST to /webhooks/fathom/{org} — does not call Fathom APIs."""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import sys
import time
import urllib.error
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import text
from app.db.session import SessionLocal

ORG = "00000000-0000-0000-0000-000000000001"


def main() -> int:
    db = SessionLocal()
    try:
        secret = db.execute(
            text("SELECT fathom_webhook_secret FROM organizations WHERE id=:id"),
            {"id": ORG},
        ).scalar()
    finally:
        db.close()

    if not secret or not str(secret).startswith("whsec_"):
        print(f"missing local webhook secret: {secret!r}", file=sys.stderr)
        return 1

    body_obj = {
        "recording_id": 900001,
        "title": "Local webhook smoke test",
        "meeting_title": "Local webhook smoke test",
        "calendar_invitees": [{"email": "prospect@example.com", "name": "Prospect"}],
        "transcript": "Rep: thanks for joining. Prospect: we are ready to buy the annual plan.",
        "default_summary": {
            "markdown_formatted": "Discovery call; prospect interested in annual."
        },
        "share_url": "https://fathom.video/share/example",
        "url": "https://fathom.video/calls/example",
    }
    raw = json.dumps(body_obj).encode("utf-8")
    webhook_id = "msg_local_smoke_1"
    webhook_ts = str(int(time.time()))
    signed = f"{webhook_id}.{webhook_ts}.{raw.decode('utf-8')}"
    sec_bytes = base64.b64decode(str(secret).split("_", 1)[1])
    sig = base64.b64encode(
        hmac.new(sec_bytes, signed.encode("utf-8"), hashlib.sha256).digest()
    ).decode()

    req = urllib.request.Request(
        f"http://127.0.0.1:8000/webhooks/fathom/{ORG}",
        data=raw,
        headers={
            "Content-Type": "application/json",
            "webhook-id": webhook_id,
            "webhook-timestamp": webhook_ts,
            "webhook-signature": f"v1,{sig}",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            print("webhook_status", resp.status)
            print("webhook_body", resp.read().decode())
    except urllib.error.HTTPError as exc:
        print("webhook_http_error", exc.code, exc.read().decode(), file=sys.stderr)
        return 1

    db = SessionLocal()
    try:
        counts = db.execute(
            text(
                "SELECT (SELECT count(*) FROM fathom_call_records) AS f, "
                "(SELECT count(*) FROM call_library_reports) AS r"
            )
        ).one()
        print("after_counts fathom=", counts[0], "reports=", counts[1])
        row = db.execute(
            text(
                "SELECT meeting_title, left(coalesce(transcript_snippet,''), 48) "
                "FROM fathom_call_records ORDER BY created_at DESC LIMIT 1"
            )
        ).one()
        print("latest_meeting", row[0], "|", row[1])
    finally:
        db.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Call Library: resolve calendar sales-call toggle for a Fathom recording."""
from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple

from sqlalchemy.orm import Session

from app.core.config import settings
from app.models.client_checkin import ClientCheckIn
from app.models.fathom_call_record import FathomCallRecord


def find_nearest_checkin_for_fathom(
    db: Session,
    org_id: uuid.UUID,
    client_id: uuid.UUID,
    meeting_at: Optional[datetime],
) -> Optional[ClientCheckIn]:
    """
    Closest ClientCheckIn for this client within ±window of meeting_at
    (sales and non-sales). Used to honor the calendar Sales call toggle.
    """
    if meeting_at is None:
        return None
    window_min = int(getattr(settings, "CALL_INSIGHT_CHECKIN_WINDOW_MINUTES", 105) or 105)
    delta = timedelta(minutes=window_min)
    if meeting_at.tzinfo is None:
        meeting_at = meeting_at.replace(tzinfo=timezone.utc)

    rows = (
        db.query(ClientCheckIn)
        .filter(
            ClientCheckIn.org_id == org_id,
            ClientCheckIn.client_id == client_id,
            ClientCheckIn.cancelled == False,  # noqa: E712
            ClientCheckIn.start_time >= meeting_at - delta,
            ClientCheckIn.start_time <= meeting_at + delta,
        )
        .all()
    )
    if not rows:
        return None

    best: Optional[ClientCheckIn] = None
    best_sec: Optional[float] = None
    for r in rows:
        st = r.start_time
        if st is None:
            continue
        if st.tzinfo is None:
            st = st.replace(tzinfo=timezone.utc)
        diff = abs((st - meeting_at).total_seconds())
        if best_sec is None or diff < best_sec:
            best_sec = diff
            best = r
    return best


def resolve_call_library_analysis_kind(
    db: Session,
    org_id: uuid.UUID,
    rec: FathomCallRecord,
) -> Tuple[str, Optional[bool]]:
    """
    Return (analysis_kind, is_sales_call).

    analysis_kind: "sales" | "glance"
    - Linked check-in with is_sales_call=True → full sales audit ("sales")
    - Linked check-in with is_sales_call=False → light glance ("glance")
    - No client / no matching check-in → "glance" (full audit is sales-only)
    """
    if not rec.client_id or not rec.meeting_at:
        return "glance", None
    ci = find_nearest_checkin_for_fathom(db, org_id, rec.client_id, rec.meeting_at)
    if ci is None:
        return "glance", None
    is_sales = bool(getattr(ci, "is_sales_call", False))
    return ("sales" if is_sales else "glance"), is_sales

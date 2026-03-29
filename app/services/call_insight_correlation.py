"""Link Fathom recordings to calendar check-ins (non–sales-call heuristic)."""
from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy.orm import Session

from app.core.config import settings
from app.models.client_checkin import ClientCheckIn


def link_fathom_to_checkin(
    db: Session,
    org_id: uuid.UUID,
    client_id: uuid.UUID,
    meeting_at: Optional[datetime],
) -> Optional[uuid.UUID]:
    """
    Find ClientCheckIn: not sales call, not cancelled, start_time within ±window of meeting_at.
    If multiple, pick closest by absolute delta.
    """
    if meeting_at is None:
        return None
    window_min = int(getattr(settings, "CALL_INSIGHT_CHECKIN_WINDOW_MINUTES", 105) or 105)
    delta = timedelta(minutes=window_min)
    if meeting_at.tzinfo is None:
        meeting_at = meeting_at.replace(tzinfo=timezone.utc)

    start_low = meeting_at - delta
    start_high = meeting_at + delta

    rows = (
        db.query(ClientCheckIn)
        .filter(
            ClientCheckIn.org_id == org_id,
            ClientCheckIn.client_id == client_id,
            ClientCheckIn.is_sales_call == False,
            ClientCheckIn.cancelled == False,
            ClientCheckIn.start_time >= start_low,
            ClientCheckIn.start_time <= start_high,
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
    return best.id if best else None

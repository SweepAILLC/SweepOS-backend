"""Combined cash window helpers (Stripe + Whop + manual)."""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Tuple


def finances_period_bounds(
    scope: str | None,
    range_days: int,
    now: datetime | None = None,
) -> Tuple[datetime, datetime]:
    """
    Primary cash window [start, end] in naive UTC (matches Stripe dashboard MTD).
    - scope=mtd: calendar month start → now
    - scope=all: epoch → now (all recorded payments for the org)
    - else: rolling range_days ending now
    """
    end = now or datetime.utcnow()
    mtd_start = end.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if scope == "mtd":
        return mtd_start, end
    if scope == "all":
        return datetime(1970, 1, 1), end
    return end - timedelta(days=range_days), end

"""Shared date-range filter: inclusive org-local dates -> naive-UTC half-open windows."""
from datetime import date, datetime
from unittest.mock import MagicMock
from zoneinfo import ZoneInfo

import pytest
from fastapi import HTTPException

from app.services import date_window as dw


def test_window_converts_local_days_to_utc():
    ny = ZoneInfo("America/New_York")
    lo, hi = dw.window_from_dates(date(2026, 9, 1), date(2026, 9, 30), ny)
    assert lo == datetime(2026, 9, 1, 4, 0)  # EDT midnight
    assert hi == datetime(2026, 10, 1, 4, 0)  # exclusive: day after end


def test_all_history_and_validation():
    utc = ZoneInfo("UTC")
    lo, hi = dw.window_from_dates(None, date(2026, 9, 26), utc)
    assert lo == dw.EPOCH and hi == datetime(2026, 9, 27)
    with pytest.raises(HTTPException):
        dw.window_from_dates(date(2026, 9, 2), date(2026, 9, 1), utc)
    with pytest.raises(HTTPException):
        dw.window_from_dates(date(2010, 1, 1), date(2026, 1, 1), utc)


def test_explicit_window_params():
    db = MagicMock()
    db.query.return_value.filter.return_value.scalar.return_value = "UTC"
    assert dw.explicit_window(db, "org", None, None) is None  # legacy params apply
    assert dw.explicit_window(db, "org", "2026-09-01", "2026-09-01") == (datetime(2026, 9, 1), datetime(2026, 9, 2))
    with pytest.raises(HTTPException):
        dw.explicit_window(db, "org", "2026-09-01", None)
    with pytest.raises(HTTPException):
        dw.explicit_window(db, "org", "09/01/2026", "2026-09-02")

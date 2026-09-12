"""First conversion = one KPI close. Form + payment must not double-count."""
from datetime import date
from uuid import uuid4

from app.services.kpi_integration_sync import (
    _min_date_map,
    count_conversions_on_day,
)


def test_min_date_map_picks_earliest_per_client():
    a, b = uuid4(), uuid4()
    pay = {a: date(2026, 9, 10), b: date(2026, 9, 12)}
    form = {a: date(2026, 9, 8), b: date(2026, 9, 12)}
    closed = {a: date(2026, 9, 8)}
    merged = _min_date_map(pay, form, closed)
    assert merged[a] == date(2026, 9, 8)
    assert merged[b] == date(2026, 9, 12)


def test_form_then_payment_counts_once_on_form_day():
    cid = uuid4()
    conv = _min_date_map(
        {cid: date(2026, 9, 10)},
        {cid: date(2026, 9, 8)},
    )
    assert count_conversions_on_day(conv, date(2026, 9, 8)) == 1
    assert count_conversions_on_day(conv, date(2026, 9, 10)) == 0


def test_payment_then_form_counts_once_on_payment_day():
    cid = uuid4()
    conv = _min_date_map(
        {cid: date(2026, 9, 8)},
        {cid: date(2026, 9, 10)},
    )
    assert count_conversions_on_day(conv, date(2026, 9, 8)) == 1
    assert count_conversions_on_day(conv, date(2026, 9, 10)) == 0


def test_two_clients_same_day():
    a, b = uuid4(), uuid4()
    conv = _min_date_map({a: date(2026, 9, 9), b: date(2026, 9, 9)})
    assert count_conversions_on_day(conv, date(2026, 9, 9)) == 2

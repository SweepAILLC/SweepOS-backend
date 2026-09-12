"""Failed-payment date window stays on latest_attempt_at."""
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from app.api.stripe import (
    _filter_failed_payments_by_window,
    _fold_payment_max_ts,
    _fold_payment_min_ts,
    _merge_failed_with_whop,
    _paginate_merged_payment_responses,
    _payments_window_from_params,
)
from app.schemas.stripe import StripeFailedPaymentResponse, StripePaymentResponse


def test_failed_window_keeps_in_range_only():
    now = datetime.now(timezone.utc)
    in_row = SimpleNamespace(latest_attempt_at=int(now.timestamp()) - 60, created_at=0)
    old = SimpleNamespace(latest_attempt_at=int((now - timedelta(days=40)).timestamp()), created_at=0)
    out = _filter_failed_payments_by_window([in_row, old], None, 30)
    assert out == [in_row]


def test_all_scope_skips_window_filter():
    row = SimpleNamespace(latest_attempt_at=1, created_at=1)
    assert _filter_failed_payments_by_window([row], None, None) == [row]


def test_payments_window_none_without_range():
    assert _payments_window_from_params(None, None) == (None, None)


def test_paginate_merged_includes_whop_rows():
    stripe = StripePaymentResponse(
        id="s1",
        stripe_id="ch_1",
        amount_cents=100,
        currency="usd",
        status="succeeded",
        created_at=100,
    )
    whop = StripePaymentResponse(
        id="w1",
        stripe_id="whop:pay_1",
        amount_cents=2500,
        currency="usd",
        status="succeeded",
        created_at=200,
        payment_method="whop",
    )
    out = _paginate_merged_payment_responses([stripe], [], 1, 20, extra_rows=[whop])
    assert [p.stripe_id for p in out] == ["whop:pay_1", "ch_1"]


def test_fold_first_and_last_paid_timestamps():
    first = {}
    _fold_payment_min_ts(first, "c1", datetime(2026, 1, 10))
    _fold_payment_min_ts(first, "c1", datetime(2026, 1, 2))
    assert first["c1"] == datetime(2026, 1, 2)
    last = {}
    _fold_payment_max_ts(last, "c1", datetime(2026, 1, 2))
    _fold_payment_max_ts(last, "c1", datetime(2026, 1, 10))
    assert last["c1"] == datetime(2026, 1, 10)


def test_merge_failed_queue_keeps_whop_rows():
    stripe_row = StripeFailedPaymentResponse(
        id="s1",
        stripe_id="ch_fail",
        amount_cents=100,
        currency="usd",
        status="failed",
        created_at=100,
        has_recovery_recommendation=False,
        attempt_count=1,
        first_attempt_at=100,
        latest_attempt_at=100,
    )
    whop_row = StripeFailedPaymentResponse(
        id="w1",
        stripe_id="whop:pay_fail",
        amount_cents=2500,
        currency="usd",
        status="failed",
        created_at=200,
        has_recovery_recommendation=False,
        attempt_count=1,
        first_attempt_at=200,
        latest_attempt_at=200,
    )
    merged = _merge_failed_with_whop([stripe_row], [whop_row])
    assert [r.stripe_id for r in merged] == ["whop:pay_fail", "ch_fail"]

"""Leaderboard cash maps use Finances combined rules."""
from uuid import uuid4

from app.services.finances_cash import _merge_usd


def test_merge_usd_sums_sources():
    org = uuid4()
    other = uuid4()
    stripe = {org: 100.0}
    whop = {org: 25.5, other: 10.0}
    manual = {org: 4.5}
    merged = _merge_usd(stripe, whop, manual)
    assert merged[org] == 130.0
    assert merged[other] == 10.0

"""Manual payment cash vs revenue mapping."""
from fastapi import HTTPException
import pytest

from app.api.clients.payments import _manual_revenue_cents


def test_revenue_defaults_to_cash():
    assert _manual_revenue_cents(None, 12_500) == 12_500


def test_revenue_explicit_dollars():
    assert _manual_revenue_cents(2000.0, 500_00) == 200_000


def test_revenue_rejects_negative():
    with pytest.raises(HTTPException) as exc:
        _manual_revenue_cents(-1.0, 100)
    assert exc.value.status_code == 400

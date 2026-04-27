"""Whop sync helpers and finances routes (unauthenticated)."""
import uuid
from datetime import datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient

from app.main import app
from app.models.oauth_token import OAuthToken
from app.models.whop_payment import WhopPayment
from app.services import whop_sync

client = TestClient(app)


def test_whop_amount_cents_usd_total():
    item = {"currency": "usd", "usd_total": 12.34, "total": 99.0}
    cents = whop_sync._amount_cents(item)
    assert cents == int((Decimal("12.34") * Decimal(100)).quantize(Decimal("1")))


def test_whop_payer_email_nested():
    item = {"member": {"user": {"email": "buyer@example.com"}}}
    assert whop_sync._payer_email(item) == "buyer@example.com"


def test_finances_summary_requires_auth():
    r = client.get("/integrations/finances/summary")
    assert r.status_code in (401, 403)


def test_whop_status_requires_auth():
    r = client.get("/integrations/whop/status")
    assert r.status_code in (401, 403)


@patch("app.services.whop_sync.decrypt_token", return_value="apikey")
@patch("app.services.whop_client.list_payments_page")
def test_sync_whop_incremental_inserts_then_updates(mock_list, _mock_decrypt):
    """First run inserts a WhopPayment row; second run updates the same row (idempotent upsert)."""
    org_id = uuid.uuid4()
    item = {
        "id": "pay_abc",
        "status": "paid",
        "currency": "usd",
        "usd_total": "10.00",
        "paid_at": "2026-03-01T12:00:00Z",
    }
    mock_list.return_value = ([item], {"has_next_page": False})

    token = MagicMock(account_id="biz_1", last_sync_at=None, access_token="x")
    existing_holder = {"row": None}

    def query_side_effect(model):
        q = MagicMock()
        if model is OAuthToken:
            q.filter.return_value.first.return_value = token
        elif model is WhopPayment:
            q.filter.return_value.first.return_value = existing_holder["row"]
        return q

    db = MagicMock()
    db.query.side_effect = query_side_effect

    def capture_add(obj):
        if isinstance(obj, WhopPayment):
            existing_holder["row"] = obj

    db.add.side_effect = capture_add

    r1 = whop_sync.sync_whop_incremental(db, org_id, force_full=True)
    assert r1["payments_upserted"] == 1
    row = existing_holder["row"]
    assert isinstance(row, WhopPayment)
    assert row.amount_cents == 1000
    db.commit.assert_called()

    mock_list.return_value = (
        [
            {
                "id": "pay_abc",
                "status": "paid",
                "currency": "usd",
                "usd_total": "25.50",
                "paid_at": "2026-03-01T12:00:00Z",
            }
        ],
        {"has_next_page": False},
    )
    r2 = whop_sync.sync_whop_incremental(db, org_id, force_full=True)
    assert r2["payments_upserted"] == 1
    assert row.amount_cents == 2550


def test_stripe_succeeded_cents_windows_dedupes_latest():
    from app.api import finances

    now = datetime(2026, 3, 20, 12, 0, 0)
    thirty_start = now.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=30)
    mtd_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    rows = [
        SimpleNamespace(
            stripe_id="ch_1",
            created_at=now - timedelta(days=1),
            amount_cents=5000,
            status="succeeded",
        ),
        SimpleNamespace(
            stripe_id="ch_1",
            created_at=now - timedelta(days=2),
            amount_cents=1000,
            status="succeeded",
        ),
        SimpleNamespace(
            stripe_id="ch_2",
            created_at=now - timedelta(days=40),
            amount_cents=999,
            status="succeeded",
        ),
    ]
    db = MagicMock()
    q = MagicMock()
    db.query.return_value = q
    q.filter.return_value = q
    q.order_by.return_value = q
    q.all.return_value = rows

    org_id = uuid.uuid4()
    c30, cmtd = finances._stripe_succeeded_cents_windows(db, org_id, thirty_start, mtd_start)
    assert c30 == 5000
    assert cmtd == 5000


def test_whop_paid_cents_windows_counts_paid_only():
    from app.api import finances

    now = datetime(2026, 3, 15, 12, 0, 0)
    thirty_start = now.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=30)
    mtd_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    rows = [
        SimpleNamespace(status="paid", created_at=now - timedelta(days=5), amount_cents=1000),
        SimpleNamespace(status="pending", created_at=now - timedelta(days=5), amount_cents=5000),
        SimpleNamespace(status="paid", created_at=now - timedelta(days=40), amount_cents=2000),
    ]
    db = MagicMock()
    q = MagicMock()
    db.query.return_value = q
    q.filter.return_value = q
    q.all.return_value = rows

    org_id = uuid.uuid4()
    c30, cmtd = finances._whop_paid_cents_windows(db, org_id, thirty_start, mtd_start)
    assert c30 == 1000
    assert cmtd == 1000

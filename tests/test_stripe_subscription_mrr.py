"""Subscription events for a known client: the client's estimated MRR sum used
`db.func.sum`, but Session has no `func`, so every such event raised before commit
and the subscription update was lost (live webhooks and inbox retries alike)."""
import uuid
from decimal import Decimal
from unittest.mock import MagicMock

from sqlalchemy.orm import Session

from app.models.client import Client
from app.models.stripe_subscription import StripeSubscription
from app.services.stripe_processor import _process_subscription_event


def _db(client, existing_sub, mrr_total):
    # spec=Session: a plain MagicMock would invent `db.func` and hide the bug.
    db = MagicMock(spec=Session)
    mrr_query = MagicMock()
    mrr_query.filter.return_value.with_entities.return_value.scalar.return_value = mrr_total

    def query(entity):
        q = MagicMock()
        if entity is Client:
            q.filter.return_value.first.return_value = client
            return q
        if entity is StripeSubscription:
            q.filter.return_value.first.return_value = existing_sub
            q.filter.return_value.with_entities.return_value.scalar.return_value = mrr_total
            return q
        return q

    db.query.side_effect = query
    return db


def test_known_client_subscription_updates_mrr_and_commits():
    org = uuid.uuid4()
    client = Client(id=uuid.uuid4(), org_id=org, stripe_customer_id="cus_1", estimated_mrr=0)
    db = _db(client, existing_sub=None, mrr_total=Decimal("300"))
    data = {
        "id": "sub_1",
        "customer": "cus_1",
        "status": "active",
        "items": {"data": [{"price": {"unit_amount": 30000, "recurring": {"interval": "month"}}, "quantity": 1}]},
    }
    _process_subscription_event(db, data, "customer.subscription.created", org)
    assert client.estimated_mrr == Decimal("300")
    db.commit.assert_called_once()


def test_no_matching_client_still_commits_subscription():
    org = uuid.uuid4()
    db = _db(None, existing_sub=None, mrr_total=None)
    _process_subscription_event(db, {"id": "sub_2", "customer": "cus_x", "status": "active"}, "customer.subscription.updated", org)
    db.commit.assert_called_once()

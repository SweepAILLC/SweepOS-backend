"""EOD setter picker: booked-clients lookup, claim resolution, and additive
merge of setter_booked_client_ids on survey-style submissions."""
import uuid
from datetime import date
from types import SimpleNamespace
from unittest.mock import MagicMock

from app.api.kpi import _merge_additive_payload
from app.services.kpi_integration_sync import find_setter_claim_for_client


class TestMergeSetterBookedClientIds:
    def test_union_of_existing_and_incoming_no_duplicates(self):
        row = SimpleNamespace(setter_booked_client_ids=["a", "b"])
        merged = _merge_additive_payload(row, {"setter_booked_client_ids": ["b", "c"]})
        assert merged["setter_booked_client_ids"] == ["a", "b", "c"]

    def test_none_existing_starts_fresh(self):
        row = SimpleNamespace(setter_booked_client_ids=None)
        merged = _merge_additive_payload(row, {"setter_booked_client_ids": ["a"]})
        assert merged["setter_booked_client_ids"] == ["a"]


class TestFindSetterClaimForClient:
    def test_returns_rep_user_id_when_client_claimed(self):
        rep_id = uuid.uuid4()
        cid = uuid.uuid4()
        row = SimpleNamespace(
            rep_user_id=rep_id, setter_booked_client_ids=[str(cid)], entry_date=date.today()
        )
        db = MagicMock()
        db.query.return_value.filter.return_value.order_by.return_value.all.return_value = [row]
        assert find_setter_claim_for_client(db, uuid.uuid4(), cid) == rep_id

    def test_returns_none_when_not_claimed(self):
        db = MagicMock()
        db.query.return_value.filter.return_value.order_by.return_value.all.return_value = []
        assert find_setter_claim_for_client(db, uuid.uuid4(), uuid.uuid4()) is None

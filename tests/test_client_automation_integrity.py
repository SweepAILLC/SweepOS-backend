"""find_active_sale_closed_mismatches: flags ACTIVE clients whose latest sales
call never got stamped sale_closed (the CalendarBookingSales/EventTypeSalesCall
tagging drift documented in the pipeline-movement audit)."""
import uuid
from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock

from app.services.client_automation import find_active_sale_closed_mismatches


def _query_mock(client_rows, checkin_rows):
    """db.query(Client)... -> client_rows; db.query(ClientCheckIn)... -> checkin_rows."""

    def query_side_effect(model):
        q = MagicMock()
        owner = getattr(model, "class_", model)
        if owner.__name__ == "Client":
            q.filter.return_value.all.return_value = client_rows
        else:
            q.filter.return_value.all.return_value = checkin_rows
        return q

    db = MagicMock()
    db.query.side_effect = query_side_effect
    return db


def _checkin(client_id, *, start_time, sale_closed):
    return SimpleNamespace(client_id=client_id, start_time=start_time, sale_closed=sale_closed)


class TestFindActiveSaleClosedMismatches:
    def test_no_active_clients_returns_empty(self):
        db = _query_mock(client_rows=[], checkin_rows=[])
        assert find_active_sale_closed_mismatches(db, uuid.uuid4()) == []

    def test_flags_active_client_whose_latest_call_not_closed(self):
        cid = uuid.uuid4()
        now = datetime.utcnow()
        db = _query_mock(
            client_rows=[(cid,)],
            checkin_rows=[_checkin(cid, start_time=now - timedelta(days=1), sale_closed=False)],
        )
        assert find_active_sale_closed_mismatches(db, uuid.uuid4()) == [cid]

    def test_does_not_flag_when_latest_call_is_closed(self):
        cid = uuid.uuid4()
        now = datetime.utcnow()
        db = _query_mock(
            client_rows=[(cid,)],
            checkin_rows=[_checkin(cid, start_time=now - timedelta(days=1), sale_closed=True)],
        )
        assert find_active_sale_closed_mismatches(db, uuid.uuid4()) == []

    def test_uses_the_most_recent_call_not_an_older_unclosed_one(self):
        cid = uuid.uuid4()
        now = datetime.utcnow()
        db = _query_mock(
            client_rows=[(cid,)],
            checkin_rows=[
                _checkin(cid, start_time=now - timedelta(days=30), sale_closed=False),
                _checkin(cid, start_time=now - timedelta(days=1), sale_closed=True),
            ],
        )
        assert find_active_sale_closed_mismatches(db, uuid.uuid4()) == []

    def test_ignores_future_calls(self):
        cid = uuid.uuid4()
        now = datetime.utcnow()
        db = _query_mock(
            client_rows=[(cid,)],
            checkin_rows=[_checkin(cid, start_time=now + timedelta(days=1), sale_closed=False)],
        )
        assert find_active_sale_closed_mismatches(db, uuid.uuid4()) == []

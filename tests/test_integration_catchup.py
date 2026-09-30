"""Worker catch-up pulls: calendar + Whop run per org, skip fresh orgs, isolate failures."""
import uuid
from datetime import datetime, timedelta
from types import SimpleNamespace

from app.services import integration_catchup as ic


class _Query:
    def __init__(self, rows):
        self._rows = rows

    def filter(self, *a, **k):
        return self

    def order_by(self, *a, **k):
        return self

    def all(self):
        return self._rows

    def first(self):
        return self._rows[0] if self._rows else None


class _Session:
    """Answers each query with the rows registered for its first entity (column key or model name)."""

    def __init__(self, rows_by_entity):
        self.rows_by_entity = rows_by_entity

    def query(self, entity, *rest):
        name = getattr(entity, "key", None) or getattr(entity, "__name__", None)
        return _Query(self.rows_by_entity.get(name, []))

    def rollback(self):
        pass

    def close(self):
        pass


def test_whop_catchup_skips_fresh_and_syncs_stale(monkeypatch):
    fresh_org, stale_org = uuid.uuid4(), uuid.uuid4()
    tokens = {
        fresh_org: SimpleNamespace(last_sync_at=datetime.utcnow() - timedelta(minutes=1), last_webhook_processed_at=None),
        stale_org: SimpleNamespace(last_sync_at=datetime.utcnow() - timedelta(hours=1), last_webhook_processed_at=None),
    }
    calls = iter([None, fresh_org, stale_org])  # listing session, then one session per org

    def fake_session():
        org = next(calls)
        if org is None:
            return _Session({"org_id": [(fresh_org,), (stale_org,)]})
        return _Session({"OAuthToken": [tokens[org]]})

    synced = []
    monkeypatch.setattr(ic, "SessionLocal", fake_session)
    monkeypatch.setattr(
        "app.services.whop_sync.sync_whop_incremental",
        lambda db, org_id, force_full: synced.append(org_id) or {"payments_upserted": 0},
    )
    stats = ic.catchup_whop_for_all_orgs()
    assert synced == [stale_org]
    assert stats == {"orgs": 2, "synced": 1, "failed": 0, "skipped": 1}


def test_calendar_catchup_isolates_org_failures(monkeypatch):
    ok_org, bad_org = sorted([uuid.uuid4(), uuid.uuid4()], key=str)
    member = uuid.uuid4()
    calls = iter([None, ok_org, bad_org])

    def fake_session():
        org = next(calls)
        if org is None:
            return _Session({"org_id": [(ok_org,), (bad_org,)]})
        return _Session({"user_id": [(member,)]})

    def fake_sync(db, org_id, user_id):
        assert user_id == member
        if org_id == bad_org:
            raise RuntimeError("calendar down")
        return {"new_bookings_calcom": 2, "new_bookings_calendly": 1}

    monkeypatch.setattr(ic, "SessionLocal", fake_session)
    monkeypatch.setattr("app.services.checkin_sync.sync_all_checkins", fake_sync)
    monkeypatch.setattr(
        "app.services.terminal_metrics_service.invalidate_terminal_monthly_trends_cache", lambda org_id: None
    )
    stats = ic.catchup_calendar_for_all_orgs()
    assert stats == {"orgs": 2, "synced": 1, "failed": 1, "skipped": 0, "new_bookings": 3}

"""Messaging Matrix routes: tier gate, admin gate, org from session, error mapping."""
import uuid
from unittest.mock import MagicMock

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from app.api import messaging_matrix as api
from app.api.deps import require_admin_or_owner
from app.api.portal import require_consulting_org_id
from app.db.session import get_db

ORG = uuid.uuid4()


def _client(*, consulting=True, admin=True):
    app = FastAPI()
    app.include_router(api.router, prefix="/content-studio/messaging-matrix")
    app.dependency_overrides[get_db] = lambda: MagicMock()

    def org_dep():
        if not consulting:
            raise HTTPException(status_code=403, detail="Consulting portal requires a consulting tier")
        return ORG

    def admin_dep():
        if not admin:
            raise HTTPException(status_code=403, detail="Admin or owner access required")
        return MagicMock(id=uuid.uuid4())

    app.dependency_overrides[require_consulting_org_id] = org_dep
    app.dependency_overrides[require_admin_or_owner] = admin_dep
    return TestClient(app)


@pytest.fixture(autouse=True)
def _no_rate_limit(monkeypatch):
    monkeypatch.setattr(api, "check_sliding_window", lambda *a, **k: None)


def test_list_uses_session_org_and_filters(monkeypatch):
    seen = {}

    def fake_list(db, org_id, **kw):
        seen.update(org_id=org_id, **kw)
        return {"angles": []}

    monkeypatch.setattr(api.mm, "list_matrix", fake_list)
    r = _client().get("/content-studio/messaging-matrix?channel=paid&min_samples=2")
    assert r.status_code == 200
    assert seen["org_id"] == ORG
    assert seen["channel"] == "paid" and seen["min_samples"] == 2


def test_non_consulting_org_is_blocked():
    assert _client(consulting=False).get("/content-studio/messaging-matrix").status_code == 403


def test_bad_channel_is_rejected():
    assert _client().get("/content-studio/messaging-matrix?channel=tiktok").status_code == 422


def test_refresh_needs_admin_and_queues_once(monkeypatch):
    queued = []
    monkeypatch.setattr(api, "schedule_background_work", lambda fn, bt, org, **kw: queued.append(org))
    monkeypatch.setattr(api.mm, "mark_queued", lambda org: None)
    monkeypatch.setattr(api.mm, "get_refresh_status", lambda org: {"state": "queued"})

    monkeypatch.setattr(api.mm, "is_refresh_active", lambda org: False)
    assert _client(admin=False).post("/content-studio/messaging-matrix/refresh").status_code == 403
    r = _client().post("/content-studio/messaging-matrix/refresh")
    assert r.status_code == 202 and r.json()["queued"] is True
    assert queued == [str(ORG)]

    monkeypatch.setattr(api.mm, "is_refresh_active", lambda org: True)
    r = _client().post("/content-studio/messaging-matrix/refresh")
    assert r.json()["queued"] is False
    assert queued == [str(ORG)]


def test_angle_not_found_is_404(monkeypatch):
    def missing(db, org_id, angle_id, **kw):
        raise LookupError("Messaging angle not found")

    monkeypatch.setattr(api.mm, "angle_detail", missing)
    monkeypatch.setattr(api.mm, "recreate_angle", missing)
    c = _client()
    aid = uuid.uuid4()
    assert c.get(f"/content-studio/messaging-matrix/angles/{aid}").status_code == 404
    assert c.post(f"/content-studio/messaging-matrix/angles/{aid}/recreate", json={}).status_code == 404


def test_archive_needs_admin_and_valid_status(monkeypatch):
    monkeypatch.setattr(api.mm, "set_angle_status", lambda db, org, aid, s: {"status": s})
    aid = uuid.uuid4()
    assert _client(admin=False).patch(f"/content-studio/messaging-matrix/angles/{aid}", json={"status": "archived"}).status_code == 403
    assert _client().patch(f"/content-studio/messaging-matrix/angles/{aid}", json={"status": "deleted"}).status_code == 422
    r = _client().patch(f"/content-studio/messaging-matrix/angles/{aid}", json={"status": "archived"})
    assert r.json() == {"status": "archived"}


@pytest.mark.parametrize(
    "exc,code",
    [
        (ValueError("This angle has no transcripts to build from yet"), 400),
        (RuntimeError("LLM is not configured"), 503),
        (RuntimeError("The draft did not meet the quality bar"), 502),
    ],
)
def test_recreate_error_mapping(monkeypatch, exc, code):
    def boom(*a, **k):
        raise exc

    monkeypatch.setattr(api.mm, "recreate_angle", boom)
    r = _client().post(f"/content-studio/messaging-matrix/angles/{uuid.uuid4()}/recreate", json={"channel": "paid"})
    assert r.status_code == code


def test_recreate_requires_admin():
    r = _client(admin=False).post(f"/content-studio/messaging-matrix/angles/{uuid.uuid4()}/recreate", json={})
    assert r.status_code == 403

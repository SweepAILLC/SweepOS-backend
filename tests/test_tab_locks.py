"""Org tab locks: base tabs are always on, only lockable tabs/modules honour org rows."""
import uuid
from types import SimpleNamespace

from app.api import deps
from app.api.users import ALWAYS_ON_TABS, AVAILABLE_TABS


class _Row:
    def __init__(self, enabled):
        self.enabled = enabled


class _FakeDb:
    """Every org-permission lookup returns `row` (None = no explicit row)."""

    def __init__(self, row):
        self._row = row

    def query(self, *_a, **_k):
        return self

    def filter(self, *_a, **_k):
        return self

    def first(self):
        return self._row


def _user():
    org = uuid.uuid4()
    return SimpleNamespace(id=uuid.uuid4(), org_id=org, selected_org_id=org, role="member")


def test_lists_do_not_overlap():
    assert not set(ALWAYS_ON_TABS) & set(AVAILABLE_TABS)


def test_base_tabs_ignore_disabled_rows():
    user, db = _user(), _FakeDb(_Row(False))
    for tab in ALWAYS_ON_TABS:
        assert deps.check_tab_access(tab, user, db) is True


def test_lockable_tabs_honour_rows():
    user = _user()
    for tab in AVAILABLE_TABS:
        assert deps.check_tab_access(tab, user, _FakeDb(_Row(False))) is False
        assert deps.check_tab_access(tab, user, _FakeDb(_Row(True))) is True
        assert deps.check_tab_access(tab, user, _FakeDb(None)) is True


def test_permissions_map_has_everything():
    perms = deps.get_user_tab_permissions(_user(), _FakeDb(_Row(False)))
    for tab in ALWAYS_ON_TABS:
        assert perms[tab] is True
    for tab in AVAILABLE_TABS:
        assert perms[tab] is False
    assert perms["settings"] is True


def test_locked_category_slugs():
    from app.api.deps import MAIN_ORG_ID
    from app.services import sop_categories as sc

    class _Q:
        def __init__(self, rows):
            self.rows = rows

        def filter(self, *_a, **_k):
            return self

        def all(self):
            return self.rows

    class _Db:
        def query(self, *_a, **_k):
            return _Q([("sop:sales",), ("sop:ops",)])

    assert sc.locked_category_slugs(_Db(), uuid.uuid4()) == {"sales", "ops"}
    assert sc.locked_category_slugs(_Db(), MAIN_ORG_ID) == set()


def test_locked_doc_summary_leaks_nothing():
    from app.api.resources import _locked_doc_summary

    out = _locked_doc_summary(
        {
            "resource_id": "r1",
            "sop_category": "sales",
            "title": "Secret playbook",
            "description": "private",
            "video_url": "https://loom.com/share/x",
            "video_urls": ["https://loom.com/share/x"],
            "sort_order": 3,
        }
    )
    assert out["locked"] is True
    assert out["title"] == "Locked SOP"
    assert out["description"] == "" and out["video_urls"] == [] and out["video_url"] is None
    assert out["sop_category"] == "sales" and out["sort_order"] == 3


def _resources_with_locks(monkeypatch, locked):
    from app.api import resources as r

    docs = [
        {"resource_id": "a", "sop_category": "sales", "title": "Sales A", "description": "d", "sort_order": 1},
        {"resource_id": "b", "sop_category": "ops", "title": "Ops B", "description": "d", "sort_order": 2},
    ]
    monkeypatch.setattr(r, "ensure_resource_documents_table", lambda db: None)
    monkeypatch.setattr(r, "list_docs", lambda db, org: docs)
    monkeypatch.setattr(r, "get_doc", lambda db, org, rid: next((d for d in docs if d["resource_id"] == rid), None))
    monkeypatch.setattr(r.sop_cats, "locked_category_slugs", lambda db, org: set(locked))
    monkeypatch.setattr(
        r.sop_cats,
        "list_categories",
        lambda db: [{"slug": "sales", "label": "Sales"}, {"slug": "ops", "label": "Ops"}],
    )
    return r, _user()


def test_docs_endpoint_hides_or_redacts_locked(monkeypatch):
    r, user = _resources_with_locks(monkeypatch, {"sales"})
    visible = r.get_docs(include_locked=False, db=None, current_user=user)
    assert [d["resource_id"] for d in visible] == ["b"]
    with_locked = r.get_docs(include_locked=True, db=None, current_user=user)
    by_id = {d["resource_id"]: d for d in with_locked}
    assert by_id["a"]["locked"] is True and by_id["a"]["title"] == "Locked SOP"
    assert by_id["b"]["title"] == "Ops B" and not by_id["b"].get("locked")


def test_doc_fetch_403_when_locked_and_categories_flagged(monkeypatch):
    import pytest
    from fastapi import HTTPException

    r, user = _resources_with_locks(monkeypatch, {"sales"})
    with pytest.raises(HTTPException) as e:
        r.get_doc_document("a", db=None, current_user=user)
    assert e.value.status_code == 403
    assert r.get_doc_document("b", db=None, current_user=user)["resource_id"] == "b"
    cats = {c["slug"]: c["locked"] for c in r.list_sop_categories(db=None, current_user=user)}
    assert cats == {"sales": True, "ops": False}

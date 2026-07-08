"""Unit tests for built-in resource documents and slug helpers."""
import hashlib
import uuid
from unittest.mock import MagicMock

from app.services.resource_documents import (
    BUILTIN_DOCS,
    _BUILTIN_IDS,
    _default_for,
    _slugify,
    get_document_content,
    sop_content_fingerprint,
)


class TestSlugify:
    def test_lowercases_and_hyphenates(self):
        assert _slugify("Discovery Call Audit") == "discovery-call-audit"

    def test_strips_non_alphanumeric(self):
        assert _slugify("  Hello!!! World???  ") == "hello-world"

    def test_empty_fallback(self):
        assert _slugify("!!!") == "sop"


class TestBuiltinCatalog:
    def test_all_builtins_have_unique_ids(self):
        ids = [d["resource_id"] for d in BUILTIN_DOCS]
        assert len(ids) == len(set(ids))

    def test_call_library_sops_exist(self):
        required = {"discovery-audit-sop", "pitching-sop", "objection-handling-sop"}
        assert required.issubset(_BUILTIN_IDS)

    def test_default_for_known_id(self):
        doc = _default_for("discovery-audit-sop")
        assert doc is not None
        assert doc["category"] == "SOP"
        assert doc["file_name"].endswith(".md")

    def test_default_for_unknown_returns_none(self):
        assert _default_for("does-not-exist") is None


class TestDocumentContentFallback:
    def test_get_document_content_uses_builtin_file_when_db_empty(self):
        db = MagicMock()
        db.execute.return_value.fetchone.return_value = None
        org_id = uuid.uuid4()
        content = get_document_content(db, org_id, "discovery-audit-sop")
        assert isinstance(content, str)
        # Built-in discovery SOP should mention discovery scoring themes
        assert len(content) > 100

    def test_sop_fingerprint_is_stable_for_same_content(self):
        db = MagicMock()
        db.execute.return_value.fetchone.return_value = None
        org_id = uuid.uuid4()
        ids = ["discovery-audit-sop", "pitching-sop"]
        a = sop_content_fingerprint(db, org_id, ids)
        b = sop_content_fingerprint(db, org_id, ids)
        assert a == b
        assert len(a) == 16

    def test_fingerprint_changes_when_resource_set_changes(self):
        db = MagicMock()
        db.execute.return_value.fetchone.return_value = None
        org_id = uuid.uuid4()
        one = sop_content_fingerprint(db, org_id, ["discovery-audit-sop"])
        two = sop_content_fingerprint(db, org_id, ["pitching-sop"])
        assert one != two

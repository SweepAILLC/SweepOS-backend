"""Tests for Call Library sales vs glance routing gate."""
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock
from uuid import uuid4

from app.services.call_library_sales_gate import resolve_call_library_analysis_kind


def test_unlinked_fathom_defaults_to_glance():
    db = MagicMock()
    rec = SimpleNamespace(client_id=None, meeting_at=datetime.now(timezone.utc))
    kind, flag = resolve_call_library_analysis_kind(db, uuid4(), rec)
    assert kind == "glance"
    assert flag is None


def test_sales_checkin_resolves_sales(monkeypatch):
    from app.services import call_library_sales_gate as gate

    ci = SimpleNamespace(is_sales_call=True)
    monkeypatch.setattr(gate, "find_nearest_checkin_for_fathom", lambda *a, **k: ci)
    db = MagicMock()
    rec = SimpleNamespace(client_id=uuid4(), meeting_at=datetime.now(timezone.utc))
    kind, flag = resolve_call_library_analysis_kind(db, uuid4(), rec)
    assert kind == "sales"
    assert flag is True


def test_nonsales_checkin_resolves_glance(monkeypatch):
    from app.services import call_library_sales_gate as gate

    ci = SimpleNamespace(is_sales_call=False)
    monkeypatch.setattr(gate, "find_nearest_checkin_for_fathom", lambda *a, **k: ci)
    db = MagicMock()
    rec = SimpleNamespace(client_id=uuid4(), meeting_at=datetime.now(timezone.utc))
    kind, flag = resolve_call_library_analysis_kind(db, uuid4(), rec)
    assert kind == "glance"
    assert flag is False

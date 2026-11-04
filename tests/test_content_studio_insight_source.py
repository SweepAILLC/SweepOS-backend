"""Unit tests for content_studio_fathom_context._insight_source — the BOF citation
attribution helper. Covers two bugs fixed together: (1) it referenced a nonexistent
Client.name field, so every citation silently fell back to "Unnamed client"; (2) a caught
lazy-load exception left the session's transaction aborted, so every later query on the
same session (collect_fathom_sales_signals makes several) failed with InFailedSqlTransaction."""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

from app.services.content_studio_fathom_context import _insight_source


def _insight(id_="i1", client=None, fathom_call_record=None):
    return SimpleNamespace(id=id_, client=client, fathom_call_record=fathom_call_record)


class TestInsightSourceClientName:
    def test_composes_first_and_last_name(self):
        client = SimpleNamespace(first_name="Anthony", last_name="Lam")
        source = _insight_source(_insight(client=client))
        assert source["client_name"] == "Anthony Lam"

    def test_falls_back_to_unnamed_client_when_no_name_fields(self):
        client = SimpleNamespace(first_name=None, last_name=None)
        source = _insight_source(_insight(client=client))
        assert source["client_name"] == "Unnamed client"

    def test_falls_back_to_unnamed_client_when_no_client(self):
        source = _insight_source(_insight(client=None))
        assert source["client_name"] == "Unnamed client"

    def test_strips_whitespace_only_name_parts(self):
        client = SimpleNamespace(first_name="  ", last_name="Lam")
        source = _insight_source(_insight(client=client))
        assert source["client_name"] == "Lam"


class TestInsightSourceFathomCallRecord:
    def test_extracts_meeting_at_recording_id_and_transcript_url(self):
        fcr = SimpleNamespace(
            meeting_at=None,
            fathom_recording_id="rec123",
            recording_url="https://fathom.video/calls/rec123",
            share_url=None,
            video_url=None,
        )
        client = SimpleNamespace(first_name="A", last_name="B")
        source = _insight_source(_insight(client=client, fathom_call_record=fcr))
        assert source["fathom_recording_id"] == "rec123"
        assert source["fathom_transcript_url"] == "https://fathom.video/calls/rec123"


class TestInsightSourceRollsBackOnLazyLoadFailure:
    def test_client_lazy_load_failure_rolls_back_session_and_does_not_raise(self, monkeypatch):
        fake_session = MagicMock()
        monkeypatch.setattr(
            "app.services.content_studio_fathom_context.object_session",
            lambda obj: fake_session,
        )

        class _Row:
            id = "i1"
            fathom_call_record = None

            @property
            def client(self):
                raise RuntimeError("lazy-load boom")

        source = _insight_source(_Row())

        assert source["client_name"] == "Unnamed client"
        fake_session.rollback.assert_called_once()

    def test_fathom_call_record_lazy_load_failure_rolls_back_session(self, monkeypatch):
        fake_session = MagicMock()
        monkeypatch.setattr(
            "app.services.content_studio_fathom_context.object_session",
            lambda obj: fake_session,
        )

        class _Row:
            id = "i1"
            client = None

            @property
            def fathom_call_record(self):
                raise RuntimeError("lazy-load boom")

        source = _insight_source(_Row())

        assert source["fathom_recording_id"] is None
        fake_session.rollback.assert_called_once()

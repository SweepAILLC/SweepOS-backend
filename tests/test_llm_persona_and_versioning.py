"""SweepBot persona text and prompt_version plumbing through record_llm_usage."""
from unittest.mock import MagicMock, patch

from app.core.ai_persona import SWEEPBOT_SYSTEM
from app.services.llm_usage import record_llm_usage


class TestSweepBotPersona:
    def test_names_sweepbot_not_kai(self):
        assert "SweepBot" in SWEEPBOT_SYSTEM
        assert "Kai" not in SWEEPBOT_SYSTEM

    def test_starts_with_required_opening(self):
        assert SWEEPBOT_SYSTEM.startswith(
            "You are SweepBot, the AI Growth Engine for Sweep Coach OS."
        )

    def test_retains_core_principles(self):
        # These lines are copied verbatim from LLM.md — a rename should never
        # quietly drop the actual persona rules.
        for line in (
            "Be warm, professional, and coach-like in tone",
            "Always cite sources and evidence for claims",
            "You are NOT a replacement for human judgment.",
        ):
            assert line in SWEEPBOT_SYSTEM


class TestRecordLlmUsagePromptVersion:
    def test_prompt_version_is_stored_on_the_row(self):
        captured = {}

        class FakeEvent:
            def __init__(self, **kwargs):
                captured.update(kwargs)

        fake_db = MagicMock()
        fake_session_local = MagicMock(return_value=fake_db)

        with patch("app.db.session.SessionLocal", fake_session_local), patch(
            "app.models.llm_usage_event.LlmUsageEvent", FakeEvent
        ):
            record_llm_usage(
                org_id=__import__("uuid").uuid4(),
                provider="openai",
                model="gpt-4o-mini",
                feature="ai_recommendation",
                prompt_version="v1.0",
                prompt_tokens=100,
                completion_tokens=50,
                total_tokens=150,
            )

        assert captured.get("prompt_version") == "v1.0"
        fake_db.add.assert_called_once()
        fake_db.commit.assert_called_once()

    def test_missing_prompt_version_stores_none_not_empty_string(self):
        captured = {}

        class FakeEvent:
            def __init__(self, **kwargs):
                captured.update(kwargs)

        fake_db = MagicMock()
        fake_session_local = MagicMock(return_value=fake_db)

        with patch("app.db.session.SessionLocal", fake_session_local), patch(
            "app.models.llm_usage_event.LlmUsageEvent", FakeEvent
        ):
            record_llm_usage(
                org_id=__import__("uuid").uuid4(),
                provider="openai",
                model="gpt-4o-mini",
                feature="call_insight",
                prompt_tokens=10,
                completion_tokens=5,
                total_tokens=15,
            )

        assert captured.get("prompt_version") is None

    def test_prompt_version_truncated_to_column_width(self):
        captured = {}

        class FakeEvent:
            def __init__(self, **kwargs):
                captured.update(kwargs)

        fake_db = MagicMock()
        fake_session_local = MagicMock(return_value=fake_db)

        with patch("app.db.session.SessionLocal", fake_session_local), patch(
            "app.models.llm_usage_event.LlmUsageEvent", FakeEvent
        ):
            record_llm_usage(
                org_id=__import__("uuid").uuid4(),
                provider="openai",
                model="gpt-4o-mini",
                feature="content_studio",
                prompt_version="v1.0-this-is-way-too-long-for-the-column",
                prompt_tokens=10,
                completion_tokens=5,
                total_tokens=15,
            )

        assert len(captured.get("prompt_version") or "") <= 16

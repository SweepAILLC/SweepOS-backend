"""Token-efficiency and payload tests for Call Library LLM pipeline."""
from unittest.mock import patch

from app.services.call_library_ai import (
    DISCOVERY_AUDIT_SOP,
    OBJECTION_HANDLING_SOP,
    PITCHING_SOP,
    _build_library_user_payload,
    generate_call_library_report,
    is_substantive_call_library_report,
)


class TestBuildLibraryUserPayload:
    def test_empty_inputs_return_empty(self):
        assert _build_library_user_payload("", "") == ""

    def test_includes_summary_and_transcript_sections(self):
        payload = _build_library_user_payload("Summary line", "Rep: Hello")
        assert "SUMMARY:" in payload
        assert "TRANSCRIPT:" in payload

    def test_truncates_long_transcript(self):
        long_transcript = "word " * 50_000
        payload = _build_library_user_payload("short summary", long_transcript)
        assert len(payload) < len(long_transcript)

    def test_rich_summary_further_caps_transcript(self):
        rich_summary = "x" * 5000
        long_transcript = "y " * 30_000
        with patch("app.services.call_library_ai.settings") as mock_settings:
            mock_settings.CALL_LIBRARY_MAX_SUMMARY_CHARS = 6000
            mock_settings.CALL_LIBRARY_MAX_TRANSCRIPT_CHARS = 12000
            payload = _build_library_user_payload(rich_summary, long_transcript)
        # Transcript section should be capped below full 12k when summary is substantive
        transcript_part = payload.split("TRANSCRIPT:\n", 1)[-1]
        assert len(transcript_part) <= 8003  # 8000 + possible ellipsis


class TestGenerateCallLibraryReportGuards:
    def test_returns_none_without_llm(self):
        with patch("app.services.call_library_ai.llm_available", return_value=False):
            assert generate_call_library_report(transcript="t", summary="s") is None

    def test_returns_none_on_empty_payload(self):
        with patch("app.services.call_library_ai.llm_available", return_value=True):
            assert generate_call_library_report(transcript="", summary="") is None


class TestSopBlocksPresent:
    def test_discovery_sop_has_scoring_guidance(self):
        assert "discovery_score" in DISCOVERY_AUDIT_SOP
        assert "PAIN_IDENTIFICATION" in DISCOVERY_AUDIT_SOP

    def test_pitching_and_objection_sops_are_bounded(self):
        # Keep SOP injection sizes reasonable for token budget (enforced again at runtime)
        assert len(PITCHING_SOP) < 6000
        assert len(OBJECTION_HANDLING_SOP) < 8000


class TestSubstantiveReportGuard:
    def test_rejects_empty_template(self):
        assert not is_substantive_call_library_report(
            {"discovery_score": None, "objections": [], "summary": ""}
        )

    def test_accepts_scored_report(self):
        assert is_substantive_call_library_report(
            {"call_score": 7, "overall_impression": "Good call"}
        )

    def test_accepts_nested_dimension_scores(self):
        assert is_substantive_call_library_report(
            {
                "discovery_audit": {
                    "pain_identification": {"score": 7, "summary": "Asked about pain"},
                }
            }
        )

"""Token-efficiency and payload tests for Call Library LLM pipeline."""
from unittest.mock import patch

from app.services.call_library_ai import (
    DISCOVERY_AUDIT_SOP,
    OBJECTION_HANDLING_SOP,
    PITCHING_SOP,
    _build_library_user_payload,
    _normalize_deal_outcome,
    _normalize_discovery_audit,
    _normalize_objection_handling_audit,
    _normalize_pitching_audit,
    _normalize_report,
    clean_fathom_summary_text,
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


class TestCleanFathomSummaryText:
    def test_unwraps_timestamp_markdown_links(self):
        raw = (
            "## Key Takeaways\n\n"
            "  - [**Coaching Paused:** One-on-one coaching is paused until December.]"
            "(https://fathom.video/share/abc?tab=summary&timestamp=591.0)\n"
            "  - [Free Group Access: Landen retains free access.]"
            "(https://fathom.video/share/abc?tab=summary&timestamp=762.0)\n"
        )
        cleaned = clean_fathom_summary_text(raw)
        assert "https://fathom.video" not in cleaned
        assert "[" not in cleaned
        assert "]" not in cleaned
        assert "Coaching Paused: One-on-one coaching is paused until December." in cleaned
        assert "Free Group Access: Landen retains free access." in cleaned
        assert "## Key Takeaways" in cleaned

    def test_preserves_plain_text(self):
        assert clean_fathom_summary_text("Simple summary.") == "Simple summary."


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

    def test_accepts_glance_with_fathom_summary(self):
        assert is_substantive_call_library_report(
            {
                "analysis_kind": "glance",
                "fathom_summary": "Checked in on progress.",
                "ai_summary": "",
            }
        )

    def test_accepts_glance_with_ai_summary(self):
        assert is_substantive_call_library_report(
            {
                "analysis_kind": "glance",
                "fathom_summary": "",
                "ai_summary": "Solid check-in covering progress and next steps.",
            }
        )

    def test_rejects_empty_glance(self):
        assert not is_substantive_call_library_report(
            {"analysis_kind": "glance", "fathom_summary": "", "ai_summary": ""}
        )


class TestNormalizeDealOutcome:
    def test_cash_collected_on_call_carries_amount(self):
        out = _normalize_deal_outcome(
            {
                "cash_collected_on_call": True,
                "amount": "500",
                "currency": "usd",
                "billing": "one_time",
                "payment_confirmation": "Card charged live on call.",
                "confidence": "high",
                "evidence": "Rep read back card confirmation.",
            }
        )
        assert out["cash_collected_on_call"] is True
        assert out["verbally_agreed_not_paid"] is False
        assert out["amount"] == 500.0
        assert out["currency"] == "USD"
        assert out["billing"] == "one_time"
        assert out["payment_confirmation"] == "Card charged live on call."

    def test_verbal_agreement_without_payment_has_no_amount(self):
        out = _normalize_deal_outcome(
            {
                "cash_collected_on_call": False,
                "verbally_agreed_not_paid": True,
                "amount": "2000",
                "confidence": "medium",
                "evidence": "Prospect said they'd pay after payday.",
            }
        )
        assert out["cash_collected_on_call"] is False
        assert out["verbally_agreed_not_paid"] is True
        assert out["amount"] is None
        assert out["payment_confirmation"] == ""
        assert out["evidence"]

    def test_cash_collected_and_verbal_agreement_are_mutually_exclusive(self):
        out = _normalize_deal_outcome(
            {"cash_collected_on_call": True, "verbally_agreed_not_paid": True, "amount": "100"}
        )
        assert out["cash_collected_on_call"] is True
        assert out["verbally_agreed_not_paid"] is False

    def test_missing_or_invalid_shape_defaults_to_not_closed(self):
        assert _normalize_deal_outcome(None)["cash_collected_on_call"] is False
        assert _normalize_deal_outcome({})["cash_collected_on_call"] is False
        assert _normalize_deal_outcome("not a dict")["amount"] is None

    def test_negative_amount_rejected(self):
        out = _normalize_deal_outcome({"cash_collected_on_call": True, "amount": "-50"})
        assert out["amount"] is None


class TestNormalizeCombinedSectionAudits:
    def test_discovery_audit_combined_shape(self):
        out = _normalize_discovery_audit(
            {"discovery_score": 85, "discovery_summary": "Strong pain digging.", "quote": "It's costing me clients."}
        )
        assert out == {
            "discovery_score": 85.0,
            "discovery_summary": "Strong pain digging.",
            "quote": "It's costing me clients.",
        }

    def test_pitching_audit_combined_shape(self):
        out = _normalize_pitching_audit({"pitch_score": 60, "pitch_summary": "Rushed the value stack."})
        assert out["pitch_score"] == 60.0
        assert out["pitch_summary"] == "Rushed the value stack."
        assert out["quote"] is None

    def test_objection_handling_audit_combined_shape_keeps_objections_list(self):
        out = _normalize_objection_handling_audit(
            {
                "objection_score": 40,
                "objection_summary": "Led with logistics, not fear.",
                "objections": [{"objection_label": "too_expensive", "classification": "fear"}],
            }
        )
        assert out["objection_score"] == 40.0
        assert out["objection_summary"] == "Led with logistics, not fear."
        assert len(out["objections"]) == 1

    def test_scores_clamped_to_0_100(self):
        assert _normalize_discovery_audit({"discovery_score": 500})["discovery_score"] == 100.0
        assert _normalize_pitching_audit({"pitch_score": -20})["pitch_score"] == 0.0


class TestNormalizeReportHasNoCustomerResponse:
    def test_customer_response_key_absent(self):
        out = _normalize_report({"call_score": 80})
        assert "customer_response" not in out

    def test_deal_outcome_present_with_new_shape(self):
        out = _normalize_report({"deal_outcome": {"cash_collected_on_call": True, "amount": "300"}})
        assert out["deal_outcome"]["cash_collected_on_call"] is True
        assert out["deal_outcome"]["amount"] == 300.0

    def test_low_signal_zeroes_deal_outcome_even_if_llm_claimed_cash(self):
        out = _normalize_report(
            {
                "low_signal": True,
                "low_signal_reason": "Call cut off after 30 seconds.",
                "deal_outcome": {"cash_collected_on_call": True, "amount": "999"},
            }
        )
        assert out["deal_outcome"]["cash_collected_on_call"] is False
        assert out["deal_outcome"]["amount"] is None

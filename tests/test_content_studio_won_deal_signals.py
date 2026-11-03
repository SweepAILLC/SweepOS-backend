"""WON_DEAL_OBJECTION_HANDLING signal wiring: win-weighted sales-call theme payload flows
into collect_fathom_sales_signals and the stage-grounding calibration text. See
docs/prd/content-generation-signal-upgrades/ISSUES.md issue 4."""
import uuid

import app.services.content_studio_fathom_context as cf
from app.services.content_studio_bundle import _stage_grounding_block


def _win_theme(label="too_expensive", closed_won_count=2):
    return {
        "theme_key": "abc123",
        "label": label,
        "distinct_client_count": 3,
        "occurrence_count": 4,
        "closed_won_count": closed_won_count,
        "sample_quotes": ["Here's how I reframe the price concern when it comes up."],
    }


class TestWonDealObjectionHandlingPayload:
    def test_caps_at_fifteen(self, monkeypatch):
        monkeypatch.setattr(cf, "ensure_org_sales_content_themes_table", lambda db: None)
        monkeypatch.setattr(
            cf, "list_validated_win_themes_payload", lambda db, org_id: [_win_theme() for _ in range(20)]
        )
        out = cf._won_deal_objection_handling_payload(None, uuid.uuid4())
        assert len(out) == 15

    def test_empty_when_no_validated_win_themes(self, monkeypatch):
        monkeypatch.setattr(cf, "ensure_org_sales_content_themes_table", lambda db: None)
        monkeypatch.setattr(cf, "list_validated_win_themes_payload", lambda db, org_id: [])
        out = cf._won_deal_objection_handling_payload(None, uuid.uuid4())
        assert out == []


class TestCollectFathomSalesSignals:
    def test_won_deal_entries_alone_make_has_any_true(self, monkeypatch):
        monkeypatch.setattr(cf, "_themes_payload", lambda db, org_id: [])
        monkeypatch.setattr(cf, "_recent_call_insights", lambda db, org_id: [])
        monkeypatch.setattr(cf, "_active_client_insights", lambda db, org_id: [])
        monkeypatch.setattr(cf, "_fathom_summaries", lambda db, org_id: [])
        monkeypatch.setattr(cf, "_client_faq_themes_payload", lambda db, org_id: [])
        monkeypatch.setattr(
            cf, "_won_deal_objection_handling_payload", lambda db, org_id: [_win_theme()]
        )

        signals = cf.collect_fathom_sales_signals(None, uuid.uuid4())

        assert signals["has_any"] is True
        assert signals["won_deal_objection_handling"] == [_win_theme()]

    def test_all_empty_including_won_deal_means_no_data(self, monkeypatch):
        monkeypatch.setattr(cf, "_themes_payload", lambda db, org_id: [])
        monkeypatch.setattr(cf, "_recent_call_insights", lambda db, org_id: [])
        monkeypatch.setattr(cf, "_active_client_insights", lambda db, org_id: [])
        monkeypatch.setattr(cf, "_fathom_summaries", lambda db, org_id: [])
        monkeypatch.setattr(cf, "_client_faq_themes_payload", lambda db, org_id: [])
        monkeypatch.setattr(cf, "_won_deal_objection_handling_payload", lambda db, org_id: [])

        signals = cf.collect_fathom_sales_signals(None, uuid.uuid4())

        assert signals["has_any"] is False
        assert signals["won_deal_objection_handling"] == []


class TestStageGroundingCalibrationLine:
    def test_won_deal_count_appears_in_calibration_text(self):
        signals = {
            "has_any": True,
            "themes": [],
            "insights": [],
            "active_client_insights": [],
            "meeting_summaries": [],
            "won_deal_objection_handling": [_win_theme(), _win_theme(label="bad_timing")],
        }
        block = _stage_grounding_block(signals)
        assert "won_deal_objection_handling=2" in block

    def test_won_deal_count_zero_when_absent(self):
        signals = {
            "has_any": True,
            "themes": [{"label": "x", "sample_quotes": []}],
            "insights": [],
            "active_client_insights": [],
            "meeting_summaries": [],
        }
        block = _stage_grounding_block(signals)
        assert "won_deal_objection_handling=0" in block

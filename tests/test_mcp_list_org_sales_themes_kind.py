"""list_org_sales_themes_for_mcp's new kind filter: default "all" preserves prior behavior,
kind="question" returns only question-themes. See
docs/prd/content-generation-signal-upgrades/ISSUES.md issue 8."""
import uuid
from unittest.mock import MagicMock, patch

from app.services.marketing_intel_bundle import list_org_sales_themes_for_mcp


def _theme(key, label):
    return {"theme_key": key, "label": label, "distinct_client_count": 3, "occurrence_count": 3, "sample_quotes": []}


class TestKindFilter:
    def test_default_kind_all_preserves_prior_behavior(self):
        db = MagicMock()
        org_id = uuid.uuid4()
        with patch(
            "app.services.marketing_intel_bundle.ensure_org_sales_content_themes_table"
        ), patch(
            "app.services.marketing_intel_bundle.list_validated_themes_payload",
            return_value=[_theme("o1", "Too expensive")],
        ) as mock_list:
            out = list_org_sales_themes_for_mcp(db, org_id)

        mock_list.assert_called_once_with(db, org_id, kind=None)
        assert out["kind"] == "all"
        assert out["themes"] == [_theme("o1", "Too expensive")]

    def test_kind_question_passes_through_and_skips_unscoped_fallback(self):
        db = MagicMock()
        org_id = uuid.uuid4()
        with patch(
            "app.services.marketing_intel_bundle.ensure_org_sales_content_themes_table"
        ), patch(
            "app.services.marketing_intel_bundle.list_validated_themes_payload",
            return_value=[],
        ) as mock_list, patch(
            "app.services.marketing_intel_bundle.collect_fathom_sales_signals"
        ) as mock_collect:
            out = list_org_sales_themes_for_mcp(db, org_id, kind="question")

        mock_list.assert_called_once_with(db, org_id, kind="question")
        mock_collect.assert_not_called()  # fallback is not kind-aware, must not leak mixed kinds
        assert out["kind"] == "question"
        assert out["themes"] == []

    def test_kind_question_with_results_returns_only_those(self):
        db = MagicMock()
        org_id = uuid.uuid4()
        with patch(
            "app.services.marketing_intel_bundle.ensure_org_sales_content_themes_table"
        ), patch(
            "app.services.marketing_intel_bundle.list_validated_themes_payload",
            return_value=[_theme("q1", "Onboarding confusion")],
        ):
            out = list_org_sales_themes_for_mcp(db, org_id, kind="question")

        assert out["themes"] == [_theme("q1", "Onboarding confusion")]
        assert out["count"] == 1

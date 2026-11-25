"""Meta Ads sync: pure parsing/conversion helpers (no DB/Composio needed)."""
from app.services.meta_ads_sync_service import (
    _as_cents,
    _as_float,
    _as_int,
    _extract_result_metrics,
    _unwrap_list,
)


def test_as_cents_converts_decimal_major_unit_to_cents():
    assert _as_cents("12.50") == 1250
    assert _as_cents(12.5) == 1250
    assert _as_cents(0) == 0


def test_as_cents_handles_missing_or_invalid():
    assert _as_cents(None) is None
    assert _as_cents("not_a_number") is None


def test_as_int_and_as_float_handle_missing_or_invalid():
    assert _as_int("42") == 42
    assert _as_int(None) is None
    assert _as_int("nope") is None
    assert _as_float("3.14") == 3.14
    assert _as_float(None) is None


def test_unwrap_list_handles_dict_with_data_key():
    assert _unwrap_list({"data": [{"id": "1"}, {"id": "2"}]}) == [{"id": "1"}, {"id": "2"}]


def test_unwrap_list_handles_dict_with_items_key():
    assert _unwrap_list({"items": [{"id": "1"}]}) == [{"id": "1"}]


def test_unwrap_list_handles_plain_list():
    assert _unwrap_list([{"id": "1"}]) == [{"id": "1"}]


def test_unwrap_list_handles_unexpected_shape():
    assert _unwrap_list(None) == []
    assert _unwrap_list("not a list or dict") == []


def test_extract_result_metrics_reads_first_action_and_cost():
    raw = {
        "actions": [{"action_type": "lead", "value": "12"}],
        "cost_per_action_type": [{"action_type": "lead", "value": "4.50"}],
    }
    results, cost_per_result_cents = _extract_result_metrics(raw)
    assert results == 12
    assert cost_per_result_cents == 450


def test_extract_result_metrics_handles_missing_actions():
    results, cost_per_result_cents = _extract_result_metrics({})
    assert results is None
    assert cost_per_result_cents is None


def test_extract_result_metrics_prefers_optimization_goal_over_first_action():
    raw = {
        "actions": [
            {"action_type": "link_click", "value": "40"},
            {"action_type": "lead", "value": "7"},
        ],
        "cost_per_action_type": [
            {"action_type": "link_click", "value": "1.00"},
            {"action_type": "lead", "value": "9.50"},
        ],
    }
    results, cost = _extract_result_metrics(raw, optimization_goal="LEAD_GENERATION")
    assert results == 7
    assert cost == 950


def test_extract_result_metrics_awareness_goal_does_not_invent_a_result():
    raw = {"actions": [{"action_type": "link_click", "value": "40"}]}
    results, cost = _extract_result_metrics(raw, optimization_goal="REACH")
    assert results is None
    assert cost is None


def test_extract_result_metrics_uses_campaign_objective_when_goal_missing():
    raw = {
        "actions": [
            {"action_type": "lead", "value": "3"},
            {"action_type": "purchase", "value": "2"},
        ],
        "cost_per_action_type": [{"action_type": "purchase", "value": "12.00"}],
    }
    results, cost = _extract_result_metrics(raw, objective="OUTCOME_SALES")
    assert results == 2
    assert cost == 1200


# --- Messaging Matrix: creative fields --------------------------------------------------

from types import SimpleNamespace as _NS

from app.services.meta_ads_sync_service import apply_creative_fields


def _blank_ad(**kw):
    base = dict(creative_id=None, creative_thumbnail_url=None, video_id=None, instagram_media_id=None, ad_copy=None)
    base.update(kw)
    return _NS(**base)


def test_apply_creative_fields_reads_top_level_video_and_instagram_ids():
    row = _blank_ad()
    apply_creative_fields(
        row,
        {"id": "c1", "video_id": 123, "effective_instagram_media_id": "1789", "title": "Hook", "body": "Primary text"},
    )
    assert row.creative_id == "c1"
    assert row.video_id == "123"
    assert row.instagram_media_id == "1789"
    assert row.ad_copy == "Hook\n\nPrimary text"


def test_apply_creative_fields_falls_back_to_object_story_spec():
    row = _blank_ad()
    apply_creative_fields(
        row,
        {"object_story_spec": {"video_data": {"video_id": "v9", "message": "Story text", "title": "T"}}},
    )
    assert row.video_id == "v9"
    assert row.ad_copy == "T\n\nStory text"


def test_apply_creative_fields_keeps_existing_values_when_missing():
    row = _blank_ad(creative_id="old", video_id="v1", ad_copy="kept")
    apply_creative_fields(row, {"thumbnail_url": "https://x/t.jpg"})
    assert row.creative_id == "old"
    assert row.video_id == "v1"
    assert row.ad_copy == "kept"
    assert row.creative_thumbnail_url == "https://x/t.jpg"


def test_apply_creative_fields_image_ad_uses_link_data_copy():
    row = _blank_ad()
    apply_creative_fields(row, {"object_story_spec": {"link_data": {"message": "Body", "name": "Headline"}}})
    assert row.video_id is None
    assert row.ad_copy == "Headline\n\nBody"

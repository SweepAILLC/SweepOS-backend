"""Messaging Matrix service: tag parsing, evidence checks, reasoning triggers, ranking."""
import uuid
from types import SimpleNamespace

import pytest

from app.services import messaging_matrix_service as mm

TRANSCRIPT = (
    "Most coaches think they need more leads. You don't. Your offer is the bottleneck, "
    "not your ads, and until you fix the offer every dollar you spend leaks out."
)


def _angle(name, **kw):
    base = dict(
        id=uuid.uuid4(),
        name=name,
        status="active",
        awareness_stage="problem_aware",
        description="",
        organic_samples=0,
        paid_samples=0,
        organic_score=None,
        paid_score=None,
        overall_score=None,
        reasoning=None,
        reasoning_score=None,
    )
    base.update(kw)
    return SimpleNamespace(**base)


def test_quote_in_source_ignores_case_punctuation_and_spacing():
    assert mm.quote_in_source("your OFFER is the bottleneck,   not your ads", TRANSCRIPT)
    assert not mm.quote_in_source("your funnel is the bottleneck", TRANSCRIPT)
    assert not mm.quote_in_source("offer", TRANSCRIPT)  # too short to be evidence


def test_parse_tag_reuses_existing_angle_by_number():
    angles = [_angle("Leads are not the problem"), _angle("Offer is the bottleneck")]
    parsed = {"existing_angle": 2, "quote": "Your offer is the bottleneck, not your ads", "confidence": 0.9}
    result, quote, conf = mm.parse_tag_response(parsed, TRANSCRIPT, angles)
    assert result is angles[1]
    assert conf == 0.9


def test_parse_tag_new_angle_is_validated():
    parsed = {
        "existing_angle": None,
        "new_angle": {
            "name": "Fix the offer before buying traffic.",
            "awareness_stage": "nonsense",
            "hook_mechanic": "weird",
        },
        "quote": "until you fix the offer every dollar you spend leaks out",
        "confidence": 0.8,
    }
    result, _, _ = mm.parse_tag_response(parsed, TRANSCRIPT, [])
    assert result["name"] == "Fix the offer before buying traffic"
    assert result["awareness_stage"] is None
    assert result["hook_mechanic"] == "other"


@pytest.mark.parametrize(
    "parsed",
    [
        {"existing_angle": 1, "quote": "Your offer is the bottleneck, not your ads", "confidence": 0.5},
        {"existing_angle": 1, "quote": "a quote the coach never said at all", "confidence": 0.95},
        {"existing_angle": 1, "quote": "", "confidence": 0.95},
        {"existing_angle": 7, "new_angle": None, "quote": "Your offer is the bottleneck", "confidence": 0.9},
        "not a dict",
    ],
)
def test_parse_tag_rejects_low_confidence_missing_evidence_or_bad_index(parsed):
    result, _, _ = mm.parse_tag_response(parsed, TRANSCRIPT, [_angle("Offer is the bottleneck")])
    assert result is None


def test_parse_tag_bool_is_not_an_index():
    parsed = {"existing_angle": True, "quote": "Your offer is the bottleneck", "confidence": 0.9}
    result, _, _ = mm.parse_tag_response(parsed, TRANSCRIPT, [_angle("A")])
    assert result is None


def test_near_duplicate_names_merge():
    existing = [_angle("Your offer is the bottleneck, not your ads")]
    assert mm.find_near_duplicate("Your offer is the bottleneck not the ads", existing) is existing[0]
    assert mm.find_near_duplicate("Stop chasing followers", existing) is None


def test_prompt_lists_only_active_angles_most_used_first():
    angles = [
        _angle("Rare", organic_samples=1),
        _angle("Common", organic_samples=9),
        _angle("Hidden", organic_samples=50, status="archived"),
    ]
    text, listed = mm.angles_for_prompt(angles)
    assert [a.name for a in listed] == ["Common", "Rare"]
    assert text.startswith("1. Common")


def test_tag_prompt_is_versioned_and_carries_transcript():
    item = mm.Item("instagram", uuid.uuid4(), TRANSCRIPT, {})
    prompt = mm.build_tag_prompt(item, "(none yet)")
    assert prompt.startswith("# version: v0.1")
    assert "organic Instagram post" in prompt
    assert "Your offer is the bottleneck" in prompt


def test_needs_reasoning_only_on_big_moves():
    assert mm.needs_reasoning(_angle("a", overall_score=60.0))
    assert not mm.needs_reasoning(_angle("a", overall_score=None))
    assert not mm.needs_reasoning(_angle("a", overall_score=65.0, reasoning="x", reasoning_score=60.0))
    assert mm.needs_reasoning(_angle("a", overall_score=72.0, reasoning="x", reasoning_score=60.0))


def test_rank_angles_by_channel_and_min_samples():
    a = _angle("A", overall_score=70.0, organic_score=80.0, paid_score=None, organic_samples=3)
    b = _angle("B", overall_score=75.0, organic_score=55.0, paid_score=90.0, organic_samples=1, paid_samples=2)
    c = _angle("C", overall_score=None)
    assert [x.name for x in mm.rank_angles([a, b, c])] == ["B", "A"]
    assert [x.name for x in mm.rank_angles([a, b, c], channel="organic")] == ["A", "B"]
    assert [x.name for x in mm.rank_angles([a, b, c], channel="paid")] == ["B"]
    assert [x.name for x in mm.rank_angles([a, b], min_samples=3)] == ["B", "A"]
    assert [x.name for x in mm.rank_angles([a, b], min_samples=4)] == []
    assert [x.name for x in mm.rank_angles([a, b], channel="organic", min_samples=2)] == ["A"]


def test_recreate_response_requires_evidence_from_examples():
    good = {
        "hooks": ["h1", "h2", "h3"],
        "outline": ["b1", "b2", "b3", "b4"],
        "cta": "Book a call",
        "evidence": ["Your offer is the bottleneck, not your ads"],
        "confidence": 0.8,
    }
    out = mm.parse_recreate_response(good, TRANSCRIPT)
    assert out["hooks"] == ["h1", "h2", "h3"]
    with pytest.raises(RuntimeError):
        mm.parse_recreate_response({**good, "evidence": ["invented line nobody said ever"]}, TRANSCRIPT)
    with pytest.raises(RuntimeError):
        mm.parse_recreate_response({**good, "confidence": 0.4}, TRANSCRIPT)
    with pytest.raises(RuntimeError):
        mm.parse_recreate_response({**good, "hooks": ["only one"]}, TRANSCRIPT)


def test_refresh_skips_when_already_running(monkeypatch):
    monkeypatch.setattr(mm, "_acquire", lambda org: False)
    assert mm.refresh_matrix(None, uuid.uuid4()) == {"skipped": "already_running"}

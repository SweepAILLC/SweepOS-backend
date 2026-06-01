"""Tests for automation_opportunity_picker."""
from __future__ import annotations

import uuid
from types import SimpleNamespace
from unittest.mock import patch

from app.services import automation_opportunity_picker as picker_mod
from app.schemas.automation import AutomationRuleUpdate
from app.services.automation_opportunity_picker import (
    OpportunityPick,
    pick_combined_ask,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _client(*, lifecycle="active", lifetime_cents=10_000, progress=50.0):
    return SimpleNamespace(
        id=uuid.uuid4(),
        org_id=uuid.uuid4(),
        first_name="Alex",
        last_name="Kim",
        email="alex@example.com",
        lifecycle_state=SimpleNamespace(value=lifecycle),
        lifetime_revenue_cents=lifetime_cents,
        program_progress_percent=progress,
        notes="",
    )


def _rule(
    *,
    playbook="win_combined_ask",
    combine_top_n=3,
    opportunity_priority=None,
):
    return SimpleNamespace(
        id=uuid.uuid4(),
        org_id=uuid.uuid4(),
        playbook=playbook,
        combine_top_n=combine_top_n,
        opportunity_priority=opportunity_priority or [],
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _patch_llm(*, available=True, response=None, raise_exc=None):
    """Context-manager helper: patch llm_available + chat_json on the picker module."""
    avail_p = patch.object(picker_mod, "llm_available", return_value=available)

    def _fake_chat(_sys, _user, **_kwargs):
        if raise_exc:
            raise raise_exc
        return response

    chat_p = patch.object(picker_mod, "chat_json", side_effect=_fake_chat)
    return avail_p, chat_p


# ---------------------------------------------------------------------------
# LLM autonomy paths
# ---------------------------------------------------------------------------

def test_llm_picks_single_ask():
    avail_p, chat_p = _patch_llm(
        available=True,
        response={
            "chosen": ["referral"],
            "rationale": "Health is high; one clean referral ask wins.",
            "per_choice": {"referral": "client just had a win"},
        },
    )
    with avail_p, chat_p:
        out = pick_combined_ask(
            client=_client(),
            insight_json={"wins": ["lost 12 lbs"], "opportunity_tags": ["referral"]},
            ladder=None,
            ai_profile={"pipeline_priorities": ["referrals"]},
            health_score=82.0,
            in_offboarding=False,
            rule=_rule(),
        )
    assert isinstance(out, OpportunityPick)
    assert [o.name for o in out.chosen] == ["referral"]
    assert out.picker_mode == "llm"
    assert out.rationale and "referral" in out.rationale.lower()
    assert out.per_choice_rationale.get("referral")


def test_llm_picks_two_combined():
    avail_p, chat_p = _patch_llm(
        available=True,
        response={
            "chosen": ["referral", "upsell"],
            "rationale": "High LTV + recent win supports a referral lead and a soft upsell.",
            "per_choice": {
                "referral": "post-win momentum",
                "upsell": "$500+ LTV justifies offering tier-2",
            },
        },
    )
    with avail_p, chat_p:
        out = pick_combined_ask(
            client=_client(lifetime_cents=100_000),
            insight_json={"wins": ["promotion"], "opportunity_tags": ["referral", "upsell"]},
            ladder={
                "upsells": [{"name": "Tier 2", "triggers": ["promotion"]}],
                "referral_offer": {"name": "Refer-a-friend"},
            },
            ai_profile={"pipeline_priorities": ["referrals", "upsells"]},
            health_score=78.0,
            in_offboarding=False,
            rule=_rule(),
        )
    assert [o.name for o in out.chosen] == ["referral", "upsell"]
    assert out.picker_mode == "llm"


def test_llm_picks_all_three():
    avail_p, chat_p = _patch_llm(
        available=True,
        response={
            "chosen": ["referral", "testimonial", "upsell"],
            "rationale": "Offboarding window with multiple wins -> recap + ask everything.",
            "per_choice": {
                "referral": "share with friends entering similar transformation",
                "testimonial": "wins are quotable",
                "upsell": "tier-2 maintenance",
            },
        },
    )
    with avail_p, chat_p:
        out = pick_combined_ask(
            client=_client(lifecycle="offboarding", lifetime_cents=200_000, progress=95.0),
            insight_json={"wins": ["12 lbs", "PR squat"], "opportunity_tags": ["testimonial"]},
            ladder={"upsells": [{"name": "Maintenance", "triggers": ["maintenance"]}]},
            ai_profile={"pipeline_priorities": ["referrals"]},
            health_score=88.0,
            in_offboarding=True,
            rule=_rule(combine_top_n=3),
        )
    assert [o.name for o in out.chosen] == ["referral", "testimonial", "upsell"]
    assert out.picker_mode == "llm"


# ---------------------------------------------------------------------------
# LLM payload contract -- keeps the prompt grounded in the four required signals
# ---------------------------------------------------------------------------

def test_llm_payload_includes_required_signals_and_intelligence():
    captured = {}

    def _capture(_sys, user_prompt, **_kwargs):
        captured["payload"] = user_prompt
        return {"chosen": ["referral"], "rationale": "ok", "per_choice": {}}

    avail_p = patch.object(picker_mod, "llm_available", return_value=True)
    chat_p = patch.object(picker_mod, "chat_json", side_effect=_capture)
    with avail_p, chat_p:
        pick_combined_ask(
            client=_client(progress=42.5, lifetime_cents=75_000),
            insight_json={"wins": ["a win"], "opportunity_tags": ["referral"]},
            ladder={
                "upsells": [{"name": "X", "triggers": ["x"]}],
                "referral_offer": {"name": "RO"},
                "core_offer": {"name": "Core"},
            },
            ai_profile={
                "pipeline_priorities": ["referrals", "testimonials"],
                "writing_tone": "warm",
                "coaching_style": "direct",
            },
            health_score=71.0,
            in_offboarding=False,
            rule=_rule(),
        )
    body = captured["payload"]
    # All four mandated decision inputs must reach the LLM.
    assert "health_score" in body and "71" in body
    assert "program_progress_percent" in body and "42.5" in body
    assert "pipeline_priorities" in body
    assert "deterministic_scores" in body
    assert "max_choices" in body


# ---------------------------------------------------------------------------
# Rule overrides
# ---------------------------------------------------------------------------

def test_rule_opportunity_priority_pin_bypasses_llm():
    """When the operator pins a priority list, the LLM is never called."""
    avail_p = patch.object(picker_mod, "llm_available", return_value=True)
    chat_p = patch.object(picker_mod, "chat_json")
    with avail_p, chat_p as chat_mock:
        out = pick_combined_ask(
            client=_client(),
            insight_json=None,
            ladder=None,
            ai_profile=None,
            health_score=None,
            in_offboarding=False,
            rule=_rule(opportunity_priority=["upsell", "referral"], combine_top_n=3),
        )
    chat_mock.assert_not_called()
    assert out.picker_mode == "rule_pinned"
    assert [o.name for o in out.chosen] == ["upsell", "referral"]


def test_combine_top_n_caps_llm_choices():
    """LLM may try to combine all three; combine_top_n=1 forces a single ask."""
    avail_p, chat_p = _patch_llm(
        available=True,
        response={
            "chosen": ["referral", "upsell", "testimonial"],
            "rationale": "everything",
            "per_choice": {},
        },
    )
    with avail_p, chat_p:
        out = pick_combined_ask(
            client=_client(),
            insight_json=None,
            ladder=None,
            ai_profile=None,
            health_score=None,
            in_offboarding=False,
            rule=_rule(combine_top_n=1),
        )
    assert len(out.chosen) == 1
    assert out.chosen[0].name == "referral"
    assert out.picker_mode == "llm"


def test_combine_top_n_schema_accepts_zero_for_full_autonomy():
    rule = AutomationRuleUpdate(enabled=False, combine_top_n=0)
    assert rule.combine_top_n == 0


def test_combine_top_n_zero_means_no_cap():
    """combine_top_n = 0/None -> picker treats it as no cap (full LLM autonomy)."""
    avail_p, chat_p = _patch_llm(
        available=True,
        response={
            "chosen": ["referral", "upsell", "testimonial"],
            "rationale": "all three",
            "per_choice": {},
        },
    )
    with avail_p, chat_p:
        out = pick_combined_ask(
            client=_client(),
            insight_json=None,
            ladder=None,
            ai_profile=None,
            health_score=None,
            in_offboarding=False,
            rule=_rule(combine_top_n=0),
        )
    assert [o.name for o in out.chosen] == ["referral", "upsell", "testimonial"]


# ---------------------------------------------------------------------------
# Validation + fallback
# ---------------------------------------------------------------------------

def test_invalid_llm_names_get_dropped():
    avail_p, chat_p = _patch_llm(
        available=True,
        response={
            "chosen": ["referral", "discount", "FOO", "upsell"],
            "rationale": "ok",
            "per_choice": {},
        },
    )
    with avail_p, chat_p:
        out = pick_combined_ask(
            client=_client(),
            insight_json=None,
            ladder=None,
            ai_profile=None,
            health_score=None,
            in_offboarding=False,
            rule=_rule(),
        )
    assert [o.name for o in out.chosen] == ["referral", "upsell"]
    assert out.picker_mode == "llm"


def test_llm_returns_no_valid_names_falls_back_to_deterministic():
    avail_p, chat_p = _patch_llm(
        available=True,
        response={"chosen": ["nope", "discount"], "rationale": "junk", "per_choice": {}},
    )
    with avail_p, chat_p:
        out = pick_combined_ask(
            client=_client(),
            insight_json=None,
            ladder=None,
            ai_profile=None,
            health_score=None,
            in_offboarding=False,
            rule=_rule(),
        )
    assert out.picker_mode == "deterministic"
    assert out.fallback_reason
    assert len(out.chosen) >= 1


def test_llm_raises_falls_back_to_deterministic():
    avail_p, chat_p = _patch_llm(
        available=True,
        raise_exc=RuntimeError("LLM HTTP 500"),
    )
    with avail_p, chat_p:
        out = pick_combined_ask(
            client=_client(),
            insight_json=None,
            ladder=None,
            ai_profile=None,
            health_score=None,
            in_offboarding=False,
            rule=_rule(),
        )
    assert out.picker_mode == "deterministic"
    assert out.fallback_reason


def test_llm_unavailable_falls_back_to_deterministic_topn():
    avail_p, chat_p = _patch_llm(available=False, response=None)
    with avail_p, chat_p as chat_mock:
        out = pick_combined_ask(
            client=_client(),
            insight_json={"wins": ["w"], "opportunity_tags": ["referral"]},
            ladder=None,
            ai_profile={"pipeline_priorities": ["referrals"]},
            health_score=80.0,
            in_offboarding=False,
            rule=_rule(combine_top_n=2),
        )
    chat_mock.assert_not_called()
    assert out.picker_mode == "deterministic"
    assert out.fallback_reason == "LLM unavailable"
    assert len(out.chosen) == 2
    assert out.chosen[0].name == "referral"  # ROI tag + win + high health


# ---------------------------------------------------------------------------
# OpportunityPick UI helpers
# ---------------------------------------------------------------------------

def test_to_notes_emits_human_readable_lines():
    avail_p, chat_p = _patch_llm(
        available=True,
        response={
            "chosen": ["referral", "testimonial"],
            "rationale": "Big win + offboarding window.",
            "per_choice": {"referral": "fresh momentum", "testimonial": "quotable wins"},
        },
    )
    with avail_p, chat_p:
        out = pick_combined_ask(
            client=_client(lifecycle="offboarding"),
            insight_json={"wins": ["w"], "opportunity_tags": ["referral", "testimonial"]},
            ladder=None,
            ai_profile=None,
            health_score=85.0,
            in_offboarding=True,
            rule=_rule(),
        )
    notes = out.to_notes()
    assert any("LLM picked combined ask" in n for n in notes)
    assert any("referral" in n and "testimonial" in n for n in notes)
    assert any("Why:" in n for n in notes)
    assert any("- referral:" in n for n in notes)


def test_to_audit_payload_is_compact_json_safe():
    avail_p, chat_p = _patch_llm(
        available=True,
        response={"chosen": ["upsell"], "rationale": "fits ladder", "per_choice": {}},
    )
    with avail_p, chat_p:
        out = pick_combined_ask(
            client=_client(),
            insight_json=None,
            ladder=None,
            ai_profile=None,
            health_score=None,
            in_offboarding=False,
            rule=_rule(),
        )
    audit = out.to_audit()
    assert audit["chosen"] == ["upsell"]
    assert audit["mode"] == "llm"
    assert "rationale" in audit
    assert isinstance(audit["per_choice"], dict)

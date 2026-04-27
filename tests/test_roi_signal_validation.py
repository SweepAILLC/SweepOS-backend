"""Unit tests for ROI signal validation (quotes, gating, tags)."""

from app.services.roi_signal_validation import (
    apply_roi_validation,
    client_has_expansion_win_basis,
    is_substantial_outcome,
    merge_client_roi_meta,
    normalize_display_tags_for_client,
    quote_in_transcript,
)


def test_normalize_display_tags_lead_strips_roi_trio_and_adds_conversion():
    tags = normalize_display_tags_for_client(
        "cold_lead",
        {"has_past_sales_call": False, "open_sales_deal": False},
        ["testimonial", "upsell", "conversion"],
    )
    assert "testimonial" not in tags
    assert "upsell" not in tags
    assert "conversion" in tags


def test_normalize_display_tags_lead_open_deal():
    tags = normalize_display_tags_for_client(
        "warm_lead",
        {"has_past_sales_call": True, "open_sales_deal": True},
        ["testimonial", "referral"],
    )
    assert "deal_follow_up" in tags
    assert "testimonial" not in tags


def test_normalize_display_tags_active_keeps_roi_strips_lead_tags():
    tags = normalize_display_tags_for_client(
        "active",
        {},
        ["testimonial", "conversion", "deal_follow_up"],
    )
    assert "testimonial" in tags
    assert "conversion" not in tags
    assert "deal_follow_up" not in tags


def test_normalize_display_tags_active_strips_upsell_referral_without_win_or_bypass():
    tags = normalize_display_tags_for_client(
        "active",
        {},
        ["upsell", "referral"],
        testimonial_gate_bypass=False,
        has_expansion_win_basis=False,
    )
    assert "upsell" not in tags
    assert "referral" not in tags


def test_normalize_display_tags_active_injects_testimonial_when_wins_basis_but_missing():
    tags = normalize_display_tags_for_client(
        "active",
        {},
        ["upsell", "referral"],
        testimonial_gate_bypass=False,
        has_expansion_win_basis=True,
    )
    assert "testimonial" in tags
    assert tags[0] == "testimonial"


def test_normalize_display_tags_active_keeps_expansion_when_testimonial_chip_in_summary():
    """Legacy summaries may still carry testimonial without roi_state backfill."""
    tags = normalize_display_tags_for_client(
        "active",
        {},
        ["testimonial", "upsell", "referral"],
        testimonial_gate_bypass=False,
        has_expansion_win_basis=False,
    )
    assert "testimonial" in tags
    assert "upsell" in tags
    assert "referral" in tags


def test_normalize_display_tags_active_keeps_upsell_with_bypass():
    tags = normalize_display_tags_for_client(
        "active",
        {},
        ["upsell", "referral"],
        testimonial_gate_bypass=True,
        has_expansion_win_basis=False,
    )
    assert "upsell" in tags
    assert "referral" in tags


def test_normalize_display_tags_active_keeps_upsell_with_prior_trigger():
    tags = normalize_display_tags_for_client(
        "active",
        {},
        ["upsell"],
        testimonial_gate_bypass=False,
        has_expansion_win_basis=True,
    )
    assert "upsell" in tags


def test_quote_in_transcript_substring():
    t = "Speaker A: I lost fifteen pounds since we started.\nCoach: Amazing."
    q = "I lost fifteen pounds since we started."
    assert quote_in_transcript(q, t) is True


def test_quote_in_transcript_rejects_missing():
    assert quote_in_transcript("not in transcript", "hello world") is False


def test_is_substantial_outcome_weight():
    assert is_substantial_outcome("I finally lost 12 lbs this month", "weight") is True


def test_is_substantial_outcome_too_vague():
    assert is_substantial_outcome("It was good", "") is False


def test_apply_roi_testimonial_client_only():
    trans = "[00:01] Client: I paid off $4,000 of debt using your system."
    insight = {
        "opportunity_tags": ["testimonial", "upsell"],
        "roi_signals": {
            "testimonial_candidates": [
                {
                    "quote": "I paid off $4,000 of debt using your system.",
                    "start_timestamp": "00:01",
                    "end_timestamp": None,
                    "outcome_type": "financial",
                    "speaker": "client",
                    "confidence": 0.9,
                    "rationale": "specific dollar outcome",
                }
            ],
            "upsell_signal": {
                "active": True,
                "rationale": "wants to continue",
                "evidence_quotes": ["next level"],
                "future_goal_language": True,
            },
            "referral_signal": {"active": False, "variant": None, "rationale": "", "evidence_quotes": []},
        },
    }
    out, delta = apply_roi_validation(dict(insight), trans, "active", {}, "2025-01-01T12:00:00+00:00")
    assert "testimonial" in out["opportunity_tags"]
    assert "upsell" in out["opportunity_tags"]  # testimonial_triggered same call
    assert delta.get("testimonial_trigger_at")
    assert out["roi_signals"]["testimonial_moments"]


def test_apply_roi_rejects_coach_speaker():
    trans = "Coach: You lost ten pounds! Client: Thanks."
    insight = {
        "opportunity_tags": ["testimonial"],
        "roi_signals": {
            "testimonial_candidates": [
                {
                    "quote": "You lost ten pounds!",
                    "speaker": "coach",
                    "outcome_type": "weight",
                    "start_timestamp": "0:00",
                    "confidence": 1,
                    "rationale": "",
                }
            ],
            "upsell_signal": {"active": False, "rationale": "", "evidence_quotes": [], "future_goal_language": False},
            "referral_signal": {"active": False, "variant": None, "rationale": "", "evidence_quotes": []},
        },
    }
    out, _ = apply_roi_validation(dict(insight), trans, "active", {}, None)
    assert "testimonial" not in out["opportunity_tags"]


def test_apply_roi_upsell_allowed_when_gate_bypass():
    trans = "Client: I want to keep going next quarter with a bigger package."
    insight = {
        "opportunity_tags": ["upsell"],
        "roi_signals": {
            "testimonial_candidates": [],
            "upsell_signal": {
                "active": True,
                "rationale": "renewal language",
                "evidence_quotes": ["keep going next quarter"],
                "future_goal_language": True,
            },
            "referral_signal": {"active": False, "variant": None, "rationale": "", "evidence_quotes": []},
        },
    }
    out, _ = apply_roi_validation(
        dict(insight), trans, "active", {}, None, testimonial_gate_bypass=True
    )
    assert "upsell" in out["opportunity_tags"]


def test_apply_roi_referral_infer_variant_when_gate_bypass():
    trans = "Client: Send my friend your signup link."
    insight = {
        "opportunity_tags": ["referral"],
        "roi_signals": {
            "testimonial_candidates": [],
            "upsell_signal": {"active": False, "rationale": "", "evidence_quotes": [], "future_goal_language": False},
            "referral_signal": {
                "active": True,
                "variant": None,
                "rationale": "friend",
                "evidence_quotes": ["friend"],
            },
        },
    }
    out, _ = apply_roi_validation(
        dict(insight), trans, "active", {}, None, testimonial_gate_bypass=True
    )
    assert "referral" in out["opportunity_tags"]


def test_apply_roi_referral_coerces_new_lead_variant_for_active_with_win():
    trans = (
        "Client: I lost twelve pounds since January. "
        "Client: Send my sister your signup link."
    )
    insight = {
        "opportunity_tags": [],
        "roi_signals": {
            "testimonial_candidates": [
                {
                    "quote": "I lost twelve pounds since January.",
                    "speaker": "client",
                    "outcome_type": "weight",
                    "start_timestamp": "0:00",
                    "confidence": 0.9,
                    "rationale": "measurable outcome",
                }
            ],
            "upsell_signal": {"active": False, "rationale": "", "evidence_quotes": [], "future_goal_language": False},
            "referral_signal": {
                "active": True,
                "variant": "new_lead",
                "rationale": "Referring family after results",
                "evidence_quotes": ["Send my sister your signup link"],
            },
        },
    }
    out, _ = apply_roi_validation(dict(insight), trans, "active", {}, None)
    assert "referral" in out["opportunity_tags"]
    assert out["roi_signals"]["referral"].get("variant") == "post_testimonial"


def test_apply_roi_referral_from_evidence_when_active_false():
    trans = (
        "Client: I paid off three thousand in debt. "
        "Client: I will tell my gym friends about you."
    )
    insight = {
        "opportunity_tags": [],
        "roi_signals": {
            "testimonial_candidates": [
                {
                    "quote": "I paid off three thousand in debt.",
                    "speaker": "client",
                    "outcome_type": "financial",
                    "start_timestamp": "0:00",
                    "confidence": 0.9,
                    "rationale": "dollar outcome",
                }
            ],
            "upsell_signal": {"active": False, "rationale": "", "evidence_quotes": [], "future_goal_language": False},
            "referral_signal": {
                "active": False,
                "variant": None,
                "rationale": "Client wants to spread word to gym friends after major win",
                "evidence_quotes": ["tell my gym friends about you"],
            },
        },
    }
    out, _ = apply_roi_validation(dict(insight), trans, "active", {}, None)
    assert "referral" in out["opportunity_tags"]


def test_apply_roi_upsell_gated_without_prior_testimonial():
    trans = "Client: I want to keep going next quarter with a bigger package."
    insight = {
        "opportunity_tags": ["upsell"],
        "roi_signals": {
            "testimonial_candidates": [],
            "upsell_signal": {
                "active": True,
                "rationale": "renewal language",
                "evidence_quotes": ["keep going next quarter"],
                "future_goal_language": True,
            },
            "referral_signal": {"active": False, "variant": None, "rationale": "", "evidence_quotes": []},
        },
    }
    out, _ = apply_roi_validation(dict(insight), trans, "active", {}, None)
    assert "upsell" not in out["opportunity_tags"]


def test_apply_roi_upsell_allowed_with_prior_win_moments_only():
    trans = "Client: I'd like to add the nutrition add-on for next phase."
    prior = {"lifetime_win_moments_count": 1}
    insight = {
        "opportunity_tags": ["upsell"],
        "roi_signals": {
            "testimonial_candidates": [],
            "upsell_signal": {
                "active": True,
                "rationale": "add-on",
                "evidence_quotes": ["add-on"],
                "future_goal_language": True,
            },
            "referral_signal": {"active": False, "variant": None, "rationale": "", "evidence_quotes": []},
        },
    }
    out, _ = apply_roi_validation(dict(insight), trans, "active", prior, None)
    assert "upsell" in out["opportunity_tags"]


def test_apply_roi_upsell_allowed_with_prior_trigger():
    trans = "Client: I'd like to add the nutrition add-on for next phase."
    prior = {"testimonial_trigger_at": "2024-12-01T00:00:00+00:00"}
    insight = {
        "opportunity_tags": ["upsell"],
        "roi_signals": {
            "testimonial_candidates": [],
            "upsell_signal": {
                "active": True,
                "rationale": "add-on",
                "evidence_quotes": ["add-on"],
                "future_goal_language": True,
            },
            "referral_signal": {"active": False, "variant": None, "rationale": "", "evidence_quotes": []},
        },
    }
    out, _ = apply_roi_validation(dict(insight), trans, "active", prior, None)
    assert "upsell" in out["opportunity_tags"]


def test_apply_roi_strips_testimonial_for_cold_lead():
    trans = "[00:01] Client: I lost 20 pounds on your program."
    insight = {
        "opportunity_tags": ["testimonial"],
        "roi_signals": {
            "testimonial_candidates": [
                {
                    "quote": "I lost 20 pounds on your program.",
                    "speaker": "client",
                    "outcome_type": "weight",
                    "start_timestamp": "00:01",
                    "confidence": 1,
                    "rationale": "",
                }
            ],
            "upsell_signal": {"active": False, "rationale": "", "evidence_quotes": [], "future_goal_language": False},
            "referral_signal": {"active": False, "variant": None, "rationale": "", "evidence_quotes": []},
            "revive_playbook": {"rationale": "", "offer_angles": [], "outreach_hooks": []},
        },
    }
    out, _ = apply_roi_validation(
        dict(insight), trans, "cold_lead", {}, None, {"has_past_sales_call": False, "open_sales_deal": False}
    )
    assert "testimonial" not in out["opportunity_tags"]
    assert "conversion" in out["opportunity_tags"]
    assert out["roi_signals"]["testimonial_moments"] == []


def test_apply_roi_deal_follow_up_when_open_deal():
    trans = "Client: I'll think about it."
    insight = {
        "opportunity_tags": [],
        "roi_signals": {
            "testimonial_candidates": [],
            "upsell_signal": {"active": False, "rationale": "", "evidence_quotes": [], "future_goal_language": False},
            "referral_signal": {"active": False, "variant": None, "rationale": "", "evidence_quotes": []},
            "revive_playbook": {"rationale": "", "offer_angles": [], "outreach_hooks": []},
        },
    }
    out, _ = apply_roi_validation(
        dict(insight),
        trans,
        "warm_lead",
        {},
        None,
        {"has_past_sales_call": True, "open_sales_deal": True},
    )
    assert "deal_follow_up" in out["opportunity_tags"]
    assert "conversion" not in out["opportunity_tags"]


def test_apply_roi_revive_for_dead():
    trans = "We miss training; budget was tight."
    insight = {
        "opportunity_tags": ["win_back"],
        "roi_signals": {
            "testimonial_candidates": [],
            "upsell_signal": {"active": False, "rationale": "", "evidence_quotes": [], "future_goal_language": False},
            "referral_signal": {"active": False, "variant": None, "rationale": "", "evidence_quotes": []},
            "revive_playbook": {
                "rationale": "Offer a 4-week re-entry tier with buddy sessions.",
                "offer_angles": ["Lower commitment entry"],
                "outreach_hooks": [],
            },
        },
    }
    out, _ = apply_roi_validation(dict(insight), trans, "dead", {}, None, {})
    assert "revive" in out["opportunity_tags"]
    assert "win_back" not in out["opportunity_tags"]


def test_merge_client_roi_meta_preserves_first_testimonial_trigger():
    class C:
        meta = {}

    c = C()
    merge_client_roi_meta(c, {"testimonial_trigger_at": "2025-01-01T00:00:00+00:00", "testimonial_best_quote": "Q1"})
    assert c.meta["roi_state"]["testimonial_trigger_at"] == "2025-01-01T00:00:00+00:00"
    merge_client_roi_meta(c, {"testimonial_trigger_at": "2025-06-01T00:00:00+00:00"})
    assert c.meta["roi_state"]["testimonial_trigger_at"] == "2025-01-01T00:00:00+00:00"


def test_merge_client_roi_meta_increments_lifetime_win_moments():
    class C:
        meta = {}

    c = C()
    merge_client_roi_meta(c, {"lifetime_win_moments_increment": 2})
    assert c.meta["roi_state"]["lifetime_win_moments_count"] == 2
    merge_client_roi_meta(c, {"lifetime_win_moments_increment": 1})
    assert c.meta["roi_state"]["lifetime_win_moments_count"] == 3


def test_client_has_expansion_win_basis_from_lifetime_count():
    class C:
        meta = {"roi_state": {"lifetime_win_moments_count": 1}}

    assert client_has_expansion_win_basis(C()) is True

    class D:
        meta = {"roi_state": {}}

    assert client_has_expansion_win_basis(D()) is False

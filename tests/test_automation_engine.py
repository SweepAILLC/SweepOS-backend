"""Tests for automation_engine and automation_dispatcher helpers."""
from __future__ import annotations

import uuid
from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock

from app.models.automation import JobState, Playbook
from app.models.client import LifecycleState
from app.services import automation_dispatcher
from app.services.automation_drafts import build_merge_tag_values, render_merge_tags
from app.services.automation_engine import (
    audience_filter_passes,
    idempotency_key_for,
    score_opportunities,
    select_upsell_for_client,
)


def _client(
    *,
    lifecycle="active",
    lifetime_cents=0,
    progress=None,
    first_name="Alex",
    last_name="Kim",
    email="alex@example.com",
):
    """In-memory Client double with the attrs the engine actually reads."""
    lifecycle_enum = SimpleNamespace(value=lifecycle)
    return SimpleNamespace(
        id=uuid.uuid4(),
        org_id=uuid.uuid4(),
        first_name=first_name,
        last_name=last_name,
        email=email,
        lifecycle_state=lifecycle_enum,
        lifetime_revenue_cents=lifetime_cents,
        program_progress_percent=progress,
        notes="",
    )


# ---------------------------------------------------------------------------
# idempotency_key_for
# ---------------------------------------------------------------------------

def test_idempotency_key_stable_across_calls():
    cid = uuid.UUID("11111111-1111-1111-1111-111111111111")
    a = idempotency_key_for(playbook="first_payment_referral", client_id=cid, discriminator="stripe:ch_1")
    b = idempotency_key_for(playbook="first_payment_referral", client_id=cid, discriminator="stripe:ch_1")
    assert a == b
    assert len(a) == 48


def test_idempotency_key_changes_when_discriminator_changes():
    cid = uuid.UUID("11111111-1111-1111-1111-111111111111")
    a = idempotency_key_for(playbook="first_payment_referral", client_id=cid, discriminator="stripe:ch_1")
    b = idempotency_key_for(playbook="first_payment_referral", client_id=cid, discriminator="stripe:ch_2")
    assert a != b


def test_idempotency_key_changes_when_playbook_changes():
    cid = uuid.UUID("11111111-1111-1111-1111-111111111111")
    a = idempotency_key_for(playbook="first_payment_referral", client_id=cid, discriminator="x")
    b = idempotency_key_for(playbook="first_payment_onboarding", client_id=cid, discriminator="x")
    assert a != b


# ---------------------------------------------------------------------------
# score_opportunities
# ---------------------------------------------------------------------------

def test_score_opportunities_referral_wins_when_referral_tag_and_wins():
    c = _client()
    insight = {
        "opportunity_tags": ["referral"],
        "wins": ["lost 12 lbs"],
    }
    ranked = score_opportunities(c, insight=insight, ladder=None, health_score=None)
    assert ranked[0].name == "referral"
    assert "ROI tag: referral" in ranked[0].rationale


def test_score_opportunities_pipeline_priorities_breaks_tie():
    """When two opportunities tie, Intelligence pipeline_priorities decides."""
    c = _client()
    # No tags / wins / offer / health -> all three start at base score 10 (tie).
    ranked_with_upsell_priority = score_opportunities(
        c,
        insight=None,
        ladder=None,
        health_score=None,
        pipeline_priorities=["upsells"],
    )
    assert ranked_with_upsell_priority[0].name == "upsell"

    ranked_with_testimonial_priority = score_opportunities(
        c,
        insight=None,
        ladder=None,
        health_score=None,
        pipeline_priorities=["testimonials"],
    )
    assert ranked_with_testimonial_priority[0].name == "testimonial"


def test_score_opportunities_rule_priority_overrides_pipeline_priority():
    c = _client()
    ranked = score_opportunities(
        c,
        insight=None,
        ladder=None,
        health_score=None,
        pipeline_priorities=["testimonials"],
        rule_priority=["upsell", "referral", "testimonial"],
    )
    assert ranked[0].name == "upsell"


def test_score_opportunities_default_tiebreaker_is_referral_first():
    c = _client()
    ranked = score_opportunities(c, insight=None, ladder=None, health_score=None)
    assert [o.name for o in ranked] == ["referral", "testimonial", "upsell"]


def test_score_opportunities_offboarding_boosts_referral_and_testimonial():
    c = _client(lifecycle="offboarding")
    ranked = score_opportunities(
        c, insight=None, ladder=None, health_score=None, in_offboarding=True
    )
    # referral and testimonial both pick up the offboarding bonus; upsell gets only half.
    upsell_score = next(o.score for o in ranked if o.name == "upsell")
    referral_score = next(o.score for o in ranked if o.name == "referral")
    assert referral_score > upsell_score


def test_score_opportunities_high_revenue_boosts_upsell():
    c = _client(lifetime_cents=100_000)  # $1000 LTV
    ranked = score_opportunities(c, insight=None, ladder=None, health_score=None)
    upsell_score = next(o.score for o in ranked if o.name == "upsell")
    assert upsell_score > 10  # base


# ---------------------------------------------------------------------------
# select_upsell_for_client
# ---------------------------------------------------------------------------

def test_select_upsell_matches_trigger_phrase_in_insight():
    ladder = {
        "upsells": [
            {"name": "Nutrition Add-On", "triggers": ["nutrition", "meal plan"]},
            {"name": "1:1 Coaching", "triggers": ["next level", "advanced"]},
        ]
    }
    insight = {
        "opportunity_tags": ["upsell"],
        "wins": [],
        "headline": "Client wants advanced programming next quarter",
    }
    chosen = select_upsell_for_client(_client(), insight, ladder)
    assert chosen and chosen["name"] == "1:1 Coaching"


def test_select_upsell_returns_none_without_ladder():
    assert select_upsell_for_client(_client(), {"opportunity_tags": ["upsell"]}, None) is None


def test_select_upsell_falls_back_to_first_offer_when_only_tag():
    ladder = {
        "upsells": [
            {"name": "Premium", "triggers": ["nope"]},
            {"name": "VIP", "triggers": []},
        ]
    }
    insight = {"opportunity_tags": ["upsell"], "wins": [], "headline": ""}
    chosen = select_upsell_for_client(_client(), insight, ladder)
    # No phrase match — first offer wins because the upsell tag bumps score >= 0.
    assert chosen and chosen["name"] in {"Premium", "VIP"}


# ---------------------------------------------------------------------------
# audience_filter_passes
# ---------------------------------------------------------------------------

def test_audience_filter_passes_no_filter():
    assert audience_filter_passes(_client(), None) is True
    assert audience_filter_passes(_client(), {}) is True


def test_audience_filter_lifecycle_in():
    c = _client(lifecycle="active")
    assert audience_filter_passes(c, {"lifecycle_in": ["active"]}) is True
    assert audience_filter_passes(c, {"lifecycle_in": ["offboarding"]}) is False


def test_audience_filter_min_revenue():
    c = _client(lifetime_cents=49_999)
    assert audience_filter_passes(c, {"min_lifetime_revenue_cents": 50_000}) is False
    c2 = _client(lifetime_cents=50_000)
    assert audience_filter_passes(c2, {"min_lifetime_revenue_cents": 50_000}) is True


def test_audience_filter_program_progress_band():
    c = _client(progress=80)
    assert audience_filter_passes(
        c, {"program_progress_min_percent": 50, "program_progress_max_percent": 90}
    ) is True
    assert audience_filter_passes(c, {"program_progress_min_percent": 95}) is False


def test_audience_filter_program_progress_missing_fails_filter():
    c = _client(progress=None)
    assert audience_filter_passes(c, {"program_progress_min_percent": 1}) is False


# ---------------------------------------------------------------------------
# Merge-tag renderer
# ---------------------------------------------------------------------------

def test_render_merge_tags_basic():
    out = render_merge_tags("Hi {{first_name}}!", {"first_name": "Alex"})
    assert out == "Hi Alex!"


def test_render_merge_tags_unknown_token_kept():
    out = render_merge_tags("Hi {{first_name}} — {{mystery}}", {"first_name": "Alex"})
    # Unknown tokens are left as-is so an operator notices the mistake.
    assert out == "Hi Alex — {{mystery}}"


def test_render_merge_tags_handles_empty_template():
    assert render_merge_tags("", {"first_name": "Alex"}) == ""


def test_build_merge_tag_values_picks_referral_link_from_asset_links():
    c = _client(first_name="Sam", last_name="Lee", email="sam@example.com")
    ai_profile = {
        "coach_name": "Coach Jordan",
        "asset_links": [
            {"label": "Website", "url": "https://example.com"},
            {"label": "Referral signup", "url": "https://refer.example.com/sam"},
        ],
    }
    ladder = {"referral_offer": {"incentive": "Free month for both", "eligibility": "active clients"}}
    values = build_merge_tag_values(
        client=c,
        org_name="Sweep Coaching",
        ai_profile=ai_profile,
        ladder=ladder,
        chosen_opportunities=["referral"],
    )
    assert values["first_name"] == "Sam"
    assert values["coach_name"] == "Coach Jordan"
    assert values["referral_link"] == "https://refer.example.com/sam"
    assert values["referral_offer"] == "Free month for both"


def test_build_merge_tag_values_falls_back_to_org_name_for_coach():
    c = _client()
    values = build_merge_tag_values(
        client=c,
        org_name="Sweep Coaching",
        ai_profile=None,
        ladder=None,
        chosen_opportunities=[],
    )
    assert values["coach_name"] == "Sweep Coaching"
    assert values["first_name"] == "Alex"


# ---------------------------------------------------------------------------
# recover_in_flight (recovery sweep)
# ---------------------------------------------------------------------------

def test_recover_in_flight_resets_stale_sending_rows():
    """A 'sending' job older than STALE_SENDING_AFTER_SECONDS gets returned to the queue."""
    db = MagicMock()
    update_count = 3
    chained = MagicMock()
    chained.filter.return_value = chained
    chained.update.return_value = update_count
    db.query.return_value = chained

    n = automation_dispatcher.recover_in_flight(db)
    assert n == update_count
    db.commit.assert_called()
    update_call = chained.update.call_args
    assert update_call.args[0]["state"] == JobState.SCHEDULED.value


def test_recover_in_flight_no_stale_does_not_commit():
    db = MagicMock()
    chained = MagicMock()
    chained.filter.return_value = chained
    chained.update.return_value = 0
    db.query.return_value = chained

    n = automation_dispatcher.recover_in_flight(db)
    assert n == 0
    db.commit.assert_not_called()


def test_expire_awaiting_approval_marks_old_jobs_skipped():
    """Awaiting-approval jobs older than approval_ttl_hours should flip to SKIPPED."""
    old_job = SimpleNamespace(
        state=JobState.AWAITING_APPROVAL.value,
        created_at=datetime.utcnow() - timedelta(hours=200),
        error_text=None,
        updated_at=None,
    )
    fresh_job = SimpleNamespace(
        state=JobState.AWAITING_APPROVAL.value,
        created_at=datetime.utcnow() - timedelta(hours=1),
        error_text=None,
        updated_at=None,
    )
    rule_old = SimpleNamespace(approval_ttl_hours=48)
    rule_fresh = SimpleNamespace(approval_ttl_hours=48)

    db = MagicMock()
    chained = MagicMock()
    chained.outerjoin.return_value = chained
    chained.filter.return_value = chained
    chained.all.return_value = [(old_job, rule_old), (fresh_job, rule_fresh)]
    db.query.return_value = chained

    expired = automation_dispatcher.expire_awaiting_approval(db)
    assert expired == 1
    assert old_job.state == JobState.SKIPPED.value
    assert fresh_job.state == JobState.AWAITING_APPROVAL.value
    db.commit.assert_called()


# ---------------------------------------------------------------------------
# Playbook enum surface (sanity)
# ---------------------------------------------------------------------------

def test_playbook_enum_values_match_expected_set():
    """If we ever rename a playbook string, default rule seeding + idempotency keys break."""
    assert {p.value for p in Playbook} == {
        "pre_sale_post_booking",
        "first_payment_onboarding",
        "first_payment_referral",
        "win_combined_ask",
        "offboarding_recap_ask",
    }

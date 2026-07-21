"""Unit tests for unified upsell/add-on validation and client matching."""

from app.services.offer_ladder import (
    OFFER_LADDER_VERSION,
    match_offer_for_client,
    offer_ladder_for_llm,
    validate_offer_ladder,
)


def test_legacy_downsells_are_preserved_as_add_ons():
    ladder = validate_offer_ladder(
        {
            "version": 1,
            "upsells": [
                {
                    "name": "Strategy sprint",
                    "promise": "Build the next plan",
                    "triggers": ["ready for next phase"],
                }
            ],
            "downsells": [
                {
                    "name": "Starter audit",
                    "promise": "Find the first bottleneck",
                    "when_to_use": "Needs a lower-commitment entry point",
                }
            ],
        }
    )

    assert ladder is not None
    assert ladder["version"] == OFFER_LADDER_VERSION
    assert "downsells" not in ladder
    assert [offer["name"] for offer in ladder["upsells"]] == [
        "Strategy sprint",
        "Starter audit",
    ]
    assert ladder["upsells"][1]["triggers"] == [
        "Needs a lower-commitment entry point"
    ]


def test_migration_deduplicates_same_offer():
    shared = {"name": "VIP day", "promise": "Resolve the launch plan"}
    ladder = validate_offer_ladder(
        {"upsells": [shared], "downsells": [{**shared, "when_to_use": "Budget fit"}]}
    )

    assert ladder is not None
    assert len(ladder["upsells"]) == 1


def test_malformed_offer_collections_are_rejected():
    assert validate_offer_ladder({"upsells": "not-a-list"}) is None


def test_llm_projection_exposes_only_unified_collection():
    ladder = validate_offer_ladder(
        {
            "downsells": [
                {
                    "name": "Audit",
                    "promise": "Find gaps",
                    "when_to_use": "Wants a diagnostic",
                }
            ]
        }
    )

    projected = offer_ladder_for_llm(ladder)

    assert projected is not None
    assert "downsells" not in projected
    assert projected["upsells"][0]["name"] == "Audit"
    assert projected["upsells"][0]["triggers"] == ["Wants a diagnostic"]


def test_add_on_requires_expansion_signal_but_matches_client_language():
    ladder = validate_offer_ladder(
        {
            "core_offer": {"name": "Core coaching", "promise": "Build foundations"},
            "upsells": [
                {
                    "name": "Hiring intensive",
                    "promise": "Build the team",
                    "triggers": ["ready to hire"],
                    "contraindications": "Cash flow is unstable",
                }
            ],
        }
    )

    lead_match = match_offer_for_client(
        ladder,
        lifecycle="qualified",
        roi_tags=["conversion"],
        headline="Ready to hire",
    )
    active_match = match_offer_for_client(
        ladder,
        lifecycle="active",
        roi_tags=["upsell"],
        headline="Client says they are ready to hire",
    )

    assert lead_match is not None
    assert lead_match["kind"] == "core"
    assert active_match is not None
    assert active_match["kind"] == "upsell"
    assert active_match["name"] == "Hiring intensive"
    assert "Cash flow is unstable" in active_match["script_hint"]


def test_contraindication_prevents_offer_selection():
    ladder = validate_offer_ladder(
        {
            "upsells": [
                {
                    "name": "VIP intensive",
                    "promise": "Accelerate delivery",
                    "triggers": ["wants faster implementation"],
                    "contraindications": "cash flow is unstable",
                }
            ]
        }
    )

    match = match_offer_for_client(
        ladder,
        lifecycle="active",
        roi_tags=["upsell"],
        headline="Wants faster implementation but cash flow remains unstable",
    )

    assert match is None

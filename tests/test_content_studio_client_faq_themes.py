"""CLIENT_FAQ_THEMES: validated kind="question" themes ground TOF/MOF concepts via the
existing _enforce_stage_signal_grounding citation backstop. See
docs/prd/content-generation-signal-upgrades/ISSUES.md issue 7."""
from app.services.content_studio_bundle import _enforce_stage_signal_grounding


def _faq_theme():
    return {
        "theme_key": "faq1",
        "label": "Onboarding schedule confusion",
        "distinct_client_count": 3,
        "occurrence_count": 4,
        "sample_quotes": ["Wait, how does the onboarding schedule actually work after I sign up?"],
    }


def test_tof_concept_gets_cited_from_client_faq_themes():
    signals = {
        "has_any": True,
        "themes": [],
        "insights": [],
        "active_client_insights": [],
        "meeting_summaries": [],
        "client_faq_themes": [_faq_theme()],
    }
    stages_out = [
        {
            "id": "TOF",
            "concepts": [
                {
                    "title": "What actually happens after you sign up",
                    "hook": "Most people have no idea how the onboarding schedule works.",
                    "bullets": ["onboarding schedule confusion", "clarify the timeline"],
                    "why_for_icp": "Clears up a common point of confusion before it becomes a hesitation.",
                }
            ],
        }
    ]

    _enforce_stage_signal_grounding(stages_out, signals)

    why = stages_out[0]["concepts"][0]["why_for_icp"]
    assert "fathom signal:" in why.lower()
    assert "onboarding" in why.lower()


def test_unmatched_concept_flagged_unverified_even_with_other_pool_entries():
    """Pool is non-empty (via `themes`) but shares no overlap with this concept — the
    backstop must flag it rather than fabricate a citation. An entirely empty pool is a
    separate, already-covered case (whole stage skipped, data-availability intro handles it)."""
    signals = {
        "has_any": True,
        "themes": [{"label": "zzzcompletelyunrelatedzzz", "sample_quotes": []}],
        "insights": [],
        "active_client_insights": [],
        "meeting_summaries": [],
        "client_faq_themes": [],
    }
    stages_out = [
        {
            "id": "TOF",
            "concepts": [
                {
                    "title": "Totally unrelated concept",
                    "hook": "Some generic hook with no overlap.",
                    "bullets": [],
                    "why_for_icp": "A generic rationale with nothing to ground it.",
                }
            ],
        }
    ]

    _enforce_stage_signal_grounding(stages_out, signals)

    why = stages_out[0]["concepts"][0]["why_for_icp"]
    assert "[unverified:" in why.lower()

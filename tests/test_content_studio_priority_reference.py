"""Unit tests for the onboarding-survey "priority reference post" citation logic in
content_studio_bundle.py — the deterministic post-processing that guarantees a structured
priority_reference badge lands on a real concept (Issue 6 of the onboarding survey PRD)."""
from __future__ import annotations

from app.services.content_studio_bundle import (
    _enforce_priority_reference_citation,
    _normalize_concept,
)


def _stage(stage_id, concepts):
    return {"id": stage_id, "title": stage_id, "intro": "", "concepts": concepts}


def _concept(why_for_icp="", **kwargs):
    base = {
        "id": "c1",
        "format": "short",
        "title": "A concept",
        "hook": "hook",
        "bullets": [],
        "why_for_icp": why_for_icp,
        "funnel_path_to_sale": "",
        "priority_reference": None,
    }
    base.update(kwargs)
    return base


class TestNormalizeConceptPriorityReferenceField:
    def test_defaults_to_none(self):
        concept = _normalize_concept({"title": "x", "why_for_icp": "y"})
        assert concept["priority_reference"] is None


class TestEnforcePriorityReferenceCitation:
    def test_noop_when_no_priority_reference_posts(self):
        stages = [_stage("MOF", [_concept()])]

        _enforce_priority_reference_citation(stages, [])

        assert stages[0]["concepts"][0]["priority_reference"] is None

    def test_matches_llm_prose_that_already_cites_a_permalink(self):
        posts = [
            {"permalink": "https://instagram.com/p/AAA", "selection_count": 4},
            {"permalink": "https://instagram.com/p/BBB", "selection_count": 1},
        ]
        concept = _concept(
            why_for_icp="Some reasoning. Proven-conversion reference: https://instagram.com/p/BBB (selected by 1 client(s))."
        )
        stages = [_stage("MOF", [concept])]

        _enforce_priority_reference_citation(stages, posts)

        assert concept["priority_reference"] == {
            "rank": 2,
            "selection_count": 1,
            "permalink": "https://instagram.com/p/BBB",
        }

    def test_force_attaches_top_post_to_first_mof_concept_when_llm_omits_citation(self):
        posts = [{"permalink": "https://instagram.com/p/TOP", "selection_count": 3}]
        tof_concept = _concept(id="tof1")
        mof_concept = _concept(id="mof1")
        stages = [_stage("TOF", [tof_concept]), _stage("MOF", [mof_concept])]

        _enforce_priority_reference_citation(stages, posts)

        assert tof_concept["priority_reference"] is None
        assert mof_concept["priority_reference"] == {
            "rank": 1,
            "selection_count": 3,
            "permalink": "https://instagram.com/p/TOP",
        }
        assert "Proven-conversion reference: https://instagram.com/p/TOP" in mof_concept["why_for_icp"]
        assert "selected by 3 client(s)" in mof_concept["why_for_icp"]

    def test_falls_back_to_bof_when_no_mof_stage(self):
        posts = [{"permalink": "https://instagram.com/p/TOP", "selection_count": 2}]
        bof_concept = _concept(id="bof1")
        stages = [_stage("TOF", [_concept(id="tof1")]), _stage("BOF", [bof_concept])]

        _enforce_priority_reference_citation(stages, posts)

        assert bof_concept["priority_reference"]["permalink"] == "https://instagram.com/p/TOP"

    def test_skips_fallback_when_top_post_has_no_permalink(self):
        posts = [{"permalink": None, "selection_count": 2}]
        concept = _concept()
        stages = [_stage("MOF", [concept])]

        _enforce_priority_reference_citation(stages, posts)

        assert concept["priority_reference"] is None

"""Content Studio automatic regen trigger.

The old trigger fired on every signals-fingerprint change, burning an LLM call per
bootstrap whenever Fathom/Instagram signals shifted even if the user hadn't touched
the current batch. It should now only fire when the stored bundle is unusable, or the
user has worked through most of a stage's concepts.
"""
from types import SimpleNamespace

from app.api.content_studio import (
    STAGE_DEPLETION_THRESHOLD,
    _bundle_is_valid,
    _stage_depletion_trigger,
)
from app.services.content_studio_bundle import BUNDLE_VERSION


def _row(ideas_json):
    return SimpleNamespace(ideas_json=ideas_json)


def _bundle(**stage_concepts):
    """stage_concepts: e.g. TOF=["a", "b", "c"] -> stage with those concept ids."""
    return {
        "version": BUNDLE_VERSION,
        "stages": [
            {"id": stage, "concepts": [{"id": cid} for cid in ids]}
            for stage, ids in stage_concepts.items()
        ],
    }


def test_missing_row_is_invalid():
    assert _bundle_is_valid(None) is False
    assert _bundle_is_valid(_row(None)) is False


def test_old_version_is_invalid():
    assert _bundle_is_valid(_row({"version": BUNDLE_VERSION - 1, "stages": []})) is False


def test_current_version_is_valid():
    assert _bundle_is_valid(_row(_bundle(TOF=["a", "b", "c"]))) is True


def test_no_depletion_with_no_completions():
    row = _row(_bundle(TOF=["a", "b", "c"], MOF=["d", "e"], BOF=["f", "g", "h"]))
    assert _stage_depletion_trigger(row, []) is False


def test_no_depletion_below_threshold():
    """2 of 3 TOF concepts done (66%) stays under the 70% threshold."""
    row = _row(_bundle(TOF=["a", "b", "c"]))
    assert _stage_depletion_trigger(row, ["a", "b"]) is False


def test_depletion_triggers_at_threshold_for_any_single_stage():
    """All of TOF done should trigger even though MOF/BOF are untouched — one depleted
    section is enough; the user doesn't need to exhaust every stage first."""
    row = _row(_bundle(TOF=["a", "b", "c"], MOF=["d", "e", "f"]))
    assert _stage_depletion_trigger(row, ["a", "b", "c"]) is True


def test_depletion_ignores_stale_ids_from_a_previous_batch():
    """Completed ids left over from an older batch don't match the current bundle's
    concept ids, so they can't falsely trigger regen."""
    row = _row(_bundle(TOF=["a", "b", "c"]))
    assert _stage_depletion_trigger(row, ["stale-1", "stale-2", "stale-3"]) is False


def test_depletion_requires_valid_bundle():
    assert _stage_depletion_trigger(_row(None), ["a"]) is False
    assert _stage_depletion_trigger(_row({"version": BUNDLE_VERSION - 1, "stages": []}), ["a"]) is False


def test_threshold_is_the_documented_value():
    assert STAGE_DEPLETION_THRESHOLD == 0.7

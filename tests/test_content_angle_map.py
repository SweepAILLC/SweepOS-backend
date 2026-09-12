"""Unit tests for Content Angle Map validation, merge, and refresh gates."""
from datetime import datetime, timedelta
from types import SimpleNamespace
from uuid import uuid4

from app.services.content_angle_map import (
    compute_input_fingerprint,
    merge_angles,
    normalize_pills,
    should_auto_refresh,
    validate_angle_phrases,
)


def test_normalize_pills_dedupes_and_allows_empty():
    assert normalize_pills([" Vlog ", "vlog", "Case studies", ""]) == ["Vlog", "Case studies"]
    assert normalize_pills([]) == []
    assert normalize_pills(None) == []


def test_validate_drops_sentences_dups_and_verbatim_source():
    source = "Busy founders who feel stuck charging too little and fear another wasted year"
    phrases = [
        "Fear of another wasted year.",
        "Fear of another wasted year",
        "Identity as the cheap option",
        "They cannot raise prices without guilt!",
        "Busy founders who feel stuck charging too little and fear another wasted year",
        "x",
        "A full sentence that goes on far too long to be a short messaging angle for this card",
        "Anxiety about staying small",
        "Anxiety about staying tiny",
    ]
    out = validate_angle_phrases(phrases, source)
    assert "Fear of another wasted year" in out
    assert "Identity as the cheap option" in out
    assert "They cannot raise prices without guilt" in out
    assert all(source.lower() not in p.lower() for p in out)
    assert out.count("Fear of another wasted year") == 1
    assert len(out) <= 6


def test_merge_preserves_manually_edited_unless_full():
    locked_id = str(uuid4())
    existing = [
        {"id": locked_id, "text": "Human kept this", "manually_edited": True},
        {"id": str(uuid4()), "text": "Old unlocked", "manually_edited": False},
    ]
    generated = [
        {"id": str(uuid4()), "text": "New angle one", "manually_edited": False},
        {"id": str(uuid4()), "text": "New angle two", "manually_edited": False},
    ]
    merged = merge_angles(existing, generated, full=False)
    assert merged[0]["text"] == "Human kept this"
    assert merged[0]["id"] == locked_id
    assert merged[1]["text"] == "New angle one"

    replaced = merge_angles(existing, generated, full=True)
    assert [a["text"] for a in replaced] == ["New angle one", "New angle two"]


def test_fingerprint_changes_with_inputs():
    a = compute_input_fingerprint("ICP A", "Story", "sig1")
    b = compute_input_fingerprint("ICP B", "Story", "sig1")
    c = compute_input_fingerprint("ICP A", "Story", "sig1")
    assert a != b
    assert a == c


def test_auto_refresh_skips_same_fingerprint():
    row = SimpleNamespace(
        input_fingerprint="abc",
        last_generated_at=datetime.utcnow() - timedelta(days=30),
        calls_seen_at_generation=0,
    )
    assert should_auto_refresh(row, current_call_count=20, new_fingerprint="abc") is False


def test_auto_refresh_after_n_new_calls():
    row = SimpleNamespace(
        input_fingerprint="old",
        last_generated_at=datetime.utcnow(),
        calls_seen_at_generation=2,
    )
    assert should_auto_refresh(row, current_call_count=7, new_fingerprint="new") is True
    assert should_auto_refresh(row, current_call_count=4, new_fingerprint="new") is False


def test_generate_card_works_without_fathom(monkeypatch):
    from app.services import content_angle_map as cam

    org_id = uuid4()
    row = SimpleNamespace(
        org_id=org_id,
        icp_angles=[],
        personal_brand_angles=[],
        format_pills_tof=list(cam.DEFAULT_PILLS_TOF),
        format_pills_mof=list(cam.DEFAULT_PILLS_MOF),
        format_pills_bof=list(cam.DEFAULT_PILLS_BOF),
        last_generated_at=None,
        last_generated_icp_at=None,
        last_generated_brand_at=None,
        calls_seen_at_generation=0,
        input_fingerprint=None,
        updated_at=None,
    )
    db = SimpleNamespace()

    monkeypatch.setattr(
        cam,
        "resolve_org_ai_profile_dict",
        lambda _db, _oid: {"target_audience": "Founders who undercharge", "personal_story": ""},
    )
    monkeypatch.setattr(cam, "_collect_signals", lambda _db, _oid: {"themes": [], "insights": [], "has_any": False})
    monkeypatch.setattr(
        cam,
        "_draft_card_llm",
        lambda *_a, **_k: ["Fear of staying small", "Guilt about raising prices", "Anxiety they are not elite"],
    )
    monkeypatch.setattr(cam, "get_or_create_map", lambda _db, _oid: row)
    monkeypatch.setattr(cam, "flag_modified", lambda *_a, **_k: None)
    monkeypatch.setattr(cam, "complete_insight_count", lambda _db, _oid: 0)
    monkeypatch.setattr(
        cam,
        "load_map_out",
        lambda _db, _oid: SimpleNamespace(icp_angles=row.icp_angles, personal_brand_angles=[]),
    )
    db.add = lambda _r: None
    db.commit = lambda: None
    db.refresh = lambda _r: None

    out = cam.generate_card(db, org_id, "icp", full=True)
    assert len(row.icp_angles) == 3
    assert row.icp_angles[0]["text"] == "Fear of staying small"
    assert out.icp_angles == row.icp_angles


def test_auto_refresh_after_seven_days():
    row = SimpleNamespace(
        input_fingerprint="old",
        last_generated_at=datetime.utcnow() - timedelta(days=8),
        calls_seen_at_generation=10,
    )
    assert should_auto_refresh(row, current_call_count=10, new_fingerprint="new") is True

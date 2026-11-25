"""Tests for the extra MCP tools: payments/MRR, Meta Ads, call transcripts, Instagram, EOD write."""

from __future__ import annotations

import inspect
import json
import uuid
from datetime import date
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from app.mcp import extra_tools as et
from app.mcp import server as mcp_server

ORG = uuid.uuid4()


def _payload(result: dict) -> dict:
    return json.loads(result["content"][0]["text"])


def _run(name, args=None, db=None, auth=None):
    return _payload(
        mcp_server._run_tool(name, args or {}, ORG, db or MagicMock(), user_id=uuid.uuid4(), auth_ctx=auth)
    )


# ---------------------------------------------------------------- registration
def test_all_extra_tools_registered_with_annotations():
    by_name = {t["name"]: t for t in mcp_server.TOOLS}
    for tool in et.EXTRA_TOOLS:
        reg = by_name[tool["name"]]
        assert reg["inputSchema"]["type"] == "object"
        expect_write = tool["name"] in et.EXTRA_WRITE_TOOLS
        assert reg["annotations"]["readOnlyHint"] is (not expect_write)
        assert reg["annotations"]["openWorldHint"] is expect_write
    assert len({t["name"] for t in mcp_server.TOOLS}) == len(mcp_server.TOOLS), "duplicate tool names"
    assert et.EXTRA_WRITE_TOOLS == {"update_kpi_daily_entry", "transcribe_instagram_post"}


# Each tool -> (module path of the REST handler it wraps, handler attribute)
_HANDLERS = {
    "get_finances_summary": ("app.api.finances", "finances_summary"),
    "get_revenue_timeline": ("app.api.finances", "finances_revenue_timeline"),
    "get_mrr": ("app.api.stripe", "get_stripe_summary"),
    "list_payments": ("app.api.stripe", "get_payments"),
    "list_subscriptions": ("app.api.stripe", "get_subscriptions"),
    "list_failed_payments": ("app.api.stripe", "get_failed_payments"),
    "get_meta_ads_dashboard": ("app.api.meta_ads", "ads_dashboard"),
    "list_meta_ad_sets": ("app.api.meta_ads", "list_ad_set_rows"),
    "list_meta_ads": ("app.api.meta_ads", "list_ad_rows"),
    "get_meta_ads_alerts": ("app.api.meta_ads", "list_ads_alerts"),
}


@pytest.mark.parametrize("tool", sorted(_HANDLERS))
def test_rest_handlers_get_every_parameter_explicitly(tool):
    """Handlers are called directly, so an omitted arg would leak a FastAPI Query(...) default."""
    import importlib

    mod_path, attr = _HANDLERS[tool]
    mod = importlib.import_module(mod_path)
    real = getattr(mod, attr)
    wanted = set(inspect.signature(real).parameters)
    fake = MagicMock(return_value={"ok": True})
    fake.__name__ = attr
    with patch.object(mod, attr, fake):
        _run(tool)  # get_mrr reshapes the payload, so only the call itself is asserted here
    assert fake.called
    passed = set(fake.call_args.kwargs)
    assert passed == wanted, f"{tool}: missing {wanted - passed}, extra {passed - wanted}"
    assert not any(type(v).__module__.startswith("fastapi") for v in fake.call_args.kwargs.values())


def test_mrr_returns_scalars_not_full_lists_and_optional_trend():
    from app.api import stripe as stripe_api

    summary = {k: 1 for k in et._MRR_KEYS}
    summary.update({"payments": [1] * 500, "customers": [1] * 500})
    with patch.object(stripe_api, "get_stripe_summary", MagicMock(return_value=summary, __name__="s")), patch.object(
        stripe_api, "get_mrr_trend", MagicMock(return_value={"points": []}, __name__="t")
    ):
        with patch.object(et, "_whop_recurring", return_value=None):
            out = _run("get_mrr")
        assert set(out) == set(et._MRR_KEYS)
        out = _run("get_mrr", {"include_trend": True, "group_by": "month"})
        assert out["trend"] == {"points": []}


def test_mrr_passes_through_errors():
    from fastapi import HTTPException

    from app.api import stripe as stripe_api

    with patch.object(stripe_api, "get_stripe_summary", MagicMock(side_effect=HTTPException(404, "Stripe not connected."), __name__="s")), patch.object(
        et, "_whop_recurring", return_value=None
    ):
        assert _run("get_mrr") == {"error": "Stripe not connected."}


def test_mrr_adds_whop_estimate_and_combined_totals():
    from app.api import stripe as stripe_api

    summary = {k: 0 for k in et._MRR_KEYS}
    summary.update({"total_mrr": 1000.0, "total_arr": 12000.0})
    whop = {"estimated_mrr": 250.5, "estimated_arr": 3006.0}
    with patch.object(stripe_api, "get_stripe_summary", MagicMock(return_value=summary, __name__="s")), patch.object(
        et, "_whop_recurring", return_value=whop
    ):
        out = _run("get_mrr")
    assert out["whop"] == whop
    assert out["combined_mrr"] == 1250.5
    assert out["combined_arr"] == 15006.0


def test_mrr_returns_whop_when_stripe_not_connected():
    from fastapi import HTTPException

    from app.api import stripe as stripe_api

    whop = {"estimated_mrr": 99.0, "estimated_arr": 1188.0}
    with patch.object(stripe_api, "get_stripe_summary", MagicMock(side_effect=HTTPException(404, "Stripe not connected."), __name__="s")), patch.object(
        et, "_whop_recurring", return_value=whop
    ):
        out = _run("get_mrr")
    assert out == {"stripe": {"error": "Stripe not connected."}, "whop": whop}


def _wp(amount, days_ago, now, key="m1", reason=None, status="paid"):
    from datetime import timedelta

    raw = {"membership_id": key}
    if reason:
        raw["billing_reason"] = reason
    return SimpleNamespace(
        status=status, amount_cents=amount, created_at=now - timedelta(days=days_ago), raw=raw, client_id=None
    )


def _whop_db(rows):
    db = MagicMock()
    db.query.return_value.filter.return_value.order_by.return_value.all.return_value = rows
    return db


def test_whop_estimate_counts_active_recurring_and_ignores_one_offs():
    from datetime import datetime

    from app.services.whop_recurring import estimate_whop_mrr

    now = datetime(2026, 6, 1)
    rows = [
        _wp(9700, 62, now, "monthly"), _wp(9700, 32, now, "monthly"), _wp(9700, 2, now, "monthly"),  # active, $97/mo
        _wp(30000, 70, now, "quarterly"), _wp(30000, 5, now, "quarterly"),                            # $300/qtr -> $100/mo
        _wp(5000, 200, now, "gone"), _wp(5000, 170, now, "gone"),                                     # lapsed
        _wp(49700, 10, now, "oneoff"),                                                                # one-off
        _wp(2000, 3, now, "failed", status="failed"),                                                 # not paid
        _wp(4900, 1, now, "fresh", reason="subscription_create"),                                     # new sub, $49/mo
    ]
    out = estimate_whop_mrr(_whop_db(rows), ORG, now=now)
    assert out["active_recurring_payers"] == 3
    assert out["lapsed_recurring_payers"] == 1
    assert out["estimated_mrr"] == pytest.approx(97 + 100 + 49, abs=0.01)
    assert out["estimated_arr"] == pytest.approx((97 + 100 + 49) * 12, abs=0.1)


def test_whop_estimate_empty():
    from datetime import datetime

    from app.services.whop_recurring import estimate_whop_mrr

    out = estimate_whop_mrr(_whop_db([]), ORG, now=datetime(2026, 6, 1))
    assert out["estimated_mrr"] == 0 and out["active_recurring_payers"] == 0


def test_meta_ads_respects_tab_lock():
    """Handlers enforce content_studio themselves; a locked tab must come back as an error."""
    with patch("app.api.meta_ads.check_tab_access", return_value=False):
        out = _run("list_meta_ads")
    assert "not enabled" in out["error"]


def test_meta_ads_bad_date_is_invalid_input_not_500():
    out = _run("get_meta_ads_dashboard", {"start": "nope"})
    assert out["error"] == "invalid_input"


# ---------------------------------------------------------------- transcripts
def test_format_transcript_variants():
    segs = {"transcript": [
        {"speaker": {"display_name": "Coach"}, "text": "Hi", "timestamp": "00:01"},
        {"speaker": {"display_name": "Client"}, "text": "  "},
        {"text": "Anon"},
    ]}
    assert et.format_transcript(segs) == "Coach [00:01]: Hi\nSpeaker: Anon"
    assert et.format_transcript("plain") == "plain"
    assert et.format_transcript({}) == ""


def _rec(**kw):
    base = dict(
        id=uuid.uuid4(), fathom_recording_id=42, meeting_title="Strategy call", meeting_at=None,
        client_id=None, related_client_ids=None, attendees_json=None, summary_text="s",
        share_url="https://fathom.video/share/x", recording_url=None, transcript_snippet="snippet",
    )
    base.update(kw)
    return SimpleNamespace(**base)


def _db_with_first(rec):
    db = MagicMock()
    chain = db.query.return_value.filter.return_value
    chain.filter.return_value.first.return_value = rec
    chain.first.return_value = rec
    return db


def test_get_call_transcript_pages_long_transcripts():
    rec = _rec()
    long_text = "x" * 2500
    with patch("app.services.fathom_client.get_recording_transcript", return_value=long_text):
        first = et.get_call_transcript(_db_with_first(rec), ORG, {"fathom_recording_id": 42, "max_chars": 1000})
        assert first["total_chars"] == 2500 and first["returned_chars"] == 1000 and first["next_offset"] == 1000
        last = et.get_call_transcript(
            _db_with_first(rec), ORG, {"fathom_recording_id": 42, "max_chars": 1000, "offset": 2000}
        )
        assert last["returned_chars"] == 500 and last["next_offset"] is None


def test_get_call_transcript_falls_back_to_snippet_when_fathom_fails():
    with patch("app.services.fathom_client.get_recording_transcript", side_effect=RuntimeError("Fathom not configured")):
        out = et.get_call_transcript(_db_with_first(_rec()), ORG, {"call_id": str(uuid.uuid4())})
    assert out["transcript"] == "snippet" and "unavailable" in out["note"]


def test_get_call_transcript_unknown_or_other_org_call_is_not_found():
    out = et.get_call_transcript(_db_with_first(None), ORG, {"call_id": str(uuid.uuid4())})
    assert out == {"error": "call not found"}
    assert et.get_call_transcript(MagicMock(), ORG, {}) == {"error": "call_id or fathom_recording_id required"}


def test_call_queries_are_org_scoped():
    """Both call tools must filter on the caller's org_id (multi-tenant isolation)."""
    from app.models.fathom_call_record import FathomCallRecord

    db = MagicMock()
    et.list_call_recordings(db, ORG, {})
    first_filter = db.query.return_value.filter.call_args.args
    assert any(str(a) == str(FathomCallRecord.org_id == ORG) for a in first_filter)


# ---------------------------------------------------------------- instagram
def test_instagram_tools_blocked_when_marketing_intel_locked():
    db = MagicMock()
    with patch("app.api.deps.check_tab_access", return_value=False):
        for tool in ("list_instagram_posts", "get_instagram_post", "get_instagram_account_history"):
            assert "not enabled" in _run(tool, {"id": str(uuid.uuid4())}, db=db)["error"]
    db.query.assert_not_called()


def _media(transcript):
    cols = {k: None for k in et._IG_METRICS}
    return SimpleNamespace(
        id=uuid.uuid4(), ig_media_id="1", permalink="p", media_type="VIDEO", media_product_type="REELS",
        posted_at=None, caption="c", hook_text="h", hook_pattern=None, format_bucket="talking_head",
        funnel_stage="TOF", theme_keys=["a"], transcript_status="ok", transcript=transcript, **cols,
    )


def test_instagram_list_excerpts_unless_full_transcript_requested():
    long = "word " * 200
    db = MagicMock()
    q = db.query.return_value.filter.return_value
    q.order_by.return_value.limit.return_value.all.return_value = [_media(long)]
    excerpt = et.list_instagram_posts(db, ORG, {})
    assert excerpt["transcripts"] == "excerpt" and len(excerpt["posts"][0]["transcript"]) <= 301
    full = et.list_instagram_posts(db, ORG, {"include_transcript": True})
    assert full["posts"][0]["transcript"] == long


def test_page_sizes_are_capped_so_tool_results_stay_valid_json():
    # _text_result truncates at 140k chars, which would corrupt the JSON.
    with patch("app.services.fathom_client.get_recording_transcript", return_value="x" * 200_000):
        out = et.get_call_transcript(_db_with_first(_rec()), ORG, {"call_id": str(uuid.uuid4()), "max_chars": 999_999})
    assert out["returned_chars"] == 60_000 and out["next_offset"] == 60_000
    db = MagicMock()
    limit = db.query.return_value.filter.return_value.order_by.return_value.limit
    limit.return_value.all.return_value = []
    et.list_instagram_posts(db, ORG, {"include_transcript": True, "limit": 100})
    limit.assert_called_with(15)


def test_instagram_get_post_requires_identifier():
    assert et.get_instagram_post(MagicMock(), ORG, {}) == {"error": "id or ig_media_id required"}


def test_instagram_sort_is_allowlisted():
    db = MagicMock()
    db.query.return_value.filter.return_value.order_by.return_value.limit.return_value.all.return_value = []
    # An attacker-controlled sort_by must fall back to posted_at, never reach getattr().
    assert et.list_instagram_posts(db, ORG, {"sort_by": "__class__"})["count"] == 0


# ---------------------------------------------------------------- EOD write
def test_eod_write_is_gated_by_scope_and_opt_in():
    db = MagicMock()
    no_scope = _run("update_kpi_daily_entry", {"entry_date": "2026-11-20", "outreach_sent": 1}, db=db, auth={"scope": "x"})
    assert no_scope["error"] == "insufficient_scope"
    db.query.return_value.filter.return_value.first.return_value = SimpleNamespace(mcp_scorecard_write_enabled=False)
    off = _run("update_kpi_daily_entry", {"entry_date": "2026-11-20", "outreach_sent": 1}, db=db, auth={"scope": "funnels:write"})
    assert off["error"] == "scorecard_writes_disabled"


@pytest.mark.parametrize(
    "args,msg",
    [
        ({"outreach_sent": -1}, "between 0"),
        ({"outreach_sent": 1.5}, "whole number"),
        ({"outreach_sent": True}, "whole number"),
        ({"cash_collected": -5}, "cash_collected"),
        ({"cash_collected": "100"}, "cash_collected"),
        ({"content_posted": "yes"}, "true or false"),
        ({"setter_context": "x" * 5001}, "5000"),
        ({"setter_booked_client_ids": ["not-a-uuid"]}, "invalid UUID"),
        ({"setter_booked_client_ids": "abc"}, "list"),
    ],
)
def test_eod_payload_validation(args, msg):
    with pytest.raises(ValueError, match=msg):
        et.build_eod_payload(MagicMock(), ORG, args)


def test_eod_rejects_client_ids_from_another_org():
    db = MagicMock()
    db.query.return_value.filter.return_value.count.return_value = 0
    with pytest.raises(ValueError, match="not in this org"):
        et.build_eod_payload(db, ORG, {"setter_booked_client_ids": [str(uuid.uuid4())]})


def test_eod_payload_normalises_values():
    out = et.build_eod_payload(MagicMock(), ORG, {"outreach_sent": 40.0, "cash_collected": 1500.456, "content_posted": False})
    assert out["outreach_sent"] == 40 and str(out["cash_collected"]) == "1500.46" and out["content_posted"] is False


def test_update_eod_rejects_unknown_fields_and_empty_writes():
    assert et.update_kpi_daily_entry(MagicMock(), ORG, {"entry_date": "2026-11-20", "bogus": 1}, None)["error"] == "unknown_fields"
    out = et.update_kpi_daily_entry(MagicMock(), ORG, {"entry_date": "2026-11-20"}, None)
    assert out["error"] == "invalid_input"
    bad_mode = et.update_kpi_daily_entry(MagicMock(), ORG, {"entry_date": "2026-11-20", "mode": "x"}, None)
    assert bad_mode["error"] == "invalid_input"
    assert et.update_kpi_daily_entry(MagicMock(), ORG, {"entry_date": "garbage", "outreach_sent": 1}, None)["error"]


def test_update_eod_never_marks_rep_submitted_and_maps_mode():
    from app.api import kpi as kpi_api

    rep = uuid.uuid4()
    with patch.object(kpi_api, "_validated_rep_user_id", return_value=rep), patch.object(
        kpi_api, "_upsert_kpi_entry_for_org", return_value={"outreach_sent": 45}
    ) as upsert:
        out = et.update_kpi_daily_entry(
            MagicMock(), ORG,
            {"entry_date": "2026-11-20", "rep_user_id": str(rep), "mode": "add", "outreach_sent": 5},
            uuid.uuid4(),
        )
    assert out["saved"] is True and out["rep_user_id"] == str(rep)
    kwargs = upsert.call_args.kwargs
    assert kwargs["mark_submitted"] is False
    assert kwargs["additive"] is True
    assert kwargs["rep_user_id"] == rep and kwargs["org_id"] == ORG
    assert kwargs["entry_day"] == date(2026, 11, 20)
    assert kwargs["payload"] == {"outreach_sent": 5}


def test_update_eod_rep_must_belong_to_org():
    from fastapi import HTTPException

    from app.api import kpi as kpi_api

    with patch.object(kpi_api, "_validated_rep_user_id", side_effect=HTTPException(400, "rep_user_id is not a member of this org")):
        out = et.update_kpi_daily_entry(
            MagicMock(), ORG, {"entry_date": "2026-11-20", "rep_user_id": str(uuid.uuid4()), "outreach_sent": 1}, None
        )
    assert "not a member" in out["error"]


def test_update_eod_rolls_back_on_failure():
    from app.api import kpi as kpi_api

    db = MagicMock()
    with patch.object(kpi_api, "_upsert_kpi_entry_for_org", side_effect=RuntimeError("boom")):
        out = et.update_kpi_daily_entry(db, ORG, {"entry_date": "2026-11-20", "outreach_sent": 1}, None)
    assert out == {"error": "internal error"}
    db.rollback.assert_called_once()


# ---------------------------------------------------------------- IG on-demand transcription
def _ig_row(**kw):
    base = dict(id=uuid.uuid4(), ig_media_id="ig1", media_type="VIDEO", media_product_type="REELS",
                transcript=None, transcript_status="unavailable", transcript_error=None, transcribed_at=None)
    base.update(kw)
    return SimpleNamespace(**base)


def _ig_db(rows, used_today=0):
    """query(...).filter(...).count() -> used_today; .filter(...).filter(...).first() -> next row."""
    db = MagicMock()
    q = db.query.return_value.filter.return_value
    q.count.return_value = used_today
    q.filter.return_value.first.side_effect = list(rows)
    return db


def _fake_transcribe(text="hello world"):
    def run(db, org_id, media_id):
        row = next(r for r in _fake_transcribe.rows if r.id == media_id)
        row.transcript, row.transcript_status = text, "ok"
        return row
    return run


def test_transcribe_tool_gated_by_marketing_intel_and_is_not_read_only():
    db = MagicMock()
    with patch("app.api.deps.check_tab_access", return_value=False):
        assert "not enabled" in _run("transcribe_instagram_post", {"ids": ["x"]}, db=db)["error"]
    db.query.assert_not_called()
    tool = next(t for t in mcp_server.TOOLS if t["name"] == "transcribe_instagram_post")
    assert tool["annotations"]["readOnlyHint"] is False


def test_transcribe_requires_ids_and_caps_per_call():
    assert "ids required" in et.transcribe_instagram_post(MagicMock(), ORG, {})["error"]
    too_many = et.transcribe_instagram_post(MagicMock(), ORG, {"ids": ["a", "b", "c", "d"]})
    assert "at most" in too_many["error"]


def test_transcribe_runs_whisper_and_reports_remaining_quota():
    row = _ig_row()
    _fake_transcribe.rows = [row]
    with patch("app.services.instagram_transcript_service.transcribe_media", _fake_transcribe("hi there")):
        out = et.transcribe_instagram_post(_ig_db([row], used_today=3), ORG, {"ids": [str(row.id)]})
    assert out["results"][0]["status"] == "ok"
    assert out["results"][0]["transcript"] == "hi there"
    assert out["attempts_remaining_today"] == 6  # default daily limit 10, 3 used, 1 spent now


def test_transcribe_skips_already_done_images_and_recent_failures_without_charging():
    from datetime import datetime

    done = _ig_row(transcript="t", transcript_status="ok")
    image = _ig_row(media_type="IMAGE", media_product_type="FEED")
    failed = _ig_row(transcript_status="failed", transcribed_at=datetime.utcnow(), transcript_error="boom")
    with patch("app.services.instagram_transcript_service.transcribe_media") as whisper:
        out = et.transcribe_instagram_post(_ig_db([done], 0), ORG, {"ids": ["ig1"]})
        assert out["results"][0] == {"id": str(done.id), "ig_media_id": "ig1", "status": "already_transcribed", "charged": False}
        assert "video" in et.transcribe_instagram_post(_ig_db([image], 0), ORG, {"ids": ["ig1"]})["results"][0]["reason"]
        assert "retry after" in et.transcribe_instagram_post(_ig_db([failed], 0), ORG, {"ids": ["ig1"]})["results"][0]["reason"]
        whisper.assert_not_called()


def test_transcribe_stops_at_daily_limit():
    from app.core.config import settings

    row = _ig_row()
    with patch("app.services.instagram_transcript_service.transcribe_media") as whisper:
        out = et.transcribe_instagram_post(
            _ig_db([row], used_today=settings.MCP_IG_TRANSCRIBE_DAILY_LIMIT), ORG, {"ids": ["ig1"]}
        )
    whisper.assert_not_called()
    assert out["results"][0]["reason"] == "daily transcription limit reached"
    assert out["attempts_remaining_today"] == 0


def test_transcribe_unknown_post_and_whisper_crash_are_reported():
    with patch("app.services.instagram_transcript_service.transcribe_media") as whisper:
        out = et.transcribe_instagram_post(_ig_db([None]), ORG, {"ids": ["nope"]})
        assert out["results"][0]["status"] == "not_found"
        whisper.side_effect = RuntimeError("openai down")
        db = _ig_db([_ig_row()])
        out = et.transcribe_instagram_post(db, ORG, {"ids": ["ig1"]})
    assert out["results"][0] == {**out["results"][0], "status": "failed", "error": "internal error"}
    db.rollback.assert_called()


# ---------------------------------------------------------------- client wire-format (Claude + ChatGPT)
def _rpc(method, params=None, db=None):
    return mcp_server._handle_jsonrpc(
        {"jsonrpc": "2.0", "id": 7, "method": method, "params": params or {}},
        ORG, db or MagicMock(), user_id=uuid.uuid4(), auth_ctx={"scopes": []},
    )


@pytest.mark.parametrize("protocol", ["2025-03-26", "2025-06-18"])  # Claude / ChatGPT
def test_initialize_then_list_includes_every_extra_tool_with_valid_schema(protocol):
    init = _rpc("initialize", {"protocolVersion": protocol})
    assert init["result"]["protocolVersion"] == protocol
    tools = {t["name"]: t for t in _rpc("tools/list")["result"]["tools"]}
    for tool in et.EXTRA_TOOLS:
        t = tools[tool["name"]]
        json.dumps(t)  # serialisable on the wire
        assert len(t["name"]) <= 64 and t["description"]
        schema = t["inputSchema"]
        assert schema["type"] == "object"
        for req in schema.get("required", []):
            assert req in schema["properties"]
        for prop in schema["properties"].values():
            assert "type" in prop
            if prop["type"] == "array":
                assert "items" in prop  # ChatGPT rejects array schemas without items


def test_every_extra_tool_call_returns_wellformed_json_even_on_empty_or_bad_args():
    """tools/call must never raise or emit non-JSON text, whatever the client sends."""
    db = MagicMock()
    for tool in et.EXTRA_TOOLS:
        for args in ({}, {"ids": "not-a-list", "id": "not-a-uuid", "start": "nope", "page": "x"}):
            resp = _rpc("tools/call", {"name": tool["name"], "arguments": args}, db=db)
            assert resp["jsonrpc"] == "2.0" and resp["id"] == 7 and "error" not in resp
            content = resp["result"]["content"]
            assert content[0]["type"] == "text"
            json.loads(content[0]["text"])  # valid JSON, not truncated mid-document
            assert len(content[0]["text"]) < 140_000


def test_gated_write_tools_explain_themselves_over_the_wire():
    out = json.loads(_rpc("tools/call", {"name": "update_kpi_daily_entry", "arguments": {"closes": 1}})
                     ["result"]["content"][0]["text"])
    assert out["error"] in ("insufficient_scope", "scorecard_writes_disabled")

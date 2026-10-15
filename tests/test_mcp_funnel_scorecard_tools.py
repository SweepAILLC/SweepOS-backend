"""Unit tests for the funnels:write MCP tools (update_funnel_ad_spend, set_funnel_scorecard_metric)."""

from __future__ import annotations

import uuid
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from app.mcp import server as mcp_server


def _org(enabled: bool):
    return SimpleNamespace(id=uuid.uuid4(), mcp_scorecard_write_enabled=enabled)


def _db_returning(org):
    db = MagicMock()
    db.query.return_value.filter.return_value.first.return_value = org
    return db


def test_tools_registered_as_writes_with_open_world_hint():
    by_name = {t["name"]: t for t in mcp_server.TOOLS}
    for name in ("update_funnel_ad_spend", "set_funnel_scorecard_metric"):
        assert name in by_name
        ann = by_name[name]["annotations"]
        assert ann["readOnlyHint"] is False
        assert ann["openWorldHint"] is True
    assert {"update_funnel_ad_spend", "set_funnel_scorecard_metric"} <= mcp_server._WRITE_TOOLS


def test_require_funnel_write_access_denies_without_scope():
    org_id = uuid.uuid4()
    # db is never touched when the scope check fails first.
    result = mcp_server._require_funnel_write_access(None, org_id, {"scope": "clients:read"})
    assert result is not None
    assert '"error": "insufficient_scope"' in result["content"][0]["text"]


def test_require_funnel_write_access_denies_when_org_opted_out():
    org_id = uuid.uuid4()
    db = _db_returning(_org(enabled=False))
    result = mcp_server._require_funnel_write_access(db, org_id, {"scope": "funnels:write"})
    assert result is not None
    assert '"error": "scorecard_writes_disabled"' in result["content"][0]["text"]


def test_require_funnel_write_access_denies_when_org_missing():
    org_id = uuid.uuid4()
    db = _db_returning(None)
    result = mcp_server._require_funnel_write_access(db, org_id, {"scope": "funnels:write"})
    assert result is not None
    assert '"error": "scorecard_writes_disabled"' in result["content"][0]["text"]


def test_require_funnel_write_access_allows_scope_plus_opt_in():
    org_id = uuid.uuid4()
    db = _db_returning(_org(enabled=True))
    assert mcp_server._require_funnel_write_access(db, org_id, {"scope": "funnels:write"}) is None


def test_run_tool_gates_write_tools_before_dispatch():
    """_run_tool must deny on the gate and never reach _run_funnel_write_tool."""
    org_id = uuid.uuid4()
    with patch.object(mcp_server, "_run_funnel_write_tool") as dispatch:
        result = mcp_server._run_tool(
            "update_funnel_ad_spend", {"week_start": "2026-09-28", "amount_usd": 100},
            org_id, None, user_id=uuid.uuid4(), auth_ctx={"scope": ""},
        )
    dispatch.assert_not_called()
    assert '"error": "insufficient_scope"' in result["content"][0]["text"]


def test_update_funnel_ad_spend_rejects_invalid_input_without_touching_db():
    org_id = uuid.uuid4()
    result = mcp_server._run_funnel_write_tool(
        "update_funnel_ad_spend", {"week_start": "not-a-date", "amount_usd": 1}, org_id, None, user_id=uuid.uuid4()
    )
    assert "invalid_input" in result["content"][0]["text"]


def test_update_funnel_ad_spend_calls_upsert_with_mcp_agent_source():
    org_id = uuid.uuid4()
    user_id = uuid.uuid4()
    fake_row = SimpleNamespace(
        funnel_id=None, week_start=__import__("datetime").date(2026, 9, 28),
        amount_cents=50000, ads_deployed=2, angles_deployed=None, source="mcp_agent",
    )
    with patch("app.api.funnels._require_org_funnel") as require_funnel, patch(
        "app.services.funnel_dashboard.upsert_ad_spend", return_value=fake_row
    ) as upsert:
        result = mcp_server._run_funnel_write_tool(
            "update_funnel_ad_spend",
            {"week_start": "2026-09-28", "amount_usd": 500, "ads_deployed": 2},
            org_id,
            None,
            user_id=user_id,
        )
    require_funnel.assert_called_once()
    assert upsert.call_args.kwargs["source"] == "mcp_agent"
    assert upsert.call_args.kwargs["ads_deployed"] == 2
    assert '"source": "mcp_agent"' in result["content"][0]["text"]


def test_update_funnel_ad_spend_reports_cleared_week():
    org_id = uuid.uuid4()
    with patch("app.api.funnels._require_org_funnel"), patch(
        "app.services.funnel_dashboard.upsert_ad_spend", return_value=None
    ):
        result = mcp_server._run_funnel_write_tool(
            "update_funnel_ad_spend", {"week_start": "2026-09-28", "amount_usd": 0}, org_id, None, user_id=uuid.uuid4()
        )
    assert '"cleared": true' in result["content"][0]["text"]


def test_set_funnel_scorecard_metric_rejects_ad_spend_keys_as_wrong_tool():
    org_id = uuid.uuid4()
    for key in ("new_ads", "new_angles", "ad_spend"):
        result = mcp_server._run_funnel_write_tool(
            "set_funnel_scorecard_metric",
            {"week_start": "2026-09-28", "metric_key": key, "value": 10},
            org_id,
            None,
            user_id=uuid.uuid4(),
        )
        assert '"error": "wrong_tool"' in result["content"][0]["text"]


def test_set_funnel_scorecard_metric_calls_set_override_with_mcp_agent_source():
    org_id = uuid.uuid4()
    user_id = uuid.uuid4()
    with patch("app.api.funnels._require_org_funnel"), patch(
        "app.services.funnel_scorecard.set_override"
    ) as set_override:
        result = mcp_server._run_funnel_write_tool(
            "set_funnel_scorecard_metric",
            {"week_start": "2026-09-28", "metric_key": "leads", "value": 42},
            org_id,
            None,
            user_id=user_id,
        )
    called_kwargs = set_override.call_args.kwargs
    assert called_kwargs.get("source") == "mcp_agent"
    assert '"metric_key": "leads"' in result["content"][0]["text"]


def test_set_funnel_scorecard_metric_surfaces_non_editable_rejection():
    """set_override raising ValueError (e.g. a derived rate/cost row) becomes an error result, not a crash."""
    org_id = uuid.uuid4()
    with patch("app.api.funnels._require_org_funnel"), patch(
        "app.services.funnel_scorecard.set_override",
        side_effect=ValueError("'close_rate' is calculated from other rows and can't be edited"),
    ):
        result = mcp_server._run_funnel_write_tool(
            "set_funnel_scorecard_metric",
            {"week_start": "2026-09-28", "metric_key": "close_rate", "value": 10},
            org_id,
            None,
            user_id=uuid.uuid4(),
        )
    assert "calculated from other rows" in result["content"][0]["text"]

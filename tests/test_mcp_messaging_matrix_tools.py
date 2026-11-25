"""MCP Messaging Matrix tools: registration, tier gate, role gate, org from token."""
from __future__ import annotations

import json
import uuid

import pytest

from app.mcp import server as mcp_server
from app.services import content_angle_map as cam
from app.services import messaging_matrix_service as mm

ORG = uuid.uuid4()


def _payload(result):
    return json.loads(result["content"][0]["text"])


@pytest.fixture()
def consulting(monkeypatch):
    monkeypatch.setattr(cam, "org_is_consulting", lambda db, org_id: True)


def test_tools_registered_with_right_annotations():
    by_name = {t["name"]: t for t in mcp_server.TOOLS}
    for name in ("get_messaging_matrix", "get_messaging_angle"):
        assert by_name[name]["annotations"]["readOnlyHint"] is True
    recreate = by_name["recreate_messaging_angle"]["annotations"]
    assert recreate["readOnlyHint"] is False
    assert recreate["openWorldHint"] is False
    assert by_name["get_messaging_angle"]["inputSchema"]["required"] == ["angle_id"]


def test_non_consulting_org_is_refused(monkeypatch):
    monkeypatch.setattr(cam, "org_is_consulting", lambda db, org_id: False)
    out = _payload(mcp_server._run_tool("get_messaging_matrix", {}, ORG, db=None))
    assert out["error"] == "consulting_tier_required"


def test_matrix_uses_token_org_and_clamps_args(monkeypatch, consulting):
    seen = {}

    def fake_list(db, org_id, **kw):
        seen.update(org_id=org_id, **kw)
        return {"angles": []}

    monkeypatch.setattr(mm, "list_matrix", fake_list)
    mcp_server._run_tool(
        "get_messaging_matrix",
        {"channel": "tiktok", "limit": 999, "min_samples": -3, "org_id": str(uuid.uuid4())},
        ORG,
        db=None,
    )
    assert seen["org_id"] == ORG  # an org_id argument is ignored
    assert seen["channel"] is None
    assert seen["limit"] == 100
    assert seen["min_samples"] == 0


def test_angle_detail_bad_id_and_missing(monkeypatch, consulting):
    assert _payload(mcp_server._run_tool("get_messaging_angle", {"angle_id": "nope"}, ORG, db=None))["error"] == "invalid angle_id"

    def missing(db, org_id, angle_id):
        raise LookupError("x")

    monkeypatch.setattr(mm, "angle_detail", missing)
    out = _payload(mcp_server._run_tool("get_messaging_angle", {"angle_id": str(uuid.uuid4())}, ORG, db=None))
    assert out["error"] == "angle_not_found"


@pytest.mark.parametrize("role", [None, "member", "MEMBER"])
def test_recreate_needs_admin_or_owner(monkeypatch, consulting, role):
    monkeypatch.setattr(mm, "recreate_angle", lambda *a, **k: pytest.fail("should not draft"))
    out = _payload(
        mcp_server._run_tool(
            "recreate_messaging_angle", {"angle_id": str(uuid.uuid4())}, ORG, db=None, auth_ctx={"role": role}
        )
    )
    assert out["error"] == "forbidden"


def test_recreate_as_owner_returns_unapproved_draft(monkeypatch, consulting):
    user = uuid.uuid4()
    seen = {}

    def fake_recreate(db, org_id, angle_id, *, channel, user_id):
        seen.update(org_id=org_id, channel=channel, user_id=user_id)
        return {"hooks": ["a", "b", "c"], "approved": False}

    monkeypatch.setattr(mm, "recreate_angle", fake_recreate)
    out = _payload(
        mcp_server._run_tool(
            "recreate_messaging_angle",
            {"angle_id": str(uuid.uuid4()), "channel": "paid"},
            ORG,
            db=None,
            user_id=user,
            auth_ctx={"role": "owner"},
        )
    )
    assert out["approved"] is False
    assert seen == {"org_id": ORG, "channel": "paid", "user_id": user}


def test_recreate_failure_is_reported_not_raised(monkeypatch, consulting):
    def fail(*a, **k):
        raise RuntimeError("The draft did not meet the quality bar")

    monkeypatch.setattr(mm, "recreate_angle", fail)
    out = _payload(
        mcp_server._run_tool(
            "recreate_messaging_angle", {"angle_id": str(uuid.uuid4())}, ORG, db=None, auth_ctx={"role": "admin"}
        )
    )
    assert out["error"] == "draft_failed"

#!/usr/bin/env python3
"""
Local-only smoke test for the owner_* MCP tools (backend/app/mcp/server.py).

Calls _run_tool() directly against a real DB session — no HTTP, no OAuth token,
no Claude connector needed. Verifies:
  1. Each owner_* tool returns data (not an error) when called as the sudo admin.
  2. A non-system-owner user gets {"error": "forbidden"} from the same tool.

Usage (inside the backend container or a local venv with DATABASE_URL set):
    python scripts/smoke_mcp_owner_tools.py [--org-id UUID]

If --org-id is omitted, the first organization found in the DB is used.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import uuid

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.db.session import SessionLocal
from app.core.config import settings
from app.models.user import User
from app.models.organization import Organization
from app.mcp import server


def _result_text(result: dict) -> str:
    return result["content"][0]["text"]


def _is_forbidden(result: dict) -> bool:
    try:
        payload = json.loads(_result_text(result))
    except (ValueError, KeyError, IndexError):
        return False
    return isinstance(payload, dict) and payload.get("error") == "forbidden"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--org-id", type=str, default=None, help="Org UUID to test against")
    args = parser.parse_args()

    db = SessionLocal()
    failures = 0
    try:
        admin = db.query(User).filter(User.email == settings.SUDO_ADMIN_EMAIL).first()
        if not admin:
            print(f"No sudo admin user found for {settings.SUDO_ADMIN_EMAIL!r} — run `make seed-admin` first.")
            return 1

        if args.org_id:
            org_id = uuid.UUID(args.org_id)
            org = db.query(Organization).filter(Organization.id == org_id).first()
            if not org:
                print(f"Org {org_id} not found.")
                return 1
        else:
            org = db.query(Organization).order_by(Organization.created_at.asc()).first()
            if not org:
                print("No organizations in DB — nothing to test against.")
                return 1
            org_id = org.id

        print(f"Testing as sudo admin: {admin.email} (user_id={admin.id})")
        print(f"Target org: {org.name} ({org_id})\n")

        # 1. owner_list_organizations
        result = server._run_tool("owner_list_organizations", {}, org_id, db, user_id=admin.id)
        payload = json.loads(_result_text(result))
        if "organizations" in payload and isinstance(payload["organizations"], list):
            print(f"[PASS] owner_list_organizations -> {len(payload['organizations'])} orgs")
        else:
            print(f"[FAIL] owner_list_organizations -> {_result_text(result)[:300]}")
            failures += 1

        # 2. owner_get_organization_dashboard
        result = server._run_tool(
            "owner_get_organization_dashboard", {"org_id": str(org_id)}, org_id, db, user_id=admin.id
        )
        payload = json.loads(_result_text(result))
        if "organization_id" in payload:
            print(
                f"[PASS] owner_get_organization_dashboard -> "
                f"cash_all_time=${payload.get('cash_collected_all_time_usd')}, "
                f"clients={payload.get('total_clients')}, "
                f"llm_usage_30d={payload.get('llm_usage_last_30d')}"
            )
        else:
            print(f"[FAIL] owner_get_organization_dashboard -> {_result_text(result)[:300]}")
            failures += 1

        # 3. owner_get_organization_kpi_snapshot
        result = server._run_tool(
            "owner_get_organization_kpi_snapshot", {"org_id": str(org_id), "days": 30}, org_id, db, user_id=admin.id
        )
        payload = json.loads(_result_text(result))
        if "cards" in payload or "generated_at" in payload:
            print(f"[PASS] owner_get_organization_kpi_snapshot -> keys={list(payload.keys())[:6]}")
        else:
            print(f"[FAIL] owner_get_organization_kpi_snapshot -> {_result_text(result)[:300]}")
            failures += 1

        # 4. owner_get_organization_shared_space
        result = server._run_tool(
            "owner_get_organization_shared_space", {"org_id": str(org_id)}, org_id, db, user_id=admin.id
        )
        payload = json.loads(_result_text(result))
        if "pads" in payload:
            print(f"[PASS] owner_get_organization_shared_space -> {len(payload['pads'])} pad(s)")
        else:
            print(f"[FAIL] owner_get_organization_shared_space -> {_result_text(result)[:300]}")
            failures += 1

        # 5. owner_get_llm_usage_timeseries
        result = server._run_tool(
            "owner_get_llm_usage_timeseries", {"org_id": str(org_id), "days": 30}, org_id, db, user_id=admin.id
        )
        payload = json.loads(_result_text(result))
        if "estimated_cost_usd" in payload:
            print(
                f"[PASS] owner_get_llm_usage_timeseries -> "
                f"calls={payload.get('calls')}, cost_usd={payload.get('estimated_cost_usd')}"
            )
        else:
            print(f"[FAIL] owner_get_llm_usage_timeseries -> {_result_text(result)[:300]}")
            failures += 1

        # 6. Denial path — any user who is NOT a system owner should get {"error": "forbidden"}
        non_owner = (
            db.query(User)
            .filter(User.email != settings.SUDO_ADMIN_EMAIL, User.role == "MEMBER")
            .first()
        )
        if non_owner:
            result = server._run_tool(
                "owner_list_organizations", {}, org_id, db, user_id=non_owner.id
            )
            if _is_forbidden(result):
                print(f"[PASS] owner_list_organizations denies non-owner ({non_owner.email})")
            else:
                print(f"[FAIL] owner_list_organizations did NOT deny non-owner -> {_result_text(result)[:300]}")
                failures += 1
        else:
            print("[SKIP] No MEMBER-role user found to test the denial path.")

        # 7. No-auth path — user_id=None should also be denied
        result = server._run_tool("owner_list_organizations", {}, org_id, db, user_id=None)
        if _is_forbidden(result):
            print("[PASS] owner_list_organizations denies missing user_id")
        else:
            print(f"[FAIL] owner_list_organizations did NOT deny missing user_id -> {_result_text(result)[:300]}")
            failures += 1

    finally:
        db.close()

    print(f"\n{'ALL PASS' if failures == 0 else f'{failures} FAILURE(S)'}")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())

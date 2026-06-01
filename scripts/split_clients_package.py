#!/usr/bin/env python3
"""One-off: split app/api/clients.py into app/api/clients/ package. Run from backend/."""
from __future__ import annotations

from pathlib import Path

BACKEND = Path(__file__).resolve().parents[1]
SRC = BACKEND / "app/api/clients.py"
OUT = BACKEND / "app/api/clients"


def read_lines() -> list[str]:
    return SRC.read_text().splitlines(keepends=True)


def join_ranges(lines: list[str], ranges: list[tuple[int, int]]) -> str:
    parts = []
    for start, end in ranges:
        parts.append("".join(lines[start - 1 : end]))
    return "".join(parts)


COMMON_ROUTE_HEADER = '''"""Clients API — {name} routes."""
from __future__ import annotations

import logging
import re
import uuid
from datetime import datetime, timedelta, timezone
from threading import Lock as ThreadingLock
from typing import List, Optional, Tuple
from uuid import UUID

import httpx
from fastapi import APIRouter, BackgroundTasks, Body, Depends, HTTPException, Query, Request, status
from fastapi.security import HTTPAuthorizationCredentials
from sqlalchemy import and_, desc, func, or_
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, defer
from sqlalchemy.orm.attributes import flag_modified
from starlette.concurrency import run_in_threadpool

from app.api.deps import get_current_user, security
from app.api.clients.helpers import (
    LOG,
    WHOP_PAID_STATUSES,
    effective_org_id,
    merge_client_meta_from_duplicates,
    normalize_email,
    client_created_sort_key,
    load_whop_payments,
    org_checkin_sync_lock,
    refresh_call_insights_after_checkin_sync,
    scope_org_id,
    sync_check_ins_in_worker,
    user_pipeline_priorities,
    brevo_merged_stats_for_client,
    fetch_brevo_email_stats,
    merge_brevo_stats,
)
from app.core.config import settings
from app.core.rate_limit import check_sliding_window
from app.db.session import get_db, SessionLocal
from app.long_jobs import schedule_background_work
from app.models.calendar_booking_sales import CalendarBookingSales
from app.models.client import Client, LifecycleState
from app.models.client_checkin import ClientCheckIn
from app.models.manual_payment import ManualPayment
from app.models.organization import Organization
from app.models.stripe_payment import StripePayment
from app.models.stripe_subscription import StripeSubscription
from app.models.stripe_treasury_transaction import StripeTreasuryTransaction, TreasuryTransactionStatus
from app.models.user import User
from app.models.whop_payment import WhopPayment
from app.utils.stripe_helpers import extract_email_from_payment_raw
from app.utils.stripe_ids import normalize_stripe_id_for_dedup

router = APIRouter()

'''


def write_helpers(lines: list[str]) -> None:
    body = join_ranges(
        lines,
        [
            (1, 90),
            (91, 358),
            (113, 185),
        ],
    )
    # Rename private helpers to public exports (drop leading _)
    replacements = [
        ("def _user_pipeline_priorities", "def user_pipeline_priorities"),
        ("def _effective_org_id", "def effective_org_id"),
        ("def _org_checkin_sync_lock", "def org_checkin_sync_lock"),
        ("def _sync_check_ins_in_worker", "def sync_check_ins_in_worker"),
        ("def _refresh_call_insights_after_checkin_sync", "def refresh_call_insights_after_checkin_sync"),
        ("def _scope_org_id", "def scope_org_id"),
        ("def _parse_campaign_stats_response", "def _parse_campaign_stats_response"),
        ("def _merge_brevo_stats", "def merge_brevo_stats"),
        ("def _brevo_merged_stats_for_client", "def brevo_merged_stats_for_client"),
        ("def _fetch_brevo_email_stats", "def fetch_brevo_email_stats"),
        ("_checkin_sync_locks_guard", "_checkin_sync_locks_guard"),
        ("_checkin_sync_org_locks", "_checkin_sync_org_locks"),
        ("router = APIRouter()", ""),
        ("from app.long_jobs", "# moved to route modules\n# from app.long_jobs"),
    ]
    for old, new in replacements:
        body = body.replace(old, new)
    content = '''"""Shared helpers for clients API package."""
from __future__ import annotations

''' + body
    (OUT / "helpers.py").write_text(content)


def write_module(name: str, ranges: list[tuple[int, int]], lines: list[str], extra_imports: str = "") -> None:
    body = join_ranges(lines, ranges)
    header = COMMON_ROUTE_HEADER.format(name=name)
    if extra_imports:
        header = header.replace("router = APIRouter()\n\n", f"router = APIRouter()\n\n{extra_imports}\n\n")
    (OUT / f"{name}.py").write_text(header + body)


def main() -> None:
    lines = read_lines()
    OUT.mkdir(exist_ok=True)

    write_helpers(lines)

    write_module("automation", [(2298, 2323)], lines, """
from app.services.client_automation import process_client_automation
""")

    write_module(
        "insights",
        [(475, 1089)],
        lines,
        """
from pydantic import ValidationError
from app.schemas.client import (
    ClientHealthFactor,
    ClientHealthScoreResponse,
    ClientAIRecommendationsResponse,
    AIRecommendationActionOut,
    AIRecommendationActionPatch,
    AIRecommendationEmailDraftResponse,
)
from app.schemas.call_insights import (
    ClientCallInsightsResponse,
    ClientInsightSummaryOut,
    CallInsightPerCallOut,
    CallInsightsRollupOut,
    OfferSuggestionOut,
    RefreshCallInsightsResponse,
)
from app.services.health_score_cache_service import resolve_health_score, batch_read_cached_health_scores
from app.services.client_ai_recommendations_service import get_recommendation_state_dict, set_action_completed
from app.services.ai_recommendation_email_draft import build_recommendation_email_draft
from app.services.call_insight_service import (
    get_client_insights_response,
    get_call_insight_tags_batch,
    refresh_latest_call_insight,
)
from app.schemas.client import Client as ClientSchema
""",
    )

    write_module(
        "terminal",
        [(20, 23), (583, 672), (675, 800), (1092, 1388)],
        lines,
        """
from threading import Lock as ThreadingLock
from app.models.organization import Organization
from app.schemas.calendar_metrics import CalendarMonthlyRateRow, CalendarMonthlyCoachingResponse, TerminalMonthlyTrendsResponse
from app.schemas.admin import HealthTrendPeriod
from app.schemas.client import (
    TerminalSummaryResponse,
    TerminalCashCollected,
    TerminalCashBySourceBreakdown,
    TerminalCashSourceTotals,
    TerminalMRR,
    TerminalTopContributor,
)
from app.services.terminal_metrics_service import (
    build_calendar_monthly_coaching_periods,
    build_terminal_monthly_trends,
    org_whop_cash_usd_window,
    terminal_monthly_trends_cache_get,
    terminal_monthly_trends_cache_set,
)
_WHOP_PAID_STATUSES = frozenset({"paid", "succeeded", "completed", "successful"})
""",
    )

    write_module(
        "payments",
        [(1391, 1874)],
        lines,
        """
from app.models.stripe_payment import StripePayment
from app.models.manual_payment import ManualPayment
""",
    )

    write_module(
        "checkins",
        [(2325, 2872)],
        lines,
        """
from app.services.client_automation import process_pipeline_lifecycle_for_client
""",
    )

    merge_helper = join_ranges(lines, [(1988, 2011)])
    crud_body = join_ranges(lines, [(361, 474), (979, 1012), (1876, 2296)])
    crud_extra = """
from app.schemas.client import Client as ClientSchema, ClientCreate, ClientUpdate, MergeClientsRequest
from app.services.client_automation import (
    apply_manual_lifecycle_change,
    resolve_lifecycle_state,
    update_client_progress,
    update_client_lifecycle_state,
    run_pipeline_lifecycle_for_org,
)
from app.services.health_score_cache_service import invalidate_health_score_cache
from app.services.call_insight_service import reconcile_call_insights_for_client_merge
"""
    header = COMMON_ROUTE_HEADER.format(name="crud")
    (OUT / "crud.py").write_text(header + crud_extra + "\n" + merge_helper + crud_body)

    init = '''"""Clients API package — aggregates domain routers under /clients."""
from fastapi import APIRouter

from app.api.clients import automation, checkins, crud, insights, payments, terminal

router = APIRouter()
# Static/collection routes before /{client_id} paths (see README.md)
router.include_router(terminal.router)
router.include_router(insights.router)
router.include_router(automation.router)
router.include_router(checkins.router)
router.include_router(payments.router)
router.include_router(crud.router)
'''
    (OUT / "__init__.py").write_text(init)
    print("Wrote package to", OUT)


if __name__ == "__main__":
    main()

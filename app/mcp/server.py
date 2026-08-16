"""
Remote MCP server for Claude custom connector (Streamable HTTP).

Mounted at /mcp. Unauthenticated requests return 401 with WWW-Authenticate
pointing at protected-resource metadata (Claude OAuth discovery).
"""
from __future__ import annotations

import json
import logging
import uuid
from typing import Any, Optional

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse
from sqlalchemy.orm import Session
import asyncio

from app.db.session import SessionLocal
from app.services.client_profile_bundle import (
    build_client_profile_bundle,
    list_clients_for_mcp,
    search_clients_by_email,
)
from app.services.brevo_mcp_bundle import list_brevo_senders_for_mcp, send_client_email_for_mcp
from app.services.marketing_intel_bundle import (
    get_client_call_insights_for_mcp,
    get_marketing_intel_bootstrap_for_mcp,
    get_org_intelligence_for_mcp,
    get_org_sales_signals_for_mcp,
    list_org_sales_themes_for_mcp,
    search_sales_clips_for_mcp,
)
from app.services.kpi_mcp_bundle import (
    get_kpi_flags_for_mcp,
    get_kpi_monthly_rollups_for_mcp,
    get_kpi_snapshot_for_mcp,
    get_kpi_trends_for_mcp,
)
from app.services.instagram_mcp_bundle import (
    get_instagram_performance_for_mcp,
    get_instagram_top_posts_for_mcp,
    get_instagram_underperforming_posts_for_mcp,
    get_marketing_ideas_for_mcp,
)
from app.services.resource_documents import (
    ensure_doc_content,
    ensure_resource_documents_table,
    list_docs,
    get_doc,
    search_resource_docs,
)
from app.services.resource_library import (
    ensure_resource_library_table,
    list_library_items,
    get_library_item,
)
from app.services.mcp_oauth_service import mcp_resource, verify_mcp_access_token
from app.services.terminal_dashboard_bundle import build_terminal_dashboard_for_mcp

logger = logging.getLogger(__name__)

router = APIRouter()

SERVER_INFO = {
    "name": "sweepos",
    "version": "1.7.0",
    "protocolVersion": "2025-03-26",
}

# Claude.ai currently prefers 2025-11-25; accept both.
SUPPORTED_PROTOCOL_VERSIONS = ("2025-11-25", "2025-03-26")


TOOLS = [
    {
        "name": "get_connection_context",
        "description": (
            "Return which SweepOS organization and user this Claude connector is authenticated as. "
            "Call this first if org data looks wrong — reconnect and pick the correct org when prompted."
        ),
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "list_clients",
        "description": "List clients in the connected SweepOS organization. Optionally filter by text query or lifecycle_state.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Search name, email, or phone"},
                "lifecycle_state": {
                    "type": "string",
                    "description": "cold_lead | nurturing | qualified | booked | active | offboarding | dead",
                },
                "limit": {"type": "integer", "default": 50, "minimum": 1, "maximum": 100},
            },
        },
    },
    {
        "name": "get_client_profile",
        "description": (
            "Return a full client profile package: contact info, pipeline/program stage, "
            "financial investments, current offer/balance due, call analysis + ROI tags, and workspace info."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "client_id": {"type": "string", "description": "Client UUID"},
            },
            "required": ["client_id"],
        },
    },
    {
        "name": "search_clients_by_email",
        "description": "Find clients in the org by email address (primary or additional emails).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "email": {"type": "string"},
            },
            "required": ["email"],
        },
    },
    {
        "name": "get_marketing_intel",
        "description": (
            "Primary Marketing Intel package for autonomous content ideation. Includes sales signals "
            "(objection themes, struggles, wins, testimonial stories, prospect voice), operator knowledge "
            "(objections/closings/reframes), sales playbook paragraphs, ICP/offer ladder, last drafted "
            "TOF/MOF/BOF content bundle (if any), and content-ideation guidance. Prefer this tool first "
            "when drafting short-form content from real sales data. Pair with get_instagram_performance "
            "for pattern trends and top/underperformer posts with Instagram permalinks + metrics."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "include_sop": {
                    "type": "boolean",
                    "default": True,
                    "description": "Include content ideation SOP / marketing guidance text",
                },
            },
        },
    },
    {
        "name": "get_marketing_ideas",
        "description": (
            "Latest drafted Marketing Intel ideas only (TOF / MOF / BOF concepts: title, hook, bullets, "
            "why_for_icp). Lighter than get_marketing_intel when you only need the content_bundle. "
            "Cross-check hooks against get_instagram_performance top_posts / underperformers."
        ),
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "get_org_sales_signals",
        "description": (
            "Raw org sales signals mined from calls: recurring objection themes, recent insights "
            "(objection quotes, struggles, wins, stories, resonated/avoid phrasing), active-client "
            "friction, and meeting summary excerpts."
        ),
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "list_org_sales_themes",
        "description": "List recurring/validated sales content themes (objections) with sample prospect quotes.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "validated_only": {
                    "type": "boolean",
                    "default": False,
                    "description": "If true, only return themes that passed validation thresholds",
                },
                "limit": {"type": "integer", "default": 25, "minimum": 1, "maximum": 50},
            },
        },
    },
    {
        "name": "get_org_intelligence_profile",
        "description": (
            "Org Intelligence bank: business context (business description, target audience/ICP, "
            "unique selling proposition, coaching style, marketing strategy/channels), sales approach "
            "(framework + tactics), pipeline priorities, brand voice, and the configured offer ladder "
            "with pricing. Call this for insight into the org's offer and business context before "
            "advising on positioning, pricing, or strategy."
        ),
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "search_sales_clips",
        "description": (
            "Search recent call-insight clips and win/story snippets across the org. "
            "Filter by kind=objection|win|testimonial|other and/or free-text query."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "kind": {
                    "type": "string",
                    "description": "objection | win | testimonial | other",
                },
                "query": {"type": "string", "description": "Substring match on quote/label"},
                "limit": {"type": "integer", "default": 40, "minimum": 1, "maximum": 100},
            },
        },
    },
    {
        "name": "get_client_call_insights",
        "description": (
            "Call-analysis package for one client (lighter than get_client_profile): summary, ROI, "
            "rollup wins/stories, and recent insights with clips, struggles, wins, and prospect voice."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "client_id": {"type": "string", "description": "Client UUID"},
                "limit": {"type": "integer", "default": 10, "minimum": 1, "maximum": 25},
            },
            "required": ["client_id"],
        },
    },
    {
        "name": "get_terminal_dashboard",
        "description": (
            "SweepOS Terminal dashboard. DEFAULT mode='overview' is fast (summary, monthly_trends, "
            "appointments, failed_payments) and should be preferred. Use mode='full' or an explicit "
            "sections list only when you need finances/stripe/calendar/leads. If the response has "
            "incomplete_sections or partial=true, retry those sections only — do not re-fetch everything. "
            "For KPI Command Center funnel metrics (outreach, bookings, show-up, close rates), use "
            "get_kpi_snapshot / get_kpi_monthly_rollups / get_kpi_trends instead."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "mode": {
                    "type": "string",
                    "enum": ["overview", "full"],
                    "default": "overview",
                    "description": "overview (default, fast) or full (all sections; slower)",
                },
                "sections": {
                    "type": "array",
                    "items": {
                        "type": "string",
                        "enum": [
                            "summary",
                            "monthly_trends",
                            "finances",
                            "stripe",
                            "calendar",
                            "appointments",
                            "failed_payments",
                            "leads",
                        ],
                    },
                    "description": "Optional subset of dashboard sections (overrides mode when set)",
                },
                "finances_range_days": {"type": "integer", "default": 30, "minimum": 1, "maximum": 365},
                "finances_scope": {
                    "type": "string",
                    "description": "Optional finances window scope: mtd | all",
                },
                "appointments_limit": {"type": "integer", "default": 20, "minimum": 1, "maximum": 100},
            },
        },
    },
    {
        "name": "get_kpi_snapshot",
        "description": (
            "KPI Command Center snapshot for the connected org: essential cards (outreach, bookings, "
            "show-up, close rate, cash), optional daily series, current-month totals, and bottleneck "
            "flags. Prefer this first for a KPI briefing. Not the Terminal cash/MRR dashboard."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "days": {
                    "type": "integer",
                    "default": 30,
                    "minimum": 1,
                    "maximum": 365,
                    "description": "Trailing window length in days (ignored when start+end set)",
                },
                "start": {"type": "string", "description": "YYYY-MM-DD inclusive"},
                "end": {"type": "string", "description": "YYYY-MM-DD inclusive"},
                "include_flags": {"type": "boolean", "default": True},
                "include_series": {"type": "boolean", "default": True},
                "sync": {
                    "type": "boolean",
                    "default": False,
                    "description": "Refresh live calendar/payment fields before building (slower)",
                },
            },
        },
    },
    {
        "name": "get_kpi_monthly_rollups",
        "description": (
            "Monthly KPI totals for the org (same as Command Center month footer): sums for volume/"
            "money and ratio-of-sums funnel rates, newest month first. Use for MoM comparisons."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "months": {
                    "type": "integer",
                    "default": 12,
                    "minimum": 1,
                    "maximum": 36,
                    "description": "How many calendar months of history to return",
                },
            },
        },
    },
    {
        "name": "get_kpi_trends",
        "description": (
            "KPI month-over-month trends: compact monthly series of funnel metrics plus multi-month "
            "decline flags (reply/booking/show-up/close rates and outreach/follow-up daily averages) "
            "and full bottleneck flags. Use after get_kpi_snapshot when advising on trajectory."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "months": {
                    "type": "integer",
                    "default": 6,
                    "minimum": 3,
                    "maximum": 24,
                    "description": "Months of history for the trend series",
                },
            },
        },
    },
    {
        "name": "get_kpi_flags",
        "description": (
            "KPI bottleneck and trend flags only (below-benchmark metrics, stage comparisons, "
            "multi-month declines). Lighter than get_kpi_trends when you only need alerts."
        ),
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "get_instagram_performance",
        "description": (
            "Marketing Intel Instagram read model: period KPIs with prior-window comparison "
            "(reach/views/saves/engagement deltas), weekly_trend pattern series, trend_flags, "
            "and top_posts + underperformers each with instagram_url (permalink) and numerical "
            "metrics (reach, views, saved, engagement_rate_pct, etc). Cite posts by URL + metrics. "
            "Requires Instagram connected in SweepOS Integrations."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "days": {
                    "type": "integer",
                    "default": 90,
                    "minimum": 7,
                    "maximum": 365,
                    "description": "Trailing window in days (also compared to the prior equal window)",
                },
            },
        },
    },
    {
        "name": "get_instagram_top_posts",
        "description": (
            "Top Instagram posts by engagement rate, each with instagram_url and numerical metrics. "
            "Optionally filter by format_bucket (reel|carousel|image|video) or theme_key."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "days": {"type": "integer", "default": 90, "minimum": 7, "maximum": 365},
                "format_bucket": {"type": "string"},
                "theme_key": {"type": "string"},
                "limit": {"type": "integer", "default": 10, "minimum": 1, "maximum": 25},
            },
        },
    },
    {
        "name": "get_instagram_underperforming_posts",
        "description": (
            "Lowest-engagement Instagram posts in the window, each with instagram_url and numerical "
            "metrics. Use with get_instagram_top_posts to contrast winners vs underperformers."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "days": {"type": "integer", "default": 90, "minimum": 7, "maximum": 365},
                "format_bucket": {"type": "string"},
                "limit": {"type": "integer", "default": 5, "minimum": 1, "maximum": 10},
            },
        },
    },
    {
        "name": "search_resource_docs",
        "description": (
            "Search the Sweep consulting knowledge base: builtin SOP library + this org's custom/overridden "
            "docs (offer building, ICP, funnels, sales, fulfillment, etc). Use for strategic consulting "
            "questions alongside live Marketing Intel / Instagram / Terminal data. Returns ranked matches "
            "with excerpts (and optional full markdown)."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "Natural-language topic, e.g. 'build an offer', 'objection handling', 'ICP'",
                },
                "category": {
                    "type": "string",
                    "description": "Optional filter: SOP | AI Skill | Template | Guide",
                },
                "sop_category": {
                    "type": "string",
                    "description": "Optional track: foundations | marketing | sales | operations | fulfillment",
                },
                "limit": {"type": "integer", "default": 8, "minimum": 1, "maximum": 25},
                "include_content": {
                    "type": "boolean",
                    "default": True,
                    "description": "Include markdown body (truncated) for each match",
                },
            },
            "required": ["query"],
        },
    },
    {
        "name": "list_resource_docs",
        "description": (
            "Catalog the org SOP library and custom resource docs (built-ins + overrides + customs). "
            "Prefer search_resource_docs for topical consulting questions; use this to browse by "
            "category / sop_category, then get_resource_doc for full content."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "category": {
                    "type": "string",
                    "description": "Optional category filter: SOP | AI Skill | Template | Guide",
                },
                "sop_category": {
                    "type": "string",
                    "description": "Optional track: foundations | marketing | sales | operations | fulfillment",
                },
                "include_content": {
                    "type": "boolean",
                    "default": False,
                    "description": "If true, include markdown content for each doc (can be large)",
                },
                "limit": {"type": "integer", "default": 200, "minimum": 1, "maximum": 500},
            },
        },
    },
    {
        "name": "get_resource_doc",
        "description": (
            "Get one SOP / resource doc by resource_id, including full markdown. "
            "Use after search_resource_docs or list_resource_docs when you need the complete framework."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "resource_id": {
                    "type": "string",
                    "description": "Document resource_id slug (e.g. building-an-offer-sop, defining-your-icp)",
                },
            },
            "required": ["resource_id"],
        },
    },
    {
        "name": "list_org_resource_library",
        "description": (
            "List org-specific resource-library items (text, markdown, image, video_url, url) uploaded "
            "in Resources — testimonials, case studies, custom SOPs, etc. Complementary to the "
            "platform SOP library (list_resource_docs / search_resource_docs)."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "kind": {"type": "string", "description": "Optional kind filter"},
                "tag": {"type": "string", "description": "Optional tag filter"},
                "limit": {"type": "integer", "default": 200, "minimum": 1, "maximum": 500},
            },
        },
    },
    {
        "name": "get_org_resource_library_item",
        "description": "Get one org-specific resource-library item by id (full text/markdown when present).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "item_id": {"type": "string", "description": "Library item UUID"},
            },
            "required": ["item_id"],
        },
    },
    {
        "name": "list_brevo_senders",
        "description": (
            "List verified Brevo sender email/name options for this org. "
            "ALWAYS call this before send_client_email and ask the user which sender_email "
            "and sender_name to use in the Claude UI."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "active_only": {
                    "type": "boolean",
                    "default": True,
                    "description": "If true, only return active/verified senders",
                },
            },
        },
    },
    {
        "name": "send_client_email",
        "description": (
            "Send a transactional email to one SweepOS client via the org Brevo integration. "
            "Required: client_id, sender_email, sender_name (from list_brevo_senders), subject, "
            "html_content and/or text_content, and confirm_send=true after the user approves. "
            "Ask the user to pick sender_email + sender_name from list_brevo_senders first."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "client_id": {"type": "string", "description": "Client UUID"},
                "sender_email": {
                    "type": "string",
                    "description": "From address chosen from list_brevo_senders",
                },
                "sender_name": {
                    "type": "string",
                    "description": "From display name (ask user; often the sender's listed name)",
                },
                "subject": {"type": "string"},
                "html_content": {"type": "string"},
                "text_content": {"type": "string"},
                "reply_to_email": {"type": "string"},
                "reply_to_name": {"type": "string"},
                "confirm_send": {
                    "type": "boolean",
                    "description": "Must be true after user confirms recipient, subject, body, and sender",
                },
            },
            "required": ["client_id", "sender_email", "sender_name", "subject", "confirm_send"],
        },
    },
]


def _www_authenticate() -> str:
    from app.services.mcp_oauth_service import mcp_issuer, mcp_resource

    issuer = mcp_issuer()
    resource = mcp_resource()
    meta = f"{issuer}/.well-known/oauth-protected-resource"
    if resource.startswith(issuer + "/"):
        suffix = resource[len(issuer) + 1 :]
        if suffix:
            meta = f"{meta}/{suffix}"
    return (
        f'Bearer realm="SweepOS", resource_metadata="{meta}", '
        f'scope="clients:read marketing:read terminal:read kpi:read instagram:read email:send"'
    )


def _unauthorized(detail: str = "Authentication required") -> JSONResponse:
    return JSONResponse(
        status_code=401,
        content={"error": "unauthorized", "detail": detail},
        headers={"WWW-Authenticate": _www_authenticate()},
    )


def _extract_bearer(request: Request) -> Optional[str]:
    auth = request.headers.get("Authorization") or request.headers.get("authorization") or ""
    if auth.lower().startswith("bearer "):
        return auth.split(" ", 1)[1].strip()
    return None


def _auth_context(request: Request) -> Optional[dict]:
    token = _extract_bearer(request)
    if not token:
        return None
    return verify_mcp_access_token(token)


def _text_result(payload: Any) -> dict:
    text = payload if isinstance(payload, str) else json.dumps(payload, default=str)
    if len(text) > 140_000:
        text = text[:140_000] + "\n…[truncated]"
    return {"content": [{"type": "text", "text": text}]}


def _run_tool(
    name: str,
    arguments: dict,
    org_id: uuid.UUID,
    db: Session,
    *,
    user_id: Optional[uuid.UUID] = None,
    auth_ctx: Optional[dict] = None,
) -> dict:
    args = arguments or {}
    if name == "get_connection_context":
        from app.models.organization import Organization
        from app.models.user import User

        org = db.query(Organization).filter(Organization.id == org_id).first()
        user = db.query(User).filter(User.id == user_id).first() if user_id else None
        return _text_result(
            {
                "org_id": str(org_id),
                "org_name": org.name if org else (auth_ctx or {}).get("org_name"),
                "user_id": str(user_id) if user_id else None,
                "user_email": (user.email if user else (auth_ctx or {}).get("sub")),
                "role": (auth_ctx or {}).get("role"),
                "scopes": (auth_ctx or {}).get("scope"),
                "hint": (
                    "If this org is wrong, disconnect the Claude connector and reconnect. "
                    "When your Google account belongs to multiple Sweep orgs, you will be asked to pick one."
                ),
            }
        )
    if name == "list_clients":
        rows = list_clients_for_mcp(
            db,
            org_id,
            query=args.get("query"),
            lifecycle_state=args.get("lifecycle_state"),
            limit=int(args.get("limit") or 50),
        )
        return _text_result({"clients": rows, "count": len(rows)})
    if name == "get_client_profile":
        cid = args.get("client_id")
        if not cid:
            return _text_result({"error": "client_id required"})
        try:
            client_uuid = uuid.UUID(str(cid))
        except ValueError:
            return _text_result({"error": "invalid client_id"})
        bundle = build_client_profile_bundle(db, org_id, client_uuid)
        if not bundle:
            return _text_result({"error": "client not found"})
        return _text_result(bundle)
    if name == "search_clients_by_email":
        email = args.get("email") or ""
        rows = search_clients_by_email(db, org_id, email)
        return _text_result({"clients": rows, "count": len(rows)})
    if name == "get_marketing_intel":
        include_sop = args.get("include_sop")
        if include_sop is None:
            include_sop = True
        return _text_result(
            get_marketing_intel_bootstrap_for_mcp(
                db,
                org_id,
                user_id=user_id,
                include_sop=bool(include_sop),
            )
        )
    if name == "get_org_sales_signals":
        return _text_result(get_org_sales_signals_for_mcp(db, org_id))
    if name == "list_org_sales_themes":
        return _text_result(
            list_org_sales_themes_for_mcp(
                db,
                org_id,
                validated_only=bool(args.get("validated_only") or False),
                limit=int(args.get("limit") or 25),
            )
        )
    if name == "get_org_intelligence_profile":
        return _text_result(get_org_intelligence_for_mcp(db, org_id, user_id=user_id))
    if name == "search_sales_clips":
        return _text_result(
            search_sales_clips_for_mcp(
                db,
                org_id,
                kind=args.get("kind"),
                query=args.get("query"),
                limit=int(args.get("limit") or 40),
            )
        )
    if name == "get_client_call_insights":
        cid = args.get("client_id")
        if not cid:
            return _text_result({"error": "client_id required"})
        try:
            client_uuid = uuid.UUID(str(cid))
        except ValueError:
            return _text_result({"error": "invalid client_id"})
        return _text_result(
            get_client_call_insights_for_mcp(
                db,
                org_id,
                client_uuid,
                limit=int(args.get("limit") or 10),
            )
        )
    if name == "get_terminal_dashboard":
        sections = args.get("sections")
        if sections is not None and not isinstance(sections, list):
            sections = None
        return _text_result(
            build_terminal_dashboard_for_mcp(
                db,
                org_id,
                user_id=user_id,
                sections=sections,
                mode=args.get("mode") or "overview",
                finances_range_days=int(args.get("finances_range_days") or 30),
                finances_scope=args.get("finances_scope"),
                appointments_limit=int(args.get("appointments_limit") or 20),
            )
        )
    if name == "get_kpi_snapshot":
        include_flags = args.get("include_flags")
        include_series = args.get("include_series")
        sync = args.get("sync")
        return _text_result(
            get_kpi_snapshot_for_mcp(
                db,
                org_id,
                days=int(args.get("days") or 30),
                start=args.get("start"),
                end=args.get("end"),
                include_flags=True if include_flags is None else bool(include_flags),
                include_series=True if include_series is None else bool(include_series),
                sync=bool(sync) if sync is not None else False,
            )
        )
    if name == "get_kpi_monthly_rollups":
        return _text_result(
            get_kpi_monthly_rollups_for_mcp(
                db,
                org_id,
                months=int(args.get("months") or 12),
            )
        )
    if name == "get_kpi_trends":
        return _text_result(
            get_kpi_trends_for_mcp(
                db,
                org_id,
                months=int(args.get("months") or 6),
            )
        )
    if name == "get_kpi_flags":
        return _text_result(get_kpi_flags_for_mcp(db, org_id))
    if name == "get_marketing_ideas":
        return _text_result(get_marketing_ideas_for_mcp(db, org_id))
    if name == "get_instagram_performance":
        return _text_result(
            get_instagram_performance_for_mcp(
                db,
                org_id,
                days=int(args.get("days") or 90),
            )
        )
    if name == "get_instagram_top_posts":
        return _text_result(
            get_instagram_top_posts_for_mcp(
                db,
                org_id,
                days=int(args.get("days") or 90),
                format_bucket=args.get("format_bucket"),
                theme_key=args.get("theme_key"),
                limit=int(args.get("limit") or 10),
            )
        )
    if name == "get_instagram_underperforming_posts":
        return _text_result(
            get_instagram_underperforming_posts_for_mcp(
                db,
                org_id,
                days=int(args.get("days") or 90),
                format_bucket=args.get("format_bucket"),
                limit=int(args.get("limit") or 5),
            )
        )
    if name == "search_resource_docs":
        ensure_resource_documents_table(db)
        include_content = args.get("include_content")
        return _text_result(
            search_resource_docs(
                db,
                org_id,
                query=str(args.get("query") or ""),
                category=args.get("category"),
                sop_category=args.get("sop_category"),
                limit=int(args.get("limit") or 8),
                include_content=True if include_content is None else bool(include_content),
            )
        )
    if name == "list_resource_docs":
        ensure_resource_documents_table(db)
        docs = list_docs(db, org_id)
        category = str(args.get("category") or "").strip().lower()
        sop_category = str(args.get("sop_category") or "").strip().lower()
        include_content = bool(args.get("include_content") or False)
        limit = int(args.get("limit") or 200)
        if category:
            docs = [d for d in docs if str(d.get("category") or "").strip().lower() == category]
        if sop_category:
            docs = [
                d for d in docs if str(d.get("sop_category") or "").strip().lower() == sop_category
            ]
        docs = docs[: max(1, min(limit, 500))]
        if include_content:
            docs = [ensure_doc_content(d) for d in docs]
            # Cap bodies for MCP size
            capped = []
            for d in docs:
                item = dict(d)
                content = str(item.get("content") or "")
                if len(content) > 18_000:
                    item["content"] = content[:18_000] + "\n\n…[truncated for MCP]"
                capped.append(item)
            docs = capped
        else:
            docs = [
                {
                    "resource_id": d.get("resource_id"),
                    "category": d.get("category"),
                    "sop_category": d.get("sop_category"),
                    "title": d.get("title"),
                    "description": d.get("description"),
                    "powered_by": d.get("powered_by"),
                    "video_url": d.get("video_url"),
                    "is_custom": d.get("is_custom"),
                    "is_builtin": d.get("is_builtin"),
                    "updated_at": d.get("updated_at"),
                    "sort_order": d.get("sort_order"),
                }
                for d in docs
            ]
        return _text_result(
            {
                "docs": docs,
                "count": len(docs),
                "usage": (
                    "SOP / resource catalog. For topical consulting questions prefer "
                    "search_resource_docs; then get_resource_doc for a full framework."
                ),
            }
        )
    if name == "get_resource_doc":
        rid = str(args.get("resource_id") or "").strip()
        if not rid:
            return _text_result({"error": "resource_id required"})
        ensure_resource_documents_table(db)
        doc = get_doc(db, org_id, rid)
        if not doc:
            return _text_result({"error": "resource doc not found"})
        return _text_result(ensure_doc_content(doc))
    if name == "list_org_resource_library":
        ensure_resource_library_table(db)
        items = list_library_items(db, org_id)
        kind = str(args.get("kind") or "").strip().lower()
        tag = str(args.get("tag") or "").strip()
        limit = int(args.get("limit") or 200)
        if kind:
            items = [i for i in items if str(i.get("kind") or "").strip().lower() == kind]
        if tag:
            items = [i for i in items if tag in (i.get("tags") or [])]
        items = items[: max(1, min(limit, 500))]
        return _text_result({"items": items, "count": len(items)})
    if name == "get_org_resource_library_item":
        raw = str(args.get("item_id") or "").strip()
        if not raw:
            return _text_result({"error": "item_id required"})
        ensure_resource_library_table(db)
        try:
            item = get_library_item(db, org_id, uuid.UUID(raw))
        except ValueError:
            return _text_result({"error": "invalid item_id"})
        if not item:
            return _text_result({"error": "resource library item not found"})
        return _text_result(item)
    if name == "list_brevo_senders":
        active_only = args.get("active_only")
        if active_only is None:
            active_only = True
        return _text_result(
            list_brevo_senders_for_mcp(
                db,
                org_id,
                user_id=user_id,
                active_only=bool(active_only),
            )
        )
    if name == "send_client_email":
        return _text_result(
            send_client_email_for_mcp(
                db,
                org_id,
                user_id=user_id,
                client_id=str(args.get("client_id") or ""),
                sender_email=str(args.get("sender_email") or ""),
                sender_name=str(args.get("sender_name") or ""),
                subject=str(args.get("subject") or ""),
                html_content=args.get("html_content"),
                text_content=args.get("text_content"),
                reply_to_email=args.get("reply_to_email"),
                reply_to_name=args.get("reply_to_name"),
                confirm_send=bool(args.get("confirm_send")),
            )
        )
    return _text_result({"error": f"Unknown tool: {name}"})


def _handle_jsonrpc(
    body: dict,
    org_id: uuid.UUID,
    db: Session,
    *,
    user_id: Optional[uuid.UUID] = None,
    auth_ctx: Optional[dict] = None,
) -> dict:
    req_id = body.get("id")
    method = body.get("method") or ""
    params = body.get("params") or {}

    if method == "initialize":
        requested = params.get("protocolVersion") or SERVER_INFO["protocolVersion"]
        negotiated = requested if requested in SUPPORTED_PROTOCOL_VERSIONS else SERVER_INFO["protocolVersion"]
        org_name = (auth_ctx or {}).get("org_name")
        return {
            "jsonrpc": "2.0",
            "id": req_id,
            "result": {
                "protocolVersion": negotiated,
                # Empty object advertises tools capability so Claude requests tools/list
                "capabilities": {"tools": {}, "resources": {}},
                "serverInfo": {
                    "name": SERVER_INFO["name"],
                    "version": SERVER_INFO["version"],
                    "title": f"SweepOS ({org_name})" if org_name else "SweepOS",
                },
            },
        }
    if method in ("notifications/initialized", "initialized"):
        return {"jsonrpc": "2.0", "id": req_id, "result": {}}
    if method == "tools/list":
        return {"jsonrpc": "2.0", "id": req_id, "result": {"tools": TOOLS}}
    if method == "tools/call":
        name = params.get("name")
        arguments = params.get("arguments") or {}
        result = _run_tool(name, arguments, org_id, db, user_id=user_id, auth_ctx=auth_ctx)
        return {"jsonrpc": "2.0", "id": req_id, "result": result}
    if method == "resources/list":
        return {
            "jsonrpc": "2.0",
            "id": req_id,
            "result": {
                "resources": [
                    {
                        "uri": "sweep://clients",
                        "name": "Clients",
                        "description": "List of clients in the connected org",
                        "mimeType": "application/json",
                    },
                    {
                        "uri": "sweep://marketing/intel",
                        "name": "Marketing Intel",
                        "description": (
                            "Org marketing intel: sales signals, themes, knowledge, playbook, "
                            "ICP, and drafted content bundle"
                        ),
                        "mimeType": "application/json",
                    },
                    {
                        "uri": "sweep://marketing/ideas",
                        "name": "Marketing ideas",
                        "description": "Latest drafted TOF/MOF/BOF Marketing Intel concepts",
                        "mimeType": "application/json",
                    },
                    {
                        "uri": "sweep://marketing/signals",
                        "name": "Sales signals",
                        "description": "Objections, struggles, wins, stories, and themes from calls",
                        "mimeType": "application/json",
                    },
                    {
                        "uri": "sweep://org/intelligence",
                        "name": "Org Intelligence profile",
                        "description": (
                            "Business context, ICP, unique selling proposition, sales approach, "
                            "brand voice, and offer ladder for the connected org"
                        ),
                        "mimeType": "application/json",
                    },
                    {
                        "uri": "sweep://terminal/dashboard",
                        "name": "Terminal dashboard",
                        "description": "Cash, MRR, trends, calendar, appointments, failed payments, leads",
                        "mimeType": "application/json",
                    },
                    {
                        "uri": "sweep://kpi/snapshot",
                        "name": "KPI snapshot",
                        "description": (
                            "KPI Command Center snapshot: cards, daily series, current-month totals, flags"
                        ),
                        "mimeType": "application/json",
                    },
                    {
                        "uri": "sweep://kpi/rollups",
                        "name": "KPI monthly totals",
                        "description": "Calendar-month KPI rollups (volume sums + funnel rates)",
                        "mimeType": "application/json",
                    },
                    {
                        "uri": "sweep://kpi/trends",
                        "name": "KPI trends",
                        "description": "Month-over-month KPI series plus decline/bottleneck flags",
                        "mimeType": "application/json",
                    },
                    {
                        "uri": "sweep://instagram/performance",
                        "name": "Instagram performance",
                        "description": (
                            "Period comparison, weekly trends, top and underperforming posts "
                            "with Instagram URLs and metrics"
                        ),
                        "mimeType": "application/json",
                    },
                    {
                        "uri": "sweep://instagram/top-posts",
                        "name": "Instagram top posts",
                        "description": "Top posts by engagement with permalinks and numerical metrics",
                        "mimeType": "application/json",
                    },
                    {
                        "uri": "sweep://instagram/underperformers",
                        "name": "Instagram underperformers",
                        "description": (
                            "Lowest-engagement posts with permalinks and numerical metrics"
                        ),
                        "mimeType": "application/json",
                    },
                    {
                        "uri": "sweep://resources/docs",
                        "name": "SOP / consulting knowledge base",
                        "description": (
                            "Catalog of builtin SOP library + org custom/overridden docs "
                            "(strategic consulting frameworks)"
                        ),
                        "mimeType": "application/json",
                    },
                    {
                        "uri": "sweep://resources/library",
                        "name": "Org resource library",
                        "description": (
                            "Per-org uploaded/linked resources (testimonials, case studies, "
                            "custom notes — text, markdown, image, video_url, url)"
                        ),
                        "mimeType": "application/json",
                    },
                    {
                        "uri": "sweep://brevo/senders",
                        "name": "Brevo senders",
                        "description": "Verified sender email/name options for outbound email",
                        "mimeType": "application/json",
                    },
                ]
            },
        }
    if method == "resources/read":
        uri = (params.get("uri") or "").strip()
        if uri.startswith("sweep://client/"):
            cid = uri.split("sweep://client/", 1)[1]
            try:
                bundle = build_client_profile_bundle(db, org_id, uuid.UUID(cid))
            except ValueError:
                bundle = None
            text = json.dumps(bundle or {"error": "not found"}, default=str)
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {
                    "contents": [
                        {"uri": uri, "mimeType": "application/json", "text": text[:140_000]}
                    ]
                },
            }
        if uri == "sweep://clients":
            rows = list_clients_for_mcp(db, org_id, limit=100)
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {
                    "contents": [
                        {
                            "uri": uri,
                            "mimeType": "application/json",
                            "text": json.dumps(rows, default=str),
                        }
                    ]
                },
            }
        if uri == "sweep://marketing/intel":
            payload = get_marketing_intel_bootstrap_for_mcp(
                db, org_id, user_id=user_id, include_sop=True
            )
            text = json.dumps(payload, default=str)
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {
                    "contents": [
                        {"uri": uri, "mimeType": "application/json", "text": text[:140_000]}
                    ]
                },
            }
        if uri == "sweep://marketing/ideas":
            payload = get_marketing_ideas_for_mcp(db, org_id)
            text = json.dumps(payload, default=str)
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {
                    "contents": [
                        {"uri": uri, "mimeType": "application/json", "text": text[:140_000]}
                    ]
                },
            }
        if uri == "sweep://marketing/signals":
            payload = get_org_sales_signals_for_mcp(db, org_id)
            text = json.dumps(payload, default=str)
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {
                    "contents": [
                        {"uri": uri, "mimeType": "application/json", "text": text[:140_000]}
                    ]
                },
            }
        if uri == "sweep://org/intelligence":
            payload = get_org_intelligence_for_mcp(db, org_id, user_id=user_id)
            text = json.dumps(payload, default=str)
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {
                    "contents": [
                        {"uri": uri, "mimeType": "application/json", "text": text[:140_000]}
                    ]
                },
            }
        if uri == "sweep://terminal/dashboard":
            payload = build_terminal_dashboard_for_mcp(
                db, org_id, user_id=user_id, mode="overview"
            )
            text = json.dumps(payload, default=str)
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {
                    "contents": [
                        {"uri": uri, "mimeType": "application/json", "text": text[:140_000]}
                    ]
                },
            }
        if uri == "sweep://kpi/snapshot":
            payload = get_kpi_snapshot_for_mcp(db, org_id)
            text = json.dumps(payload, default=str)
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {
                    "contents": [
                        {"uri": uri, "mimeType": "application/json", "text": text[:140_000]}
                    ]
                },
            }
        if uri == "sweep://kpi/rollups":
            payload = get_kpi_monthly_rollups_for_mcp(db, org_id, months=12)
            text = json.dumps(payload, default=str)
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {
                    "contents": [
                        {"uri": uri, "mimeType": "application/json", "text": text[:140_000]}
                    ]
                },
            }
        if uri == "sweep://kpi/trends":
            payload = get_kpi_trends_for_mcp(db, org_id, months=6)
            text = json.dumps(payload, default=str)
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {
                    "contents": [
                        {"uri": uri, "mimeType": "application/json", "text": text[:140_000]}
                    ]
                },
            }
        if uri == "sweep://instagram/performance":
            payload = get_instagram_performance_for_mcp(db, org_id, days=90)
            text = json.dumps(payload, default=str)
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {
                    "contents": [
                        {"uri": uri, "mimeType": "application/json", "text": text[:140_000]}
                    ]
                },
            }
        if uri == "sweep://instagram/top-posts":
            payload = get_instagram_top_posts_for_mcp(db, org_id, days=90, limit=10)
            text = json.dumps(payload, default=str)
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {
                    "contents": [
                        {"uri": uri, "mimeType": "application/json", "text": text[:140_000]}
                    ]
                },
            }
        if uri == "sweep://instagram/underperformers":
            payload = get_instagram_underperforming_posts_for_mcp(db, org_id, days=90, limit=5)
            text = json.dumps(payload, default=str)
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {
                    "contents": [
                        {"uri": uri, "mimeType": "application/json", "text": text[:140_000]}
                    ]
                },
            }
        if uri == "sweep://brevo/senders":
            payload = list_brevo_senders_for_mcp(db, org_id, user_id=user_id, active_only=True)
            text = json.dumps(payload, default=str)
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {
                    "contents": [
                        {"uri": uri, "mimeType": "application/json", "text": text[:140_000]}
                    ]
                },
            }
        if uri == "sweep://resources/docs":
            ensure_resource_documents_table(db)
            payload = {"docs": list_docs(db, org_id)}
            text = json.dumps(payload, default=str)
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {
                    "contents": [
                        {"uri": uri, "mimeType": "application/json", "text": text[:140_000]}
                    ]
                },
            }
        if uri.startswith("sweep://resources/docs/"):
            rid = uri.split("sweep://resources/docs/", 1)[1]
            ensure_resource_documents_table(db)
            payload = get_doc(db, org_id, rid) or {"error": "resource doc not found"}
            text = json.dumps(payload, default=str)
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {
                    "contents": [
                        {"uri": uri, "mimeType": "application/json", "text": text[:140_000]}
                    ]
                },
            }
        if uri == "sweep://resources/library":
            ensure_resource_library_table(db)
            payload = {"items": list_library_items(db, org_id)}
            text = json.dumps(payload, default=str)
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {
                    "contents": [
                        {"uri": uri, "mimeType": "application/json", "text": text[:140_000]}
                    ]
                },
            }
        if uri.startswith("sweep://resources/library/"):
            raw = uri.split("sweep://resources/library/", 1)[1]
            ensure_resource_library_table(db)
            try:
                payload = get_library_item(db, org_id, uuid.UUID(raw)) or {"error": "resource library item not found"}
            except ValueError:
                payload = {"error": "invalid resource library item id"}
            text = json.dumps(payload, default=str)
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {
                    "contents": [
                        {"uri": uri, "mimeType": "application/json", "text": text[:140_000]}
                    ]
                },
            }
        return {
            "jsonrpc": "2.0",
            "id": req_id,
            "error": {"code": -32002, "message": f"Unknown resource: {uri}"},
        }
    if method == "ping":
        return {"jsonrpc": "2.0", "id": req_id, "result": {}}

    return {
        "jsonrpc": "2.0",
        "id": req_id,
        "error": {"code": -32601, "message": f"Method not found: {method}"},
    }


@router.api_route("/mcp", methods=["GET", "POST", "DELETE", "OPTIONS"])
@router.api_route("/mcp/", methods=["GET", "POST", "DELETE", "OPTIONS"])
async def mcp_endpoint(request: Request):
    if request.method == "OPTIONS":
        return Response(status_code=204)

    # Unauthenticated GET is used by Claude for discovery handshake → 401
    ctx = _auth_context(request)
    if ctx is None:
        return _unauthorized()

    try:
        org_id = uuid.UUID(str(ctx["org_id"]))
    except (KeyError, ValueError):
        return _unauthorized("Invalid token claims")

    user_id: Optional[uuid.UUID] = None
    try:
        if ctx.get("user_id"):
            user_id = uuid.UUID(str(ctx["user_id"]))
    except ValueError:
        user_id = None

    session_id = request.headers.get("mcp-session-id") or request.headers.get("Mcp-Session-Id") or str(uuid.uuid4())

    def _mcp_headers(extra: Optional[dict] = None) -> dict:
        headers = {
            "Mcp-Session-Id": session_id,
            "MCP-Protocol-Version": request.headers.get("mcp-protocol-version")
            or request.headers.get("MCP-Protocol-Version")
            or SERVER_INFO["protocolVersion"],
        }
        if extra:
            headers.update(extra)
        return headers

    if request.method == "GET":
        accept = (request.headers.get("accept") or "").lower()
        # Streamable HTTP: GET opens a server→client SSE stream. Claude/Cowork expect
        # a held-open stream (not a one-shot body). Spec also allows 405; Claude.ai
        # often fails if GET is not a real SSE stream.
        wants_sse = (not accept) or ("text/event-stream" in accept) or ("*/*" in accept)
        if wants_sse:

            async def _sse_gen():
                # Prime stream (MCP resumability guidance)
                yield f"id: {uuid.uuid4().hex}\ndata:\n\n"
                yield (
                    "event: message\n"
                    f"data: {json.dumps({'jsonrpc': '2.0', 'method': 'notifications/message', 'params': {'level': 'info', 'data': 'connected'}})}\n\n"
                )
                # Keep connection open with comment heartbeats until client disconnects
                while True:
                    if await request.is_disconnected():
                        break
                    yield ": keepalive\n\n"
                    await asyncio.sleep(15)

            return StreamingResponse(
                _sse_gen(),
                status_code=200,
                media_type="text/event-stream",
                headers=_mcp_headers(
                    {
                        "Cache-Control": "no-cache, no-transform",
                        "Connection": "keep-alive",
                        # Some Claude clients reject charset suffix on SSE
                        "Content-Type": "text/event-stream",
                        "X-Accel-Buffering": "no",
                    }
                ),
            )
        return JSONResponse(
            {
                "status": "ok",
                "server": SERVER_INFO,
                "org_id": str(org_id),
                "tools": [t["name"] for t in TOOLS],
            },
            headers=_mcp_headers(),
        )

    if request.method == "DELETE":
        return JSONResponse({"status": "closed"}, headers=_mcp_headers())

    try:
        body = await request.json()
    except Exception:
        return JSONResponse(
            status_code=400,
            content={"jsonrpc": "2.0", "error": {"code": -32700, "message": "Parse error"}},
            headers=_mcp_headers(),
        )

    db = SessionLocal()
    try:
        if isinstance(body, list):
            results = [
                _handle_jsonrpc(item, org_id, db, user_id=user_id, auth_ctx=ctx)
                for item in body
                if isinstance(item, dict)
            ]
            return JSONResponse(results, headers=_mcp_headers())
        if not isinstance(body, dict):
            return JSONResponse(
                status_code=400,
                content={"jsonrpc": "2.0", "error": {"code": -32600, "message": "Invalid Request"}},
                headers=_mcp_headers(),
            )
        # Notifications may omit id
        if body.get("method") and body.get("id") is None and str(body.get("method", "")).startswith("notifications/"):
            _handle_jsonrpc(body, org_id, db, user_id=user_id, auth_ctx=ctx)
            return Response(status_code=202, headers=_mcp_headers())
        return JSONResponse(
            _handle_jsonrpc(body, org_id, db, user_id=user_id, auth_ctx=ctx),
            headers=_mcp_headers(),
        )
    finally:
        db.close()

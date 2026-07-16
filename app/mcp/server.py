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
from fastapi.responses import JSONResponse, Response
from sqlalchemy.orm import Session

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
from app.services.mcp_oauth_service import mcp_resource, verify_mcp_access_token
from app.services.terminal_dashboard_bundle import build_terminal_dashboard_for_mcp

logger = logging.getLogger(__name__)

router = APIRouter()

SERVER_INFO = {
    "name": "sweepos",
    "version": "1.2.1",
    "protocolVersion": "2025-03-26",
}

# Claude.ai currently prefers 2025-11-25; accept both.
SUPPORTED_PROTOCOL_VERSIONS = ("2025-11-25", "2025-03-26")


TOOLS = [
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
            "when drafting short-form content from real sales data."
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
            "Org Intelligence bank for content: ICP / positioning / brand voice fields and offer ladder "
            "(when configured in SweepOS Intelligence settings)."
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
            "Full SweepOS Terminal dashboard snapshot: cash collected/MRR/top contributors, "
            "monthly trends, finances KPIs, Stripe summary, calendar show-up/close rates, "
            "upcoming appointments, failed-payment queue, and funnel/leads analytics. "
            "Optionally pass sections to request a subset."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
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
                    "description": "Optional subset of dashboard sections (default: all)",
                },
                "finances_range_days": {"type": "integer", "default": 30, "minimum": 1, "maximum": 365},
                "finances_scope": {
                    "type": "string",
                    "description": "Optional finances window scope: mtd | all",
                },
                "appointments_limit": {"type": "integer", "default": 40, "minimum": 1, "maximum": 100},
            },
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
        f'scope="clients:read marketing:read terminal:read email:send"'
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
) -> dict:
    args = arguments or {}
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
                finances_range_days=int(args.get("finances_range_days") or 30),
                finances_scope=args.get("finances_scope"),
                appointments_limit=int(args.get("appointments_limit") or 40),
            )
        )
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
) -> dict:
    req_id = body.get("id")
    method = body.get("method") or ""
    params = body.get("params") or {}

    if method == "initialize":
        requested = params.get("protocolVersion") or SERVER_INFO["protocolVersion"]
        negotiated = requested if requested in SUPPORTED_PROTOCOL_VERSIONS else SERVER_INFO["protocolVersion"]
        return {
            "jsonrpc": "2.0",
            "id": req_id,
            "result": {
                "protocolVersion": negotiated,
                # Empty object advertises tools capability so Claude requests tools/list
                "capabilities": {"tools": {}, "resources": {}},
                "serverInfo": {"name": SERVER_INFO["name"], "version": SERVER_INFO["version"]},
            },
        }
    if method in ("notifications/initialized", "initialized"):
        return {"jsonrpc": "2.0", "id": req_id, "result": {}}
    if method == "tools/list":
        return {"jsonrpc": "2.0", "id": req_id, "result": {"tools": TOOLS}}
    if method == "tools/call":
        name = params.get("name")
        arguments = params.get("arguments") or {}
        result = _run_tool(name, arguments, org_id, db, user_id=user_id)
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
                        "uri": "sweep://marketing/signals",
                        "name": "Sales signals",
                        "description": "Objections, struggles, wins, stories, and themes from calls",
                        "mimeType": "application/json",
                    },
                    {
                        "uri": "sweep://terminal/dashboard",
                        "name": "Terminal dashboard",
                        "description": "Cash, MRR, trends, calendar, appointments, failed payments, leads",
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
        if uri == "sweep://terminal/dashboard":
            payload = build_terminal_dashboard_for_mcp(db, org_id, user_id=user_id)
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
        if "text/event-stream" in accept:
            # Minimal SSE stream so Streamable HTTP clients can hold a GET session open
            payload = (
                "event: message\n"
                f"data: {json.dumps({'jsonrpc': '2.0', 'method': 'notifications/message', 'params': {'level': 'info', 'data': 'connected'}})}\n\n"
            )
            return Response(
                content=payload,
                status_code=200,
                media_type="text/event-stream",
                headers=_mcp_headers({"Cache-Control": "no-cache", "Connection": "keep-alive"}),
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
                _handle_jsonrpc(item, org_id, db, user_id=user_id)
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
            _handle_jsonrpc(body, org_id, db, user_id=user_id)
            return Response(status_code=202, headers=_mcp_headers())
        return JSONResponse(_handle_jsonrpc(body, org_id, db, user_id=user_id), headers=_mcp_headers())
    finally:
        db.close()

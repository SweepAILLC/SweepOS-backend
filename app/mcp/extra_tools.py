"""
Additional MCP tools: payments / MRR, Meta Ads, raw call transcripts, Instagram posts +
transcripts, and one write tool for the organic EOD (daily KPI) ledger.

server.py registers EXTRA_TOOLS and routes calls to run_extra_tool(). Helpers from
server.py (_text_result, _call_api, ...) are imported lazily to avoid an import cycle.

Reads are thin wrappers over the same REST handlers the app uses, so org scoping and
tab permissions (e.g. Meta Ads needs the content_studio tab) match the UI exactly.
"""
from __future__ import annotations

import logging
import uuid
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional

from fastapi import HTTPException
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


def _tool(name: str, description: str, properties: Optional[dict] = None, required: Optional[list] = None) -> dict:
    schema: Dict[str, Any] = {"type": "object", "properties": properties or {}}
    if required:
        schema["required"] = required
    return {"name": name, "description": description, "inputSchema": schema}


_DATE = {"type": "string", "description": "YYYY-MM-DD"}
_RANGE_PROPS = {
    "start": {**_DATE, "description": "YYYY-MM-DD inclusive; with end, overrides range_days/scope"},
    "end": {**_DATE, "description": "YYYY-MM-DD inclusive"},
    "range_days": {"type": "integer", "default": 30, "minimum": 1, "maximum": 365},
    "scope": {"type": "string", "enum": ["mtd", "all"], "description": "mtd = month to date; all = all history"},
}
_PAGE_PROPS = {
    "page": {"type": "integer", "default": 1, "minimum": 1},
    "page_size": {"type": "integer", "default": 25, "minimum": 1, "maximum": 100},
}

# Fields of the daily KPI ledger an agent may write (mirrors KpiDailyEntryUpdate).
_EOD_INT_FIELDS = (
    "total_followers", "new_followers", "inboxes_checked", "outreach_sent", "respondents",
    "inbound_icp_leads", "followups_sent", "new_conversations", "conversations_nurtured",
    "calls_pitched", "inbound_bookings", "outbound_bookings", "calls_booked",
    "calls_booked_activity", "calls_taken", "offers_made", "no_shows", "closes",
)
_EOD_MONEY_FIELDS = ("cash_collected", "revenue")
_EOD_OTHER_FIELDS = ("content_posted", "best_content_type", "setter_context", "setter_booked_client_ids")
_EOD_FIELDS = _EOD_INT_FIELDS + _EOD_MONEY_FIELDS + _EOD_OTHER_FIELDS

EXTRA_WRITE_TOOLS = {"update_kpi_daily_entry", "transcribe_instagram_post"}

EXTRA_TOOLS: List[dict] = [
    # ---- payments / MRR -------------------------------------------------------------
    _tool(
        "get_finances_summary",
        "Combined cash collected (Stripe + Whop + manual payments) for a window: revenue, order "
        "count, AOV, per-source split and the prior equal-length period for comparison. "
        "Defaults to the last 30 days.",
        {**_RANGE_PROPS},
    ),
    _tool(
        "get_revenue_timeline",
        "Cash collected over time (Stripe, Whop, manual and total per day or week). Defaults to "
        "the last 30 days.",
        {**_RANGE_PROPS, "group_by": {"type": "string", "enum": ["day", "week"], "default": "day"}},
    ),
    _tool(
        "get_mrr",
        "Recurring-revenue snapshot. Stripe: MRR, ARR, MRR change vs previous period, active "
        "subscriptions, churned subscriptions, new customers, failed payments and average client "
        "LTV. When Whop is connected, also a 'whop' block with ESTIMATED MRR/ARR inferred from "
        "payment history (Whop has no subscription objects here) plus combined_mrr/combined_arr. "
        "Set include_trend=true for the Stripe MRR time series.",
        {
            **_RANGE_PROPS,
            "include_trend": {"type": "boolean", "default": False},
            "group_by": {"type": "string", "enum": ["day", "week", "month"], "default": "week",
                         "description": "Trend bucket size (only with include_trend)"},
        },
    ),
    _tool(
        "list_payments",
        "Individual payments (Stripe, Whop and manual, deduplicated, newest first) with amount, "
        "status and client. Filter by status and window; paginated.",
        {**_RANGE_PROPS, **_PAGE_PROPS, "status": {"type": "string", "description": "e.g. succeeded, failed, refunded"}},
    ),
    _tool(
        "list_subscriptions",
        "Stripe subscriptions with per-subscription MRR, status, plan and client. Filter by "
        "status (active, past_due, canceled, ...) or search text; paginated.",
        {**_PAGE_PROPS, "status": {"type": "string"}, "search": {"type": "string", "description": "Client name/email"}},
    ),
    _tool(
        "list_failed_payments",
        "Failed-payment recovery queue (who failed, how much, when). Defaults to all open items.",
        {
            **_RANGE_PROPS,
            **_PAGE_PROPS,
            "exclude_resolved": {"type": "boolean", "default": True},
        },
    ),
    # ---- Meta Ads (requires the Marketing Intel / content_studio tab) -------------------
    _tool(
        "get_meta_ads_dashboard",
        "Meta Ads dashboard from the synced cache: account KPIs (spend, leads, CPL, CTR, ...), "
        "campaign table with signals, and active alerts, vs a comparison window. Optional "
        "funnel_id narrows to that funnel's linked campaigns. Requires the Marketing Intel tab.",
        {
            "days": {"type": "integer", "default": 28, "minimum": 7, "maximum": 90},
            "start": _DATE, "end": _DATE,
            "compare_start": _DATE, "compare_end": _DATE,
            "funnel_id": {"type": "string", "description": "Funnel UUID (from list_funnels)"},
        },
    ),
    _tool(
        "list_meta_ad_sets",
        "Meta ad-set rows (spend, results, CPL, CTR, frequency, status) for the window, "
        "optionally limited to campaign_ids (comma-separated UUIDs from get_meta_ads_dashboard).",
        {
            "days": {"type": "integer", "default": 28, "minimum": 7, "maximum": 90},
            "start": _DATE, "end": _DATE,
            "campaign_ids": {"type": "string", "description": "Comma-separated campaign UUIDs"},
        },
    ),
    _tool(
        "list_meta_ads",
        "Meta ad (creative) rows with performance for the window. Limit with ad_set_ids or "
        "campaign_ids (comma-separated UUIDs).",
        {
            "days": {"type": "integer", "default": 28, "minimum": 7, "maximum": 90},
            "start": _DATE, "end": _DATE,
            "ad_set_ids": {"type": "string", "description": "Comma-separated ad set UUIDs"},
            "campaign_ids": {"type": "string", "description": "Comma-separated campaign UUIDs"},
        },
    ),
    _tool("get_meta_ads_alerts", "Active Meta Ads alerts (fatigue, CPL spikes, delivery problems)."),
    # ---- call transcripts ----------------------------------------------------------------
    _tool(
        "list_call_recordings",
        "Recorded calls (Fathom) for the org: id, title, date, client, attendees, summary and "
        "share link. Newest first. Use the id with get_call_transcript.",
        {
            "query": {"type": "string", "description": "Match on call title"},
            "client_id": {"type": "string", "description": "Client UUID"},
            "days": {"type": "integer", "default": 90, "minimum": 1, "maximum": 730},
            "limit": {"type": "integer", "default": 25, "minimum": 1, "maximum": 100},
        },
    ),
    _tool(
        "get_call_transcript",
        "Full raw speaker-labelled transcript of one recorded call, fetched live from Fathom. "
        "Pass call_id (from list_call_recordings) or fathom_recording_id. Long calls are paged: "
        "re-call with offset=next_offset until next_offset is null.",
        {
            "call_id": {"type": "string", "description": "Call record UUID"},
            "fathom_recording_id": {"type": "integer"},
            "offset": {"type": "integer", "default": 0, "minimum": 0},
            "max_chars": {"type": "integer", "default": 40000, "minimum": 1000, "maximum": 60000},
        },
    ),
    # ---- Instagram (requires the Marketing Intel / content_studio tab) -----------------
    _tool(
        "list_instagram_posts",
        "Synced Instagram posts with full metrics (views, reach, saves, shares, watch time, "
        "engagement/save/share rates), hook text, theme tags and transcript status. Sort and "
        "filter; set include_transcript=true for full spoken-word transcripts (otherwise a short "
        "excerpt). Requires the Marketing Intel tab.",
        {
            "days": {"type": "integer", "default": 90, "minimum": 1, "maximum": 365},
            "media_product_type": {"type": "string", "enum": ["REELS", "FEED", "STORY"]},
            "sort_by": {
                "type": "string",
                "enum": ["posted_at", "views", "reach", "saved", "shares", "likes", "comments",
                         "engagement_rate_pct", "save_rate_pct", "share_rate_pct"],
                "default": "posted_at",
            },
            "has_transcript": {"type": "boolean", "description": "Only posts that have a transcript"},
            "include_transcript": {
                "type": "boolean", "default": False,
                "description": "Full transcripts; limit is capped at 15 posts when true",
            },
            "limit": {"type": "integer", "default": 25, "minimum": 1, "maximum": 100},
        },
    ),
    _tool(
        "get_instagram_post",
        "One Instagram post in full: caption, every metric and the complete transcript. Pass id "
        "(UUID from list_instagram_posts) or ig_media_id. Requires the Marketing Intel tab.",
        {"id": {"type": "string"}, "ig_media_id": {"type": "string"}},
    ),
    _tool(
        "get_instagram_account_history",
        "Daily Instagram account snapshots (followers, reach, views, profile views, accounts "
        "engaged, follows/unfollows). Requires the Marketing Intel tab.",
        {"days": {"type": "integer", "default": 90, "minimum": 1, "maximum": 365}},
    ),
    _tool(
        "transcribe_instagram_post",
        "COSTS MONEY (OpenAI Whisper). Transcribe Instagram reels/videos that have no transcript "
        "yet, then read them with get_instagram_post. Pass ids (post UUIDs or ig_media_ids from "
        "list_instagram_posts). Limits: a few posts per call, a per-org daily attempt cap, video "
        "posts only, files over 25MB are unavailable, already-transcribed posts are returned free, "
        "and a failed post is not retried for an hour. Requires the Marketing Intel tab.",
        {"ids": {"type": "array", "items": {"type": "string"}, "minItems": 1,
                 "description": "Post UUIDs or ig_media_ids"}},
        ["ids"],
    ),
    # ---- WRITE: organic EOD / daily KPI ledger --------------------------------------------
    _tool(
        "update_kpi_daily_entry",
        "WRITE. Create/update one day of the organic KPI (EOD) ledger. Only the fields you pass "
        "are touched. mode='set' (default) overwrites those fields; mode='add' adds numbers onto "
        "the existing day (setter_context is appended). Omit rep_user_id for the org-aggregate "
        "row, or pass a team member's user_id (from get_team_members) to write their own row; "
        "writing a rep's row does NOT mark their EOD as submitted. Counts must be >= 0. Requires "
        "the org to have enabled automated scorecard writes, and the connection to hold the "
        "'funnels:write' scope.",
        {
            "entry_date": _DATE,
            "rep_user_id": {"type": "string"},
            "mode": {"type": "string", "enum": ["set", "add"], "default": "set"},
            **{f: {"type": "integer", "minimum": 0, "maximum": 1000000000} for f in _EOD_INT_FIELDS},
            **{f: {"type": "number", "minimum": 0, "maximum": 1000000000} for f in _EOD_MONEY_FIELDS},
            "content_posted": {"type": "boolean"},
            "best_content_type": {"type": "string", "maxLength": 512},
            "setter_context": {"type": "string", "maxLength": 5000},
            "setter_booked_client_ids": {
                "type": "array", "items": {"type": "string"}, "maxItems": 100,
                "description": "Client UUIDs the setter's booked calls refer to",
            },
        },
        ["entry_date"],
    ),
]

EXTRA_TOOL_NAMES = {t["name"] for t in EXTRA_TOOLS}


# ---------------------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------------------
def _int(args: dict, key: str, default: int, lo: int, hi: int) -> int:
    try:
        value = int(args.get(key) if args.get(key) is not None else default)
    except (TypeError, ValueError):
        value = default
    return max(lo, min(hi, value))


def _opt_date(args: dict, key: str) -> Optional[date]:
    raw = args.get(key)
    if not raw:
        return None
    return date.fromisoformat(str(raw))


def _opt_str(args: dict, key: str) -> Optional[str]:
    raw = args.get(key)
    if raw is None:
        return None
    return str(raw).strip() or None


def _scope(args: dict) -> Optional[str]:
    return args.get("scope") if args.get("scope") in ("mtd", "all") else None


def _iso(value: Any) -> Optional[str]:
    return value.isoformat() if value is not None else None


def _gate_content_studio(db: Session, org_id: uuid.UUID, user_id: Optional[uuid.UUID]) -> Optional[dict]:
    """Same tab gate the Marketing Intel REST endpoints use (honours per-org locks)."""
    from app.api.deps import check_tab_access
    from app.mcp.server import _McpUser, _text_result

    if not check_tab_access("content_studio", _McpUser(user_id, org_id), db):
        return _text_result({"error": "Marketing Intel is not enabled for your organization."})
    return None


# ---------------------------------------------------------------------------------------
# call transcripts
# ---------------------------------------------------------------------------------------
def format_transcript(raw: Any) -> str:
    """Fathom transcript payload -> 'Speaker [mm:ss]: text' lines (tolerates plain strings)."""
    if isinstance(raw, str):
        return raw
    segments = raw.get("transcript") if isinstance(raw, dict) else raw
    if isinstance(segments, str):
        return segments
    lines: List[str] = []
    for seg in segments or []:
        if not isinstance(seg, dict):
            continue
        text = str(seg.get("text") or "").strip()
        if not text:
            continue
        speaker = seg.get("speaker")
        name = speaker.get("display_name") if isinstance(speaker, dict) else speaker
        stamp = seg.get("timestamp")
        label = f"{name or 'Speaker'}" + (f" [{stamp}]" if stamp else "")
        lines.append(f"{label}: {text}")
    return "\n".join(lines)


def _call_row(rec: Any) -> Dict[str, Any]:
    return {
        "call_id": str(rec.id),
        "fathom_recording_id": rec.fathom_recording_id,
        "title": rec.meeting_title,
        "meeting_at": _iso(rec.meeting_at),
        "client_id": str(rec.client_id) if rec.client_id else None,
        "related_client_ids": rec.related_client_ids or [],
        "attendees": rec.attendees_json or [],
        "summary": rec.summary_text,
        "share_url": rec.share_url or rec.recording_url,
    }


def list_call_recordings(db: Session, org_id: uuid.UUID, args: dict) -> Dict[str, Any]:
    from app.models.fathom_call_record import FathomCallRecord

    q = db.query(FathomCallRecord).filter(FathomCallRecord.org_id == org_id)
    days = _int(args, "days", 90, 1, 730)
    q = q.filter(FathomCallRecord.meeting_at >= datetime.now(timezone.utc) - timedelta(days=days))
    if args.get("client_id"):
        q = q.filter(FathomCallRecord.client_id == uuid.UUID(str(args["client_id"])))
    text = _opt_str(args, "query")
    if text:
        q = q.filter(FathomCallRecord.meeting_title.ilike(f"%{text}%"))
    rows = q.order_by(FathomCallRecord.meeting_at.desc()).limit(_int(args, "limit", 25, 1, 100)).all()
    return {"count": len(rows), "calls": [_call_row(r) for r in rows]}


def get_call_transcript(db: Session, org_id: uuid.UUID, args: dict) -> Dict[str, Any]:
    from app.models.fathom_call_record import FathomCallRecord
    from app.services import fathom_client

    q = db.query(FathomCallRecord).filter(FathomCallRecord.org_id == org_id)
    if args.get("call_id"):
        q = q.filter(FathomCallRecord.id == uuid.UUID(str(args["call_id"])))
    elif args.get("fathom_recording_id") is not None:
        q = q.filter(FathomCallRecord.fathom_recording_id == int(args["fathom_recording_id"]))
    else:
        return {"error": "call_id or fathom_recording_id required"}
    rec = q.first()
    if rec is None:
        return {"error": "call not found"}

    note = None
    try:
        text = format_transcript(
            fathom_client.get_recording_transcript(int(rec.fathom_recording_id), db=db, org_id=org_id)
        )
    except Exception as exc:  # Fathom not configured / rate-limited / recording gone
        logger.info("fathom transcript fetch failed org=%s call=%s: %s", org_id, rec.id, exc)
        text = rec.transcript_snippet or ""
        note = "Live transcript unavailable; returning the stored snippet only."
    if not text:
        return {"error": "no transcript available for this call", "call": _call_row(rec)}

    offset = _int(args, "offset", 0, 0, len(text))
    # Capped well under the 140k-char tool-result limit: json.dumps escapes non-ASCII, and a
    # truncated result would be invalid JSON (and lose next_offset).
    limit = _int(args, "max_chars", 40000, 1000, 60000)
    chunk = text[offset : offset + limit]
    end = offset + len(chunk)
    out: Dict[str, Any] = {
        "call": _call_row(rec),
        "total_chars": len(text),
        "offset": offset,
        "returned_chars": len(chunk),
        "next_offset": end if end < len(text) else None,
        "transcript": chunk,
    }
    if note:
        out["note"] = note
    return out


# ---------------------------------------------------------------------------------------
# Instagram
# ---------------------------------------------------------------------------------------
_IG_SORTS = {
    "posted_at", "views", "reach", "saved", "shares", "likes", "comments",
    "engagement_rate_pct", "save_rate_pct", "share_rate_pct",
}
_IG_METRICS = (
    "views", "reach", "saved", "likes", "comments", "shares", "reposts", "total_interactions",
    "avg_watch_time_sec", "skip_rate_pct", "engagement_rate_pct", "save_rate_pct",
    "share_rate_pct", "reach_vs_followers_pct",
)


def _ig_post(m: Any, *, transcript: str) -> Dict[str, Any]:
    return {
        "id": str(m.id),
        "ig_media_id": m.ig_media_id,
        "permalink": m.permalink,
        "media_type": m.media_type,
        "media_product_type": m.media_product_type,
        "posted_at": _iso(m.posted_at),
        "caption": m.caption,
        "hook_text": m.hook_text,
        "hook_pattern": m.hook_pattern,
        "format_bucket": m.format_bucket,
        "funnel_stage": m.funnel_stage,
        "theme_keys": m.theme_keys or [],
        "metrics": {k: getattr(m, k) for k in _IG_METRICS},
        "transcript_status": m.transcript_status,
        "transcript": transcript,
    }


def list_instagram_posts(db: Session, org_id: uuid.UUID, args: dict) -> Dict[str, Any]:
    from app.models.instagram_media import InstagramMedia

    q = db.query(InstagramMedia).filter(
        InstagramMedia.org_id == org_id,
        InstagramMedia.posted_at >= datetime.utcnow() - timedelta(days=_int(args, "days", 90, 1, 365)),
    )
    if args.get("media_product_type") in ("REELS", "FEED", "STORY"):
        q = q.filter(InstagramMedia.media_product_type == args["media_product_type"])
    if args.get("has_transcript"):
        q = q.filter(InstagramMedia.transcript.isnot(None), InstagramMedia.transcript != "")
    sort_key = args.get("sort_by") if args.get("sort_by") in _IG_SORTS else "posted_at"
    full = bool(args.get("include_transcript"))
    # Full transcripts are large; keep the page small enough that the JSON is never truncated.
    rows = (
        q.order_by(getattr(InstagramMedia, sort_key).desc().nullslast())
        .limit(_int(args, "limit", 25, 1, 15 if full else 100))
        .all()
    )
    posts = []
    for m in rows:
        t = m.transcript or ""
        posts.append(_ig_post(m, transcript=t if full else (t[:300] + ("…" if len(t) > 300 else ""))))
    return {"count": len(posts), "transcripts": "full" if full else "excerpt", "posts": posts}


_RETRY_AFTER = timedelta(hours=1)
_TRANSCRIPT_PREVIEW_CHARS = 4000


def transcribe_instagram_post(db: Session, org_id: uuid.UUID, args: dict) -> Dict[str, Any]:
    from app.core.config import settings
    from app.models.instagram_media import InstagramMedia
    from app.services.instagram_transcript_service import transcribe_media

    raw_ids = args.get("ids")
    if isinstance(raw_ids, str):
        raw_ids = [raw_ids]
    ids = [str(i).strip() for i in (raw_ids or []) if str(i).strip()]
    if not ids:
        return {"error": "ids required (post UUIDs or ig_media_ids)"}
    per_call = max(1, int(settings.MCP_IG_TRANSCRIBE_MAX_PER_CALL))
    if len(ids) > per_call:
        return {"error": f"at most {per_call} posts per call", "max_per_call": per_call}

    daily_limit = max(0, int(settings.MCP_IG_TRANSCRIBE_DAILY_LIMIT))
    day_start = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    used = (
        db.query(InstagramMedia)
        .filter(
            InstagramMedia.org_id == org_id,
            InstagramMedia.transcribed_at >= day_start,
            InstagramMedia.transcript_status.in_(("ok", "failed")),
        )
        .count()
    )
    remaining = max(0, daily_limit - used)

    results: List[Dict[str, Any]] = []
    for ident in ids:
        try:
            media_uuid: Optional[uuid.UUID] = uuid.UUID(ident)
        except ValueError:
            media_uuid = None
        q = db.query(InstagramMedia).filter(InstagramMedia.org_id == org_id)
        q = q.filter(InstagramMedia.id == media_uuid) if media_uuid else q.filter(InstagramMedia.ig_media_id == ident)
        m = q.first()
        if m is None:
            results.append({"id": ident, "status": "not_found"})
            continue
        base = {"id": str(m.id), "ig_media_id": m.ig_media_id}
        if m.transcript_status == "ok" and m.transcript:
            results.append({**base, "status": "already_transcribed", "charged": False})
            continue
        if (m.media_type or "").upper() != "VIDEO" and (m.media_product_type or "").upper() != "REELS":
            results.append({**base, "status": "skipped", "reason": "not a video/reel"})
            continue
        if (
            m.transcript_status in ("failed", "unavailable")
            and m.transcribed_at
            and datetime.utcnow() - m.transcribed_at.replace(tzinfo=None) < _RETRY_AFTER
        ):
            results.append({**base, "status": "skipped", "reason": "recent attempt failed; retry after 1 hour",
                            "error": m.transcript_error})
            continue
        if remaining <= 0:
            results.append({**base, "status": "skipped", "reason": "daily transcription limit reached"})
            continue
        remaining -= 1
        try:
            m = transcribe_media(db, org_id, m.id)
        except Exception:
            logger.exception("mcp transcribe failed media=%s", m.id)
            db.rollback()
            results.append({**base, "status": "failed", "error": "internal error"})
            continue
        text = m.transcript or ""
        results.append({
            **base,
            "status": m.transcript_status,
            "error": m.transcript_error,
            "transcript": text[:_TRANSCRIPT_PREVIEW_CHARS] if m.transcript_status == "ok" else None,
            "truncated": len(text) > _TRANSCRIPT_PREVIEW_CHARS,
        })
    return {
        "results": results,
        "daily_limit": daily_limit,
        "attempts_remaining_today": remaining,
    }


def get_instagram_post(db: Session, org_id: uuid.UUID, args: dict) -> Dict[str, Any]:
    from app.models.instagram_media import InstagramMedia

    q = db.query(InstagramMedia).filter(InstagramMedia.org_id == org_id)
    if args.get("id"):
        q = q.filter(InstagramMedia.id == uuid.UUID(str(args["id"])))
    elif args.get("ig_media_id"):
        q = q.filter(InstagramMedia.ig_media_id == str(args["ig_media_id"]))
    else:
        return {"error": "id or ig_media_id required"}
    m = q.first()
    if m is None:
        return {"error": "post not found"}
    return _ig_post(m, transcript=m.transcript or "")


def get_instagram_account_history(db: Session, org_id: uuid.UUID, args: dict) -> Dict[str, Any]:
    from app.models.instagram_account_snapshot import InstagramAccountSnapshot as S

    since = date.today() - timedelta(days=_int(args, "days", 90, 1, 365))
    rows = (
        db.query(S).filter(S.org_id == org_id, S.snapshot_date >= since).order_by(S.snapshot_date.asc()).all()
    )
    fields = ("followers_count", "reach", "views", "profile_views", "accounts_engaged", "follows", "unfollows")
    return {
        "count": len(rows),
        "snapshots": [{"date": r.snapshot_date.isoformat(), **{f: getattr(r, f) for f in fields}} for r in rows],
    }


# ---------------------------------------------------------------------------------------
# EOD write
# ---------------------------------------------------------------------------------------
def build_eod_payload(db: Session, org_id: uuid.UUID, args: dict) -> Dict[str, Any]:
    """Validate + normalise the writable EOD fields. Raises ValueError with a readable message."""
    from app.models.client import Client

    payload: Dict[str, Any] = {}
    for f in _EOD_INT_FIELDS:
        if f in args and args[f] is not None:
            if isinstance(args[f], bool) or not isinstance(args[f], (int, float)) or int(args[f]) != args[f]:
                raise ValueError(f"{f} must be a whole number")
            if not 0 <= args[f] <= 1_000_000_000:
                raise ValueError(f"{f} must be between 0 and 1000000000")
            payload[f] = int(args[f])
    for f in _EOD_MONEY_FIELDS:
        if f in args and args[f] is not None:
            if isinstance(args[f], bool) or not isinstance(args[f], (int, float)) or not 0 <= args[f] <= 1_000_000_000:
                raise ValueError(f"{f} must be a number between 0 and 1000000000")
            payload[f] = Decimal(str(args[f])).quantize(Decimal("0.01"))
    if "content_posted" in args and args["content_posted"] is not None:
        if not isinstance(args["content_posted"], bool):
            raise ValueError("content_posted must be true or false")
        payload["content_posted"] = args["content_posted"]
    for f, cap in (("best_content_type", 512), ("setter_context", 5000)):
        if f in args and args[f] is not None:
            if not isinstance(args[f], str) or len(args[f]) > cap:
                raise ValueError(f"{f} must be text up to {cap} characters")
            payload[f] = args[f]
    if args.get("setter_booked_client_ids") is not None:
        ids = args["setter_booked_client_ids"]
        if not isinstance(ids, list) or len(ids) > 100:
            raise ValueError("setter_booked_client_ids must be a list of up to 100 client UUIDs")
        try:
            parsed = list(dict.fromkeys(uuid.UUID(str(i)) for i in ids))
        except ValueError as exc:
            raise ValueError("setter_booked_client_ids contains an invalid UUID") from exc
        if parsed:
            found = db.query(Client.id).filter(Client.org_id == org_id, Client.id.in_(parsed)).count()
            if found != len(parsed):
                raise ValueError("setter_booked_client_ids contains a client that is not in this org")
        payload["setter_booked_client_ids"] = [str(i) for i in parsed]
    return payload


def update_kpi_daily_entry(
    db: Session, org_id: uuid.UUID, args: dict, user_id: Optional[uuid.UUID]
) -> Dict[str, Any]:
    from app.api import kpi as kpi_api

    unknown = sorted(set(args) - set(_EOD_FIELDS) - {"entry_date", "rep_user_id", "mode"})
    if unknown:
        return {"error": "unknown_fields", "detail": f"Unknown field(s): {', '.join(unknown)}"}
    mode = args.get("mode") or "set"
    if mode not in ("set", "add"):
        return {"error": "invalid_input", "detail": "mode must be 'set' or 'add'"}
    try:
        entry_day = kpi_api._parse_date(str(args.get("entry_date") or ""))
        rep_uuid = kpi_api._validated_rep_user_id(db, org_id, _opt_str(args, "rep_user_id"))
        payload = build_eod_payload(db, org_id, args)
    except HTTPException as exc:
        return {"error": exc.detail}
    except ValueError as exc:
        return {"error": "invalid_input", "detail": str(exc)}
    if not payload:
        return {"error": "invalid_input", "detail": "Provide at least one field to write."}

    try:
        # mark_submitted=False on purpose: an agent write must not count as the rep's own
        # EOD submission (that flag drives the Team KPIs accountability view).
        row = kpi_api._upsert_kpi_entry_for_org(
            db=db,
            org_id=org_id,
            entry_day=entry_day,
            payload=payload,
            additive=(mode == "add"),
            rep_user_id=rep_uuid,
            mark_submitted=False,
        )
    except HTTPException as exc:
        db.rollback()
        return {"error": exc.detail}
    except Exception:
        logger.exception("update_kpi_daily_entry failed org=%s", org_id)
        db.rollback()
        return {"error": "internal error"}
    logger.info(
        "mcp eod write org=%s user=%s date=%s rep=%s mode=%s fields=%s",
        org_id, user_id, entry_day, rep_uuid, mode, sorted(payload),
    )
    return {"saved": True, "mode": mode, "rep_user_id": str(rep_uuid) if rep_uuid else None, "entry": row}


# ---------------------------------------------------------------------------------------
# dispatch
# ---------------------------------------------------------------------------------------
_MRR_KEYS = (
    "total_mrr", "total_arr", "mrr_change", "mrr_change_percent", "new_customers",
    "churned_subscriptions", "failed_payments", "active_subscriptions", "total_customers",
    "last_30_days_revenue", "average_client_ltv",
)


def _whop_recurring(db: Session, org_id: uuid.UUID) -> Optional[Dict[str, Any]]:
    """Estimated Whop MRR block, or None when Whop isn't connected (or the estimate fails)."""
    try:
        from app.api.whop import check_whop_connected
        from app.services.whop_recurring import estimate_whop_mrr

        if not check_whop_connected(db, org_id):
            return None
        return estimate_whop_mrr(db, org_id)
    except Exception:
        logger.exception("whop recurring estimate failed org=%s", org_id)
        db.rollback()
        return None


def run_extra_tool(
    name: str,
    args: dict,
    org_id: uuid.UUID,
    db: Session,
    *,
    user_id: Optional[uuid.UUID] = None,
    auth_ctx: Optional[dict] = None,
) -> dict:
    from app.mcp.server import _McpUser, _call_api, _dump, _require_funnel_write_access, _text_result

    user = _McpUser(user_id, org_id)
    try:
        if name == "get_finances_summary":
            from app.api import finances as fin

            return _text_result(_call_api(
                db, fin.finances_summary,
                range_days=_int(args, "range_days", 30, 1, 365), scope=_scope(args),
                start=_opt_str(args, "start"), end=_opt_str(args, "end"),
                compare_start=None, compare_end=None, current_user=user,
            ))
        if name == "get_revenue_timeline":
            from app.api import finances as fin

            return _text_result(_call_api(
                db, fin.finances_revenue_timeline,
                range_days=_int(args, "range_days", 30, 1, 365), scope=_scope(args),
                group_by="week" if args.get("group_by") == "week" else "day",
                start=_opt_str(args, "start"), end=_opt_str(args, "end"), current_user=user,
            ))
        if name == "get_mrr":
            from app.api import stripe as stripe_api

            summary = _call_api(
                db, stripe_api.get_stripe_summary,
                range_days=_int(args, "range_days", 30, 1, 365), scope=_scope(args),
                start=_opt_str(args, "start"), end=_opt_str(args, "end"),
                compare_start=None, compare_end=None, current_user=user,
            )
            whop = _whop_recurring(db, org_id)
            stripe_ok = isinstance(summary, dict) and "error" not in summary
            if not stripe_ok and whop is None:
                return _text_result(summary)  # neither source available: surface Stripe's error
            out: Dict[str, Any] = (
                {k: summary.get(k) for k in _MRR_KEYS} if stripe_ok else {"stripe": summary}
            )
            if whop is not None:
                out["whop"] = whop
                if stripe_ok:
                    out["combined_mrr"] = round(float(out.get("total_mrr") or 0) + whop["estimated_mrr"], 2)
                    out["combined_arr"] = round(float(out.get("total_arr") or 0) + whop["estimated_arr"], 2)
            if args.get("include_trend") and stripe_ok:
                group = args.get("group_by") if args.get("group_by") in ("day", "week", "month") else "week"
                out["trend"] = _call_api(
                    db, stripe_api.get_mrr_trend,
                    range_days=max(7, _int(args, "range_days", 90, 7, 365)), scope=_scope(args),
                    group_by=group, current_user=user,
                )
            return _text_result(out)
        if name == "list_payments":
            from app.api import stripe as stripe_api

            return _text_result(_call_api(
                db, stripe_api.get_payments,
                status_filter=_opt_str(args, "status"),
                range_days=_int(args, "range_days", 30, 1, 365) if not _scope(args) else None,
                scope=_scope(args), page=_int(args, "page", 1, 1, 100000),
                page_size=_int(args, "page_size", 25, 1, 100), use_treasury=True,
                start=_opt_str(args, "start"), end=_opt_str(args, "end"), current_user=user,
            ))
        if name == "list_subscriptions":
            from app.api import stripe as stripe_api

            return _text_result(_call_api(
                db, stripe_api.get_subscriptions,
                status_filter=_opt_str(args, "status"), search=_opt_str(args, "search"),
                page=_int(args, "page", 1, 1, 100000), page_size=_int(args, "page_size", 25, 1, 100),
                current_user=user,
            ))
        if name == "list_failed_payments":
            from app.api import stripe as stripe_api

            windowed = bool(args.get("start") or args.get("end") or args.get("scope") or args.get("range_days"))
            return _text_result(_call_api(
                db, stripe_api.get_failed_payments,
                page=_int(args, "page", 1, 1, 100000), page_size=_int(args, "page_size", 25, 1, 100),
                range_days=_int(args, "range_days", 30, 1, 365) if windowed and not _scope(args) else None,
                scope=_scope(args), use_treasury=True,
                exclude_resolved=args.get("exclude_resolved") is not False,
                start=_opt_str(args, "start"), end=_opt_str(args, "end"), current_user=user,
            ))

        if name in ("get_meta_ads_dashboard", "list_meta_ad_sets", "list_meta_ads", "get_meta_ads_alerts"):
            from app.api import meta_ads as ads

            days = _int(args, "days", 28, 7, 90)
            start, end = _opt_date(args, "start"), _opt_date(args, "end")
            if name == "get_meta_ads_dashboard":
                return _text_result(_call_api(
                    db, ads.ads_dashboard, days=days, start=start, end=end,
                    compare_start=_opt_date(args, "compare_start"), compare_end=_opt_date(args, "compare_end"),
                    funnel_id=_opt_str(args, "funnel_id"), current_user=user,
                ))
            if name == "list_meta_ad_sets":
                return _text_result(_call_api(
                    db, ads.list_ad_set_rows, days=days, start=start, end=end,
                    campaign_ids=_opt_str(args, "campaign_ids"), current_user=user,
                ))
            if name == "list_meta_ads":
                return _text_result(_call_api(
                    db, ads.list_ad_rows, days=days, start=start, end=end,
                    ad_set_ids=_opt_str(args, "ad_set_ids"), campaign_ids=_opt_str(args, "campaign_ids"),
                    current_user=user,
                ))
            return _text_result(_call_api(db, ads.list_ads_alerts, current_user=user))

        if name == "list_call_recordings":
            return _text_result(list_call_recordings(db, org_id, args))
        if name == "get_call_transcript":
            return _text_result(get_call_transcript(db, org_id, args))

        if name in ("list_instagram_posts", "get_instagram_post", "get_instagram_account_history", "transcribe_instagram_post"):
            denial = _gate_content_studio(db, org_id, user_id)
            if denial:
                return denial
            fn = {
                "list_instagram_posts": list_instagram_posts,
                "get_instagram_post": get_instagram_post,
                "get_instagram_account_history": get_instagram_account_history,
                "transcribe_instagram_post": transcribe_instagram_post,
            }[name]
            return _text_result(_dump(fn(db, org_id, args)))

        if name == "update_kpi_daily_entry":
            denial = _require_funnel_write_access(db, org_id, auth_ctx)
            if denial:
                return denial
            return _text_result(_dump(update_kpi_daily_entry(db, org_id, args, user_id)))
    except ValueError as exc:  # malformed uuid / date argument
        return _text_result({"error": "invalid_input", "detail": str(exc)})
    except Exception:
        logger.exception("MCP extra tool %s failed", name)
        db.rollback()
        return _text_result({"error": "internal error"})
    return _text_result({"error": f"Unknown tool: {name}"})

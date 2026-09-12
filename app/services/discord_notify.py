"""
Discord notification dispatch.

Architecture: ONE Discord Application (bot) is owned by Sweep OS (DISCORD_BOT_TOKEN
in .env). Each org runs its own OAuth install flow (see app/api/oauth.py discord/*
routes) to invite that bot into their own Discord server ("guild") and grant it
permission to view/send in specific channels. The org then maps Sweep event types
(eod_form, post_call, new_lead, new_booking, new_transaction) to one channel_id
each via discord_channel_mappings.

Sending always uses the shared bot token against the org's chosen channel_id —
never a per-user OAuth token (Discord bots, unlike Stripe/Brevo, don't send
messages "as" the connecting user's personal token).
"""
from __future__ import annotations

import logging
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import httpx
from sqlalchemy.orm import Session

from app.core.config import settings
from app.models.discord_channel_mapping import DiscordChannelMapping
from app.models.oauth_token import OAuthProvider, OAuthToken

LOG = logging.getLogger("app.discord_notify")


def format_org_local_datetime(db: Session, org_id: uuid.UUID, dt: datetime) -> str:
    """
    Render a UTC instant as "DD/MM/YYYY HH:MM TZ" in the org's configured
    timezone (organizations.timezone, default "UTC" — set via
    PATCH /organizations/{org_id}/timezone). Used for notification display
    only; all storage/comparison elsewhere in the app stays UTC.
    """
    from app.models.organization import Organization

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    tz_name = "UTC"
    try:
        org = db.query(Organization).filter(Organization.id == org_id).first()
        if org and org.timezone:
            tz_name = org.timezone
    except Exception:
        pass
    try:
        local_dt = dt.astimezone(ZoneInfo(tz_name))
    except ZoneInfoNotFoundError:
        local_dt = dt.astimezone(timezone.utc)
    return local_dt.strftime("%d/%m/%Y %H:%M %Z")

DISCORD_API_BASE = "https://discord.com/api/v10"

# One shared, connection-pooled client for every org's outbound Discord call.
# Avoids a fresh TCP+TLS handshake per notification when many orgs send
# concurrently, and gives httpx a bounded pool instead of unbounded ad-hoc
# connections under load. httpx.Client is safe for concurrent use across threads.
_client_lock = threading.Lock()
_shared_client: Optional[httpx.Client] = None


def _get_client() -> httpx.Client:
    global _shared_client
    if _shared_client is None:
        with _client_lock:
            if _shared_client is None:
                _shared_client = httpx.Client(
                    timeout=10.0,
                    limits=httpx.Limits(max_connections=20, max_keepalive_connections=10),
                )
    return _shared_client

# Known event types surfaced in the UI. send_discord_event() accepts any string,
# but the channel-mapping picker only offers these.
EVENT_TYPES: List[Dict[str, str]] = [
    {"key": "eod_form", "label": "EOD / daily KPI form submissions"},
    {"key": "post_call", "label": "Post-call form submissions"},
    {"key": "new_lead", "label": "New leads (funnel captures)"},
    {"key": "new_booking", "label": "New bookings (Cal.com / Calendly)"},
    {"key": "new_transaction", "label": "New transactions (Stripe payments)"},
]
_KNOWN_EVENT_KEYS = {e["key"] for e in EVENT_TYPES}


def build_sample_event(
    event_type: str,
    *,
    db: Optional[Session] = None,
    org_id: Optional[uuid.UUID] = None,
) -> Dict[str, Any]:
    """
    Realistic fake payload per event_type, mirroring the exact title/description/
    fields shape each real caller sends (kpi.py, call_insight_service.py,
    funnel_lead_notifications.py, calendar_webhooks.py, stripe_processor.py /
    webhooks.py) so "Send test" shows admins what production messages actually
    look like, not a generic placeholder.

    Pass db + org_id to render the new_booking sample's "When" field through
    the org's real configured timezone (same as a live booking notification)
    instead of a raw UTC fallback.
    """
    today = datetime.utcnow().date().isoformat()
    if db is not None and org_id is not None:
        sample_when = format_org_local_datetime(db, org_id, datetime.now(timezone.utc))
    else:
        sample_when = datetime.now(timezone.utc).strftime("%d/%m/%Y %H:%M UTC")

    samples: Dict[str, Dict[str, Any]] = {
        "eod_form": {
            "title": f"[TEST] EOD form submitted — {today}",
            "description": "Submitted by: Jordan (sample rep)",
            "fields": [
                ("Outreach Sent", "42"),
                ("Respondents", "11"),
                ("Calls Booked", "3"),
                ("Cash Collected", "1500.0"),
            ],
        },
        "post_call": {
            "title": "[TEST] Post-call form submitted — Alex Example",
            "description": "Great energy on the call, ready to start next week.",
            "fields": [
                ("Outcome", "Closed"),
                ("Payment source", "stripe"),
                ("Cash collected", "$997.00"),
                ("Offer", "12-Week Coaching"),
                ("Closer", "Jordan Sample"),
                ("Lead source", "Instagram DMs"),
            ],
        },
        "new_lead": {
            "title": "[TEST] New lead: Jordan Sample",
            "description": "New client",
            "fields": [
                ("Email", "jordan@example.com"),
                ("Phone", "+1 555 0100"),
                ("Funnel", "Sample Funnel"),
                ("Source", "quiz"),
            ],
        },
        "new_booking": {
            "title": "[TEST] New booking: Alex Example",
            "description": "Strategy Call",
            "fields": [
                ("Provider", "Calendly"),
                ("When", sample_when),
                ("Email", "alex@example.com"),
            ],
        },
        "new_transaction": {
            "title": "[TEST] New transaction: $497.00 USD",
            "description": "Alex Example",
            "fields": [("Event", "charge.succeeded"), ("Payment ID", "ch_sample123")],
        },
    }
    return samples.get(
        event_type,
        {
            "title": "[TEST] Sample Sweep OS notification",
            "description": "If you can see this, this channel is wired up correctly.",
            "fields": [],
        },
    )

# Text (0) and announcement (5) channels are the only sendable types for this feature.
_SENDABLE_CHANNEL_TYPES = {0, 5}

_EMBED_COLOR_BY_EVENT = {
    "eod_form": 0x5865F2,       # Discord blurple
    "post_call": 0x22C55E,      # green
    "new_lead": 0xF59E0B,       # amber
    "new_booking": 0x8B5CF6,    # violet
    "new_transaction": 0x10B981,  # emerald
}
_DEFAULT_EMBED_COLOR = 0x9CA3AF  # gray, for any unlisted event_type


def is_event_type_known(event_type: str) -> bool:
    return event_type in _KNOWN_EVENT_KEYS


def bot_configured() -> bool:
    return bool(getattr(settings, "DISCORD_BOT_TOKEN", None))


def oauth_configured() -> bool:
    return bool(getattr(settings, "DISCORD_CLIENT_ID", None) and getattr(settings, "DISCORD_CLIENT_SECRET", None))


def _bot_headers() -> Dict[str, str]:
    return {
        "Authorization": f"Bot {settings.DISCORD_BOT_TOKEN}",
        "Content-Type": "application/json",
    }


def get_discord_token_row(db: Session, org_id: uuid.UUID) -> Optional[OAuthToken]:
    return (
        db.query(OAuthToken)
        .filter(OAuthToken.provider == OAuthProvider.DISCORD, OAuthToken.org_id == org_id)
        .first()
    )


def get_guild_id_for_org(db: Session, org_id: uuid.UUID) -> Optional[str]:
    row = get_discord_token_row(db, org_id)
    return row.account_id if row else None


def fetch_guild_name(guild_id: str) -> Optional[str]:
    if not bot_configured():
        return None
    try:
        resp = _get_client().get(f"{DISCORD_API_BASE}/guilds/{guild_id}", headers=_bot_headers())
        if resp.status_code == 200:
            return resp.json().get("name")
    except httpx.HTTPError:
        LOG.warning("discord fetch_guild_name failed for guild %s", guild_id, exc_info=True)
    return None


def list_guild_channels(guild_id: str) -> Tuple[bool, List[Dict[str, Any]], Optional[str]]:
    """
    Returns (ok, channels, error). channels: [{id, name, type, position}], text/announcement only.
    """
    if not bot_configured():
        return False, [], "DISCORD_BOT_TOKEN is not configured on the server"
    try:
        resp = _get_client().get(
            f"{DISCORD_API_BASE}/guilds/{guild_id}/channels",
            headers=_bot_headers(),
        )
    except httpx.HTTPError as e:
        return False, [], f"network error: {e}"

    if resp.status_code == 403:
        return False, [], "Bot no longer has access to this server (kicked, or missing permissions)"
    if resp.status_code == 404:
        return False, [], "Server not found (bot may have been removed)"
    if resp.status_code != 200:
        return False, [], f"Discord API error: HTTP {resp.status_code}"

    raw = resp.json() if isinstance(resp.json(), list) else []
    channels = [
        {"id": c["id"], "name": c.get("name") or c["id"], "type": c.get("type"), "position": c.get("position", 0)}
        for c in raw
        if isinstance(c, dict) and c.get("type") in _SENDABLE_CHANNEL_TYPES
    ]
    channels.sort(key=lambda c: c["position"])
    return True, channels, None


def list_channel_mappings(db: Session, org_id: uuid.UUID) -> List[DiscordChannelMapping]:
    return (
        db.query(DiscordChannelMapping)
        .filter(DiscordChannelMapping.org_id == org_id)
        .all()
    )


def get_channel_mapping(db: Session, org_id: uuid.UUID, event_type: str) -> Optional[DiscordChannelMapping]:
    return (
        db.query(DiscordChannelMapping)
        .filter(DiscordChannelMapping.org_id == org_id, DiscordChannelMapping.event_type == event_type)
        .first()
    )


def set_channel_mapping(
    db: Session,
    org_id: uuid.UUID,
    event_type: str,
    channel_id: str,
    channel_name: Optional[str] = None,
) -> DiscordChannelMapping:
    row = get_channel_mapping(db, org_id, event_type)
    if row:
        row.channel_id = channel_id
        row.channel_name = channel_name
    else:
        row = DiscordChannelMapping(
            org_id=org_id, event_type=event_type, channel_id=channel_id, channel_name=channel_name
        )
        db.add(row)
    db.commit()
    db.refresh(row)
    return row


def delete_channel_mapping(db: Session, org_id: uuid.UUID, event_type: str) -> bool:
    row = get_channel_mapping(db, org_id, event_type)
    if not row:
        return False
    db.delete(row)
    db.commit()
    return True


def send_discord_event(
    db: Session,
    org_id: uuid.UUID,
    event_type: str,
    *,
    title: str,
    description: Optional[str] = None,
    fields: Optional[Sequence[Tuple[str, str]]] = None,
    url: Optional[str] = None,
) -> Tuple[bool, Optional[str]]:
    """
    Best-effort send: never raises. Returns (sent, skip_or_error_reason).
    Callers (KPI entry submit, call-insight completion, etc.) should treat a
    False return as a no-op, not a failure of the underlying operation.
    """
    if not bot_configured():
        return False, "bot not configured"

    mapping = get_channel_mapping(db, org_id, event_type)
    if not mapping:
        return False, "no channel mapped for this event type"

    embed: Dict[str, Any] = {
        "title": title[:256],
        "color": _EMBED_COLOR_BY_EVENT.get(event_type, _DEFAULT_EMBED_COLOR),
    }
    if description:
        embed["description"] = description[:4000]
    if url:
        embed["url"] = url
    if fields:
        embed["fields"] = [
            {"name": str(name)[:256], "value": str(value)[:1024] or "​", "inline": True}
            for name, value in list(fields)[:25]
        ]

    url_path = f"{DISCORD_API_BASE}/channels/{mapping.channel_id}/messages"
    body = {"embeds": [embed]}

    def _post() -> httpx.Response:
        return _get_client().post(url_path, headers=_bot_headers(), json=body)

    try:
        resp = _post()
    except httpx.HTTPError as e:
        LOG.warning("discord send failed org=%s event=%s: %s", org_id, event_type, e)
        return False, f"network error: {e}"

    # Discord's bot token is shared across every org — under concurrent load from
    # many orgs firing notifications at once, its global/per-route rate limit can
    # be hit even though each org targets its own channel. Honor Retry-After with
    # one bounded retry instead of silently dropping the notification.
    if resp.status_code == 429:
        try:
            retry_after = float(resp.json().get("retry_after", 1))
        except Exception:
            retry_after = 1.0
        LOG.warning(
            "discord rate limited org=%s event=%s retry_after=%.2fs", org_id, event_type, retry_after
        )
        time.sleep(min(max(retry_after, 0.1), 5.0))
        try:
            resp = _post()
        except httpx.HTTPError as e:
            LOG.warning("discord send retry failed org=%s event=%s: %s", org_id, event_type, e)
            return False, f"network error on retry: {e}"

    if resp.status_code not in (200, 201):
        LOG.warning(
            "discord send non-2xx org=%s event=%s status=%s body=%s",
            org_id, event_type, resp.status_code, resp.text[:500],
        )
        return False, f"HTTP {resp.status_code}"

    return True, None


def send_discord_event_background(
    org_id: uuid.UUID,
    event_type: str,
    *,
    title: str,
    description: Optional[str] = None,
    fields: Optional[Sequence[Tuple[str, str]]] = None,
    url: Optional[str] = None,
) -> None:
    """
    Fire-and-forget wrapper for use from request/webhook handlers. Opens its own
    DB session in a background thread so a slow or rate-limited Discord call never
    blocks the caller's response (Stripe/Whop/Calendly/Cal.com webhooks have their
    own delivery timeouts) and never holds the caller's pooled DB connection open
    while waiting on network I/O. Prefer this over send_discord_event() from any
    request-handling code path; call send_discord_event() directly only from
    non-request contexts (e.g. the /discord/test endpoint, which wants the result).
    """

    def _run() -> None:
        from app.db.session import SessionLocal

        bg_db = SessionLocal()
        try:
            sent, reason = send_discord_event(
                bg_db, org_id, event_type, title=title, description=description, fields=fields, url=url,
            )
            if not sent and reason not in ("bot not configured", "no channel mapped for this event type"):
                LOG.warning(
                    "discord background send failed org=%s event=%s reason=%s", org_id, event_type, reason
                )
        except Exception:
            LOG.exception("discord background send crashed org=%s event=%s", org_id, event_type)
        finally:
            bg_db.close()

    threading.Thread(target=_run, daemon=True, name=f"discord-notify-{event_type}").start()

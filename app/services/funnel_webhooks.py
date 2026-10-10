"""Custom funnel webhooks: any opt-in tool (ClickFunnels, Kajabi, Typeform, Zapier,
Make, n8n, a hand-rolled form...) posts leads to a per-funnel secret URL:

    {BACKEND_PUBLIC_URL}/webhooks/funnels/{token}

The token is the credential (most form tools can't sign a body or add headers),
so it is 32 random bytes, stored only as a sha256 lookup hash plus a Fernet copy
for re-displaying the URL to admins. Rotating it revokes the old URL.

Pipeline, built for bursts across many orgs at once:

1. Ingest (request path, a few ms): token -> funnel via a short TTL cache, a
   per-funnel rate limit, parse + normalize the body, then one
   INSERT ... ON CONFLICT DO NOTHING into inbound_webhook_events and a 202.
   No client/lead work happens before the sender gets its answer.
2. Process: the worker's drainer threads (drain_due) claim due rows with
   FOR UPDATE SKIP LOCKED and run them through upsert_funnel_lead (the same
   path as POST /funnels/leads). SKIP LOCKED lets any number of threads and
   worker instances drain in parallel without double-processing.
   FUNNEL_WEBHOOK_INLINE_PROCESSING=true instead processes on the web process
   right after the 202 (for deployments without a worker); the row then gets a
   grace lease so the drainer leaves it alone meanwhile.
3. Retry: failures back off exponentially (inbound_webhook_inbox.mark_inbound_retry)
   and are retried by the drainer, then marked failed and shown in the
   funnel's delivery log for a manual retry. A claimed row whose process dies
   is re-claimed when its lease expires.

Duplicates: the sender's idempotency id (header or body), else a hash of the
body, keys the inbox row per funnel, so a retried delivery is acked as a
duplicate. Concurrent deliveries for the same person are serialized with a
transaction advisory lock so they can't create two clients.
"""
from __future__ import annotations

import hashlib
import json
import logging
import re
import secrets
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Mapping, Optional, Tuple
from urllib.parse import parse_qsl

from sqlalchemy import func, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session
from sqlalchemy.orm.attributes import flag_modified

from app.core.config import settings
from app.models.funnel import Funnel
from app.models.inbound_webhook_event import InboundWebhookEvent
from app.schemas.funnel import MAX_PROSPECT_BYTES, FunnelLeadIn

LOG = logging.getLogger(__name__)

PROVIDER = "funnel_webhook"
TOKEN_PREFIX = "swh_"
_TOKEN_RE = re.compile(r"^swh_[A-Za-z0-9_-]{20,80}$")

# Inline mode: fresh rows wait this long before the drainer may pick them up.
INLINE_GRACE_SEC = 60
# A claimed row whose processor dies is retried by the worker after this lease.
PROCESS_LEASE_SEC = 300

# Fields a field_map may target, beyond the five UTM keys.
LEAD_FIELDS = ("email", "phone", "name", "first_name", "last_name", "instagram", "notes", "visitor_id", "session_id")
UTM_FIELDS = ("utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content")
MAP_TARGETS = LEAD_FIELDS + UTM_FIELDS
MAX_MAP_PATH = 200
IDENTITY_FIELDS = ("email", "phone", "name", "first_name", "last_name", "instagram")

# Auto-detect aliases, compared against a key's last segment lowercased with
# non-alphanumerics removed ("Email Address" -> "emailaddress").
_ALIASES: Dict[str, Tuple[str, ...]] = {
    "email": ("email", "emailaddress", "contactemail", "youremail", "useremail", "workemail", "mail"),
    "phone": (
        "phone", "phonenumber", "mobile", "mobilephone", "mobilenumber", "cell", "cellphone",
        "tel", "telephone", "whatsapp", "whatsappnumber", "contactphone", "yourphone",
    ),
    "first_name": ("firstname", "fname", "givenname", "first"),
    "last_name": ("lastname", "lname", "surname", "familyname", "last"),
    "name": ("name", "fullname", "yourname", "contactname"),
    "instagram": ("instagram", "instagramhandle", "instagramusername", "ig", "ighandle", "igusername"),
    "visitor_id": ("visitorid", "sweepvisitorid"),
    "session_id": ("sessionid", "sweepsessionid"),
    "utm_source": ("utmsource",),
    "utm_medium": ("utmmedium",),
    "utm_campaign": ("utmcampaign",),
    "utm_term": ("utmterm",),
    "utm_content": ("utmcontent",),
}
_ALIAS_LOOKUP = {alias: target for target, aliases in _ALIASES.items() for alias in aliases}

_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
_DEDUPE_HEADERS = ("idempotency-key", "x-idempotency-key", "webhook-id", "x-webhook-id", "x-event-id")
_DEDUPE_BODY_KEYS = ("event_id", "submission_id", "entry_id", "response_id", "webhook_id", "idempotency_key")

_MAX_FLAT_KEYS = 300
_MAX_DEPTH = 6
_MAX_VALUE_CHARS = 2000
# Headroom under FunnelLeadIn's MAX_PROSPECT_BYTES for the JSON wrapper.
_OPT_IN_BUDGET = MAX_PROSPECT_BYTES - 2000


class WebhookPayloadError(ValueError):
    """Body can't become a lead; message is safe to return to the sender."""


# ---------------------------------------------------------------------------
# Tokens
# ---------------------------------------------------------------------------

def generate_token() -> str:
    return TOKEN_PREFIX + secrets.token_urlsafe(32)


def hash_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def webhook_url(token: str, base_url: Optional[str] = None) -> str:
    base = (settings.BACKEND_PUBLIC_URL or base_url or "").rstrip("/")
    return f"{base}/webhooks/funnels/{token}"


def enable_or_rotate(db: Session, funnel: Funnel) -> str:
    """Issue a new token for the funnel (revoking any previous one). Returns the token. Commits."""
    from app.core.encryption import encrypt_token

    token = generate_token()
    old_hash = funnel.webhook_token_hash
    cfg = dict(funnel.webhook_config or {})
    cfg.update(
        {
            "token_enc": encrypt_token(token),
            "token_prefix": token[:10],
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
    )
    funnel.webhook_config = cfg
    flag_modified(funnel, "webhook_config")
    funnel.webhook_token_hash = hash_token(token)
    db.commit()
    db.refresh(funnel)
    if old_hash:
        _cache_drop(old_hash)
    return token


def disable(db: Session, funnel: Funnel) -> None:
    """Revoke the URL; the field map is kept for re-enabling. Commits."""
    old_hash = funnel.webhook_token_hash
    cfg = dict(funnel.webhook_config or {})
    cfg.pop("token_enc", None)
    cfg.pop("token_prefix", None)
    funnel.webhook_config = cfg
    flag_modified(funnel, "webhook_config")
    funnel.webhook_token_hash = None
    db.commit()
    if old_hash:
        _cache_drop(old_hash)


def current_token(funnel: Funnel) -> Optional[str]:
    from app.core.encryption import decrypt_token

    enc = (funnel.webhook_config or {}).get("token_enc")
    if not funnel.webhook_token_hash or not enc:
        return None
    try:
        return decrypt_token(enc)
    except Exception:
        LOG.warning("funnel webhook token decrypt failed funnel=%s", funnel.id)
        return None


def set_field_map(db: Session, funnel: Funnel, field_map: Mapping[str, Any]) -> Dict[str, str]:
    """Validate + store {target: path}. Empty paths drop the target. Commits."""
    clean: Dict[str, str] = {}
    for target, path in (field_map or {}).items():
        if target not in MAP_TARGETS:
            raise WebhookPayloadError(f"Unknown field '{target}'. Allowed: {', '.join(MAP_TARGETS)}")
        p = str(path or "").strip()
        if not p:
            continue
        if len(p) > MAX_MAP_PATH:
            raise WebhookPayloadError(f"Path for '{target}' is too long")
        clean[target] = p
    cfg = dict(funnel.webhook_config or {})
    cfg["field_map"] = clean
    funnel.webhook_config = cfg
    flag_modified(funnel, "webhook_config")
    db.commit()
    if funnel.webhook_token_hash:
        _cache_drop(funnel.webhook_token_hash)
    return clean


# ---------------------------------------------------------------------------
# Token -> funnel cache (per process). Rotation/disable reaches other processes
# within _CACHE_TTL_SEC; unknown tokens are cached briefly so a flood of bad
# requests doesn't become a flood of queries.
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class WebhookTarget:
    funnel_id: uuid.UUID
    org_id: uuid.UUID
    field_map: Dict[str, str] = field(default_factory=dict)


_CACHE_TTL_SEC = 60.0
_NEG_CACHE_TTL_SEC = 10.0
_CACHE_MAX = 10000
_cache: Dict[str, Tuple[float, Optional[WebhookTarget]]] = {}
_cache_lock = threading.Lock()


def _cache_get(key: str) -> Tuple[bool, Optional[WebhookTarget]]:
    with _cache_lock:
        hit = _cache.get(key)
        if hit is None:
            return False, None
        expires, target = hit
        if expires < time.monotonic():
            _cache.pop(key, None)
            return False, None
        return True, target


def _cache_put(key: str, target: Optional[WebhookTarget]) -> None:
    ttl = _CACHE_TTL_SEC if target is not None else _NEG_CACHE_TTL_SEC
    with _cache_lock:
        if len(_cache) >= _CACHE_MAX:
            _cache.clear()
        _cache[key] = (time.monotonic() + ttl, target)


def _cache_drop(key: str) -> None:
    with _cache_lock:
        _cache.pop(key, None)


def resolve_target(db: Session, token: str) -> Optional[WebhookTarget]:
    if not token or not _TOKEN_RE.match(token):
        return None
    key = hash_token(token)
    found, target = _cache_get(key)
    if found:
        return target
    row = (
        db.query(Funnel.id, Funnel.org_id, Funnel.webhook_config)
        .filter(Funnel.webhook_token_hash == key)
        .first()
    )
    target = None
    if row is not None:
        fmap = (row.webhook_config or {}).get("field_map") or {}
        target = WebhookTarget(funnel_id=row.id, org_id=row.org_id, field_map=dict(fmap))
    _cache_put(key, target)
    return target


def try_acquire_rate(target: WebhookTarget) -> bool:
    from app.core.rate_limit import sliding_window_try_acquire

    limit = int(getattr(settings, "FUNNEL_WEBHOOK_RATE_LIMIT_PER_MINUTE", 600) or 0)
    return sliding_window_try_acquire(f"fwh:{target.funnel_id}", limit, 60)


def try_acquire_bad_token(ip: str) -> bool:
    """Per-IP budget for unknown tokens (URL guessing, misconfigured senders)."""
    from app.core.rate_limit import sliding_window_try_acquire

    return sliding_window_try_acquire(f"fwh-bad:{ip or 'unknown'}", 60, 60)


# ---------------------------------------------------------------------------
# Parsing + normalization
# ---------------------------------------------------------------------------

def parse_body(content_type: Optional[str], raw: bytes) -> Dict[str, Any]:
    """JSON object or form-urlencoded body -> dict."""
    ctype = (content_type or "").split(";")[0].strip().lower()
    if ctype.startswith("multipart/"):
        raise WebhookPayloadError("multipart/form-data is not supported; send JSON or form-urlencoded")
    text_body = raw.decode("utf-8", errors="replace").strip()
    if not text_body:
        raise WebhookPayloadError("Empty body")
    looks_json = text_body.startswith(("{", "["))
    if ctype == "application/x-www-form-urlencoded" or (ctype != "application/json" and not looks_json):
        pairs = parse_qsl(text_body, keep_blank_values=False)
        if not pairs:
            raise WebhookPayloadError("Could not parse body; send JSON or form-urlencoded")
        out: Dict[str, Any] = {}
        for k, v in pairs:
            if k in out:
                prev = out[k]
                out[k] = prev + [v] if isinstance(prev, list) else [prev, v]
            else:
                out[k] = v
        return out
    try:
        body = json.loads(text_body)
    except ValueError as exc:
        raise WebhookPayloadError("Invalid JSON") from exc
    if isinstance(body, list) and len(body) == 1 and isinstance(body[0], dict):
        body = body[0]  # Make / n8n often wrap a single item in an array
    if not isinstance(body, dict):
        raise WebhookPayloadError("Expected a JSON object")
    return body


def _label_value_list(items: List[Any]) -> Optional[Dict[str, Any]]:
    """[{"name"|"label"|"key"|...: k, "value": v}, ...] -> {k: v} (Webflow / Elementor style)."""
    out: Dict[str, Any] = {}
    for item in items:
        if not isinstance(item, dict) or "value" not in item:
            return None
        label = next((item.get(k) for k in ("name", "label", "key", "field", "title", "id") if item.get(k)), None)
        if label is None:
            return None
        out[str(label)] = item["value"]
    return out or None


def flatten(payload: Any) -> List[Tuple[str, Any]]:
    """Dotted (path, scalar) pairs, shallowest first. Lists of scalars become ", "-joined text."""
    out: List[Tuple[str, Any]] = []

    def walk(node: Any, prefix: str, depth: int) -> None:
        if len(out) >= _MAX_FLAT_KEYS or depth > _MAX_DEPTH:
            return
        if isinstance(node, dict):
            for k, v in node.items():
                walk(v, f"{prefix}.{k}" if prefix else str(k), depth + 1)
            return
        if isinstance(node, list):
            as_map = _label_value_list(node)
            if as_map is not None:
                walk(as_map, prefix, depth)
                return
            if all(not isinstance(x, (dict, list)) for x in node):
                joined = ", ".join(str(x) for x in node if x is not None and str(x).strip())
                if joined and prefix:
                    out.append((prefix, joined))
                return
            for i, v in enumerate(node):
                walk(v, f"{prefix}.{i}" if prefix else str(i), depth + 1)
            return
        if node is None or not prefix or (isinstance(node, str) and not node.strip()):
            return
        out.append((prefix, node))

    walk(payload, "", 0)
    out.sort(key=lambda kv: kv[0].count("."))  # stable: same depth keeps body order
    return out


def _norm_key(path: str) -> str:
    return re.sub(r"[^a-z0-9]", "", path.rsplit(".", 1)[-1].lower())


def resolve_path(payload: Any, path: str, flat: Mapping[str, Any]) -> Any:
    """Dotted path into the raw payload (list indices allowed); falls back to an exact flattened key."""
    node = payload
    for part in path.split("."):
        if isinstance(node, dict) and part in node:
            node = node[part]
        elif isinstance(node, list) and part.isdigit() and int(part) < len(node):
            node = node[int(part)]
        else:
            node = None
            break
    if node is None:
        node = flat.get(path)
    return None if isinstance(node, (dict, list)) else node


def _clean_value(target: str, value: Any) -> Optional[str]:
    if value is None:
        return None
    s = str(value).strip()[:500]
    if not s:
        return None
    if target == "email":
        s = s.lower()
        return s if _EMAIL_RE.match(s) else None
    if target == "phone":
        return s if sum(ch.isdigit() for ch in s) >= 7 else None
    return s


@dataclass
class NormalizedLead:
    fields: Dict[str, str]
    utm: Dict[str, str]
    opt_in_data: Dict[str, Any]

    @property
    def has_identity(self) -> bool:
        return any(self.fields.get(k) for k in IDENTITY_FIELDS)

    def detected(self) -> Dict[str, bool]:
        return {k: bool(self.fields.get(k)) for k in IDENTITY_FIELDS}

    def to_payload(self) -> Dict[str, Any]:
        return {"fields": self.fields, "utm": self.utm, "opt_in_data": self.opt_in_data}


def normalize_payload(payload: Dict[str, Any], field_map: Optional[Mapping[str, str]] = None) -> NormalizedLead:
    """Explicit field_map first, then alias auto-detect. Everything else lands in opt_in_data."""
    flat_pairs = flatten(payload)
    flat = dict(flat_pairs)
    found: Dict[str, str] = {}
    used_paths = set()

    for target, path in (field_map or {}).items():
        if target not in MAP_TARGETS:
            continue
        val = _clean_value(target, resolve_path(payload, path, flat))
        if val:
            found[target] = val
            used_paths.add(path)

    for path, value in flat_pairs:
        target = _ALIAS_LOOKUP.get(_norm_key(path))
        if target is None or target in found:
            continue
        val = _clean_value(target, value)
        if val:
            found[target] = val
            used_paths.add(path)

    fields = {k: v for k, v in found.items() if k in LEAD_FIELDS}
    utm = {k[len("utm_"):]: v[:200] for k, v in found.items() if k in UTM_FIELDS}

    opt_in: Dict[str, Any] = {}
    budget = _OPT_IN_BUDGET
    for path, value in flat_pairs:
        if path in used_paths:
            continue
        v = value if isinstance(value, (int, float, bool)) else str(value)[:_MAX_VALUE_CHARS]
        cost = len(path) + len(str(v)) + 8
        if cost > budget:
            break
        opt_in[path] = v
        budget -= cost
    return NormalizedLead(fields=fields, utm=utm, opt_in_data=opt_in)


def dedupe_key(headers: Mapping[str, str], payload: Dict[str, Any]) -> str:
    """Sender idempotency id (header, then body), else a hash of the canonical body."""
    for h in _DEDUPE_HEADERS:
        v = (headers.get(h) or "").strip()
        if v:
            return f"h:{v[:200]}"
    for k in _DEDUPE_BODY_KEYS:
        v = payload.get(k)
        if v is not None and str(v).strip():
            return f"b:{str(v).strip()[:200]}"
    canonical = json.dumps(payload, sort_keys=True, default=str, separators=(",", ":"))
    return "s:" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Inbox
# ---------------------------------------------------------------------------

def _event_id(funnel_id: uuid.UUID, key: str) -> str:
    return f"{funnel_id}:{key}"[:255]


def enqueue_delivery(
    db: Session,
    target: WebhookTarget,
    lead: NormalizedLead,
    raw: Dict[str, Any],
    key: str,
) -> Tuple[Optional[uuid.UUID], bool]:
    """One INSERT ... ON CONFLICT DO NOTHING. Returns (row_id, duplicate). Commits."""
    now = datetime.utcnow()
    grace = INLINE_GRACE_SEC if inline_enabled() else 0
    stmt = (
        pg_insert(InboundWebhookEvent)
        .values(
            id=uuid.uuid4(),
            org_id=target.org_id,
            provider=PROVIDER,
            event_id=_event_id(target.funnel_id, key),
            event_type="opt_in",
            payload={
                "funnel_id": str(target.funnel_id),
                "received_at": datetime.now(timezone.utc).isoformat(),
                "lead": lead.to_payload(),
                "raw": raw,
            },
            status="pending",
            attempts=0,
            next_attempt_at=now + timedelta(seconds=grace),
            received_at=now,
            updated_at=now,
        )
        .on_conflict_do_nothing(constraint="uq_inbound_webhook_org_provider_event")
        .returning(InboundWebhookEvent.id)
    )
    inserted = db.execute(stmt).scalar_one_or_none()
    db.commit()
    return inserted, inserted is None


def _parse_dt(value: Any) -> Optional[datetime]:
    if not isinstance(value, str):
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _identity_lock_key(org_id: uuid.UUID, fields: Mapping[str, str]) -> Optional[str]:
    if fields.get("email"):
        return f"funnel-lead:{org_id}:e:{fields['email'].lower()}"
    digits = "".join(ch for ch in fields.get("phone") or "" if ch.isdigit())
    if digits:
        return f"funnel-lead:{org_id}:p:{digits[-10:]}"
    return None


def process_funnel_webhook_payload(db: Session, org_id: uuid.UUID, payload: Dict[str, Any]) -> None:
    """Inbox processor (inline and worker retries). Commits via upsert_funnel_lead."""
    from app.api.funnels import _resolve_lead_utm
    from app.services.funnel_leads import upsert_funnel_lead

    try:
        funnel_id = uuid.UUID(str(payload.get("funnel_id")))
    except (TypeError, ValueError):
        LOG.warning("funnel webhook row without funnel_id org=%s; dropping", org_id)
        return
    funnel = db.query(Funnel).filter(Funnel.id == funnel_id, Funnel.org_id == org_id).first()
    if funnel is None:  # deleted since the delivery arrived
        return
    norm = payload.get("lead") or {}
    fields = dict(norm.get("fields") or {})
    lead = FunnelLeadIn(
        funnel_id=funnel.id,
        source="webhook",
        opt_in_data=norm.get("opt_in_data") or None,
        utm=norm.get("utm") or None,
        **{k: fields.get(k) for k in LEAD_FIELDS},
    )
    received_at = _parse_dt(payload.get("received_at")) or datetime.now(timezone.utc)

    lock_key = _identity_lock_key(org_id, fields)
    if lock_key:
        # Held until upsert_funnel_lead's first commit, i.e. across find-or-create,
        # so two deliveries for one person can't both create a client.
        db.execute(text("SELECT pg_advisory_xact_lock(hashtextextended(:k, 0))"), {"k": lock_key})
    upsert_funnel_lead(
        db,
        funnel,
        lead,
        utm=_resolve_lead_utm(db, org_id, lead),
        opted_in_at=received_at,
        reattribute=True,
    )


def inline_enabled() -> bool:
    return bool(getattr(settings, "FUNNEL_WEBHOOK_INLINE_PROCESSING", False))


def _process_claimed(db: Session, row_id: uuid.UUID) -> bool:
    """Process a row this caller has already claimed (status=processing + lease)."""
    from app.services.inbound_webhook_inbox import mark_inbound_done, mark_inbound_retry

    row = db.query(InboundWebhookEvent).filter(InboundWebhookEvent.id == row_id).first()
    if row is None:
        return False
    org_id = row.org_id
    try:
        process_funnel_webhook_payload(db, org_id, row.payload if isinstance(row.payload, dict) else {})
        mark_inbound_done(db, row)
        return True
    except Exception as exc:
        LOG.exception("funnel webhook processing failed row=%s org=%s", row_id, org_id)
        try:
            db.rollback()
        except Exception:
            pass
        fresh = db.query(InboundWebhookEvent).filter(InboundWebhookEvent.id == row_id).first()
        if fresh is not None:
            mark_inbound_retry(db, fresh, str(exc))
        return False


def process_delivery_now(row_id: uuid.UUID) -> None:
    """Inline mode / manual retry: claim one pending row and process it."""
    from app.db.session import SessionLocal

    with SessionLocal() as db:
        now = datetime.utcnow()
        claimed = (
            db.query(InboundWebhookEvent)
            .filter(InboundWebhookEvent.id == row_id, InboundWebhookEvent.status == "pending")
            .update(
                {
                    "status": "processing",
                    "next_attempt_at": now + timedelta(seconds=PROCESS_LEASE_SEC),
                    "updated_at": now,
                },
                synchronize_session=False,
            )
        )
        db.commit()
        if claimed:  # else the drainer already has it
            _process_claimed(db, row_id)


def drain_due(db: Session, *, limit: int = 25) -> int:
    """Worker drainer: claim up to `limit` due rows (SKIP LOCKED) and process them. Returns rows claimed."""
    now = datetime.utcnow()
    rows = (
        db.query(InboundWebhookEvent)
        .filter(
            InboundWebhookEvent.provider == PROVIDER,
            InboundWebhookEvent.status.in_(("pending", "processing")),
            InboundWebhookEvent.next_attempt_at <= now,
        )
        .order_by(InboundWebhookEvent.next_attempt_at.asc())
        .limit(limit)
        .with_for_update(skip_locked=True)
        .all()
    )
    ids = []
    for row in rows:
        row.status = "processing"
        row.next_attempt_at = now + timedelta(seconds=PROCESS_LEASE_SEC)
        row.updated_at = now
        ids.append(row.id)
    db.commit()
    for row_id in ids:
        _process_claimed(db, row_id)
    return len(ids)


def _deliveries_query(db: Session, org_id: uuid.UUID, funnel_id: uuid.UUID):
    return db.query(InboundWebhookEvent).filter(
        InboundWebhookEvent.org_id == org_id,
        InboundWebhookEvent.provider == PROVIDER,
        InboundWebhookEvent.event_id.like(f"{funnel_id}:%"),
    )


def retry_delivery(db: Session, org_id: uuid.UUID, funnel_id: uuid.UUID, row_id: uuid.UUID) -> bool:
    """Re-queue a failed delivery of this funnel as pending. Commits."""
    now = datetime.utcnow()
    n = (
        _deliveries_query(db, org_id, funnel_id)
        .filter(InboundWebhookEvent.id == row_id, InboundWebhookEvent.status == "failed")
        .update(
            {
                "status": "pending",
                "attempts": 0,
                "error_text": None,
                "next_attempt_at": now + timedelta(seconds=INLINE_GRACE_SEC),
                "updated_at": now,
            },
            synchronize_session=False,
        )
    )
    db.commit()
    return bool(n)


def _iso_utc(dt: Optional[datetime]) -> Optional[str]:
    return dt.replace(tzinfo=timezone.utc).isoformat() if dt else None


def delivery_summary(db: Session, org_id: uuid.UUID, funnel_id: uuid.UUID, *, limit: int = 25) -> Dict[str, Any]:
    since = datetime.utcnow() - timedelta(hours=24)
    counts = dict(
        _deliveries_query(db, org_id, funnel_id)
        .filter(InboundWebhookEvent.received_at >= since)
        .with_entities(InboundWebhookEvent.status, func.count())
        .group_by(InboundWebhookEvent.status)
        .all()
    )
    rows = (
        _deliveries_query(db, org_id, funnel_id)
        .order_by(InboundWebhookEvent.received_at.desc())
        .limit(limit)
        .all()
    )
    recent = []
    for r in rows:
        lead = (r.payload or {}).get("lead") or {}
        fields = lead.get("fields") or {}
        name = fields.get("name") or " ".join(x for x in (fields.get("first_name"), fields.get("last_name")) if x)
        recent.append(
            {
                "id": str(r.id),
                "received_at": _iso_utc(r.received_at),
                "status": r.status,
                "attempts": r.attempts or 0,
                "error": r.error_text[:300] if r.error_text else None,
                "email": fields.get("email"),
                "phone": fields.get("phone"),
                "name": name or None,
                "extra_fields": sorted((lead.get("opt_in_data") or {}).keys())[:40],
            }
        )
    return {
        "last_24h": {
            "received": sum(counts.values()),
            "done": counts.get("done", 0),
            "pending": counts.get("pending", 0) + counts.get("processing", 0),
            "failed": counts.get("failed", 0),
        },
        "last_received_at": recent[0]["received_at"] if recent else None,
        "recent": recent,
    }


def prune_done_deliveries(db: Session, *, batch: int = 5000) -> int:
    """Delete one bounded batch of done funnel-webhook rows past retention. Commits."""
    days = int(getattr(settings, "FUNNEL_WEBHOOK_RETENTION_DAYS", 30) or 0)
    if days <= 0:
        return 0
    cutoff = datetime.utcnow() - timedelta(days=days)
    res = db.execute(
        text(
            "DELETE FROM inbound_webhook_events WHERE id IN ("
            " SELECT id FROM inbound_webhook_events"
            " WHERE provider = :p AND status = 'done' AND received_at < :cutoff"
            " LIMIT :batch)"
        ),
        {"p": PROVIDER, "cutoff": cutoff, "batch": batch},
    )
    db.commit()
    return int(res.rowcount or 0)

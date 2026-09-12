"""
Sync Whop payments into whop_payments (Company API key per org).
"""
from __future__ import annotations

import uuid
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any, Dict, List, Optional

from sqlalchemy import func, or_
from sqlalchemy.orm import Session

from app.core.encryption import decrypt_token
from app.models.oauth_token import OAuthToken, OAuthProvider
from app.models.whop_payment import WhopPayment
from app.models.client import Client, LifecycleState, find_client_by_email


SYNC_BUFFER_SECONDS = 300
WHOP_PAID_STATUSES = frozenset({"paid", "succeeded", "completed", "successful"})
WHOP_FAILED_STATUSES = frozenset(
    {"failed", "past_due", "void", "uncollectible", "declined", "unpaid"}
)
# ISO currencies that have no minor units (Whop amounts are already whole units).
_ZERO_DECIMAL_CURRENCIES = frozenset(
    {"jpy", "krw", "vnd", "clp", "bif", "djf", "gnf", "isk", "kmf", "pyg", "rwf", "ugx", "vuv", "xaf", "xof", "xpf"}
)


def _parse_whop_datetime(value: Any) -> Optional[datetime]:
    if value is None or value is False:
        return None
    if isinstance(value, datetime):
        return value.replace(tzinfo=None) if value.tzinfo else value
    if isinstance(value, (int, float)) and value > 1_000_000_000:
        return datetime.utcfromtimestamp(int(value))
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        if s.isdigit() and len(s) >= 10:
            try:
                return datetime.utcfromtimestamp(int(s))
            except Exception:
                return None
        try:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            return dt.replace(tzinfo=None) if dt.tzinfo else dt
        except Exception:
            return None
    return None


def _parse_created_at(item: Dict[str, Any]) -> datetime:
    for key in ("paid_at", "created_at", "updated_at"):
        dt = _parse_whop_datetime(item.get(key))
        if dt:
            return dt
    return datetime.utcnow()


def _normalize_currency(raw: Optional[str]) -> str:
    code = (raw or "usd").strip().lower()
    if code in {"whop_usd", "usdt"}:
        return "usd"
    return (code[:3] if code else "usd") or "usd"


def _payment_currency(item: Dict[str, Any]) -> str:
    return _normalize_currency(item.get("settlement_currency") or item.get("currency"))


def _money_to_cents(value: Any, currency: str) -> Optional[int]:
    """
    Accept OpenAPI decimals (6.9) and live API money objects
    {"amount": "1295.82", "currency": "usd", "decimals": 2}.
    """
    if value is None or value == "":
        return None
    if isinstance(value, dict):
        currency = _normalize_currency(value.get("currency") or currency)
        raw_amount = value.get("amount")
        if raw_amount is None or raw_amount == "":
            return None
        try:
            d = Decimal(str(raw_amount))
        except Exception:
            return None
        decimals = value.get("decimals")
        if decimals is None:
            decimals = 0 if currency in _ZERO_DECIMAL_CURRENCIES else 2
        try:
            decimals = int(decimals)
        except Exception:
            decimals = 2
        scale = Decimal(10) ** decimals
        return max(0, int((d * scale).quantize(Decimal("1"))))
    try:
        d = Decimal(str(value))
    except Exception:
        return None
    if currency in _ZERO_DECIMAL_CURRENCIES:
        return max(0, int(d.quantize(Decimal("1"))))
    return max(0, int((d * Decimal("100")).quantize(Decimal("1"))))


def _amount_cents(item: Dict[str, Any]) -> int:
    """
    Live Company API money objects + OpenAPI number fallback.
    Prefer presentment/total (customer charge), then usd_total, then subtotal.
    Skip empty/zero until a later key has a real amount.
    """
    currency = _payment_currency(item)
    for key in (
        "settlement_amount",
        "presentment_total",
        "total",
        "usd_total",
        "subtotal",
        "amount_after_fees",
    ):
        cents = _money_to_cents(item.get(key), currency if key != "usd_total" else "usd")
        if cents:
            return cents
    return 0


def _nested_str(item: Dict[str, Any], path: tuple) -> Optional[str]:
    d: Any = item
    for p in path:
        if not isinstance(d, dict):
            return None
        d = d.get(p)
    if isinstance(d, str) and d.strip():
        return d.strip()
    return None


def _payer_email(item: Dict[str, Any]) -> Optional[str]:
    """Live list/webhook uses customer_email; retrieve may add user.email."""
    top = item.get("customer_email") or item.get("email")
    if isinstance(top, str) and "@" in top.strip():
        return top.strip()
    for path in (
        ("user", "email"),
        ("member", "user", "email"),
        ("metadata", "email"),
        ("metadata", "buyer_email"),
        ("metadata", "customer_email"),
    ):
        found = _nested_str(item, path)
        if found and "@" in found:
            return found
    return None


def _payer_name(item: Dict[str, Any]) -> Optional[str]:
    for path in (
        ("user", "name"),
        ("member", "user", "name"),
        ("billing_address", "name"),
        ("shipping_address", "name"),
        ("user", "username"),
        ("member", "user", "username"),
    ):
        found = _nested_str(item, path)
        if found:
            return found
    return None


def _member_id(item: Dict[str, Any]) -> Optional[str]:
    mid = item.get("member_id")
    if isinstance(mid, str) and mid.strip():
        return mid.strip()
    member = item.get("member")
    if isinstance(member, dict) and member.get("id"):
        return str(member["id"]).strip() or None
    if isinstance(member, str) and member.strip():
        return member.strip()
    return None


def payment_needs_hydrate(item: Dict[str, Any]) -> bool:
    if not isinstance(item, dict) or not item.get("id"):
        return False
    user = item.get("user")
    if isinstance(user, str) or user is None:
        return True
    if not _payer_email(item):
        return True
    if not _payer_name(item):
        return True
    if _amount_cents(item) <= 0:
        return True
    return False


def hydrate_whop_payment_item(api_key: str, item: Dict[str, Any]) -> Dict[str, Any]:
    """Fill email/amount from GET /payments/{id} and GET /members/{id} when list/webhook is thin."""
    if not payment_needs_hydrate(item):
        return item
    from app.services import whop_client

    merged = dict(item)
    pay_id = str(item.get("id") or "")
    if pay_id.startswith("pay_"):
        try:
            fetched = whop_client.retrieve_payment(api_key, pay_id)
            if isinstance(fetched, dict) and fetched.get("id"):
                merged = {**merged, **fetched}
        except Exception:
            pass
    if _payer_email(merged):
        return merged
    mid = _member_id(merged)
    if not mid:
        return merged
    try:
        member = whop_client.retrieve_member(api_key, mid)
    except Exception:
        return merged
    if not isinstance(member, dict):
        return merged
    existing_member = merged.get("member") if isinstance(merged.get("member"), dict) else {}
    merged["member"] = {**existing_member, **member}
    user = member.get("user")
    if isinstance(user, dict) and not isinstance(merged.get("user"), dict):
        merged["user"] = user
    elif isinstance(user, dict) and isinstance(merged.get("user"), dict):
        merged["user"] = {**merged["user"], **{k: v for k, v in user.items() if v}}
    return merged


def _split_display_name(name: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    if not name or not str(name).strip():
        return None, None
    parts = str(name).strip().split()
    if len(parts) == 1:
        return parts[0], None
    return parts[0], " ".join(parts[1:])


def apply_whop_identity_to_client(client: Client, email: Optional[str], name: Optional[str]) -> None:
    """Always stamp missing email + name on a Whop-created/linked card."""
    first, last = _split_display_name(name)
    if email:
        email = email.strip()
        current = (client.email or "").strip()
        if not current:
            client.email = email
        elif email.lower() != current.lower():
            extras = list(client.emails) if isinstance(client.emails, list) else []
            seen = {e.lower() for e in extras if isinstance(e, str)}
            if email.lower() not in seen:
                extras.append(email)
                client.emails = extras
    if first and not (client.first_name or "").strip():
        client.first_name = first
    if last and not (client.last_name or "").strip():
        client.last_name = last
    client.updated_at = datetime.utcnow()


def _find_name_only_client(
    db: Session, org_id: uuid.UUID, first: Optional[str], last: Optional[str]
) -> Optional[Client]:
    if not first:
        return None
    q = db.query(Client).filter(
        Client.org_id == org_id,
        or_(Client.email.is_(None), Client.email == ""),
        func.lower(Client.first_name) == first.lower(),
    )
    if last:
        q = q.filter(func.lower(Client.last_name) == last.lower())
    else:
        q = q.filter(or_(Client.last_name.is_(None), Client.last_name == ""))
    return q.first()


def ensure_client_for_whop_payer(
    db: Session, org_id: uuid.UUID, item: Dict[str, Any]
) -> Optional[Client]:
    """
    Find or create a pipeline client with Whop email + name.
    Prefer email match; reclaim name-only cards created before customer_email mapped.
    Do not create a new card without an email.
    """
    email = _payer_email(item)
    name = _payer_name(item)
    first, last = _split_display_name(name)

    if email:
        existing = find_client_by_email(db, org_id, email)
        if existing:
            apply_whop_identity_to_client(existing, email, name)
            return existing
        orphan = _find_name_only_client(db, org_id, first, last)
        if orphan:
            apply_whop_identity_to_client(orphan, email, name)
            return orphan
        client = Client(
            org_id=org_id,
            email=email,
            first_name=first,
            last_name=last,
            lifecycle_state=LifecycleState.ACTIVE,
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow(),
        )
        db.add(client)
        db.flush()
        return client

    if first:
        orphan = _find_name_only_client(db, org_id, first, last)
        if orphan:
            apply_whop_identity_to_client(orphan, None, name)
            return orphan
    return None


def _normalize_status(raw: Optional[str]) -> str:
    if not raw:
        return "unknown"
    s = str(raw).strip().lower()
    return s or "unknown"


def upsert_whop_payment_item(db: Session, org_id: uuid.UUID, item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Insert or update one Whop payment payload.
    Returns a paid-payment signal when linked to a client (first=True on first sale).
    """
    if not isinstance(item, dict) or not item.get("id"):
        return None
    whop_id = str(item["id"])
    status = _normalize_status(item.get("status"))
    amount_cents = _amount_cents(item)
    currency = _payment_currency(item)
    created_at = _parse_created_at(item)
    existing = (
        db.query(WhopPayment)
        .filter(WhopPayment.org_id == org_id, WhopPayment.whop_id == whop_id)
        .first()
    )
    client = None
    if existing and existing.client_id:
        client = (
            db.query(Client)
            .filter(Client.id == existing.client_id, Client.org_id == org_id)
            .first()
        )
        if client:
            apply_whop_identity_to_client(client, _payer_email(item), _payer_name(item))
    if client is None:
        client = ensure_client_for_whop_payer(db, org_id, item)
    client_id = client.id if client else None
    if existing:
        existing.amount_cents = amount_cents
        existing.currency = currency
        existing.status = status
        existing.client_id = client_id
        existing.raw = item
        existing.updated_at = datetime.utcnow()
    else:
        db.add(
            WhopPayment(
                id=uuid.uuid4(),
                org_id=org_id,
                whop_id=whop_id,
                amount_cents=amount_cents,
                currency=currency,
                status=status,
                client_id=client_id,
                raw=item,
                created_at=created_at,
                updated_at=datetime.utcnow(),
            )
        )
    db.flush()
    if client:
        from app.api.clients.helpers import recompute_client_lifetime_revenue

        recompute_client_lifetime_revenue(db, org_id, client)

    if client_id is None or status not in WHOP_PAID_STATUSES:
        return None
    from app.services.client_automation import _succeeded_payment_count

    first = _succeeded_payment_count(db, org_id, client_id) == 1
    return {
        "client_id": client_id,
        "whop_id": whop_id,
        "amount_cents": amount_cents,
        "paid_at": created_at,
        "first": first,
        "paid": True,
    }


def stored_or_raw_amount_cents(row: Any) -> int:
    stored = int(getattr(row, "amount_cents", 0) or 0)
    if stored:
        return stored
    raw = getattr(row, "raw", None)
    if isinstance(raw, dict):
        return _amount_cents(raw)
    return 0


def apply_whop_first_payment_signals(db: Session, org_id: uuid.UUID, signals: List[Dict[str, Any]]) -> None:
    """Stripe-equivalent side effects: lifecycle + KPI on every paid; first-sale automations once."""
    paid_signals = [sig for sig in signals if sig.get("client_id")]
    if not paid_signals:
        return
    try:
        from app.services.kpi_integration_sync import sync_kpi_for_datetime

        days_seen = set()
        for sig in paid_signals:
            paid_at = sig.get("paid_at")
            if paid_at is None:
                continue
            day_key = paid_at.date() if hasattr(paid_at, "date") else None
            if day_key and day_key not in days_seen:
                days_seen.add(day_key)
                sync_kpi_for_datetime(db, org_id, paid_at, commit=True)
    except Exception:
        pass

    try:
        from app.models.client import Client
        from app.services.automation_engine import on_payment_received
        from app.services.client_automation import (
            apply_automatic_lifecycle_for_client,
            enqueue_payment_pipeline_effects,
            mark_latest_sales_call_closed,
            move_client_to_active_on_payment,
        )
        from app.services.terminal_metrics_service import invalidate_terminal_monthly_trends_cache

        for cid in {sig["client_id"] for sig in paid_signals}:
            client_row = db.query(Client).filter(Client.id == cid).first()
            if not client_row:
                continue
            apply_automatic_lifecycle_for_client(db, client_row)
            move_client_to_active_on_payment(db, client_row)
        db.flush()

        first_signals = [sig for sig in paid_signals if sig.get("first")]
        for sig in first_signals:
            client_row = db.query(Client).filter(Client.id == sig["client_id"]).first()
            if client_row:
                enqueue_payment_pipeline_effects(org_id, client_row.id)
                try:
                    call_when = mark_latest_sales_call_closed(db, org_id, client_row)
                    if call_when is not None:
                        from app.services.kpi_integration_sync import sync_kpi_for_datetime

                        sync_kpi_for_datetime(db, org_id, call_when, commit=True)
                except Exception:
                    pass
            on_payment_received(
                db,
                org_id=org_id,
                client_id=sig["client_id"],
                payment_source="whop",
                payment_external_id=sig["whop_id"],
                amount_cents=int(sig["amount_cents"] or 0),
                paid_at=sig.get("paid_at"),
            )
        invalidate_terminal_monthly_trends_cache(org_id)
        db.commit()
    except Exception:
        db.rollback()


def sync_whop_incremental(db: Session, org_id: uuid.UUID, force_full: bool = False) -> Dict[str, Any]:
    token = (
        db.query(OAuthToken)
        .filter(OAuthToken.org_id == org_id, OAuthToken.provider == OAuthProvider.WHOP)
        .first()
    )
    if not token:
        return {"error": "Whop not connected"}

    company_id = (token.account_id or "").strip()
    if not company_id:
        return {"error": "Whop company_id missing"}

    api_key = decrypt_token(token.access_token)
    from app.services import whop_client

    updated_after: Optional[datetime] = None
    if not force_full and token.last_sync_at:
        updated_after = token.last_sync_at - timedelta(seconds=SYNC_BUFFER_SECONDS)

    total_upserted = 0
    cursor: Optional[str] = None
    pages = 0
    new_first_payment_signals: List[Dict[str, Any]] = []

    while True:
        pages += 1
        if pages > 500:
            break
        rows, page_info = whop_client.list_payments_page(
            api_key,
            company_id,
            first=100,
            after=cursor,
            updated_after=updated_after,
        )

        for item in rows:
            if isinstance(item, dict):
                item = hydrate_whop_payment_item(api_key, item)
            sig = upsert_whop_payment_item(db, org_id, item)
            if sig:
                new_first_payment_signals.append(sig)
            if isinstance(item, dict) and item.get("id"):
                total_upserted += 1

        db.flush()
        if not page_info.get("has_next_page"):
            break
        cursor = page_info.get("end_cursor")
        if not cursor:
            break

    token.last_sync_at = datetime.utcnow()
    token.last_webhook_processed_at = token.last_sync_at
    db.commit()
    apply_whop_first_payment_signals(db, org_id, new_first_payment_signals)

    return {
        "payments_upserted": total_upserted,
        "pages": pages,
        "incremental": updated_after is not None,
    }

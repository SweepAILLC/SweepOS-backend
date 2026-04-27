"""
Sync Whop payments into whop_payments (Company API key per org).
"""
from __future__ import annotations

import uuid
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any, Dict, Optional

from sqlalchemy.orm import Session

from app.core.encryption import decrypt_token
from app.models.oauth_token import OAuthToken, OAuthProvider
from app.models.whop_payment import WhopPayment
from app.models.client import find_client_by_email


SYNC_BUFFER_SECONDS = 300


def _parse_created_at(item: Dict[str, Any]) -> datetime:
    for key in ("paid_at", "created_at"):
        v = item.get(key)
        if not v or not isinstance(v, str):
            continue
        try:
            s = v.replace("Z", "+00:00")
            dt = datetime.fromisoformat(s)
            if dt.tzinfo:
                dt = dt.replace(tzinfo=None)  # store naive UTC
            return dt
        except Exception:
            continue
    return datetime.utcnow()


def _amount_cents(item: Dict[str, Any]) -> int:
    """Whop list items expose totals in currency units (dollars for USD)."""
    cur = (item.get("currency") or "usd").lower()
    for key in ("usd_total", "total", "subtotal", "amount_after_fees"):
        v = item.get(key)
        if v is None:
            continue
        try:
            d = Decimal(str(v))
            cents = int((d * Decimal(100)).quantize(Decimal("1")))
            return max(0, cents)
        except Exception:
            continue
    return 0


def _payer_email(item: Dict[str, Any]) -> Optional[str]:
    for path in (
        ("user", "email"),
        ("member", "email"),
        ("member", "user", "email"),
    ):
        d: Any = item
        for p in path:
            if not isinstance(d, dict):
                d = None
                break
            d = d.get(p)
        if isinstance(d, str) and d.strip():
            return d.strip()
    return None


def _normalize_status(raw: Optional[str]) -> str:
    if not raw:
        return "unknown"
    s = str(raw).strip().lower()
    return s or "unknown"


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
            if not isinstance(item, dict) or not item.get("id"):
                continue
            whop_id = str(item["id"])
            status = _normalize_status(item.get("status"))
            amount_cents = _amount_cents(item)
            currency = (item.get("currency") or "usd").lower()[:3]
            created_at = _parse_created_at(item)
            email = _payer_email(item)
            client_id = None
            if email:
                c = find_client_by_email(db, org_id, email)
                if c:
                    client_id = c.id

            existing = (
                db.query(WhopPayment)
                .filter(WhopPayment.org_id == org_id, WhopPayment.whop_id == whop_id)
                .first()
            )
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
            total_upserted += 1

        db.flush()
        if not page_info.get("has_next_page"):
            break
        cursor = page_info.get("end_cursor")
        if not cursor:
            break

    token.last_sync_at = datetime.utcnow()
    db.commit()

    return {
        "payments_upserted": total_upserted,
        "pages": pages,
        "incremental": updated_after is not None,
    }

"""Shared helpers for the clients API package."""
from __future__ import annotations

import logging
import re
import uuid
from datetime import datetime, timedelta, timezone
from threading import Lock as ThreadingLock
from typing import List, Optional, Tuple
from uuid import UUID

import httpx
from sqlalchemy.orm import Session
from sqlalchemy.orm.attributes import flag_modified

from app.db.session import SessionLocal
from app.models.client import Client
from app.models.user import User
from app.models.whop_payment import WhopPayment
from app.services.call_insight_service import refresh_latest_call_insight_background

LOG = logging.getLogger(__name__)

WHOP_PAID_STATUSES = frozenset({"paid", "succeeded", "completed", "successful"})

_checkin_sync_locks_guard = ThreadingLock()
_checkin_sync_org_locks: dict[str, ThreadingLock] = {}


def user_pipeline_priorities(user: User):
    """Extract pipeline_priorities list from user.ai_profile, or None."""
    raw = getattr(user, "ai_profile", None)
    if not raw or not isinstance(raw, dict):
        return None
    pp = raw.get("pipeline_priorities")
    if isinstance(pp, list) and all(isinstance(x, str) for x in pp):
        return pp
    return None


def effective_org_id(user: User):
    """Org to scope queries (JWT selected org when present; else user's primary org)."""
    return getattr(user, "selected_org_id", user.org_id)


def org_checkin_sync_lock(org_key: str) -> ThreadingLock:
    with _checkin_sync_locks_guard:
        if org_key not in _checkin_sync_org_locks:
            _checkin_sync_org_locks[org_key] = ThreadingLock()
        return _checkin_sync_org_locks[org_key]


def sync_check_ins_in_worker(
    token: str,
    *,
    apply_pipeline_lifecycle_rules: bool = True,
    force_lifecycle: bool = False,
) -> dict:
    """Short auth session, then one org-serialized sync with its own session (no route-level get_db)."""
    from app.api.deps import resolve_org_and_user_ids_for_checkin_sync
    from app.services.checkin_sync import sync_all_checkins
    from app.services.terminal_metrics_service import invalidate_terminal_monthly_trends_cache

    db_auth = SessionLocal()
    try:
        org_id, user_id = resolve_org_and_user_ids_for_checkin_sync(db_auth, token)
    finally:
        db_auth.close()

    org_key = str(org_id)
    lk = org_checkin_sync_lock(org_key)
    lk.acquire()
    try:
        db_sync = SessionLocal()
        try:
            result = sync_all_checkins(
                db_sync,
                org_id,
                user_id,
                apply_pipeline_lifecycle_rules=apply_pipeline_lifecycle_rules,
                force_lifecycle=force_lifecycle,
            )
            invalidate_terminal_monthly_trends_cache(org_id)
            return result
        finally:
            db_sync.close()
    finally:
        lk.release()


def refresh_call_insights_after_checkin_sync(org_id_str: str, client_ids: list[str]) -> None:
    """
    Run Fathom call-insight refreshes for clients touched by calendar sync.

    A short deferral lets the browser's follow-up bookings read complete before DB-heavy work.
    """
    import time

    time.sleep(2)
    for cid in client_ids:
        refresh_latest_call_insight_background(org_id_str, str(cid))


def scope_org_id(user: User) -> UUID:
    """Selected org from JWT as a UUID (matches create/delete/list filtering)."""
    raw = getattr(user, "selected_org_id", None) or user.org_id
    if isinstance(raw, UUID):
        return raw
    return UUID(str(raw))


def parse_client_uuid(client_id: str) -> UUID:
    """Parse path client_id; 404 (not 500) when static paths like terminal-summary are misrouted."""
    from fastapi import HTTPException, status

    try:
        return UUID(str(client_id).strip())
    except ValueError:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Client not found")


def parse_campaign_stats_response(
    data: dict,
) -> Tuple[int, int, int, int, int, int, Optional[float], Optional[float]]:
    """Parse Brevo GET /v3/contacts/{identifier}/campaignStats response."""
    messages_sent_list = data.get("messagesSent") or []
    opened_list = data.get("opened") or []
    clicked_list = data.get("clicked") or []

    def _campaign_id(item: dict):
        return item.get("campaignId") if isinstance(item, dict) else None

    messages_sent = len(messages_sent_list)
    messages_opened = sum(int(i.get("count", 0) or 0) for i in opened_list if isinstance(i, dict))
    messages_clicked = 0
    for i in clicked_list:
        if not isinstance(i, dict):
            continue
        for link in i.get("links") or []:
            messages_clicked += int(link.get("count", 0) or 0)

    trans_sent = sum(1 for i in messages_sent_list if _campaign_id(i) == 0)
    trans_opened = sum(
        int(i.get("count", 0) or 0) for i in opened_list if isinstance(i, dict) and _campaign_id(i) == 0
    )
    trans_clicked = 0
    for i in clicked_list:
        if not isinstance(i, dict) or _campaign_id(i) != 0:
            continue
        for link in i.get("links") or []:
            trans_clicked += int(link.get("count", 0) or 0)

    trans_open_rate = (trans_opened / trans_sent * 100.0) if trans_sent > 0 else None
    trans_click_rate = (trans_clicked / trans_sent * 100.0) if trans_sent > 0 else None

    return (
        messages_sent,
        messages_opened,
        messages_clicked,
        trans_sent,
        trans_opened,
        trans_clicked,
        trans_open_rate,
        trans_click_rate,
    )


def merge_brevo_stats(stats_list: List[dict]) -> Optional[dict]:
    """Merge Brevo stats from multiple emails into one dict for health score."""
    if not stats_list:
        return None
    total_sent = sum(s.get("messages_sent") or 0 for s in stats_list)
    total_opened = sum(s.get("messages_opened") or 0 for s in stats_list)
    total_clicked = sum(s.get("messages_clicked") or 0 for s in stats_list)
    total_trans_sent = sum(s.get("trans_sent") or 0 for s in stats_list)
    total_trans_opened = sum(s.get("trans_opened") or 0 for s in stats_list)
    total_trans_clicked = sum(s.get("trans_clicked") or 0 for s in stats_list)
    if total_sent == 0 and total_trans_sent == 0:
        return None
    return {
        "campaign_open_rate": (total_opened / total_sent * 100.0) if total_sent > 0 else None,
        "campaign_click_rate": (total_clicked / total_sent * 100.0) if total_sent > 0 else None,
        "messages_sent": total_sent,
        "trans_open_rate": (total_trans_opened / total_trans_sent * 100.0) if total_trans_sent > 0 else None,
        "trans_click_rate": (total_trans_clicked / total_trans_sent * 100.0) if total_trans_sent > 0 else None,
    }


def brevo_merged_stats_for_client(
    db: Session, org_id: uuid.UUID, user_id: uuid.UUID, client: Client
) -> Optional[dict]:
    """Same Brevo aggregation as GET /clients/{id}/health-score (multi-email clients)."""
    emails_to_try = [client.email] if getattr(client, "email", None) else []
    if getattr(client, "emails", None) and isinstance(client.emails, list):
        emails_to_try = list(emails_to_try) + [e for e in client.emails if e and str(e).strip()]
    seen = set()
    emails_unique = []
    for e in emails_to_try:
        e = str(e).strip().lower()
        if e and e not in seen:
            seen.add(e)
            emails_unique.append(e)
    all_stats = [fetch_brevo_email_stats(db, org_id, user_id, email) for email in emails_unique]
    return merge_brevo_stats(all_stats)


def fetch_brevo_email_stats(db: Session, org_id: uuid.UUID, user_id: uuid.UUID, email: str) -> dict:
    """Fetch Brevo email stats for a contact (campaign + transactional, last 90 days)."""
    from urllib.parse import quote

    from app.core.encryption import decrypt_token
    from app.models.oauth_token import OAuthProvider, OAuthToken

    out = {
        "campaign_open_rate": None,
        "campaign_click_rate": None,
        "messages_sent": 0,
        "messages_opened": 0,
        "messages_clicked": 0,
        "trans_sent": 0,
        "trans_opened": 0,
        "trans_clicked": 0,
        "trans_open_rate": None,
        "trans_click_rate": None,
    }
    brevo_token = db.query(OAuthToken).filter(
        OAuthToken.provider == OAuthProvider.BREVO,
        OAuthToken.org_id == org_id,
    ).first()
    if not brevo_token:
        return out
    try:
        access_token = decrypt_token(
            brevo_token.access_token,
            audit_context={
                "db": db,
                "org_id": org_id,
                "user_id": user_id,
                "resource_type": "brevo_token",
                "resource_id": str(brevo_token.id),
            },
        )
    except Exception:
        return out
    headers = {"accept": "application/json", "content-type": "application/json"}
    if getattr(brevo_token, "scope", None) == "api_key":
        headers["api-key"] = access_token
    else:
        headers["Authorization"] = f"Bearer {access_token}"

    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=90)
    params = {"startDate": start_date.strftime("%Y-%m-%d"), "endDate": end_date.strftime("%Y-%m-%d")}
    encoded_email = quote(email, safe="")
    url = f"https://api.brevo.com/v3/contacts/{encoded_email}/campaignStats"
    try:
        with httpx.Client(timeout=10.0) as client_http:
            r = client_http.get(url, headers=headers, params=params)
        if r.status_code != 200:
            return out
        data = r.json()
        if not isinstance(data, dict):
            return out
        (
            messages_sent,
            messages_opened,
            messages_clicked,
            trans_sent,
            trans_opened,
            trans_clicked,
            trans_open_rate,
            trans_click_rate,
        ) = parse_campaign_stats_response(data)
        out["messages_sent"] = messages_sent
        out["messages_opened"] = messages_opened
        out["messages_clicked"] = messages_clicked
        out["trans_sent"] = trans_sent
        out["trans_opened"] = trans_opened
        out["trans_clicked"] = trans_clicked
        if messages_sent > 0:
            out["campaign_open_rate"] = (messages_opened / messages_sent) * 100.0
            out["campaign_click_rate"] = (messages_clicked / messages_sent) * 100.0
        out["trans_open_rate"] = trans_open_rate
        out["trans_click_rate"] = trans_click_rate
    except Exception:
        pass
    return out


def normalize_email(email: str | None) -> str | None:
    if not email:
        return None
    return re.sub(r"\s+", "", email.lower().strip()) or None


def client_created_sort_key(client: Client) -> tuple:
    """Stable sort key for picking a primary client in a merged group."""
    ts = client.created_at
    if ts is None:
        return (1, "")
    try:
        if getattr(ts, "tzinfo", None):
            ts = ts.astimezone(timezone.utc).replace(tzinfo=None)
        return (0, ts.isoformat())
    except (TypeError, ValueError, OSError):
        return (1, str(client.id))


def load_whop_payments(db: Session, org_id: uuid.UUID) -> list:
    """Return Whop payments for org; empty list if table missing or query fails."""
    try:
        return db.query(WhopPayment).filter(WhopPayment.org_id == org_id).all()
    except Exception:
        LOG.exception("terminal-summary: whop_payments query failed for org %s", org_id)
        db.rollback()
        return []


def merge_client_meta_from_duplicates(keep: Client, to_remove: List[Client]) -> None:
    """Carry prospect/ROI opportunity blobs from merged profiles onto the kept client."""
    base: dict = {}
    if isinstance(keep.meta, dict):
        base = dict(keep.meta)
    for c in to_remove:
        if not isinstance(c.meta, dict):
            continue
        m = c.meta
        for key in ("prospect_voice_profile", "roi_state"):
            v = m.get(key)
            bv = base.get(key)
            if isinstance(v, dict) and v:
                if not isinstance(bv, dict) or not bv:
                    base[key] = dict(v)
                elif key == "prospect_voice_profile":
                    base[key] = {**dict(v), **dict(bv)}
        lf = m.get("lead_follow_up")
        if lf and not base.get("lead_follow_up"):
            base["lead_follow_up"] = lf
    if base:
        keep.meta = base
        flag_modified(keep, "meta")

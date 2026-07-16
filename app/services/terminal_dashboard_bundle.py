"""
Terminal dashboard packages for MCP (Claude custom connector).

Reuses existing Terminal / finances / calendar / Stripe / funnel endpoints so Claude
sees the same numbers as the in-app Terminal tab.

Reliability notes (Claude.ai HTTP MCP):
- First response byte often must arrive within ~60s or the client times out and retries.
- Default path is a fast "overview"; full sections are opt-in.
- Sections build in parallel with a soft deadline and short TTL cache.
"""
from __future__ import annotations

import logging
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any, Dict, List, Optional, Set, Tuple

from sqlalchemy.orm import Session, joinedload

from app.db.session import SessionLocal
from app.models.client_checkin import ClientCheckIn
from app.models.organization import Organization
from app.services.calendar_booking_time import effective_end_sql_expression
from app.services.calendar_trend_summary import compute_calendar_trend_summary
from app.services.checkin_sync import is_calendar_placeholder_email
from app.services.terminal_metrics_service import get_or_build_terminal_monthly_trends

logger = logging.getLogger(__name__)

# Fast default — enough for most Claude prompts without cold-path timeouts.
_OVERVIEW_SECTIONS: Tuple[str, ...] = (
    "summary",
    "monthly_trends",
    "appointments",
    "failed_payments",
)

_SECTION_KEYS = (
    "summary",
    "monthly_trends",
    "finances",
    "stripe",
    "calendar",
    "appointments",
    "failed_payments",
    "leads",
)

# Soft budget so Claude's ~60s first-byte window is respected with margin.
_SOFT_DEADLINE_SEC = 40.0
_CACHE_TTL_SEC = 45.0
_MAX_FUNNELS_FOR_LEADS = 5

_dashboard_cache: dict[str, tuple[float, Dict[str, Any]]] = {}
_dashboard_cache_lock = threading.Lock()


def _user_proxy(org_id: uuid.UUID, user_id: Optional[uuid.UUID] = None) -> SimpleNamespace:
    return SimpleNamespace(
        id=user_id or org_id,
        org_id=org_id,
        selected_org_id=org_id,
    )


def _serialize(obj: Any) -> Any:
    if obj is None:
        return None
    if hasattr(obj, "model_dump"):
        return obj.model_dump()
    if hasattr(obj, "dict"):
        return obj.dict()
    if isinstance(obj, list):
        return [_serialize(x) for x in obj]
    if isinstance(obj, dict):
        return {k: _serialize(v) for k, v in obj.items()}
    if isinstance(obj, (datetime,)):
        return obj.isoformat()
    if isinstance(obj, uuid.UUID):
        return str(obj)
    return obj


def _terminal_summary(db: Session, org_id: uuid.UUID, user_id: Optional[uuid.UUID]) -> Dict[str, Any]:
    from app.api.clients.terminal import get_terminal_summary

    try:
        resp = get_terminal_summary(db=db, current_user=_user_proxy(org_id, user_id))
        return _serialize(resp)
    except Exception as e:
        logger.warning("terminal summary for mcp failed: %s", e)
        return {"error": str(e)}


def _monthly_trends(db: Session, org_id: uuid.UUID) -> Dict[str, Any]:
    org = db.query(Organization).filter(Organization.id == org_id).first()
    if not org:
        return {"error": "organization not found", "periods": []}
    try:
        resp = get_or_build_terminal_monthly_trends(db, org)
        return _serialize(resp)
    except Exception as e:
        logger.warning("terminal monthly trends for mcp failed: %s", e)
        return {"error": str(e), "periods": []}


def _finances_summary(
    db: Session,
    org_id: uuid.UUID,
    user_id: Optional[uuid.UUID],
    *,
    range_days: int = 30,
    scope: Optional[str] = None,
) -> Dict[str, Any]:
    from app.api.finances import finances_summary

    try:
        resp = finances_summary(
            range_days=range_days,
            scope=scope,
            db=db,
            current_user=_user_proxy(org_id, user_id),
        )
        return _serialize(resp)
    except Exception as e:
        logger.warning("finances summary for mcp failed: %s", e)
        return {"error": str(e)}


def _stripe_summary(db: Session, org_id: uuid.UUID, user_id: Optional[uuid.UUID]) -> Dict[str, Any]:
    from app.api.stripe import get_stripe_summary
    from fastapi import HTTPException

    try:
        resp = get_stripe_summary(
            range_days=30,
            scope=None,
            current_user=_user_proxy(org_id, user_id),
            db=db,
        )
        return _serialize(resp)
    except HTTPException as e:
        return {"connected": False, "detail": e.detail}
    except Exception as e:
        return {"connected": False, "detail": str(e)}


def _calendar_trend(
    db: Session,
    org_id: uuid.UUID,
    *,
    scope: Optional[str] = None,
    range_days: Optional[int] = None,
) -> Dict[str, Any]:
    try:
        return compute_calendar_trend_summary(
            db, org_id, scope=scope, range_days=range_days
        )
    except Exception as e:
        logger.warning("calendar trend for mcp failed: %s", e)
        return {"error": str(e)}


def _upcoming_appointments(
    db: Session,
    org_id: uuid.UUID,
    *,
    limit: int = 20,
) -> Dict[str, Any]:
    lim = max(1, min(int(limit or 20), 100))
    now = datetime.now(timezone.utc)
    effective_end = effective_end_sql_expression()
    rows = (
        db.query(ClientCheckIn)
        .options(joinedload(ClientCheckIn.client))
        .filter(
            ClientCheckIn.org_id == org_id,
            ClientCheckIn.provider.in_(["calcom", "calendly"]),
            effective_end >= now,
        )
        .order_by(ClientCheckIn.start_time.asc())
        .limit(lim)
        .all()
    )
    out: List[Dict[str, Any]] = []
    for ci in rows:
        client_name = None
        client_id = str(ci.client_id) if ci.client_id else None
        if ci.client:
            if is_calendar_placeholder_email(ci.client.email):
                client_name = (ci.attendee_name or ci.title or "Calendar event").strip()
            else:
                client_name = ci.client.name
        out.append(
            {
                "id": str(ci.id),
                "client_id": client_id,
                "client_name": client_name,
                "attendee_email": ci.attendee_email,
                "attendee_name": ci.attendee_name,
                "title": ci.title,
                "provider": ci.provider,
                "start_time": ci.start_time.isoformat() if ci.start_time else None,
                "end_time": ci.end_time.isoformat() if ci.end_time else None,
                "status": ci.status,
                "sale_closed": getattr(ci, "sale_closed", None),
                "no_show": getattr(ci, "no_show", None),
            }
        )
    return {"appointments": out, "count": len(out)}


def _failed_payments(
    db: Session,
    org_id: uuid.UUID,
    user_id: Optional[uuid.UUID],
    *,
    page_size: int = 25,
) -> Dict[str, Any]:
    from app.api.stripe import get_failed_payments
    from fastapi import HTTPException

    try:
        rows = get_failed_payments(
            page=1,
            page_size=max(1, min(int(page_size or 25), 50)),
            range_days=None,
            scope=None,
            use_treasury=True,
            exclude_resolved=True,
            current_user=_user_proxy(org_id, user_id),
            db=db,
        )
        return {"failed_payments": _serialize(rows), "count": len(rows or [])}
    except HTTPException as e:
        return {"failed_payments": [], "count": 0, "detail": e.detail, "connected": False}
    except Exception as e:
        return {"failed_payments": [], "count": 0, "error": str(e)}


def _leads_by_source(db: Session, org_id: uuid.UUID, user_id: Optional[uuid.UUID]) -> Dict[str, Any]:
    from app.api.funnels import get_funnel_analytics, list_funnels
    from fastapi import HTTPException

    try:
        funnels = list_funnels(
            client_id=None,
            db=db,
            current_user=_user_proxy(org_id, user_id),
        )
    except Exception as e:
        return {"funnels": [], "error": str(e)}

    funnel_list = _serialize(funnels) or []
    if not isinstance(funnel_list, list):
        funnel_list = []

    analytics_out: List[Dict[str, Any]] = []
    # Cap funnel deep-dives — this was the main cold-path timeout source.
    for f in funnel_list[:_MAX_FUNNELS_FOR_LEADS]:
        if not isinstance(f, dict):
            continue
        fid = f.get("id")
        if not fid:
            continue
        try:
            fid_uuid = uuid.UUID(str(fid))
        except ValueError:
            continue
        try:
            analytics = get_funnel_analytics(
                funnel_id=fid_uuid,
                range_days=30,
                db=db,
                current_user=_user_proxy(org_id, user_id),
            )
            analytics_out.append(
                {
                    "funnel_id": str(fid),
                    "funnel_name": f.get("name"),
                    "analytics": _serialize(analytics),
                }
            )
        except HTTPException as e:
            analytics_out.append(
                {"funnel_id": str(fid), "funnel_name": f.get("name"), "error": e.detail}
            )
        except Exception as e:
            analytics_out.append(
                {"funnel_id": str(fid), "funnel_name": f.get("name"), "error": str(e)}
            )

    return {
        "funnels": funnel_list,
        "analytics_by_funnel": analytics_out,
        "analytics_funnel_limit": _MAX_FUNNELS_FOR_LEADS,
        "funnels_total": len(funnel_list),
    }


def _cache_key(
    org_id: uuid.UUID,
    wanted: Set[str],
    *,
    finances_range_days: int,
    finances_scope: Optional[str],
    appointments_limit: int,
) -> str:
    return "|".join(
        [
            str(org_id),
            ",".join(sorted(wanted)),
            str(finances_range_days),
            str(finances_scope or ""),
            str(appointments_limit),
        ]
    )


def _cache_get(key: str) -> Optional[Dict[str, Any]]:
    now = time.monotonic()
    with _dashboard_cache_lock:
        hit = _dashboard_cache.get(key)
        if not hit:
            return None
        ts, payload = hit
        if now - ts > _CACHE_TTL_SEC:
            _dashboard_cache.pop(key, None)
            return None
        return payload


def _cache_set(key: str, payload: Dict[str, Any]) -> None:
    with _dashboard_cache_lock:
        _dashboard_cache[key] = (time.monotonic(), payload)
        # Bound memory: drop oldest if map grows large
        if len(_dashboard_cache) > 64:
            oldest = sorted(_dashboard_cache.items(), key=lambda kv: kv[1][0])[:16]
            for k, _ in oldest:
                _dashboard_cache.pop(k, None)


def _build_one_section(
    section: str,
    org_id: uuid.UUID,
    user_id: Optional[uuid.UUID],
    *,
    finances_range_days: int,
    finances_scope: Optional[str],
    appointments_limit: int,
) -> Tuple[str, Any]:
    db = SessionLocal()
    t0 = time.monotonic()
    try:
        if section == "summary":
            value = _terminal_summary(db, org_id, user_id)
        elif section == "monthly_trends":
            value = _monthly_trends(db, org_id)
        elif section == "finances":
            value = _finances_summary(
                db, org_id, user_id, range_days=finances_range_days, scope=finances_scope
            )
        elif section == "stripe":
            value = _stripe_summary(db, org_id, user_id)
        elif section == "calendar":
            value = _calendar_trend(db, org_id)
        elif section == "appointments":
            value = _upcoming_appointments(db, org_id, limit=appointments_limit)
        elif section == "failed_payments":
            value = _failed_payments(db, org_id, user_id)
        elif section == "leads":
            value = _leads_by_source(db, org_id, user_id)
        else:
            value = {"error": f"unknown section: {section}"}
        return section, value
    except Exception as e:
        logger.warning("mcp dashboard section %s failed: %s", section, e)
        return section, {"error": str(e)}
    finally:
        elapsed = time.monotonic() - t0
        if elapsed > 5:
            logger.info("mcp dashboard section %s took %.2fs", section, elapsed)
        db.close()


def build_terminal_dashboard_for_mcp(
    db: Session,
    org_id: uuid.UUID,
    *,
    user_id: Optional[uuid.UUID] = None,
    sections: Optional[List[str]] = None,
    mode: Optional[str] = None,
    finances_range_days: int = 30,
    finances_scope: Optional[str] = None,
    appointments_limit: int = 20,
) -> Dict[str, Any]:
    """
    Terminal dashboard snapshot for Claude.

    Default `mode=overview` returns a fast subset. Pass `mode=full` or an explicit
    `sections` list for heavier blocks (finances/stripe/calendar/leads).
    """
    mode_norm = (mode or "").strip().lower()
    wanted: Set[str]
    if sections:
        wanted = {str(s).strip().lower() for s in sections if str(s).strip()}
        wanted &= set(_SECTION_KEYS)
        if not wanted:
            wanted = set(_OVERVIEW_SECTIONS)
    elif mode_norm in ("full", "all", "complete"):
        wanted = set(_SECTION_KEYS)
    else:
        # overview / default
        wanted = set(_OVERVIEW_SECTIONS)

    appt_lim = max(1, min(int(appointments_limit or 20), 100))
    key = _cache_key(
        org_id,
        wanted,
        finances_range_days=finances_range_days,
        finances_scope=finances_scope,
        appointments_limit=appt_lim,
    )
    cached = _cache_get(key)
    if cached is not None:
        out = dict(cached)
        out["cache"] = {"hit": True, "ttl_seconds": _CACHE_TTL_SEC}
        return out

    started = time.monotonic()
    deadline = started + _SOFT_DEADLINE_SEC

    out: Dict[str, Any] = {
        "org_id": str(org_id),
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "mode": "full" if wanted == set(_SECTION_KEYS) else "overview",
        "sections_included": sorted(wanted),
        "sections_available": list(_SECTION_KEYS),
        "usage": (
            "Default is a fast overview (summary, monthly_trends, appointments, failed_payments). "
            "For finances/stripe/calendar/leads, call again with mode='full' or sections=[...]. "
            "If incomplete_sections is present, retry those sections only."
        ),
        "cache": {"hit": False, "ttl_seconds": _CACHE_TTL_SEC},
    }

    # Prefer overview order so critical KPIs finish first under the soft deadline.
    ordered = [s for s in _OVERVIEW_SECTIONS if s in wanted] + [
        s for s in _SECTION_KEYS if s in wanted and s not in _OVERVIEW_SECTIONS
    ]

    incomplete: List[str] = []
    timings: Dict[str, float] = {}

    # Parallel build — each worker uses its own DB session.
    max_workers = min(4, max(1, len(ordered)))
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {
            pool.submit(
                _build_one_section,
                section,
                org_id,
                user_id,
                finances_range_days=finances_range_days,
                finances_scope=finances_scope,
                appointments_limit=appt_lim,
            ): section
            for section in ordered
        }
        try:
            for fut in as_completed(futures, timeout=max(1.0, deadline - time.monotonic())):
                section = futures[fut]
                try:
                    name, value = fut.result()
                    out[name] = value
                    timings[name] = round(time.monotonic() - started, 3)
                except Exception as e:
                    out[section] = {"error": str(e)}
                    incomplete.append(section)
                if time.monotonic() >= deadline:
                    break
        except TimeoutError:
            pass

        for fut, section in futures.items():
            if section in out:
                continue
            if fut.done():
                try:
                    name, value = fut.result()
                    out[name] = value
                except Exception as e:
                    out[section] = {"error": str(e)}
                    incomplete.append(section)
            else:
                fut.cancel()
                incomplete.append(section)
                out[section] = {
                    "error": "deadline_exceeded",
                    "hint": f"Retry get_terminal_dashboard with sections=['{section}']",
                }

    if incomplete:
        out["incomplete_sections"] = sorted(set(incomplete))
        out["partial"] = True
    out["elapsed_seconds"] = round(time.monotonic() - started, 3)
    out["section_timings_seconds"] = timings

    # Only cache complete successful builds (no deadline cuts).
    if not incomplete:
        _cache_set(key, out)

    logger.info(
        "mcp terminal dashboard org=%s mode=%s sections=%s elapsed=%.2fs incomplete=%s",
        org_id,
        out.get("mode"),
        sorted(wanted),
        out["elapsed_seconds"],
        incomplete,
    )
    return out

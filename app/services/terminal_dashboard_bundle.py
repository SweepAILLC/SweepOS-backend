"""
Terminal dashboard packages for MCP (Claude custom connector).

Reuses existing Terminal / finances / calendar / Stripe / funnel endpoints so Claude
sees the same numbers as the in-app Terminal tab.
"""
from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any, Dict, List, Optional, Set

from sqlalchemy.orm import Session, joinedload

from app.models.client_checkin import ClientCheckIn
from app.models.organization import Organization
from app.services.calendar_booking_time import effective_end_sql_expression
from app.services.calendar_trend_summary import compute_calendar_trend_summary
from app.services.checkin_sync import is_calendar_placeholder_email
from app.services.terminal_metrics_service import get_or_build_terminal_monthly_trends

logger = logging.getLogger(__name__)


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
    limit: int = 40,
) -> Dict[str, Any]:
    lim = max(1, min(int(limit or 40), 100))
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
    for f in funnel_list[:20]:
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

    return {"funnels": funnel_list, "analytics_by_funnel": analytics_out}


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


def build_terminal_dashboard_for_mcp(
    db: Session,
    org_id: uuid.UUID,
    *,
    user_id: Optional[uuid.UUID] = None,
    sections: Optional[List[str]] = None,
    finances_range_days: int = 30,
    finances_scope: Optional[str] = None,
    appointments_limit: int = 40,
) -> Dict[str, Any]:
    """
    Full Terminal dashboard snapshot for Claude.
    `sections` optional subset of: summary, monthly_trends, finances, stripe,
    calendar, appointments, failed_payments, leads.
    """
    wanted: Set[str]
    if sections:
        wanted = {str(s).strip().lower() for s in sections if str(s).strip()}
        wanted &= set(_SECTION_KEYS)
        if not wanted:
            wanted = set(_SECTION_KEYS)
    else:
        wanted = set(_SECTION_KEYS)

    out: Dict[str, Any] = {
        "org_id": str(org_id),
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "sections_included": sorted(wanted),
        "usage": (
            "This mirrors the SweepOS Terminal dashboard: cash/MRR, monthly trends, "
            "finances KPIs, Stripe summary, calendar show-up/close rates, upcoming "
            "appointments, failed-payment queue, and funnel/leads analytics."
        ),
    }

    if "summary" in wanted:
        out["summary"] = _terminal_summary(db, org_id, user_id)
    if "monthly_trends" in wanted:
        out["monthly_trends"] = _monthly_trends(db, org_id)
    if "finances" in wanted:
        out["finances"] = _finances_summary(
            db, org_id, user_id, range_days=finances_range_days, scope=finances_scope
        )
    if "stripe" in wanted:
        out["stripe"] = _stripe_summary(db, org_id, user_id)
    if "calendar" in wanted:
        out["calendar"] = _calendar_trend(db, org_id)
    if "appointments" in wanted:
        out["appointments"] = _upcoming_appointments(db, org_id, limit=appointments_limit)
    if "failed_payments" in wanted:
        out["failed_payments"] = _failed_payments(db, org_id, user_id)
    if "leads" in wanted:
        out["leads"] = _leads_by_source(db, org_id, user_id)

    return out

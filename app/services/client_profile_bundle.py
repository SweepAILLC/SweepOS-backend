"""
Aggregate a single client profile package for REST + MCP tools.

Fields:
  - contact / identity
  - pipeline + program stage
  - financial investments
  - offer enrollment + balance due
  - call analysis profile + ROI tags
  - workspace (org) info
"""
from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy.orm import Session

from app.models.client import Client
from app.models.organization import Organization
from app.schemas.client import ClientOfferEnrollmentPublic


def _offer_public(raw: Any) -> Optional[Dict[str, Any]]:
    if not raw or not isinstance(raw, dict):
        return None
    try:
        pub = ClientOfferEnrollmentPublic.model_validate(raw)
        return pub.model_dump()
    except Exception:
        total = int(raw.get("total_cents") or 0)
        paid = int(raw.get("paid_cents") or 0)
        out = dict(raw)
        out["balance_cents"] = total - paid
        return out


def _trim_insight_json(insight: Any, max_chars: int = 12000) -> Any:
    """Keep call analysis useful but bounded for MCP tool result size."""
    if not isinstance(insight, dict):
        return insight
    out = dict(insight)
    # Drop bulky transcript/clip payloads if present
    for heavy in ("transcript", "full_transcript", "raw_transcript", "clips"):
        if heavy in out and isinstance(out[heavy], (str, list)):
            if isinstance(out[heavy], str) and len(out[heavy]) > 2000:
                out[heavy] = out[heavy][:2000] + "…"
            elif isinstance(out[heavy], list) and len(out[heavy]) > 8:
                out[heavy] = out[heavy][:8]
    # Soft cap whole JSON via string length check later
    return out


def build_client_profile_bundle(
    db: Session,
    org_id: uuid.UUID,
    client_id: uuid.UUID,
    *,
    include_payments: bool = True,
    include_call_insights: bool = True,
    payment_limit: int = 50,
    insight_limit: int = 10,
) -> Optional[Dict[str, Any]]:
    client = (
        db.query(Client)
        .filter(Client.id == client_id, Client.org_id == org_id)
        .first()
    )
    if not client:
        return None

    org = db.query(Organization).filter(Organization.id == org_id).first()

    contact = {
        "first_name": client.first_name,
        "last_name": client.last_name,
        "email": client.email,
        "emails": client.emails if isinstance(client.emails, list) else [],
        "phone": client.phone,
        "instagram": client.instagram,
    }

    pipeline = {
        "lifecycle_state": (
            client.lifecycle_state.value
            if hasattr(client.lifecycle_state, "value")
            else str(client.lifecycle_state)
        ),
        "program_start_date": client.program_start_date.isoformat() if client.program_start_date else None,
        "program_end_date": client.program_end_date.isoformat() if client.program_end_date else None,
        "program_duration_days": client.program_duration_days,
        "program_progress_percent": (
            float(client.program_progress_percent)
            if client.program_progress_percent is not None
            else None
        ),
        "last_activity_at": client.last_activity_at.isoformat() if client.last_activity_at else None,
    }

    financials: Dict[str, Any] = {
        "lifetime_revenue_cents": client.lifetime_revenue_cents or 0,
        "estimated_mrr": float(client.estimated_mrr) if client.estimated_mrr is not None else None,
        "stripe_customer_id": client.stripe_customer_id,
        "total_amount_paid_cents": client.lifetime_revenue_cents or 0,
        "payments": [],
    }

    if include_payments:
        try:
            from app.api.clients.payments import get_client_payments
            # Prefer calling the internal aggregation by reusing query logic via a thin helper.
            # Importing the route function is awkward (Depends); duplicate a light summary instead.
            from app.models.stripe_payment import StripePayment
            from app.models.manual_payment import ManualPayment
            from app.models.whop_payment import WhopPayment

            payments_out: List[Dict[str, Any]] = []
            total = 0

            stripe_rows = (
                db.query(StripePayment)
                .filter(StripePayment.org_id == org_id, StripePayment.client_id == client_id)
                .order_by(StripePayment.created_at.desc())
                .limit(payment_limit)
                .all()
            )
            for p in stripe_rows:
                if (p.status or "") == "succeeded":
                    total += int(p.amount_cents or 0)
                payments_out.append(
                    {
                        "source": "stripe",
                        "id": str(p.id),
                        "amount_cents": p.amount_cents,
                        "status": p.status,
                        "type": p.type,
                        "created_at": p.created_at.isoformat() if p.created_at else None,
                    }
                )

            manual_rows = (
                db.query(ManualPayment)
                .filter(ManualPayment.org_id == org_id, ManualPayment.client_id == client_id)
                .order_by(ManualPayment.payment_date.desc())
                .limit(payment_limit)
                .all()
            )
            for p in manual_rows:
                total += int(p.amount_cents or 0)
                payments_out.append(
                    {
                        "source": "manual",
                        "id": str(p.id),
                        "amount_cents": p.amount_cents,
                        "status": "succeeded",
                        "description": p.description,
                        "created_at": p.payment_date.isoformat() if p.payment_date else None,
                    }
                )

            try:
                whop_rows = (
                    db.query(WhopPayment)
                    .filter(WhopPayment.org_id == org_id, WhopPayment.client_id == client_id)
                    .order_by(WhopPayment.created_at.desc())
                    .limit(payment_limit)
                    .all()
                )
                for p in whop_rows:
                    if (getattr(p, "status", None) or "") in ("succeeded", "paid", "complete"):
                        total += int(p.amount_cents or 0)
                    payments_out.append(
                        {
                            "source": "whop",
                            "id": str(p.id),
                            "amount_cents": p.amount_cents,
                            "status": getattr(p, "status", None),
                            "created_at": p.created_at.isoformat() if p.created_at else None,
                        }
                    )
            except Exception:
                pass

            payments_out.sort(key=lambda x: x.get("created_at") or "", reverse=True)
            financials["payments"] = payments_out[:payment_limit]
            financials["total_amount_paid_cents"] = total or (client.lifetime_revenue_cents or 0)
            financials["total_amount_paid"] = (financials["total_amount_paid_cents"] or 0) / 100.0
        except Exception:
            financials["total_amount_paid"] = (client.lifetime_revenue_cents or 0) / 100.0

    offer = _offer_public(client.offer_enrollment)

    call_analysis: Dict[str, Any] = {
        "summary": None,
        "roi_tags": [],
        "roi_state": None,
        "rollup": None,
        "recent_insights": [],
        "offer_suggestion": None,
    }
    if include_call_insights:
        try:
            from app.services.call_insight_service import get_client_insights_response

            raw = get_client_insights_response(db, org_id, client_id, limit=insight_limit) or {}
            summary = raw.get("summary")
            call_analysis["summary"] = summary
            if isinstance(summary, dict) and isinstance(summary.get("tags"), list):
                call_analysis["roi_tags"] = list(summary["tags"])
            if isinstance(client.meta, dict) and isinstance(client.meta.get("roi_state"), dict):
                call_analysis["roi_state"] = dict(client.meta["roi_state"])
            call_analysis["rollup"] = raw.get("rollup")
            call_analysis["offer_suggestion"] = raw.get("offer_suggestion")
            recent = []
            for row in (raw.get("insights") or [])[:insight_limit]:
                if not isinstance(row, dict):
                    continue
                recent.append(
                    {
                        "id": row.get("id"),
                        "meeting_at": row.get("meeting_at"),
                        "status": row.get("status"),
                        "computed_at": row.get("computed_at"),
                        "insight": _trim_insight_json(row.get("insight")),
                        "failure_reason": row.get("failure_reason"),
                    }
                )
            call_analysis["recent_insights"] = recent
        except Exception as e:
            call_analysis["error"] = str(e)[:200]

    workspace = {
        "org_id": str(org_id),
        "org_name": org.name if org else None,
    }

    notes = (client.notes or "")[:4000] if client.notes else None

    return {
        "client_id": str(client.id),
        "contact": contact,
        "pipeline": pipeline,
        "financials": financials,
        "offer": offer,
        "call_analysis": call_analysis,
        "workspace": workspace,
        "notes": notes,
        "meta": {
            "roi_state": (client.meta or {}).get("roi_state") if isinstance(client.meta, dict) else None,
            "follow_up_due_at": (client.meta or {}).get("follow_up_due_at") if isinstance(client.meta, dict) else None,
        },
        "generated_at": datetime.utcnow().isoformat() + "Z",
    }


def list_clients_for_mcp(
    db: Session,
    org_id: uuid.UUID,
    *,
    query: Optional[str] = None,
    lifecycle_state: Optional[str] = None,
    limit: int = 50,
) -> List[Dict[str, Any]]:
    q = db.query(Client).filter(Client.org_id == org_id)
    if lifecycle_state:
        q = q.filter(Client.lifecycle_state == lifecycle_state)
    if query:
        like = f"%{query.strip()}%"
        q = q.filter(
            (Client.email.ilike(like))
            | (Client.first_name.ilike(like))
            | (Client.last_name.ilike(like))
            | (Client.phone.ilike(like))
        )
    rows = q.order_by(Client.updated_at.desc()).limit(min(limit, 100)).all()
    out = []
    for c in rows:
        out.append(
            {
                "client_id": str(c.id),
                "first_name": c.first_name,
                "last_name": c.last_name,
                "email": c.email,
                "lifecycle_state": (
                    c.lifecycle_state.value
                    if hasattr(c.lifecycle_state, "value")
                    else str(c.lifecycle_state)
                ),
                "lifetime_revenue_cents": c.lifetime_revenue_cents or 0,
            }
        )
    return out


def search_clients_by_email(db: Session, org_id: uuid.UUID, email: str) -> List[Dict[str, Any]]:
    from app.models.client import Client as ClientModel

    email_n = (email or "").strip().lower()
    if not email_n:
        return []
    rows = (
        db.query(ClientModel)
        .filter(ClientModel.org_id == org_id, ClientModel.email.ilike(email_n))
        .limit(20)
        .all()
    )
    # Also match emails JSON loosely
    extra = (
        db.query(ClientModel)
        .filter(ClientModel.org_id == org_id)
        .limit(500)
        .all()
    )
    seen = {r.id for r in rows}
    for c in extra:
        if c.id in seen:
            continue
        emails = []
        if c.email:
            emails.append(c.email.lower())
        if isinstance(c.emails, list):
            emails.extend([str(e).lower() for e in c.emails if e])
        if email_n in emails:
            rows.append(c)
            seen.add(c.id)
    return [
        {
            "client_id": str(c.id),
            "first_name": c.first_name,
            "last_name": c.last_name,
            "email": c.email,
            "lifecycle_state": (
                c.lifecycle_state.value
                if hasattr(c.lifecycle_state, "value")
                else str(c.lifecycle_state)
            ),
        }
        for c in rows[:20]
    ]

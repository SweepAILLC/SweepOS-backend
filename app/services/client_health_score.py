"""
Logic-based client/lead health scoring. Designed so factors can be fed into an AI layer later
for referral/testimonial/retention/upsell recommendations.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
import uuid

from sqlalchemy.orm import Session, defer

from app.models.client import Client
from app.models.client_checkin import ClientCheckIn
from app.models.stripe_payment import StripePayment
from app.models.event import Event


def _factor_show_rate(db: Session, client_id: uuid.UUID, org_id: uuid.UUID) -> Dict[str, Any]:
    """
    Show rate / check-in rate: of past scheduled (non-cancelled) check-ins, what share were attended (completed, not no-show).
    """
    now = datetime.now(timezone.utc)
    past = db.query(ClientCheckIn).options(
        defer(ClientCheckIn.is_sales_call),
        defer(ClientCheckIn.sale_closed),
    ).filter(
        ClientCheckIn.client_id == client_id,
        ClientCheckIn.org_id == org_id,
        ClientCheckIn.cancelled == False,
        ClientCheckIn.start_time <= now,
    ).all()

    total = len(past)
    attended = sum(1 for c in past if c.completed and not getattr(c, "no_show", False))

    rate = (attended / total * 100.0) if total else None
    return {
        "key": "show_rate",
        "label": "Show / check-in rate",
        "value": round(rate, 1) if rate is not None else None,
        "raw": {"scheduled": total, "attended": attended},
        "unit": "percent",
        "description": f"{attended} of {total} past check-ins attended" if total else "No past check-ins",
    }


def _factor_failed_payments(db: Session, client_id: uuid.UUID, org_id: uuid.UUID) -> Dict[str, Any]:
    """Count of failed Stripe payments for this client."""
    count = db.query(StripePayment).filter(
        StripePayment.client_id == client_id,
        StripePayment.org_id == org_id,
        StripePayment.status == "failed",
    ).count()

    return {
        "key": "failed_payments",
        "label": "Failed payments",
        "value": count,
        "raw": {"count": count},
        "unit": "count",
        "description": f"{count} failed payment(s)" if count else "No failed payments",
    }


def _factor_program_timeline(client: Client) -> Dict[str, Any]:
    """
    Where they are on program: program_progress_percent if they have a program;
    otherwise tenure (days as client since created_at).
    """
    if client.program_start_date and (client.program_duration_days or client.program_end_date):
        progress = client.program_progress_percent
        if progress is None and hasattr(client, "calculate_progress"):
            progress = client.calculate_progress()
        return {
            "key": "program_timeline",
            "label": "Program progress",
            "value": round(float(progress), 1) if progress is not None else None,
            "raw": {
                "program_start_date": client.program_start_date.isoformat() if client.program_start_date else None,
                "program_end_date": client.program_end_date.isoformat() if getattr(client, "program_end_date", None) else None,
                "program_duration_days": client.program_duration_days,
                "progress_percent": float(progress) if progress is not None else None,
            },
            "unit": "percent",
            "description": f"{progress:.0f}% through program" if progress is not None else "No program set",
        }
    # Tenure: days as client
    created = client.created_at or datetime.utcnow()
    if hasattr(created, "replace") and getattr(created, "tzinfo", None) is None:
        created = created.replace(tzinfo=timezone.utc)
    now = datetime.now(timezone.utc)
    tenure_days = max(0, (now - created).days)
    return {
        "key": "program_timeline",
        "label": "Tenure (days as client)",
        "value": tenure_days,
        "raw": {"tenure_days": tenure_days, "created_at": client.created_at.isoformat() if client.created_at else None},
        "unit": "days",
        "description": f"{tenure_days} days as client",
    }


def _factor_days_since_last_contact(
    db: Session, client_id: uuid.UUID, org_id: uuid.UUID
) -> Dict[str, Any]:
    """
    Time since last contact: latest of funnel/email events (Event) or completed check-in (ClientCheckIn).
    """
    # Latest event (funnel, message, etc.) for this client
    latest_event = (
        db.query(Event)
        .filter(Event.client_id == client_id, Event.org_id == org_id)
        .order_by(Event.occurred_at.desc())
        .limit(1)
        .first()
    )
    # Latest completed check-in
    latest_checkin = (
        db.query(ClientCheckIn)
        .options(
            defer(ClientCheckIn.is_sales_call),
            defer(ClientCheckIn.sale_closed),
        )
        .filter(
            ClientCheckIn.client_id == client_id,
            ClientCheckIn.org_id == org_id,
            ClientCheckIn.completed == True,
        )
        .order_by(ClientCheckIn.start_time.desc())
        .limit(1)
        .first()
    )

    candidates = []
    if latest_event and latest_event.occurred_at:
        candidates.append(latest_event.occurred_at)
    if latest_checkin and latest_checkin.start_time:
        candidates.append(latest_checkin.start_time)

    if not candidates:
        return {
            "key": "days_since_last_contact",
            "label": "Days since last contact",
            "value": None,
            "raw": {"last_contact_at": None},
            "unit": "days",
            "description": "No contact events or check-ins found",
        }

    last_contact = max(candidates)
    if getattr(last_contact, "tzinfo", None) is None:
        last_contact = last_contact.replace(tzinfo=timezone.utc)
    now = datetime.now(timezone.utc)
    days = max(0, (now - last_contact).days)

    return {
        "key": "days_since_last_contact",
        "label": "Days since last contact",
        "value": days,
        "raw": {"last_contact_at": last_contact.isoformat(), "days": days},
        "unit": "days",
        "description": f"{days} days since last contact",
    }


def _score_from_factors(
    factors: List[Dict[str, Any]],
    campaign_open_rate: Optional[float],
    emails_sent: Optional[int] = None,
) -> float:
    """
    Combine factors into a 0–100 health score. Logic-based; tuned so AI can override later.
    - show_rate: higher is better (weight ~25)
    - email (campaign) open rate: higher is better (weight ~20); only applied when emails were sent.
    - failed_payments: lower is better (weight ~25)
    - program_timeline: used for context, not penalizing
    - days_since_last_contact: lower is better (weight ~30)
    """
    score = 50.0  # baseline

    for f in factors:
        key = f.get("key")
        value = f.get("value")
        raw = f.get("raw") or {}

        if key == "show_rate" and value is not None:
            score += (value / 100.0 - 0.5) * 50
        if key == "failed_payments":
            count = raw.get("count", 0) or 0
            if count >= 2:
                score -= 25
            elif count == 1:
                score -= 12
        if key == "days_since_last_contact" and value is not None:
            if value <= 7:
                score += 15
            elif value <= 30:
                score += 5
            elif value <= 60:
                score -= 5
            else:
                score -= 20

    if campaign_open_rate is not None and (emails_sent is None or emails_sent > 0):
        score += (campaign_open_rate / 100.0 - 0.25) * 40

    return max(0.0, min(100.0, round(score, 1)))


def compute_health_factors(
    db: Session, client: Client, org_id: uuid.UUID
) -> List[Dict[str, Any]]:
    """Compute all DB-backed factors for a client. Caller can add email_open_rate from Brevo."""
    factors = [
        _factor_show_rate(db, client.id, org_id),
        _factor_failed_payments(db, client.id, org_id),
        _factor_program_timeline(client),
        _factor_days_since_last_contact(db, client.id, org_id),
    ]
    return factors


def get_health_score(
    db: Session,
    client_id: uuid.UUID,
    org_id: uuid.UUID,
    brevo_email_stats: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Full health score for a client. Returns score (0–100), grade, and factors list.
    brevo_email_stats: optional dict with campaign_open_rate, campaign_click_rate, messages_sent,
                       trans_open_rate, trans_click_rate (from Brevo).
    """
    client = db.query(Client).filter(
        Client.id == client_id,
        Client.org_id == org_id,
    ).first()
    if not client:
        return {}

    campaign_open = brevo_email_stats.get("campaign_open_rate") if brevo_email_stats else None
    emails_sent = brevo_email_stats.get("messages_sent") if brevo_email_stats else None

    factors = compute_health_factors(db, client, org_id)
    score = _score_from_factors(factors, campaign_open, emails_sent)

    # Letter grade for quick scan
    if score >= 80:
        grade = "A"
    elif score >= 65:
        grade = "B"
    elif score >= 50:
        grade = "C"
    elif score >= 35:
        grade = "D"
    else:
        grade = "F"

    # Add email engagement factor (campaign + transactional) whenever we have Brevo data
    if brevo_email_stats is not None:
        messages_sent = brevo_email_stats.get("messages_sent", 0)
        campaign_open = brevo_email_stats.get("campaign_open_rate")
        campaign_click = brevo_email_stats.get("campaign_click_rate")
        trans_open = brevo_email_stats.get("trans_open_rate")
        trans_click = brevo_email_stats.get("trans_click_rate")

        parts = []
        if messages_sent > 0 and campaign_open is not None:
            open_str = f"{campaign_open:.0f}% open"
            click_str = f"{campaign_click:.0f}% click" if campaign_click is not None else "—% click"
            parts.append(f"Campaign: {open_str}, {click_str} ({messages_sent} sent)")
        elif messages_sent == 0:
            parts.append("Campaign: no emails sent (not used in score)")
        if trans_open is not None or trans_click is not None:
            to = f"{trans_open:.0f}% open" if trans_open is not None else "—% open"
            tc = f"{trans_click:.0f}% click" if trans_click is not None else "—% click"
            parts.append(f"Transactional: {to}, {tc}")
        description = "Brevo, last 90 days. " + "; ".join(parts) if parts else "No Brevo data"

        value_for_display = round(campaign_open, 1) if campaign_open is not None and messages_sent > 0 else None
        factors.append({
            "key": "email_open_rate",
            "label": "Email engagement (Brevo)",
            "value": value_for_display,
            "raw": {
                "campaign_open_rate": campaign_open,
                "campaign_click_rate": campaign_click,
                "emails_sent": messages_sent,
                "trans_open_rate": trans_open,
                "trans_click_rate": trans_click,
            },
            "unit": "percent",
            "description": description,
        })

    return {
        "client_id": str(client_id),
        "score": score,
        "grade": grade,
        "factors": factors,
        "computed_at": datetime.utcnow().isoformat() + "Z",
    }

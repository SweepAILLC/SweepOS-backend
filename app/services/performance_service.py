"""
Deterministic Performance snapshot + ROI-ranked tasks for the Performance tab.
Funnel step queries mirror logic in app.api.funnels.get_funnel_analytics (same org scoping).
"""

from __future__ import annotations

import re
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set, Tuple

from sqlalchemy import func, asc, desc
from sqlalchemy.orm import Session

from app.models.client import Client, LifecycleState
from app.models.client_ai_recommendation_state import ClientAIRecommendationState
from app.models.event import Event
from app.models.funnel import Funnel, FunnelStep
from app.models.manual_payment import ManualPayment
from app.models.stripe_payment import StripePayment
from app.models.stripe_subscription import StripeSubscription
from app.api.stripe import check_stripe_connected
from app.services.call_insight_service import get_call_insight_tags_batch
from app.services.client_ai_recommendations_service import (
    _CATEGORY_TO_PRIORITY_ID,
    get_recommendation_state_dict,
)
from app.services.health_score_cache_service import batch_read_cached_health_scores, resolve_health_score
from app.services.offer_ladder import (
    extract_offer_ladder,
    match_offer_for_client,
)
from app.services.roi_signal_validation import client_has_expansion_win_basis


def _normalize_email(email: str | None) -> str | None:
    if not email:
        return None
    return re.sub(r"\s+", "", email.lower().strip()) or None


def _org_mrr(db: Session, org_id: uuid.UUID) -> float:
    current_mrr = 0.0
    mrr_result = (
        db.query(func.coalesce(func.sum(StripeSubscription.mrr), 0))
        .filter(
            StripeSubscription.org_id == org_id,
            StripeSubscription.status.in_(["active", "trialing"]),
        )
        .scalar()
    )
    if mrr_result is not None:
        try:
            current_mrr = float(mrr_result)
        except (TypeError, ValueError):
            pass
    if current_mrr > 0:
        return current_mrr
    clients = db.query(Client).filter(Client.org_id == org_id).all()
    grouped: Dict[str, List[Client]] = {}
    processed = set()
    for c in clients:
        if c.id in processed:
            continue
        key = _normalize_email(c.email) if c.email else (
            f"stripe:{c.stripe_customer_id}" if c.stripe_customer_id else str(c.id)
        )
        if key not in grouped:
            grouped[key] = []
        same = [
            x
            for x in clients
            if x.id not in processed
            and (
                (_normalize_email(x.email) == _normalize_email(c.email) and c.email)
                or (
                    x.stripe_customer_id == c.stripe_customer_id
                    and c.stripe_customer_id
                    and not c.email
                )
                or (x.id == c.id)
            )
        ]
        for x in same:
            grouped[key].append(x)
            processed.add(x.id)
    for group in grouped.values():
        max_mrr = max((float(c.estimated_mrr or 0) for c in group), default=0)
        current_mrr += max_mrr
    return current_mrr


def _cash_collected_between(
    db: Session,
    org_id: uuid.UUID,
    start: datetime,
    end: Optional[datetime] = None,
) -> float:
    """Succeeded Stripe (deduped by stripe_id) + manual payments in [start, end). End None = no upper bound."""
    total = 0.0
    seen: set[str] = set()
    q = (
        db.query(StripePayment)
        .filter(
            StripePayment.org_id == org_id,
            StripePayment.status == "succeeded",
            StripePayment.created_at >= start,
        )
    )
    if end is not None:
        q = q.filter(StripePayment.created_at < end)
    for p in q.all():
        if p.stripe_id and p.stripe_id in seen:
            continue
        if p.stripe_id:
            seen.add(p.stripe_id)
        total += (p.amount_cents or 0) / 100.0

    mp_q = (
        db.query(ManualPayment)
        .filter(
            ManualPayment.org_id == org_id,
            ManualPayment.payment_date >= start,
        )
    )
    if end is not None:
        mp_q = mp_q.filter(ManualPayment.payment_date < end)
    for p in mp_q.all():
        total += (p.amount_cents or 0) / 100.0
    return total


def _cash_last_30_days(db: Session, org_id: uuid.UUID) -> float:
    now = datetime.utcnow()
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    thirty_days_ago = today_start - timedelta(days=30)
    return _cash_collected_between(db, org_id, thirty_days_ago, now)


def _lifecycle_counts(db: Session, org_id: uuid.UUID) -> Dict[str, int]:
    rows = (
        db.query(Client.lifecycle_state, func.count(Client.id))
        .filter(Client.org_id == org_id)
        .group_by(Client.lifecycle_state)
        .all()
    )
    out: Dict[str, int] = {s.value: 0 for s in LifecycleState}
    for st, cnt in rows:
        key = st.value if hasattr(st, "value") else str(st)
        out[key] = int(cnt or 0)
    return out


def _failed_payments_summary(db: Session, org_id: uuid.UUID) -> Tuple[int, List[Dict[str, Any]]]:
    q = (
        db.query(StripePayment)
        .filter(
            StripePayment.org_id == org_id,
            StripePayment.status.in_(["failed", "past_due"]),
        )
        .order_by(StripePayment.created_at.desc())
        .limit(50)
        .all()
    )
    seen: set[str] = set()
    unique_groups: Dict[str, StripePayment] = {}
    for p in q:
        key = p.invoice_id or p.subscription_id or p.stripe_id or str(p.id)
        if key in seen:
            continue
        seen.add(key)
        unique_groups[key] = p
    items = list(unique_groups.values())
    items.sort(key=lambda x: x.created_at or datetime.min, reverse=True)
    count = len(items)
    samples: List[Dict[str, Any]] = []
    for p in items[:8]:
        samples.append(
            {
                "id": str(p.id),
                "amount_cents": p.amount_cents or 0,
                "currency": p.currency or "usd",
                "created_at": p.created_at.isoformat() if p.created_at else None,
                "client_id": str(p.client_id) if p.client_id else None,
            }
        )
    return count, samples


def _funnel_step_summary(
    db: Session,
    org_id: uuid.UUID,
    funnel_id: uuid.UUID,
    range_days: int = 30,
    *,
    window_start: Optional[datetime] = None,
    window_end: Optional[datetime] = None,
) -> Optional[Dict[str, Any]]:
    funnel = (
        db.query(Funnel)
        .filter(Funnel.id == funnel_id, Funnel.org_id == org_id)
        .first()
    )
    if not funnel:
        return None
    steps = (
        db.query(FunnelStep)
        .filter(FunnelStep.funnel_id == funnel_id, FunnelStep.org_id == org_id)
        .order_by(asc(FunnelStep.step_order))
        .all()
    )
    if not steps:
        return {
            "funnel_id": str(funnel_id),
            "name": funnel.name or "Funnel",
            "range_days": range_days,
            "total_visitors": 0,
            "total_conversions": 0,
            "overall_conversion_rate_pct": 0.0,
            "step_drops": [],
        }
    if window_start is not None and window_end is not None:
        end_date = window_end
        start_date = window_start
        range_days = max(1, int((end_date - start_date).total_seconds() // 86400))
    else:
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=range_days)
    # Explicit windows use half-open [start, end) so adjacent 30d periods do not double-count.
    use_exclusive_end = window_start is not None and window_end is not None
    step_drops: List[Dict[str, Any]] = []
    previous_count: Optional[int] = None
    for step in steps:
        count = (
            db.query(func.count(Event.id))
            .filter(
                Event.funnel_id == funnel_id,
                Event.org_id == org_id,
                Event.event_name == step.event_name,
                Event.occurred_at >= start_date,
                Event.occurred_at < end_date if use_exclusive_end else Event.occurred_at <= end_date,
            )
            .scalar()
            or 0
        )
        conv = None
        if previous_count is not None and previous_count > 0:
            conv = (count / previous_count) * 100.0
        step_drops.append(
            {
                "step_order": step.step_order,
                "label": step.label,
                "event_name": step.event_name,
                "count": int(count),
                "conversion_rate_pct": conv,
            }
        )
        previous_count = int(count)
    first_step = steps[0]
    total_visitors = 0
    if first_step:
        total_visitors = (
            db.query(func.count(func.distinct(Event.visitor_id)))
            .filter(
                Event.funnel_id == funnel_id,
                Event.org_id == org_id,
                Event.event_name == first_step.event_name,
                Event.occurred_at >= start_date,
                Event.occurred_at < end_date if use_exclusive_end else Event.occurred_at <= end_date,
                Event.visitor_id.isnot(None),
            )
            .scalar()
            or 0
        )
    last_step = steps[-1]
    total_conversions = (
        db.query(func.count(func.distinct(Event.visitor_id)))
        .filter(
            Event.funnel_id == funnel_id,
            Event.org_id == org_id,
            Event.event_name == last_step.event_name,
            Event.occurred_at >= start_date,
            Event.occurred_at < end_date if use_exclusive_end else Event.occurred_at <= end_date,
            Event.visitor_id.isnot(None),
        )
        .scalar()
        or 0
    )
    overall = 0.0
    if total_visitors > 0:
        overall = (total_conversions / total_visitors) * 100.0
    return {
        "funnel_id": str(funnel_id),
        "name": funnel.name or "Funnel",
        "range_days": range_days,
        "total_visitors": int(total_visitors),
        "total_conversions": int(total_conversions),
        "overall_conversion_rate_pct": round(overall, 2),
        "step_drops": step_drops,
    }


def _pipeline_priorities_from_user(ai_profile: Any) -> Optional[List[str]]:
    if not ai_profile or not isinstance(ai_profile, dict):
        return None
    pp = ai_profile.get("pipeline_priorities")
    if isinstance(pp, list) and all(isinstance(x, str) for x in pp):
        return pp
    return None


def _priority_boost(category: str, priorities: Optional[List[str]]) -> float:
    """
    Boost org-level tasks when their theme matches Intelligence `pipeline_priorities`
    (testimonials, revenue, retention, conversion, referrals, win_back, onboarding, content).
    """
    if not priorities:
        return 0.0
    keys_by_cat: Dict[str, Set[str]] = {
        "pipeline": {"conversion", "retention", "onboarding", "win_back"},
        "funnel": {"conversion", "content"},
        "payments": {"revenue", "retention"},
        "revenue": {"revenue"},
    }
    keys = keys_by_cat.get(category, set())
    bonus = 0.0
    for i, p in enumerate(priorities):
        if p in keys:
            bonus = max(bonus, 12.0 - float(i) * 1.5)
    return bonus


ROI_BOARD_TAGS = frozenset({"testimonial", "upsell", "referral", "conversion", "deal_follow_up"})
_ROI_TAG_TO_PRIORITY: Dict[str, str] = {
    "testimonial": "testimonials",
    "upsell": "revenue",
    "referral": "referrals",
    "conversion": "conversion",
    "deal_follow_up": "conversion",
}
_ROI_TAG_WEIGHT = {"testimonial": 5.0, "upsell": 6.5, "referral": 6.5, "conversion": 4.0, "deal_follow_up": 4.0}


def _roi_tags_priority_boost(tags: List[str], priorities: Optional[List[str]]) -> float:
    """Extra impact when surfaced ROI chips align with the coach's ordered Intelligence priorities."""
    if not priorities or not tags:
        return 0.0
    bonus = 0.0
    seen_pid: Set[str] = set()
    for t in tags:
        pid = _ROI_TAG_TO_PRIORITY.get(str(t).lower().strip())
        if not pid or pid not in priorities or pid in seen_pid:
            continue
        seen_pid.add(pid)
        rank = priorities.index(pid)
        bonus += max(0.0, 16.0 - float(rank) * 1.75)
    return bonus


MAX_ROI_SIGNAL_TASKS = 32


def build_roi_signal_tasks(
    db: Session,
    org_id: uuid.UUID,
    pipeline_priorities: Optional[List[str]],
    completed_ids: set[str],
    offer_ladder: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """
    One task per client with non-empty ROI-style call-insight tags (same chips as Kanban / drawer).
    Ranked by MRR, tag strength, health, and alignment with Intelligence pipeline_priorities.
    When `offer_ladder` is provided, each task also gets a deterministic offer suggestion
    drawn from the ladder, tailored to that client's prospect voice profile.
    """
    candidates = (
        db.query(Client)
        .filter(
            Client.org_id == org_id,
            Client.lifecycle_state.in_(
                (
                    LifecycleState.ACTIVE,
                    LifecycleState.OFFBOARDING,
                    LifecycleState.WARM_LEAD,
                    LifecycleState.COLD_LEAD,
                )
            ),
        )
        .order_by(desc(Client.estimated_mrr), desc(Client.updated_at))
        .limit(320)
        .all()
    )
    if not candidates:
        return []
    cids = [c.id for c in candidates]
    tag_map = get_call_insight_tags_batch(db, org_id, cids)
    health = batch_read_cached_health_scores(db, cids, org_id)
    missing_h = [cid for cid in cids if cid not in health][:100]
    for cid in missing_h:
        try:
            filled = resolve_health_score(
                db,
                cid,
                org_id,
                brevo_email_stats=None,
                use_ai=False,
                persist_cache=True,
                record_outcome_snapshot=False,
            )
            if filled and filled.get("score") is not None:
                health[cid] = {"score": float(filled["score"]), "grade": filled.get("grade")}
        except Exception:
            health[cid] = {"score": 50.0, "grade": "?"}

    scored: List[Tuple[Client, List[str], float, str, str, str]] = []
    for c in candidates:
        entry = tag_map.get(str(c.id)) or {}
        raw_tags = [str(x).lower().strip() for x in (entry.get("tags") or []) if str(x).strip()]
        sig = [t for t in raw_tags if t in ROI_BOARD_TAGS]
        if not sig:
            continue
        fn = (c.first_name or "").strip()
        display_name = fn or ((c.email or "").split("@")[0] if c.email else "Client")
        headline = str(entry.get("headline") or "").strip()
        try:
            est_mrr = float(c.estimated_mrr or 0)
        except (TypeError, ValueError):
            est_mrr = 0.0
        hs = float((health.get(c.id) or {}).get("score", 50) or 50)
        mrr_boost = min(18.0, (max(0.0, est_mrr) ** 0.5) * 2.2)
        tw = sum(_ROI_TAG_WEIGHT.get(t, 2.0) for t in sig)
        tag_boost = _roi_tags_priority_boost(sig, pipeline_priorities)
        impact = 54.0 + tw + mrr_boost + hs * 0.2 + tag_boost
        label = ", ".join(s.replace("_", " ") for s in sig[:5])
        scored.append((c, sig, impact, display_name, label, headline))

    scored.sort(key=lambda x: -x[2])
    out: List[Dict[str, Any]] = []
    for c, sig, impact, display_name, label, headline in scored[:MAX_ROI_SIGNAL_TASKS]:
        tid = f"roi_signal.{c.id}"
        try:
            est_mrr = float(c.estimated_mrr or 0)
        except (TypeError, ValueError):
            est_mrr = 0.0
        hs = float((health.get(c.id) or {}).get("score", 50) or 50)
        hgrade = (health.get(c.id) or {}).get("grade")
        hl = (headline[:200] + "…") if len(headline) > 200 else headline
        hl_part = f" Drawer headline: {hl}" if hl else ""

        prospect_voice: Optional[Dict[str, Any]] = None
        if isinstance(c.meta, dict):
            pv = c.meta.get("prospect_voice_profile")
            if isinstance(pv, dict):
                prospect_voice = pv
        lifecycle_str = (
            c.lifecycle_state.value if hasattr(c.lifecycle_state, "value") else str(c.lifecycle_state or "")
        ).lower()
        offer_suggestion = match_offer_for_client(
            offer_ladder,
            lifecycle=lifecycle_str,
            roi_tags=sig,
            headline=headline,
            health_score=hs,
            prospect_voice=prospect_voice,
            has_testimonial_trigger=client_has_expansion_win_basis(c),
        )

        why_parts = [
            f"Call-insight tags: {label}.{hl_part} "
            f"Health {hs:.0f}/100 ({hgrade or '—'}) · est. MRR ${est_mrr:,.0f}. "
            "Buying-signal / ROI triggers from transcripts (same chips as Kanban + client drawer)."
        ]
        if offer_suggestion:
            why_parts.append(
                f"Suggested offer: {offer_suggestion['kind_label']} — {offer_suggestion['name']}. "
                f"{offer_suggestion['rationale']}"
            )
        why = " ".join(why_parts)

        if offer_suggestion:
            prescription = (
                f"Prescribe the {offer_suggestion['kind_label']} (\"{offer_suggestion['name']}\") on the next touch. "
                f"{offer_suggestion['script_hint']}".strip()
            )
        else:
            prescription = (
                "Open the client in Terminal, review Client profile & opportunity in the drawer, "
                "and act on the strongest tag first (testimonial capture, upsell fit, referral ask, or lead follow-up)."
            )

        evidence: Dict[str, Any] = {
            "client_id": str(c.id),
            "client_name": display_name,
            "roi_tags": sig,
            "health_score": round(hs, 1),
            "health_grade": hgrade,
            "estimated_mrr": round(est_mrr, 2),
            "source": "call_insight_roi",
        }
        if offer_suggestion:
            evidence["offer_suggestion"] = offer_suggestion

        out.append(
            {
                "id": tid,
                "title": f"{display_name}: {label}",
                "category": "roi_signal",
                "impact_score": float(impact),
                "confidence": 1.0,
                "evidence": evidence,
                "recommended_actions": [
                    "Review call context + ROI tags in the client drawer",
                    "Re-analyze the latest call if tags look stale",
                    "Complete checklist items tied to this client when done",
                ],
                "why": why[:1200],
                "prescription": prescription[:1200],
                "next_step": f"Terminal → open {display_name}'s card → Client profile & opportunity.",
            }
        )

    for t in out:
        tid = str(t["id"])
        t["completed"] = tid in completed_ids
        if not t.get("why"):
            t["why"] = t["title"]
    return out


def _client_action_pipeline_boost(action_category: Optional[str], priorities: Optional[List[str]]) -> float:
    """Align client-card action categories with Intelligence pipeline_priorities (same mapping as board)."""
    if not priorities:
        return 0.0
    pid = _CATEGORY_TO_PRIORITY_ID.get(str(action_category or "general"), "retention")
    if pid in priorities:
        return max(0.0, 10.0 - float(priorities.index(pid)))
    return 0.0


MAX_CLIENT_PERF_TASKS = 36
MAX_OPEN_ACTIONS_PER_CLIENT = 3


def build_client_recommendation_tasks(
    db: Session,
    org_id: uuid.UUID,
    pipeline_priorities: Optional[List[str]],
    completed_ids: set[str],
) -> List[Dict[str, Any]]:
    """
    Open checklist items from Terminal client cards, ordered by healthiest clients first,
    then high-ROI signals (priority, est. MRR) within each client.
    """
    rows = (
        db.query(ClientAIRecommendationState)
        .filter(ClientAIRecommendationState.org_id == org_id)
        .all()
    )
    if not rows:
        return []
    cids = [r.client_id for r in rows]
    clients = {
        c.id: c
        for c in db.query(Client).filter(Client.id.in_(cids), Client.org_id == org_id).all()
    }
    health = batch_read_cached_health_scores(db, cids, org_id)
    missing = [cid for cid in cids if cid not in health][:80]
    for cid in missing:
        try:
            filled = resolve_health_score(
                db,
                cid,
                org_id,
                brevo_email_stats=None,
                use_ai=False,
                persist_cache=True,
                record_outcome_snapshot=False,
            )
            if filled and filled.get("score") is not None:
                health[cid] = {
                    "score": float(filled["score"]),
                    "grade": filled.get("grade"),
                }
        except Exception:
            health[cid] = {"score": 50.0, "grade": "?"}

    ranked: List[Tuple[Client, float]] = []
    for row in rows:
        c = clients.get(row.client_id)
        if not c:
            continue
        row_h = health.get(row.client_id) or {}
        hs = float(row_h.get("score", 50) or 50)
        ranked.append((c, hs))
    ranked.sort(key=lambda x: -x[1])

    out: List[Dict[str, Any]] = []
    n = 0
    for c, hs in ranked:
        if n >= MAX_CLIENT_PERF_TASKS:
            break
        data = get_recommendation_state_dict(db, c, pipeline_priorities=pipeline_priorities)
        open_actions = [
            a for a in data.get("actions", []) if isinstance(a, dict) and not a.get("completed")
        ]
        if not open_actions:
            continue
        fn = (c.first_name or "").strip()
        display_name = fn or ((c.email or "").split("@")[0] if c.email else "Client")
        try:
            est_mrr = float(c.estimated_mrr or 0)
        except (TypeError, ValueError):
            est_mrr = 0.0
        hinfo = health.get(c.id) or {}
        grade = hinfo.get("grade")
        for a in open_actions[:MAX_OPEN_ACTIONS_PER_CLIENT]:
            if n >= MAX_CLIENT_PERF_TASKS:
                break
            aid = str(a.get("id") or "").strip()
            if not aid:
                continue
            tid = f"client.{c.id}.rec.{aid}"
            pri = int(a.get("priority") or 0)
            acat = str(a.get("category") or "general")
            mrr_boost = min(20.0, (max(0.0, est_mrr) ** 0.5) * 2.4)
            roi_core = pri * 3.6 + mrr_boost
            impact = (
                50.0
                + roi_core
                + hs * 0.24
                + _client_action_pipeline_boost(acat, pipeline_priorities)
            )
            title = f"{display_name}: {a.get('title') or 'Action'}"
            detail = str(a.get("detail") or "").strip()
            why = (
                f"Client health {hs:.0f}/100 ({grade or '—'}) · est. MRR ${est_mrr:,.0f}. {detail[:300]}"
                if detail
                else f"Client health {hs:.0f}/100 ({grade or '—'}) · est. MRR ${est_mrr:,.0f} — open recommendation from Terminal."
            )
            prescription = (
                detail[:500]
                if detail
                else "Work this from the client card in Terminal; use email draft when available."
            )
            out.append(
                {
                    "id": tid,
                    "title": title,
                    "category": "client",
                    "impact_score": float(impact),
                    "confidence": 1.0,
                    "evidence": {
                        "client_id": str(c.id),
                        "client_name": display_name,
                        "health_score": round(hs, 1),
                        "health_grade": grade,
                        "estimated_mrr": round(est_mrr, 2),
                        "action_category": acat,
                        "action_priority": pri,
                        "source": "client_card",
                    },
                    "recommended_actions": [
                        "Complete from the Terminal client card",
                        "Use email draft when the action supports it",
                    ],
                    "why": why[:1200],
                    "prescription": prescription[:1200],
                    "next_step": f"Open Terminal → {display_name}'s card and check off this action.",
                }
            )
            n += 1

    for t in out:
        tid = str(t["id"])
        t["completed"] = tid in completed_ids
        if not t.get("why"):
            t["why"] = t["title"]

    return out


def build_diagnosis(
    lifecycle: Dict[str, int],
    funnel_summaries: List[Dict[str, Any]],
    failed_count: int,
) -> Dict[str, str]:
    cold = lifecycle.get("cold_lead", 0)
    warm = lifecycle.get("warm_lead", 0)
    active = lifecycle.get("active", 0)
    total_leads = cold + warm
    total_visitors = sum(f.get("total_visitors", 0) or 0 for f in funnel_summaries)
    worst_conv = 0.0
    if funnel_summaries:
        worst_conv = min((f.get("overall_conversion_rate_pct") or 0) for f in funnel_summaries)

    if total_visitors < 30 and total_leads < 8:
        traffic = "risk"
        traffic_hint = "Low top-of-funnel volume (visitors + leads). Prioritize traffic or list growth."
    elif total_visitors < 80 or total_leads < 15:
        traffic = "watch"
        traffic_hint = "Traffic is modest. Test one acquisition or referral lever this week."
    else:
        traffic = "ok"
        traffic_hint = "Traffic volume looks workable vs. your current base."

    if warm > 0 and active > 0 and warm > active * 4:
        nurture = "risk"
        nurture_hint = "Many warm leads vs. actives — nurture or sales follow-up may be bottlenecked."
    elif warm > 12 and active < 5:
        nurture = "watch"
        nurture_hint = "Warm inventory is high relative to active clients."
    else:
        nurture = "ok"
        nurture_hint = "Warm/active balance is within a normal range."

    if funnel_summaries and worst_conv < 1.5 and total_visitors >= 40:
        conversion = "risk"
        conversion_hint = "Funnel conversion is very low relative to visitor volume."
    elif funnel_summaries and worst_conv < 4 and total_visitors >= 25:
        conversion = "watch"
        conversion_hint = "Step-through could improve; inspect largest step drop-offs."
    elif failed_count >= 3:
        conversion = "watch"
        conversion_hint = "Failed payments add friction — recovery may lift realized conversion."
    else:
        conversion = "ok"
        conversion_hint = "No severe conversion red flag from aggregate funnel metrics."

    return {
        "traffic": traffic,
        "nurture": nurture,
        "conversion": conversion,
        "traffic_hint": traffic_hint,
        "nurture_hint": nurture_hint,
        "conversion_hint": conversion_hint,
    }


def _pct_change(prev: float, curr: float) -> Optional[float]:
    if prev <= 0 and curr <= 0:
        return None
    if prev <= 0:
        return None
    return round((curr - prev) / prev * 100.0, 1)


def _pipeline_strip_from_lifecycle(lifecycle: Dict[str, int]) -> Dict[str, Any]:
    columns = [
        ("cold_lead", "Cold"),
        ("warm_lead", "Warm"),
        ("active", "Active"),
        ("offboarding", "Offboarding"),
        ("dead", "Dead"),
    ]
    segments = [
        {"id": k, "title": t, "count": int(lifecycle.get(k, 0) or 0)} for k, t in columns
    ]
    total = sum(s["count"] for s in segments)
    return {"segments": segments, "total_clients": total}


def _aggregate_funnel_totals(
    db: Session, org_id: uuid.UUID, window_start: datetime, window_end: datetime
) -> Dict[str, Any]:
    funnels = (
        db.query(Funnel)
        .filter(Funnel.org_id == org_id)
        .order_by(desc(Funnel.created_at))
        .limit(10)
        .all()
    )
    total_visitors = 0
    total_conversions = 0
    for fn in funnels:
        summ = _funnel_step_summary(
            db, org_id, fn.id, 30, window_start=window_start, window_end=window_end
        )
        if summ:
            total_visitors += int(summ.get("total_visitors") or 0)
            total_conversions += int(summ.get("total_conversions") or 0)
    rate = 0.0
    if total_visitors > 0:
        rate = round((total_conversions / total_visitors) * 100.0, 2)
    return {
        "visitors": total_visitors,
        "conversions": total_conversions,
        "conversion_rate_pct": rate,
    }


def build_signals_insights(
    *,
    cash_last_30: float,
    cash_prior_30: float,
    visitors_last: int,
    visitors_prior: int,
    conv_last: float,
    conv_prior: float,
    cash_mtd: float,
    cash_mtd_prev: float,
    mrr: float,
    warm: int,
    active: int,
) -> List[str]:
    insights: List[str] = []
    cash_pct = _pct_change(cash_prior_30, cash_last_30)
    v_pct = _pct_change(float(visitors_prior), float(visitors_last))
    conv_delta = conv_last - conv_prior
    mtd_pct = _pct_change(cash_mtd_prev, cash_mtd)

    if cash_pct is not None and (cash_last_30 > 0 or cash_prior_30 > 0):
        if cash_pct < -8.0:
            insights.append(
                f"Cash collected is down about {abs(cash_pct):.0f}% rolling 30d vs the prior 30d "
                f"(${cash_prior_30:,.0f} → ${cash_last_30:,.0f}). Check failed payments, invoices, and close timing."
            )
        elif cash_pct > 8.0:
            insights.append(
                f"Cash collected is up about {cash_pct:.0f}% rolling 30d vs the prior 30d "
                f"(${cash_prior_30:,.0f} → ${cash_last_30:,.0f}). Keep the same motion while it compounds."
            )

    funnel_vol = visitors_last + visitors_prior
    if funnel_vol >= 20 and v_pct is not None:
        if v_pct < -15.0 and cash_pct is not None and cash_pct < -5.0:
            insights.append(
                "Funnel visitors dropped vs last period — top-of-funnel softness likely contributes to lower cash."
            )
        elif v_pct > 15.0:
            insights.append(
                "Funnel traffic is up vs last period — double down on conversion and fast follow-up so volume becomes revenue."
            )

    if funnel_vol >= 30 and abs(conv_delta) >= 1.0:
        if conv_delta < -1.5:
            insights.append(
                f"Blended funnel conversion slipped ({conv_prior:.1f}% → {conv_last:.1f}%). "
                "Inspect the steepest step drops and payment friction."
            )
        elif conv_delta > 1.5:
            insights.append(
                f"Blended funnel conversion improved ({conv_prior:.1f}% → {conv_last:.1f}%). "
                "Reinvest in the steps and offers that moved the needle."
            )

    if mtd_pct is not None and (cash_mtd > 0 or cash_mtd_prev > 0):
        if mtd_pct < -10.0:
            insights.append(
                f"Month-to-date cash trails the same span last month by ~{abs(mtd_pct):.0f}% "
                "— prioritize collections and scheduled closes before month-end."
            )
        elif mtd_pct > 10.0:
            insights.append(
                f"Month-to-date cash is ahead of the same days last month (~{mtd_pct:.0f}%) "
                "— protect pipeline quality and avoid slowing follow-ups."
            )

    if warm > 0 and active > 0 and warm > active * 3:
        insights.append(
            f"Warm leads ({warm}) outweigh actives ({active}) — focus on moving warm to started/paid this week."
        )

    if mrr > 0 and cash_pct is not None and cash_pct < -10.0:
        insights.append(
            f"MRR is ${mrr:,.0f}; if subscriptions look stable, the cash dip may be one-time charges or timing — confirm renewals."
        )

    return insights[:8]


def build_tasks(
    snapshot: Dict[str, Any],
    completed_ids: set[str],
    pipeline_priorities: Optional[List[str]],
    client_recommendation_tasks: Optional[List[Dict[str, Any]]] = None,
    roi_signal_tasks: Optional[List[Dict[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    tasks: List[Dict[str, Any]] = []
    lifecycle = snapshot.get("pipeline") or {}
    counts = lifecycle.get("lifecycle_counts") or {}
    cold = int(counts.get("cold_lead", 0) or 0)
    warm = int(counts.get("warm_lead", 0) or 0)
    active = int(counts.get("active", 0) or 0)
    revenue = snapshot.get("revenue") or {}
    mrr = float(revenue.get("mrr", 0) or 0)
    cash_30 = float(revenue.get("cash_last_30_days", 0) or 0)
    failed = snapshot.get("failed_payments") or {}
    failed_count = int(failed.get("count", 0) or 0)

    if cold + warm < 5:
        tasks.append(
            {
                "id": "pipeline.low_lead_inventory",
                "title": "Grow lead inventory",
                "category": "pipeline",
                "impact_score": 72.0,
                "evidence": {"cold_lead": cold, "warm_lead": warm},
                "recommended_actions": [
                    "Run one list-building or outbound block this week",
                    "Ensure every inbound lead lands in Terminal within 24h",
                ],
                "why": f"You have {cold + warm} combined cold/warm leads — thin pipeline for growth.",
                "prescription": "Pick a single channel (referral, content, or paid) and add 10 qualified conversations.",
                "next_step": "Book 2h for prospecting; log new leads in Terminal.",
            }
        )

    if warm >= 8 and active < max(3, warm // 6):
        tasks.append(
            {
                "id": "pipeline.warm_to_active_gap",
                "title": "Convert warm leads to active clients",
                "category": "pipeline",
                "impact_score": 78.0,
                "evidence": {"warm_lead": warm, "active": active},
                "recommended_actions": [
                    "Review stalled warm leads older than 14 days",
                    "Send a direct booking or offer message to top 5 warm leads",
                ],
                "why": f"{warm} warm leads vs {active} active — activation may be stalling.",
                "prescription": "Tighten follow-up SLA and add one clear CTA (book / pay / start).",
                "next_step": "Open Terminal warm column and message the oldest 5 leads today.",
            }
        )

    if mrr <= 0 and active >= 1:
        tasks.append(
            {
                "id": "revenue.mrr_not_visible",
                "title": "Clarify recurring revenue",
                "category": "revenue",
                "impact_score": 65.0,
                "evidence": {"mrr": mrr, "active_clients": active},
                "recommended_actions": [
                    "Connect or refresh Stripe subscriptions sync",
                    "Add estimated MRR on key clients if subscriptions are off-Stripe",
                ],
                "why": "MRR reads as $0 while you have active clients — data or packaging gap.",
                "prescription": "Align Stripe subs with clients so MRR reflects reality.",
                "next_step": "Stripe tab → verify subscriptions; backfill client estimated MRR if needed.",
            }
        )

    if cash_30 < 500 and active >= 2:
        tasks.append(
            {
                "id": "revenue.low_cash_30d",
                "title": "Lift 30-day cash collected",
                "category": "revenue",
                "impact_score": 70.0,
                "evidence": {"cash_last_30_days": round(cash_30, 2), "active_clients": active},
                "recommended_actions": [
                    "Invoice or collect any open balances",
                    "Offer a limited upsell to engaged actives",
                ],
                "why": f"Cash collected last 30d is ${cash_30:.0f} with {active} active clients.",
                "prescription": "Prioritize collections and one revenue event this week.",
                "next_step": "Terminal → review payments; ping clients with open invoices.",
            }
        )

    if failed_count > 0:
        tasks.append(
            {
                "id": "payments.failed_queue",
                "title": "Resolve failed payments",
                "category": "payments",
                "impact_score": 85.0 + min(10.0, float(failed_count)),
                "evidence": {"failed_payment_groups": failed_count},
                "recommended_actions": [
                    "Work the failed-payments queue in Stripe / Terminal",
                    "Send recovery messaging where a client is mapped",
                ],
                "why": f"{failed_count} failed or past-due payment group(s) need attention.",
                "prescription": "Recovering these payments is often the fastest ROI fix.",
                "next_step": "Open Stripe dashboard failed payments and resolve top 3.",
            }
        )

    for f in snapshot.get("funnels") or []:
        fid = f.get("funnel_id")
        visitors = int(f.get("total_visitors", 0) or 0)
        conv = float(f.get("overall_conversion_rate_pct") or 0)
        name = f.get("name") or "Funnel"
        if visitors >= 30 and conv < 2.5:
            tasks.append(
                {
                    "id": f"funnel.{fid}.low_conversion",
                    "title": f"Fix conversion: {name}",
                    "category": "funnel",
                    "impact_score": 80.0,
                    "evidence": {
                        "funnel_id": fid,
                        "visitors_30d": visitors,
                        "conversion_pct": conv,
                    },
                    "recommended_actions": [
                        "Identify the largest step drop in this funnel",
                        "Simplify the next step after the biggest loss",
                    ],
                    "why": f"~{visitors} visitors / 30d but {conv:.1f}% conversion — leakage is costly.",
                    "prescription": "Treat the worst step as a hypothesis: test copy, friction, or speed.",
                    "next_step": f"Open funnel analytics for {name} and note the steepest drop.",
                }
            )
        for step in f.get("step_drops") or []:
            prev_rate = step.get("conversion_rate_pct")
            if prev_rate is None:
                continue
            if prev_rate < 25 and step.get("count", 0) >= 10:
                ev = step.get("event_name") or "step"
                tasks.append(
                    {
                        "id": f"funnel.{fid}.step_drop.{ev}",
                        "title": f"Step drop: {step.get('label') or ev}",
                        "category": "funnel",
                        "impact_score": 75.0,
                        "evidence": {
                            "funnel_id": fid,
                            "event_name": ev,
                            "step_conversion_pct": round(prev_rate, 1),
                            "count": step.get("count"),
                        },
                        "recommended_actions": [
                            "Review messaging and UX on this step",
                            "Add a reminder or shorter path to the next step",
                        ],
                        "why": f"Only {prev_rate:.0f}% move through this step — major friction signal.",
                        "prescription": "Reduce fields, clarify value, or add trust on this transition.",
                        "next_step": "Edit the page or email tied to this funnel event.",
                    }
                )

    if roi_signal_tasks:
        tasks.extend(roi_signal_tasks)

    if client_recommendation_tasks:
        tasks.extend(client_recommendation_tasks)

    for t in tasks:
        cat = str(t.get("category") or "")
        if cat == "client":
            extra = 0.0
        elif cat == "roi_signal":
            extra = _roi_tags_priority_boost(
                list((t.get("evidence") or {}).get("roi_tags") or []),
                pipeline_priorities,
            )
        else:
            extra = _priority_boost(cat, pipeline_priorities)
        t["impact_score"] = float(t["impact_score"]) + extra
        tid = str(t["id"])
        t["completed"] = tid in completed_ids
        if not t.get("why"):
            t["why"] = t["title"]
        if not t.get("prescription"):
            t["prescription"] = t["recommended_actions"][0] if t["recommended_actions"] else ""

    tasks.sort(key=lambda x: (-x["impact_score"], x["id"]))
    return tasks


def build_performance_snapshot(
    db: Session,
    org_id: uuid.UUID,
    *,
    user_ai_profile: Any = None,
    completed_task_ids: Optional[List[str]] = None,
) -> Dict[str, Any]:
    completed_set = set(completed_task_ids or [])
    lifecycle = _lifecycle_counts(db, org_id)
    total_clients = sum(lifecycle.values())
    thirty_ago = datetime.utcnow() - timedelta(days=30)
    seven_ago = datetime.utcnow() - timedelta(days=7)
    activated_approx = (
        db.query(func.count(Client.id))
        .filter(
            Client.org_id == org_id,
            Client.lifecycle_state == LifecycleState.ACTIVE,
            Client.updated_at >= thirty_ago,
        )
        .scalar()
        or 0
    )
    warm_inventory = max(1, int(lifecycle.get("warm_lead", 0) or 0))
    warm_to_active_rate_30d = round((activated_approx / warm_inventory) * 100.0, 2)
    new_warm_7d = (
        db.query(func.count(Client.id))
        .filter(
            Client.org_id == org_id,
            Client.lifecycle_state == LifecycleState.WARM_LEAD,
            Client.created_at >= seven_ago,
        )
        .scalar()
        or 0
    )
    mrr = _org_mrr(db, org_id)
    now_snap = datetime.utcnow()
    today_start_snap = now_snap.replace(hour=0, minute=0, second=0, microsecond=0)
    last30_start = today_start_snap - timedelta(days=30)
    prior30_start = today_start_snap - timedelta(days=60)
    cash_30 = _cash_collected_between(db, org_id, last30_start, now_snap)
    cash_prior_30 = _cash_collected_between(db, org_id, prior30_start, last30_start)
    mtd_start = today_start_snap.replace(day=1)
    if mtd_start.month == 1:
        prev_m_start = mtd_start.replace(year=mtd_start.year - 1, month=12, day=1)
    else:
        prev_m_start = mtd_start.replace(month=mtd_start.month - 1, day=1)
    prev_m_end = prev_m_start + (now_snap - mtd_start)
    cash_mtd = _cash_collected_between(db, org_id, mtd_start, now_snap)
    cash_mtd_prev = _cash_collected_between(db, org_id, prev_m_start, prev_m_end)
    stripe_on = check_stripe_connected(db, org_id)
    failed_count, failed_samples = _failed_payments_summary(db, org_id)

    funnels = (
        db.query(Funnel)
        .filter(Funnel.org_id == org_id)
        .order_by(desc(Funnel.created_at))
        .limit(10)
        .all()
    )
    funnel_summaries: List[Dict[str, Any]] = []
    for fn in funnels:
        summ = _funnel_step_summary(db, org_id, fn.id, 30)
        if summ:
            funnel_summaries.append(summ)

    pipeline_block = {
        "lifecycle_counts": lifecycle,
        "total_clients": total_clients,
        "warm_to_active_rate_30d": warm_to_active_rate_30d,
        "new_warm_leads_7d": int(new_warm_7d),
    }
    revenue_block = {
        "mrr": round(mrr, 2),
        "arr": round(mrr * 12, 2),
        "cash_last_30_days": round(cash_30, 2),
        "cash_prior_30_days": round(cash_prior_30, 2),
        "cash_mtd": round(cash_mtd, 2),
        "cash_mtd_prev_month_same_range": round(cash_mtd_prev, 2),
        "stripe_connected": stripe_on,
    }
    failed_block = {"count": failed_count, "sample": failed_samples}
    diagnosis = build_diagnosis(lifecycle, funnel_summaries, failed_count)
    agg_last = _aggregate_funnel_totals(db, org_id, last30_start, now_snap)
    agg_prior = _aggregate_funnel_totals(db, org_id, prior30_start, last30_start)
    v_last = int(agg_last["visitors"])
    v_prior = int(agg_prior["visitors"])
    c_last = float(agg_last["conversion_rate_pct"])
    c_prior = float(agg_prior["conversion_rate_pct"])
    warm_n = int(lifecycle.get("warm_lead", 0) or 0)
    active_n = int(lifecycle.get("active", 0) or 0)
    pct_cash_30 = _pct_change(cash_prior_30, cash_30)
    pct_mtd = _pct_change(cash_mtd_prev, cash_mtd)
    pct_vis = _pct_change(float(v_prior), float(v_last))
    cv_last = int(agg_last["conversions"])
    cv_prior = int(agg_prior["conversions"])
    pct_conv_n = _pct_change(float(cv_prior), float(cv_last))
    diagnosis.update(
        {
            "pipeline_strip": _pipeline_strip_from_lifecycle(lifecycle),
            "revenue_compare": {
                "cash_last_30_days": round(cash_30, 2),
                "cash_prior_30_days": round(cash_prior_30, 2),
                "pct_change_30d": pct_cash_30,
                "cash_mtd": round(cash_mtd, 2),
                "cash_mtd_prev_month_same_range": round(cash_mtd_prev, 2),
                "pct_change_mtd": pct_mtd,
                "mrr": round(mrr, 2),
            },
            "funnel_compare": {
                "visitors_last_30": v_last,
                "visitors_prior_30": v_prior,
                "conversions_last_30": cv_last,
                "conversions_prior_30": cv_prior,
                "conversion_rate_last_30": c_last,
                "conversion_rate_prior_30": c_prior,
                "pct_change_visitors": pct_vis,
                "pct_change_conversions": pct_conv_n,
            },
            "insights": build_signals_insights(
                cash_last_30=cash_30,
                cash_prior_30=cash_prior_30,
                visitors_last=v_last,
                visitors_prior=v_prior,
                conv_last=c_last,
                conv_prior=c_prior,
                cash_mtd=cash_mtd,
                cash_mtd_prev=cash_mtd_prev,
                mrr=mrr,
                warm=warm_n,
                active=active_n,
            ),
        }
    )
    priorities = _pipeline_priorities_from_user(user_ai_profile)
    ladder = extract_offer_ladder(user_ai_profile)
    snap_for_tasks = {
        "pipeline": pipeline_block,
        "revenue": revenue_block,
        "failed_payments": failed_block,
        "funnels": funnel_summaries,
    }
    client_tasks = build_client_recommendation_tasks(
        db, org_id, priorities, completed_set
    )
    roi_tasks = build_roi_signal_tasks(db, org_id, priorities, completed_set, offer_ladder=ladder)
    tasks = build_tasks(
        snap_for_tasks,
        completed_set,
        priorities,
        client_tasks,
        roi_signal_tasks=roi_tasks,
    )

    return {
        "generated_at": datetime.utcnow().replace(tzinfo=None).isoformat() + "Z",
        "pipeline": pipeline_block,
        "revenue": revenue_block,
        "failed_payments": failed_block,
        "funnels": funnel_summaries,
        "diagnosis": diagnosis,
        "tasks": tasks,
        "pipeline_priorities": list(priorities or []),
    }

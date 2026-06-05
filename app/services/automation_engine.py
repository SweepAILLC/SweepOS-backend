"""Automation engine: deterministic trigger -> job enqueue with idempotency, scoring, audiencing.

The four entry points (`on_payment_received`, `on_call_insight_processed`,
`on_lifecycle_entered_offboarding`, plus the manual `enqueue_for_preview`) only insert
``AutomationEmailJob`` rows on COMMITTED transactions. They never call the LLM or
Brevo; all materialization happens in the worker so the request path stays fast and
delivery survives API restarts.

Key invariants:
- Idempotency is enforced at the DB layer (UNIQUE org_id + idempotency_key); we use
  ``ON CONFLICT DO NOTHING`` so retried webhooks / sync passes are no-ops.
- Audience filters and combined-ask scoring stay deterministic so previews and
  worker-time renders are byte-identical.
- pipeline_priorities (Intelligence) breaks ties when multiple opportunities score
  the same, matching what the user already configured for the AI's prioritization.
"""
from __future__ import annotations

import hashlib
import logging
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from sqlalchemy import and_, func, or_, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from app.models.automation import (
    AutomationEmailJob,
    AutomationRule,
    ContentMode,
    JobState,
    Playbook,
)
from app.models.client import Client, LifecycleState
from app.models.client_call_insight import ClientCallInsight
from app.models.stripe_payment import StripePayment
from app.models.whop_payment import WhopPayment
from app.services.offer_ladder import (
    extract_offer_ladder,
    resolve_org_offer_ladder,
)
from app.services.user_ai_profile_context import extract_intelligence_profile_for_automation_llm

LOG = logging.getLogger(__name__)


# Default scoring weights for the combined-ask playbooks. These are intentionally
# additive integers so the order is stable across Python versions / DB rows.
_REFERRAL_BASE = 10
_UPSELL_BASE = 10
_TESTIMONIAL_BASE = 10

_HEALTH_HIGH_BONUS = 6     # health_score >= 75 boosts referral/testimonial
_LIFETIME_REV_BONUS = 5    # lifetime revenue >= $500 boosts upsell
_WIN_TAG_BONUS = 8         # 'win' or testimonial story present
_REFERRAL_TAG_BONUS = 8
_UPSELL_TAG_BONUS = 8
_TESTIMONIAL_TAG_BONUS = 8
_OFFBOARDING_REFERRAL_BONUS = 4  # offboarding favors referral and testimonial recap

# Hard cap on how old a "first" payment can be and still trigger onboarding/referral.
# Live webhooks arrive within seconds of the charge; sync backfills can be months/years
# old. We pick 36h to leave a wide cushion for delayed webhooks but reject any backfill
# of historical data.
FIRST_PAYMENT_RECENCY_HOURS = 36

# Same idea for newly created calendar bookings: only react to bookings whose start_time
# is in the future (or just barely in the past). Backfill imports of historical events
# must not trigger pre-sale outreach.
POST_BOOKING_FUTURE_TOLERANCE_HOURS = 6


# ---------------------------------------------------------------------------
# Idempotency keys
# ---------------------------------------------------------------------------

def idempotency_key_for(
    *,
    playbook: str,
    client_id: uuid.UUID,
    discriminator: str,
) -> str:
    """
    Produce a stable, short idempotency key.

    `discriminator` carries trigger-specific identity (e.g. ``stripe:<charge_id>``,
    ``insight:<insight_id>``, ``lifecycle:offboarding:<date>``).
    """
    raw = f"{playbook}|{client_id}|{discriminator}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:48]


# ---------------------------------------------------------------------------
# Audience filter
# ---------------------------------------------------------------------------

def audience_filter_passes(client: Client, audience_filter: Optional[Dict[str, Any]]) -> bool:
    """Apply rule.audience_filter (lifecycle_in, min revenue, program-progress band)."""
    if not audience_filter or not isinstance(audience_filter, dict):
        return True

    lifecycle_in = audience_filter.get("lifecycle_in")
    if isinstance(lifecycle_in, list) and lifecycle_in:
        ls = client.lifecycle_state.value if hasattr(client.lifecycle_state, "value") else str(client.lifecycle_state)
        if ls not in {str(x).lower() for x in lifecycle_in}:
            return False

    min_rev = audience_filter.get("min_lifetime_revenue_cents")
    if isinstance(min_rev, (int, float)):
        if int(client.lifetime_revenue_cents or 0) < int(min_rev):
            return False

    progress = client.program_progress_percent
    progress = float(progress) if progress is not None else None
    p_min = audience_filter.get("program_progress_min_percent")
    p_max = audience_filter.get("program_progress_max_percent")
    if p_min is not None and (progress is None or progress < float(p_min)):
        return False
    if p_max is not None and (progress is None or progress > float(p_max)):
        return False

    return True


# ---------------------------------------------------------------------------
# Combined ask scoring (referral / upsell / testimonial)
# ---------------------------------------------------------------------------

@dataclass
class OpportunityScore:
    name: str  # "referral" | "upsell" | "testimonial"
    score: int
    rationale: List[str]
    upsell_offer: Optional[Dict[str, Any]] = None  # populated when name == "upsell"


def _insight_summary(insight: Optional[Dict[str, Any]]) -> Tuple[List[str], List[str], Optional[str]]:
    """Return (roi_tags, wins, headline) from a ClientCallInsight.insight_json."""
    if not isinstance(insight, dict):
        return [], [], None
    tags = [str(t).lower().strip() for t in (insight.get("opportunity_tags") or []) if str(t).strip()]
    wins = [str(w).strip() for w in (insight.get("wins") or []) if str(w).strip()]
    headline = insight.get("headline") or insight.get("client_state_synthesis")
    if isinstance(headline, str):
        headline = headline.strip()[:240] or None
    else:
        headline = None
    return tags, wins, headline


def _score_referral(
    client: Client,
    roi_tags: List[str],
    wins: List[str],
    health_score: Optional[float],
    in_offboarding: bool,
) -> OpportunityScore:
    score = _REFERRAL_BASE
    rationale: List[str] = []
    if "referral" in roi_tags:
        score += _REFERRAL_TAG_BONUS
        rationale.append("ROI tag: referral")
    if wins:
        score += _WIN_TAG_BONUS
        rationale.append(f"{len(wins)} win(s) on call")
    if health_score is not None and health_score >= 75:
        score += _HEALTH_HIGH_BONUS
        rationale.append(f"health {health_score:.0f}")
    if in_offboarding:
        score += _OFFBOARDING_REFERRAL_BONUS
        rationale.append("offboarding window")
    return OpportunityScore("referral", score, rationale)


def _score_upsell(
    client: Client,
    roi_tags: List[str],
    wins: List[str],
    upsell_offer: Optional[Dict[str, Any]],
    in_offboarding: bool,
) -> OpportunityScore:
    score = _UPSELL_BASE
    rationale: List[str] = []
    if "upsell" in roi_tags:
        score += _UPSELL_TAG_BONUS
        rationale.append("ROI tag: upsell")
    if upsell_offer:
        rationale.append(f"matched offer: {upsell_offer.get('name','upsell')[:60]}")
    if wins:
        # smaller bump than referral (we still need them to want more, not just be happy)
        score += _WIN_TAG_BONUS // 2
        rationale.append("recent win")
    lr = int(client.lifetime_revenue_cents or 0)
    if lr >= 50_000:  # $500
        score += _LIFETIME_REV_BONUS
        rationale.append(f"LTV ≥ $500 (${lr / 100:.0f})")
    if in_offboarding:
        score += _OFFBOARDING_REFERRAL_BONUS // 2
        rationale.append("offboarding window")
    return OpportunityScore("upsell", score, rationale, upsell_offer=upsell_offer)


def _score_testimonial(
    client: Client,
    roi_tags: List[str],
    wins: List[str],
    health_score: Optional[float],
    in_offboarding: bool,
) -> OpportunityScore:
    score = _TESTIMONIAL_BASE
    rationale: List[str] = []
    if "testimonial" in roi_tags:
        score += _TESTIMONIAL_TAG_BONUS
        rationale.append("ROI tag: testimonial")
    if wins:
        score += _WIN_TAG_BONUS
        rationale.append(f"{len(wins)} win(s) on call")
    if health_score is not None and health_score >= 70:
        score += _HEALTH_HIGH_BONUS // 2
        rationale.append(f"health {health_score:.0f}")
    if in_offboarding:
        score += _OFFBOARDING_REFERRAL_BONUS
        rationale.append("offboarding window")
    return OpportunityScore("testimonial", score, rationale)


def select_upsell_for_client(
    client: Client,
    insight: Optional[Dict[str, Any]],
    ladder: Optional[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    """
    Pick the upsell offer whose `triggers` best match the client's signals.

    Mirrors `offer_ladder._best_upsell` but stays self-contained so the engine doesn't
    drag in the deterministic prescription path's other dependencies.
    """
    if not ladder:
        return None
    upsells = ladder.get("upsells") or []
    if not upsells:
        return None

    roi_tags, wins, headline = _insight_summary(insight)
    blob = " ".join(roi_tags + wins + ([headline] if headline else [])).lower()

    best: Optional[Dict[str, Any]] = None
    best_score = -1
    for offer in upsells:
        triggers = [str(t).lower() for t in (offer.get("triggers") or []) if str(t).strip()]
        score = 0
        for t in triggers:
            if t and t in blob:
                score += 2
        if "upsell" in roi_tags and score == 0:
            score = 1
        if score > best_score:
            best_score = score
            best = offer
    return best


def score_opportunities(
    client: Client,
    *,
    insight: Optional[Dict[str, Any]],
    ladder: Optional[Dict[str, Any]],
    health_score: Optional[float],
    pipeline_priorities: Optional[Sequence[str]] = None,
    in_offboarding: bool = False,
    rule_priority: Optional[Sequence[str]] = None,
) -> List[OpportunityScore]:
    """
    Rank ["referral", "upsell", "testimonial"] for this client.

    Tie-break order:
      1. rule_priority (per-rule override, if set)
      2. pipeline_priorities (from Intelligence ai_profile)
      3. raw enum order: referral, testimonial, upsell

    Returned list is ordered highest->lowest; suitable for ``[:combine_top_n]``.
    """
    roi_tags, wins, _headline = _insight_summary(insight)
    upsell_offer = select_upsell_for_client(client, insight, ladder)

    items = [
        _score_referral(client, roi_tags, wins, health_score, in_offboarding),
        _score_upsell(client, roi_tags, wins, upsell_offer, in_offboarding),
        _score_testimonial(client, roi_tags, wins, health_score, in_offboarding),
    ]

    # Tiebreaker keys
    pri_lookup: Dict[str, int] = {}
    if rule_priority:
        for i, name in enumerate(rule_priority):
            pri_lookup.setdefault(str(name).lower(), -1000 + i)
    if pipeline_priorities:
        # Intelligence pipeline_priorities is a free-form list; if user has set one of
        # ['referrals', 'upsells', 'testimonials'] (or singular), use it as a tiebreaker.
        norm = {
            "referrals": "referral",
            "referral": "referral",
            "upsells": "upsell",
            "upsell": "upsell",
            "testimonials": "testimonial",
            "testimonial": "testimonial",
        }
        for i, name in enumerate(pipeline_priorities):
            key = norm.get(str(name).lower().strip())
            if key and key not in pri_lookup:
                pri_lookup[key] = i
    fallback = {"referral": 0, "testimonial": 1, "upsell": 2}

    def _key(o: OpportunityScore) -> Tuple[int, int, int]:
        return (-o.score, pri_lookup.get(o.name, 99), fallback[o.name])

    items.sort(key=_key)
    return items


# ---------------------------------------------------------------------------
# Job enqueue helper
# ---------------------------------------------------------------------------

def _now() -> datetime:
    return datetime.utcnow()


def _get_rule(db: Session, org_id: uuid.UUID, playbook: str) -> Optional[AutomationRule]:
    return (
        db.query(AutomationRule)
        .filter(AutomationRule.org_id == org_id, AutomationRule.playbook == playbook)
        .first()
    )


def _resolve_initial_state_and_schedule(
    rule: AutomationRule,
) -> Tuple[str, datetime]:
    """Return (state, scheduled_at) given rule.delay_seconds and require_approval."""
    delay = max(0, int(rule.delay_seconds or 0))
    scheduled = _now() + timedelta(seconds=delay)
    if rule.require_approval:
        return JobState.AWAITING_APPROVAL.value, scheduled
    return JobState.SCHEDULED.value, scheduled


def _enqueue_job(
    db: Session,
    *,
    org_id: uuid.UUID,
    client_id: uuid.UUID,
    rule: AutomationRule,
    trigger_event: str,
    discriminator: str,
    payload: Optional[Dict[str, Any]] = None,
) -> Optional[uuid.UUID]:
    """Insert one job row idempotently. Returns the row id, or None when the conflict was hit."""
    if not rule.enabled:
        return None

    state, scheduled = _resolve_initial_state_and_schedule(rule)
    idemp = idempotency_key_for(
        playbook=rule.playbook,
        client_id=client_id,
        discriminator=discriminator,
    )
    new_id = uuid.uuid4()
    stmt = (
        pg_insert(AutomationEmailJob)
        .values(
            id=new_id,
            org_id=org_id,
            rule_id=rule.id,
            client_id=client_id,
            playbook=rule.playbook,
            trigger_event=trigger_event,
            idempotency_key=idemp,
            scheduled_at=scheduled,
            state=state,
            payload_json=payload or {},
            attempts=0,
            created_at=_now(),
            updated_at=_now(),
        )
        .on_conflict_do_nothing(constraint="uq_automation_email_jobs_org_idemp")
        .returning(AutomationEmailJob.id)
    )
    res = db.execute(stmt).fetchone()
    db.flush()
    return uuid.UUID(str(res[0])) if res else None


# ---------------------------------------------------------------------------
# Trigger entry points
# ---------------------------------------------------------------------------

def _is_first_succeeded_payment(
    db: Session,
    org_id: uuid.UUID,
    client_id: uuid.UUID,
    *,
    excluding_stripe_id: Optional[str] = None,
    excluding_whop_id: Optional[str] = None,
) -> bool:
    """True iff this client has no other succeeded payment beyond the one we're handling."""
    stripe_q = db.query(func.count(StripePayment.id)).filter(
        StripePayment.org_id == org_id,
        StripePayment.client_id == client_id,
        StripePayment.status == "succeeded",
    )
    if excluding_stripe_id:
        stripe_q = stripe_q.filter(StripePayment.stripe_id != excluding_stripe_id)
    stripe_count = int(stripe_q.scalar() or 0)
    if stripe_count > 0:
        return False

    whop_q = db.query(func.count(WhopPayment.id)).filter(
        WhopPayment.org_id == org_id,
        WhopPayment.client_id == client_id,
        WhopPayment.status.in_(("paid", "succeeded", "completed", "successful")),
    )
    if excluding_whop_id:
        whop_q = whop_q.filter(WhopPayment.whop_id != excluding_whop_id)
    whop_count = int(whop_q.scalar() or 0)
    return whop_count == 0


def _client_has_any_onboarding_automation_job(
    db: Session,
    org_id: uuid.UUID,
    client_id: uuid.UUID,
) -> bool:
    """True if any onboarding automation job row exists for this client (any state).

    ``first_payment_onboarding`` uses a lifetime-stable idempotency discriminator (see
    ``on_payment_received``) so replays dedupe. This query also blocks a second row when
    older jobs were keyed only by payment id.
    """
    row = (
        db.query(AutomationEmailJob.id)
        .filter(
            AutomationEmailJob.org_id == org_id,
            AutomationEmailJob.client_id == client_id,
            AutomationEmailJob.playbook == Playbook.FIRST_PAYMENT_ONBOARDING.value,
        )
        .limit(1)
        .first()
    )
    return row is not None


def on_payment_received(
    db: Session,
    *,
    org_id: uuid.UUID,
    client_id: uuid.UUID,
    payment_source: str,  # "stripe" | "whop"
    payment_external_id: str,
    amount_cents: int,
    paid_at: Optional[datetime] = None,
) -> List[uuid.UUID]:
    """
    Enqueue first_payment_onboarding + first_payment_referral when this is the
    client's first successful payment.

    Three independent gates -- ALL must pass -- so onboarding never fires twice
    or on backfilled history:

    1. ``_is_first_succeeded_payment``: the client has no other succeeded
       Stripe/Whop payment row aside from this one. Defends against re-fired
       webhooks and against Whop sync bringing in N rows for one client.
    2. ``client.lifetime_revenue_cents`` is at most ``amount_cents``. If the
       client already has accumulated revenue (manual entry, prior import, or
       another integration), they aren't actually new and we shouldn't email
       them onboarding copy.
    3. Recency: ``paid_at`` must be within ``FIRST_PAYMENT_RECENCY_HOURS`` of
       now. Historical payments imported in a backfill produce idempotency
       rows but never trigger onboarding/referral.

    Onboarding is additionally capped at **one job per client per org** for
    ``first_payment_onboarding``: we use a lifetime idempotency discriminator
    (``lifetime_once:<client_id>``) and skip enqueue if any onboarding job row
    already exists, so duplicate webhooks or legacy payment-keyed rows cannot
    queue a second welcome email.

    Returns the list of newly created job ids.
    """
    client = (
        db.query(Client)
        .filter(Client.id == client_id, Client.org_id == org_id)
        .first()
    )
    if not client:
        return []

    is_first = _is_first_succeeded_payment(
        db,
        org_id,
        client_id,
        excluding_stripe_id=payment_external_id if payment_source == "stripe" else None,
        excluding_whop_id=payment_external_id if payment_source == "whop" else None,
    )
    if not is_first:
        return []

    # Cross-source revenue gate: lifetime_revenue_cents tracks ALL revenue
    # this client has ever attributed to them, not just the rows we just
    # counted. If it exceeds this charge, they're not actually a brand-new
    # payer -- skip onboarding rather than misfire.
    try:
        existing_lifetime = int(client.lifetime_revenue_cents or 0)
    except (TypeError, ValueError):
        existing_lifetime = 0
    if existing_lifetime > int(amount_cents or 0):
        LOG.info(
            "automation: skipping first-payment onboarding for client %s -- "
            "lifetime_revenue_cents=%s already exceeds this payment (%s).",
            client_id,
            existing_lifetime,
            amount_cents,
        )
        return []

    # Recency gate: drop anything older than the configured window so backfill
    # imports never trigger an onboarding blast against historical payers.
    if paid_at is not None:
        try:
            paid_naive = (
                paid_at.replace(tzinfo=None) if paid_at.tzinfo is not None else paid_at
            )
            age_seconds = (_now() - paid_naive).total_seconds()
        except (TypeError, ValueError, AttributeError):
            age_seconds = 0
        max_age_seconds = FIRST_PAYMENT_RECENCY_HOURS * 3600
        if age_seconds > max_age_seconds:
            LOG.info(
                "automation: skipping first-payment onboarding for client %s -- "
                "payment is %.0fh old (cap %sh).",
                client_id,
                age_seconds / 3600,
                FIRST_PAYMENT_RECENCY_HOURS,
            )
            return []

    created: List[uuid.UUID] = []
    discriminator = f"{payment_source}:{payment_external_id}"
    payload = {
        "trigger": "first_payment",
        "payment_source": payment_source,
        "payment_external_id": payment_external_id,
        "amount_cents": amount_cents,
        "paid_at": (paid_at or _now()).isoformat(),
    }

    onboarding_rule = _get_rule(db, org_id, Playbook.FIRST_PAYMENT_ONBOARDING.value)
    onboarding_blocked = _client_has_any_onboarding_automation_job(db, org_id, client_id)
    if onboarding_blocked:
        LOG.info(
            "automation: skipping first-payment onboarding for client %s -- "
            "an onboarding automation job already exists for this client.",
            client_id,
        )
    onboarding_discriminator = f"lifetime_once:{client_id}"
    if (
        onboarding_rule
        and audience_filter_passes(client, onboarding_rule.audience_filter)
        and not onboarding_blocked
    ):
        nid = _enqueue_job(
            db,
            org_id=org_id,
            client_id=client_id,
            rule=onboarding_rule,
            trigger_event="payment.first.onboarding",
            discriminator=onboarding_discriminator,
            payload=payload,
        )
        if nid:
            created.append(nid)

    referral_rule = _get_rule(db, org_id, Playbook.FIRST_PAYMENT_REFERRAL.value)
    if referral_rule and audience_filter_passes(client, referral_rule.audience_filter):
        nid = _enqueue_job(
            db,
            org_id=org_id,
            client_id=client_id,
            rule=referral_rule,
            trigger_event="payment.first.referral",
            discriminator=discriminator,
            payload=payload,
        )
        if nid:
            created.append(nid)
    return created


def on_call_insight_processed(
    db: Session,
    *,
    org_id: uuid.UUID,
    client_id: uuid.UUID,
    insight_id: uuid.UUID,
) -> List[uuid.UUID]:
    """
    Enqueue the win_combined_ask playbook when an insight has a win signal.

    The "combined ask" decision (referral vs upsell vs testimonial) is computed at
    worker render time from the same insight + ladder + ai_profile, so previews and
    actual sends agree. We only enqueue here.
    """
    rule = _get_rule(db, org_id, Playbook.WIN_COMBINED_ASK.value)
    if not rule or not rule.enabled:
        return []

    insight = db.query(ClientCallInsight).filter(ClientCallInsight.id == insight_id).first()
    if not insight or insight.status != "complete":
        return []
    json_data = insight.insight_json or {}
    roi_tags = [str(t).lower() for t in (json_data.get("opportunity_tags") or [])]
    wins = json_data.get("wins") or []
    has_signal = any(t in roi_tags for t in ("referral", "upsell", "testimonial")) or bool(wins)
    if not has_signal:
        return []

    client = (
        db.query(Client)
        .filter(Client.id == client_id, Client.org_id == org_id)
        .first()
    )
    if not client:
        return []
    if not audience_filter_passes(client, rule.audience_filter):
        return []

    payload = {
        "trigger": "win_detected",
        "insight_id": str(insight_id),
        "fathom_call_record_id": str(insight.fathom_call_record_id),
        "opportunity_tags": roi_tags,
        "wins": wins[:8],
    }
    nid = _enqueue_job(
        db,
        org_id=org_id,
        client_id=client_id,
        rule=rule,
        trigger_event="call_insight.win",
        discriminator=f"insight:{insight_id}",
        payload=payload,
    )
    return [nid] if nid else []


def _has_no_recorded_sale(
    db: Session,
    org_id: uuid.UUID,
    client_id: uuid.UUID,
) -> bool:
    """True iff this client has zero succeeded Stripe rows AND zero paid-like Whop rows.

    Mirrors :func:`_is_first_succeeded_payment` but treats any prior succeeded payment
    as "already a customer" — used to gate the pre-sale post-booking playbook so we
    never email "thanks for booking" copy to someone who already paid.
    """
    stripe_count = int(
        db.query(func.count(StripePayment.id))
        .filter(
            StripePayment.org_id == org_id,
            StripePayment.client_id == client_id,
            StripePayment.status == "succeeded",
        )
        .scalar()
        or 0
    )
    if stripe_count > 0:
        return False
    whop_count = int(
        db.query(func.count(WhopPayment.id))
        .filter(
            WhopPayment.org_id == org_id,
            WhopPayment.client_id == client_id,
            WhopPayment.status.in_(("paid", "succeeded", "completed", "successful")),
        )
        .scalar()
        or 0
    )
    return whop_count == 0


def _booking_matches_trigger_config(
    *,
    provider: str,
    event_type_id: Optional[str],
    trigger_config: Optional[Dict[str, Any]],
) -> bool:
    """Decide whether a freshly created booking matches the rule's trigger_config.

    Empty / missing config = do NOT fire (safety: prevents accidental blast-emails on
    every booking before the operator has picked an event). To opt into "every booking",
    set ``match_all_events = True`` in the trigger config.
    """
    if not isinstance(trigger_config, dict):
        return False
    cfg_provider = str(trigger_config.get("provider") or "any").lower()
    if cfg_provider not in ("any", provider):
        return False
    if trigger_config.get("match_all_events"):
        return True
    raw_ids = trigger_config.get("event_type_ids") or []
    if not isinstance(raw_ids, (list, tuple)) or not raw_ids:
        return False
    if not event_type_id:
        return False
    target = str(event_type_id).strip()
    return any(str(x).strip() == target for x in raw_ids if str(x).strip())


def on_booking_created_pre_sale(
    db: Session,
    *,
    org_id: uuid.UUID,
    client_id: uuid.UUID,
    provider: str,                      # "calcom" | "calendly"
    external_booking_id: str,           # Cal.com event id / Calendly event uuid
    event_type_id: Optional[str] = None,
    event_type_label: Optional[str] = None,
    attendee_email: Optional[str] = None,
    start_time: Optional[datetime] = None,
) -> List[uuid.UUID]:
    """
    Enqueue the ``pre_sale_post_booking`` playbook for a freshly created booking.

    Three gates -- ALL must pass -- so post-booking emails never blast historical
    bookings or paying customers:

    1. The rule's ``trigger_config`` matches this booking (provider + event type id, or
       ``match_all_events`` opt-in). Defaults to no-match so an empty config = silent.
    2. ``_has_no_recorded_sale``: zero succeeded Stripe + zero paid-like Whop rows
       attributed to the client. Once they pay, win/onboarding playbooks take over.
    3. Recency: ``start_time`` (when known) must be roughly in the future. Backfilled
       sync of years-old past bookings does not trigger live emails.

    Idempotency keys off ``provider:<external_booking_id>`` so re-syncing or webhook
    redelivery is a no-op.
    """
    rule = _get_rule(db, org_id, Playbook.PRE_SALE_POST_BOOKING.value)
    if not rule or not rule.enabled:
        return []

    trigger_config = rule.trigger_config if isinstance(rule.trigger_config, dict) else None
    if not _booking_matches_trigger_config(
        provider=str(provider).lower(),
        event_type_id=event_type_id,
        trigger_config=trigger_config,
    ):
        return []

    client = (
        db.query(Client)
        .filter(Client.id == client_id, Client.org_id == org_id)
        .first()
    )
    if not client:
        return []
    if not audience_filter_passes(client, rule.audience_filter):
        return []

    if not _has_no_recorded_sale(db, org_id, client_id):
        LOG.info(
            "automation: skipping pre_sale_post_booking for client %s -- "
            "client already has a recorded sale.",
            client_id,
        )
        return []

    # Recency: drop bookings whose start_time is well in the past (backfill imports).
    if start_time is not None:
        try:
            start_naive = (
                start_time.replace(tzinfo=None) if start_time.tzinfo is not None else start_time
            )
            age_seconds = (_now() - start_naive).total_seconds()
        except (TypeError, ValueError, AttributeError):
            age_seconds = 0
        max_past_seconds = POST_BOOKING_FUTURE_TOLERANCE_HOURS * 3600
        if age_seconds > max_past_seconds:
            LOG.info(
                "automation: skipping pre_sale_post_booking for client %s -- "
                "booking start is %.0fh in the past (cap %sh).",
                client_id,
                age_seconds / 3600,
                POST_BOOKING_FUTURE_TOLERANCE_HOURS,
            )
            return []

    payload = {
        "trigger": "booking.created.pre_sale",
        "provider": str(provider).lower(),
        "external_booking_id": external_booking_id,
        "event_type_id": event_type_id,
        "event_type_label": event_type_label,
        "attendee_email": attendee_email,
        "start_time": start_time.isoformat() if start_time else None,
    }
    nid = _enqueue_job(
        db,
        org_id=org_id,
        client_id=client_id,
        rule=rule,
        trigger_event="booking.created.pre_sale",
        discriminator=f"{str(provider).lower()}:{external_booking_id}",
        payload=payload,
    )
    return [nid] if nid else []


def on_lifecycle_entered_offboarding(
    db: Session,
    *,
    org_id: uuid.UUID,
    client_id: uuid.UUID,
) -> List[uuid.UUID]:
    """Enqueue offboarding_recap_ask once per client per offboarding window (date-bucketed)."""
    rule = _get_rule(db, org_id, Playbook.OFFBOARDING_RECAP_ASK.value)
    if not rule or not rule.enabled:
        return []
    client = (
        db.query(Client)
        .filter(Client.id == client_id, Client.org_id == org_id)
        .first()
    )
    if not client:
        return []
    if not audience_filter_passes(client, rule.audience_filter):
        return []

    today = _now().strftime("%Y-%m-%d")
    payload = {"trigger": "lifecycle.offboarding", "entered_at": _now().isoformat()}
    nid = _enqueue_job(
        db,
        org_id=org_id,
        client_id=client_id,
        rule=rule,
        trigger_event="lifecycle.offboarding",
        discriminator=f"offboarding:{today}",
        payload=payload,
    )
    return [nid] if nid else []


# ---------------------------------------------------------------------------
# Default rule seeding (idempotent)
# ---------------------------------------------------------------------------

def seed_default_rules(db: Session, org_id: uuid.UUID) -> List[AutomationRule]:
    """
    Insert disabled default rules for any playbooks the org doesn't have yet.

    This is called from the ``GET /automations/rules`` handler so the four defaults
    show up the first time a user opens the Automations tab.
    """
    existing = {
        r.playbook: r
        for r in db.query(AutomationRule).filter(AutomationRule.org_id == org_id).all()
    }
    defaults: List[Tuple[str, Dict[str, Any]]] = [
        (
            Playbook.PRE_SALE_POST_BOOKING.value,
            {
                "delay_seconds": 0,
                "content_mode": ContentMode.AI_GENERATED.value,
                "subject_template": "Quick note before our call, {{first_name}}",
                # Lifecycle filter intentionally permissive: pre-sale leads can be in any
                # non-paying state. The "no recorded sale" check inside the engine is the
                # real gate; audience_filter is left for advanced segmentation if needed.
                "audience_filter": None,
                # Trigger config defaults to "no events selected" so seeding doesn't
                # accidentally email every new booking. Operator opens the card and picks
                # the specific Cal.com / Calendly events that should fire.
                "trigger_config": {
                    "provider": "any",
                    "event_type_ids": [],
                    "match_all_events": False,
                },
                "combine_top_n": 1,
                "require_approval": False,
            },
        ),
        (
            Playbook.FIRST_PAYMENT_ONBOARDING.value,
            {
                "delay_seconds": 0,
                "content_mode": ContentMode.AI_GENERATED.value,
                "subject_template": "Welcome to the program — your first steps",
                "audience_filter": {"lifecycle_in": ["active"]},
                "combine_top_n": 1,
                "require_approval": False,
            },
        ),
        (
            Playbook.FIRST_PAYMENT_REFERRAL.value,
            {
                "delay_seconds": 60 * 60,  # one hour after onboarding
                "content_mode": ContentMode.AI_GENERATED.value,
                "subject_template": "One quick favor — share with a friend",
                "audience_filter": {"lifecycle_in": ["active"]},
                "combine_top_n": 1,
                "require_approval": False,
            },
        ),
        (
            Playbook.WIN_COMBINED_ASK.value,
            {
                "delay_seconds": 60 * 30,
                "content_mode": ContentMode.AI_GENERATED.value,
                "subject_template": None,
                "audience_filter": {"lifecycle_in": ["active", "offboarding"]},
                # Empty opportunity_priority + combine_top_n=3 = full LLM autonomy.
                # The picker is free to choose 1, 2, or all 3 of
                # {referral, upsell, testimonial} based on the client's signals.
                "opportunity_priority": [],
                "combine_top_n": 3,
                "require_approval": True,
                "approval_ttl_hours": 48,
            },
        ),
        (
            Playbook.OFFBOARDING_RECAP_ASK.value,
            {
                "delay_seconds": 0,
                "content_mode": ContentMode.AI_GENERATED.value,
                "subject_template": "Your wins so far — and what's next",
                "audience_filter": {"lifecycle_in": ["offboarding"]},
                "opportunity_priority": [],
                "combine_top_n": 3,
                "require_approval": True,
                "approval_ttl_hours": 72,
            },
        ),
    ]

    created: List[AutomationRule] = []
    for playbook, defaults_dict in defaults:
        if playbook in existing:
            continue
        rule = AutomationRule(
            id=uuid.uuid4(),
            org_id=org_id,
            playbook=playbook,
            enabled=False,
            **defaults_dict,
        )
        db.add(rule)
        created.append(rule)
    if created:
        db.flush()
    return created


# ---------------------------------------------------------------------------
# Convenience: pull AI profile context for the worker / preview path
# ---------------------------------------------------------------------------

def resolve_ai_profile_context(
    db: Session,
    org_id: uuid.UUID,
) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    """Return (ai_profile_for_llm, offer_ladder) using the org's primary intelligence user.

    Mirrors ``resolve_org_offer_ladder``; extracted into the engine so the worker can
    bootstrap rendering context without a request-bound user. Selects ``role::text`` so
    legacy mixed-case ``users.role`` rows do not trip the strict PG enum reader.
    """
    role_rank = {"owner": 0, "admin": 1, "member": 2}
    from types import SimpleNamespace

    candidates: list[tuple[int, Any, Any]] = []

    user_rows = db.execute(
        text(
            "SELECT id, email, ai_profile, role::text AS role, created_at FROM users "
            "WHERE org_id = :org_id AND ai_profile IS NOT NULL"
        ),
        {"org_id": str(org_id)},
    ).fetchall()
    for row in user_rows:
        rank = role_rank.get(str(row.role or "").strip().lower(), 99)
        candidates.append((rank, row.created_at or datetime.min, row))

    uo_rows = db.execute(
        text(
            "SELECT u.id, u.email, uo.ai_profile, u.role::text AS role, u.created_at "
            "FROM user_organizations uo "
            "JOIN users u ON u.id = uo.user_id "
            "WHERE uo.org_id = :org_id AND uo.ai_profile IS NOT NULL"
        ),
        {"org_id": str(org_id)},
    ).fetchall()
    seen_ids = {str(r.id) for r in user_rows}
    for row in uo_rows:
        if str(row.id) in seen_ids:
            continue
        rank = role_rank.get(str(row.role or "").strip().lower(), 99)
        candidates.append((rank, row.created_at or datetime.min, row))

    if not candidates:
        ladder = resolve_org_offer_ladder(db, org_id)
        return None, ladder

    candidates.sort(key=lambda item: (item[0], item[1]))
    profile: Optional[Dict[str, Any]] = None
    for _, _, row in candidates:
        raw_profile = getattr(row, "ai_profile", None)
        candidate = extract_intelligence_profile_for_automation_llm(
            SimpleNamespace(id=row.id, email=row.email, ai_profile=raw_profile)
        )
        if candidate:
            profile = candidate
            break
    ladder = (profile or {}).get("offer_ladder") if profile else None
    if not ladder:
        ladder = resolve_org_offer_ladder(db, org_id)
    return profile, ladder

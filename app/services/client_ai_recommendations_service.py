"""
AI recommendation checklist: default lifecycle-based actions (no LLM yet).
Future: merge call-insight actions while preserving completion by stable `id`.

Personalized `detail` lines are recomputed on read from the current client record
so they stay relevant without resetting completion state.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from sqlalchemy.orm import Session

from app.models.client import Client, LifecycleState
from app.models.client_ai_recommendation_state import ClientAIRecommendationState


def _new_action(
    title: str,
    *,
    detail: Optional[str] = None,
    category: Optional[str] = None,
    priority: int = 0,
) -> Dict[str, Any]:
    return {
        "id": uuid.uuid4().hex,
        "title": title,
        "detail": detail,
        "category": category,
        "priority": priority,
        "completed": False,
        "completed_at": None,
    }


def _lifecycle_value(client: Client) -> str:
    ls = client.lifecycle_state
    if hasattr(ls, "value"):
        return str(ls.value)
    return str(ls)


def merge_call_insight_actions_for_fathom_record(
    db: Session,
    client: Client,
    fathom_call_record_id: uuid.UUID,
    next_steps: List[Dict[str, Any]],
) -> None:
    """
    Upsert checklist items from call-insight next_steps for one Fathom recording.
    Stable ids `ci-fr-{fathom_call_record_id}-{i}` so re-analysis replaces the same slots.
    """
    row = ensure_recommendation_state(db, client)
    prefix = f"ci-fr-{fathom_call_record_id}-"
    raw_actions = row.actions if isinstance(row.actions, list) else []
    actions = [dict(a) for a in raw_actions if isinstance(a, dict)]
    actions = [a for a in actions if not str(a.get("id", "")).startswith(prefix)]

    for i, ns in enumerate((next_steps or [])[:15]):
        if not isinstance(ns, dict):
            continue
        title = str(ns.get("title") or "").strip()[:300]
        detail = str(ns.get("detail") or "").strip()[:1200]
        if not title and not detail:
            continue
        aid = f"{prefix}{i}"
        try:
            prio = int(ns.get("priority") or 0)
        except (TypeError, ValueError):
            prio = 5
        actions.append(
            {
                "id": aid,
                "title": title or "Follow up from call",
                "detail": detail,
                "category": "call_follow_up",
                "priority": max(1, min(prio, 9)),
                "completed": False,
                "completed_at": None,
                "source": "call_insight",
                "preserve_detail": True,
                "fathom_call_record_id": str(fathom_call_record_id),
            }
        )

    row.actions = actions
    row.updated_at = datetime.now(timezone.utc)


def merge_prospect_voice_from_insight_into_client(client: Client, prospect_voice: Any) -> None:
    """Roll prospect language signals into client.meta for email mirroring (transcript-grounded)."""
    from sqlalchemy.orm.attributes import flag_modified

    if not isinstance(prospect_voice, dict):
        return
    meta = dict(client.meta) if isinstance(client.meta, dict) else {}
    prof = meta.get("prospect_voice_profile")
    if not isinstance(prof, dict):
        prof = {}
    for key, cap in (
        ("phrases_that_resonated", 25),
        ("tone_notes", 20),
        ("avoid_phrasing", 20),
    ):
        new_items = prospect_voice.get(key) or []
        if not isinstance(new_items, list):
            continue
        old = prof.get(key) or []
        if not isinstance(old, list):
            old = []
        merged = list(
            dict.fromkeys(
                [str(x)[:500] for x in old + new_items if str(x).strip()]
            )
        )[:cap]
        prof[key] = merged
    sol = prospect_voice.get("summary_one_liner")
    if sol and str(sol).strip():
        prof["summary_one_liner"] = str(sol).strip()[:400]
    prof["updated_at"] = datetime.now(timezone.utc).isoformat()
    meta["prospect_voice_profile"] = prof
    client.meta = meta
    flag_modified(client, "meta")


def action_supports_email_draft(client: Client, action: Dict[str, Any]) -> bool:
    """
    Email drafts are only offered when they match high-value intents:
    - Leads: follow-up / reminders for future calls (cold 1–2, warm 1–2)
    - Onboarding assist: warm lead materials + booking (warm 2); cold clear next step (cold 2)
    - Active: soliciting a (video) testimonial (active, priority 3 only)
    - Offboarding / dead: revive, re-sign, or re-engage (offboarding alumni path; all dead win-backs)

    Merged/custom actions may set ``supports_email_draft`` true/false explicitly in stored JSON.
    """
    if "supports_email_draft" in action and action.get("supports_email_draft") is not None:
        return bool(action.get("supports_email_draft"))

    if action.get("source") == "call_insight" or str(action.get("category") or "") == "call_follow_up":
        return True

    ls = _lifecycle_value(client)
    try:
        pri = int(action.get("priority") or 0)
    except (TypeError, ValueError):
        pri = 0

    cold = LifecycleState.COLD_LEAD.value
    warm = LifecycleState.WARM_LEAD.value
    active = LifecycleState.ACTIVE.value
    off = LifecycleState.OFFBOARDING.value
    dead = LifecycleState.DEAD.value

    eligible = {
        (cold, 1),
        (cold, 2),
        (warm, 1),
        (warm, 2),
        (active, 3),
        (off, 2),
        (dead, 1),
        (dead, 2),
        (dead, 3),
    }
    return (ls, pri) in eligible


def _client_context(client: Client) -> Dict[str, Any]:
    """Safe, short context for personalized copy (no raw notes in output)."""
    first = (client.first_name or "").strip()
    display = first if first else "this client"
    their = "their" if not first else f"{first}'s"

    progress = None
    try:
        if client.program_progress_percent is not None:
            progress = float(client.program_progress_percent)
    except (TypeError, ValueError):
        progress = None

    phase = "program"
    if progress is not None:
        if progress < 33:
            phase = "early program"
        elif progress < 66:
            phase = "mid-program"
        elif progress < 100:
            phase = "late program"
        else:
            phase = "completed program"

    has_notes = bool((client.notes or "").strip())
    notes_hint = " Pull specifics from your notes so it feels one-to-one." if has_notes else ""

    mrr = 0.0
    try:
        mrr = float(client.estimated_mrr or 0)
    except (TypeError, ValueError):
        mrr = 0.0
    mrr_line = ""
    if mrr > 0:
        mrr_line = f" With ~${mrr:.0f}/mo on the books, tie asks to the value they already see."

    program_dates = ""
    if client.program_end_date:
        try:
            end = client.program_end_date
            if isinstance(end, datetime):
                program_dates = f" Program wraps around {end.strftime('%b %d, %Y')}—use that in your timing."
        except Exception:
            pass

    return {
        "display": display,
        "their": their,
        "progress": progress,
        "phase": phase,
        "has_notes": has_notes,
        "notes_hint": notes_hint,
        "mrr_line": mrr_line,
        "program_dates": program_dates,
    }


def _personalized_detail_for_action(client: Client, action: Dict[str, Any]) -> str:
    """One short sentence under the title, grounded in this client's context."""
    ls = _lifecycle_value(client)
    pri = int(action.get("priority") or 0)
    ctx = _client_context(client)
    d, th, nh = ctx["display"], ctx["their"], ctx["notes_hint"]
    prog = ctx["progress"]
    phase = ctx["phase"]
    mrr_line = ctx["mrr_line"]
    pd = ctx["program_dates"]

    # Per (lifecycle, priority) — matches default_actions_for_client ordering
    if ls == LifecycleState.COLD_LEAD.value:
        if pri == 1:
            return f"Reach {d} while your last touch is still fresh—reference what they said they want next.{nh}"
        if pri == 2:
            return f"Reduce friction for {d}: one calendar link or concrete offer beats a vague “let me know.”{nh}"
        if pri == 3:
            return f"If {d} mentioned how they found you, log it—best leads often come from the same channels."

    if ls == LifecycleState.WARM_LEAD.value:
        if pri == 1:
            return f"On your next message to {d}, confirm fit on goals, timeline, and budget so nothing stalls quietly.{nh}"
        if pri == 2:
            return f"Send {d} a tight recap of options and a single CTA—decision energy fades fast.{nh}"
        if pri == 3:
            return f"Warm leads like {d} often know peers in the same situation—plant a soft referral seed after value lands."

    if ls == LifecycleState.ACTIVE.value:
        if pri == 1:
            pg = f" They are in {phase}" + (f" (~{prog:.0f}%)." if prog is not None else ".")
            return f"Anchor praise to something measurable for {d}.{pg}{nh}{mrr_line}"
        if pri == 2:
            return f"Pick one add-on that matches {th} stated goals—not every upsell fits every client.{mrr_line}{nh}"
        if pri == 3:
            return f"When {d} shares a win, that is the moment to ask for a short quote or story you can repurpose.{nh}"
        if pri == 4:
            return f"If {d} sounds thrilled, ask who else could use the same outcome—make it easy to forward.{nh}"

    if ls == LifecycleState.OFFBOARDING.value:
        if pri == 1:
            return f"Close the loop with {d} on outcomes before the relationship goes quiet—testimonials land best here.{pd}{nh}"
        if pri == 2:
            return f"Offer {d} a clear “what’s next” path (alumni, maintenance, next cohort) so momentum continues.{nh}"
        if pri == 3:
            return f"If the experience was strong, {d} is more likely to refer now than six months from now.{nh}"

    if ls == LifecycleState.DEAD.value:
        if pri == 1:
            return f"Lead with something specific {d} cared about—events, content, or a small win—not a generic blast.{nh}"
        if pri == 2:
            return f"Ask what changed for {d} in one sentence; offer a single low-effort way to restart if they are open.{nh}"
        if pri == 3:
            return f"Keep tone light until {d} engages; hard pitches on cold outreach usually bury replies."

    # Fallback by category
    cat = str(action.get("category") or "")
    if cat == "conversion":
        return f"Tailor the next step to what you know about {d}.{nh}"
    if cat == "referral":
        return f"Only after {d} signals satisfaction—make the ask specific and easy to act on.{nh}"
    if cat == "testimonial":
        return f"Capture {th} words soon after a peak moment so the story stays vivid.{nh}"
    if cat == "upsell":
        return f"Match any offer to {th} goals and current results.{mrr_line}{nh}"
    if cat == "win_back":
        return f"Reconnect with {d} using context you already have—never sound mass-broadcast.{nh}"
    if cat == "engagement":
        return f"Keep {d} oriented on progress and the next milestone.{nh}"

    return f"Shape this around what you know about {d} today.{nh}"


def _personalized_headline(client: Client, base_headline: Optional[str]) -> str:
    """Optional short personalization prefix; keep under ~120 chars total."""
    ctx = _client_context(client)
    d = ctx["display"]
    if d == "this client":
        return base_headline or "Suggested next steps"
    base = base_headline or "Suggested next steps"
    # Light touch: name in headline for warmth without rewriting entire string
    if base and d.lower() not in base.lower():
        return f"{base} — for {d}"
    return base


def default_actions_for_client(client: Client) -> tuple[Optional[str], List[Dict[str, Any]]]:
    """Return (headline, actions) for initial checklist."""
    ls = _lifecycle_value(client)

    if ls == LifecycleState.COLD_LEAD.value:
        return (
            "Move this lead toward a booked conversation",
            [
                _new_action("Send a personalized follow-up within 24–48 hours", category="conversion", priority=1),
                _new_action("Offer a clear next step (short call, audit, or trial)", category="conversion", priority=2),
                _new_action("Capture referral source if they mentioned how they found you", category="referral", priority=3),
            ],
        )
    if ls == LifecycleState.WARM_LEAD.value:
        return (
            "Convert interest into a committed start",
            [
                _new_action("Confirm goals, timeline, and budget fit on the next touchpoint", category="conversion", priority=1),
                _new_action("Send program/options summary and booking link", category="conversion", priority=2),
                _new_action("Ask who else they train with or know (referral seed)", category="referral", priority=3),
            ],
        )
    if ls == LifecycleState.ACTIVE.value:
        return (
            "Maximize retention, results, and expansion",
            [
                _new_action("Reinforce a recent win and next milestone", category="engagement", priority=1),
                _new_action("Identify one upsell or add-on that fits their goals", category="upsell", priority=2),
                _new_action("Ask for a testimonial or story you can use (with permission)", category="testimonial", priority=3),
                _new_action("Invite a referral when they express high satisfaction", category="referral", priority=4),
            ],
        )
    if ls == LifecycleState.OFFBOARDING.value:
        return (
            "Finish strong and stay connected",
            [
                _new_action("Confirm outcomes achieved and document a testimonial ask", category="testimonial", priority=1),
                _new_action("Offer alumni check-in or next program path", category="upsell", priority=2),
                _new_action("Request referrals if they had a strong experience", category="referral", priority=3),
            ],
        )
    if ls == LifecycleState.DEAD.value:
        return (
            "Win-back and re-engagement",
            [
                _new_action("Send a low-pressure check-in with a specific hook (event, offer, content)", category="win_back", priority=1),
                _new_action("Clarify what changed; offer one simple way to restart", category="win_back", priority=2),
                _new_action("Avoid hard sell until they respond positively", category="win_back", priority=3),
            ],
        )

    return ("Suggested next steps", [_new_action("Schedule next touchpoint and log outcome in notes", category="general", priority=1)])


def ensure_recommendation_state(db: Session, client: Client) -> ClientAIRecommendationState:
    row = (
        db.query(ClientAIRecommendationState)
        .filter(ClientAIRecommendationState.client_id == client.id)
        .first()
    )
    if row is not None and isinstance(row.actions, list) and len(row.actions) > 0:
        return row

    headline, actions = default_actions_for_client(client)
    now = datetime.now(timezone.utc)
    if row is None:
        row = ClientAIRecommendationState(
            client_id=client.id,
            org_id=client.org_id,
            headline=headline,
            actions=actions,
            created_at=now,
            updated_at=now,
        )
        db.add(row)
    else:
        row.headline = headline
        row.actions = actions
        row.updated_at = now
    db.commit()
    db.refresh(row)
    return row


_CATEGORY_TO_PRIORITY_ID = {
    "testimonial": "testimonials",
    "upsell": "revenue",
    "engagement": "retention",
    "conversion": "conversion",
    "referral": "referrals",
    "win_back": "win_back",
    "call_follow_up": "conversion",
    "general": "retention",
}


def _priority_sort_key(
    action: Dict[str, Any],
    pipeline_priorities: Optional[List[str]],
) -> tuple:
    """
    Sort key that respects the user's pipeline_priorities ordering.
    Actions whose category maps to a higher-ranked priority float up.
    Within the same priority rank, fall back to the action's own priority number.
    """
    base_pri = int(action.get("priority") or 0)
    if not pipeline_priorities:
        return (0, base_pri, str(action.get("title") or ""))

    cat = str(action.get("category") or "")
    mapped = _CATEGORY_TO_PRIORITY_ID.get(cat)
    if mapped and mapped in pipeline_priorities:
        rank = pipeline_priorities.index(mapped)
    else:
        rank = len(pipeline_priorities)
    return (rank, base_pri, str(action.get("title") or ""))


def get_recommendation_state_dict(
    db: Session,
    client: Client,
    *,
    pipeline_priorities: Optional[List[str]] = None,
) -> Dict[str, Any]:
    row = ensure_recommendation_state(db, client)
    actions = row.actions if isinstance(row.actions, list) else []
    actions_sorted = sorted(
        actions,
        key=lambda a: _priority_sort_key(a, pipeline_priorities),
    )
    enriched: List[Dict[str, Any]] = []
    for a in actions_sorted:
        if not isinstance(a, dict):
            continue
        ac = dict(a)
        if ac.get("preserve_detail") and ac.get("detail"):
            pass
        else:
            ac["detail"] = _personalized_detail_for_action(client, ac)
        ac["supports_email_draft"] = action_supports_email_draft(client, ac)
        enriched.append(ac)

    headline = _personalized_headline(client, row.headline)
    return {
        "client_id": str(client.id),
        "headline": headline,
        "actions": enriched,
        "updated_at": row.updated_at.isoformat() if row.updated_at else None,
    }


def set_action_completed(
    db: Session,
    client: Client,
    action_id: str,
    completed: bool,
    *,
    user_id: Optional[uuid.UUID] = None,
) -> Optional[Dict[str, Any]]:
    """Toggle one action by `id`. Returns updated action dict or None if not found."""
    row = ensure_recommendation_state(db, client)
    actions = row.actions if isinstance(row.actions, list) else []
    now = datetime.now(timezone.utc)
    found = False
    out: Optional[Dict[str, Any]] = None
    new_actions = []
    for a in actions:
        if not isinstance(a, dict):
            continue
        ac = dict(a)
        if str(ac.get("id")) == str(action_id):
            found = True
            ac["completed"] = bool(completed)
            ac["completed_at"] = now.isoformat() if completed else None
            if user_id:
                ac["completed_by_user_id"] = str(user_id)
            elif "completed_by_user_id" in ac and not completed:
                ac.pop("completed_by_user_id", None)
            out = ac
        new_actions.append(ac)
    if not found:
        return None
    row.actions = new_actions
    row.updated_at = now
    db.commit()
    db.refresh(row)
    return out

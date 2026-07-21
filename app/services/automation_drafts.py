"""Draft assembly for automation email jobs (AI mode + HTML-template mode).

Single source of truth for what the worker actually sends. Reused by the
``/automations/preview`` endpoint so the user sees the exact bytes that will hit
Brevo.

Both modes share:
- merge-tag rendering ({{first_name}}, {{coach_name}}, {{referral_link}}, ...).
- Combined-ask copy assembly: when ``combine_top_n`` > 1 we stack the chosen
  opportunities into one email body instead of generating multiple sends.
  Writing-sample HTML templates (per sample) are used as-is when content_mode
  is html_template — there is no global outer wrapper.
"""
from __future__ import annotations

import html
import json
import logging
import re
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.config import settings as app_settings
from app.models.automation import (
    AutomationEmailJob,
    AutomationRule,
    ContentMode,
    Playbook,
)
from app.models.client import Client
from app.models.client_call_insight import ClientCallInsight
from app.models.organization import Organization
from app.services.automation_engine import (
    OpportunityScore,
    audience_filter_passes,
    resolve_ai_profile_context,
)
from app.services.automation_opportunity_picker import (
    OpportunityPick,
    pick_combined_ask,
)
from app.services.llm_client import chat_json, llm_available, truncate_for_tokens
from app.services.user_ai_profile_context import normalize_writing_samples_for_llm

LOG = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Result shape
# ---------------------------------------------------------------------------

@dataclass
class AutomationDraft:
    subject: str
    body_plain: str
    html: str
    chosen_opportunities: List[str] = field(default_factory=list)
    merge_tags_resolved: Dict[str, str] = field(default_factory=dict)
    notes: List[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Merge tags
# ---------------------------------------------------------------------------

_MERGE_TAG_RE = re.compile(r"{{\s*([a-zA-Z0-9_\.]+)\s*}}")


def _coach_name(ai_profile: Optional[Dict[str, Any]], org_name: str) -> str:
    if isinstance(ai_profile, dict):
        for k in ("coach_name", "operator_name", "first_name"):
            v = ai_profile.get(k)
            if isinstance(v, str) and v.strip():
                return v.strip()[:80]
    return org_name


def _referral_link(ai_profile: Optional[Dict[str, Any]]) -> str:
    if not isinstance(ai_profile, dict):
        return ""
    links = ai_profile.get("asset_links") or []
    if isinstance(links, list):
        for entry in links:
            if isinstance(entry, dict):
                label = str(entry.get("label", "")).lower()
                if "referral" in label and entry.get("url"):
                    return str(entry["url"]).strip()
    return ""


def build_merge_tag_values(
    *,
    client: Client,
    org_name: str,
    ai_profile: Optional[Dict[str, Any]],
    ladder: Optional[Dict[str, Any]],
    chosen_opportunities: List[str],
    upsell_offer: Optional[Dict[str, Any]] = None,
    insight: Optional[ClientCallInsight] = None,
) -> Dict[str, str]:
    first_name = (client.first_name or "").strip()
    last_name = (client.last_name or "").strip()
    full_name = (first_name + " " + last_name).strip() or (client.email or "")
    referral_offer = (ladder or {}).get("referral_offer") or {}
    incentive = str(referral_offer.get("incentive", "")).strip()
    eligibility = str(referral_offer.get("eligibility", "")).strip()
    upsell_name = ""
    upsell_promise = ""
    if upsell_offer:
        upsell_name = str(upsell_offer.get("name", "")).strip()
        upsell_promise = str(upsell_offer.get("promise", "")).strip()
    insight_headline = ""
    if insight is not None and isinstance(insight.insight_json, dict):
        h = insight.insight_json.get("headline") or insight.insight_json.get("client_state_synthesis")
        if isinstance(h, str):
            insight_headline = h.strip()[:240]

    return {
        "first_name": first_name or "there",
        "last_name": last_name,
        "full_name": full_name,
        "client_email": (client.email or "").strip(),
        "coach_name": _coach_name(ai_profile, org_name),
        "org_name": org_name,
        "referral_offer": incentive or "share with a friend",
        "referral_eligibility": eligibility,
        "referral_link": _referral_link(ai_profile),
        "upsell_name": upsell_name,
        "upsell_promise": upsell_promise,
        "chosen_opportunities": ", ".join(chosen_opportunities),
        "insight_headline": insight_headline,
    }


def render_merge_tags(template: str, values: Dict[str, str]) -> str:
    if not template:
        return ""

    def _sub(m: re.Match) -> str:
        key = m.group(1)
        if key in values:
            return values[key] or ""
        return m.group(0)

    return _MERGE_TAG_RE.sub(_sub, template)


# ---------------------------------------------------------------------------
# Plain HTML helpers
# ---------------------------------------------------------------------------


def _plain_to_html(body: str) -> str:
    body = body.strip()
    if not body:
        return "<p></p>"
    parts = [p for p in body.split("\n\n") if p.strip()]
    pieces = []
    for p in parts:
        inner = html.escape(p).replace("\n", "<br/>")
        pieces.append(f"<p>{inner}</p>")
    return "".join(pieces)


# ---------------------------------------------------------------------------
# HTML-template mode
# ---------------------------------------------------------------------------

_PLAYBOOK_DEFAULT_SAMPLE_KIND = {
    Playbook.FIRST_PAYMENT_ONBOARDING.value: "onboarding_email",
    Playbook.FIRST_PAYMENT_REFERRAL.value: "referral_campaign",
    Playbook.WIN_COMBINED_ASK.value: "referral_campaign",
    Playbook.OFFBOARDING_RECAP_ASK.value: "re_sign_campaign",
    Playbook.PRE_SALE_POST_BOOKING.value: "onboarding_email",
    Playbook.PRE_SALE_PRE_MEETING.value: "onboarding_email",
}


def _resolve_writing_sample(
    ai_profile: Optional[Dict[str, Any]],
    ref: Optional[Dict[str, Any]],
    *,
    playbook: Optional[str] = None,
) -> Optional[Dict[str, str]]:
    """Resolve {kind, title|sample_kind} reference -> writing sample dict.

    When no ref is supplied (or the ref didn't match) and the caller passed a
    ``playbook``, fall back to the playbook's default sample kind so an operator
    who simply added a writing sample of the right type doesn't have to wire up
    a reference manually.
    """
    if not isinstance(ai_profile, dict):
        return None
    samples = normalize_writing_samples_for_llm(ai_profile.get("writing_samples")) or []
    if not samples:
        return None
    if isinstance(ref, dict):
        kind = ref.get("kind")
        if kind == "writing_samples_by_title":
            title = str(ref.get("title", "")).strip().lower()
            if title:
                for s in samples:
                    if str(s.get("title", "")).strip().lower() == title:
                        return s
        elif kind == "writing_samples_by_kind":
            sample_kind = str(ref.get("sample_kind", "")).strip()
            if sample_kind:
                for s in samples:
                    if s.get("kind") == sample_kind:
                        return s
    if playbook:
        default_kind = _PLAYBOOK_DEFAULT_SAMPLE_KIND.get(playbook)
        if default_kind:
            for s in samples:
                if s.get("kind") == default_kind:
                    return s
    return None


def _html_template_subject_default(playbook: str) -> str:
    return {
        Playbook.PRE_SALE_POST_BOOKING.value: "Quick note before our call",
        Playbook.PRE_SALE_PRE_MEETING.value: "Looking forward to talking soon",
        Playbook.FIRST_PAYMENT_ONBOARDING.value: "Welcome — your first steps",
        Playbook.FIRST_PAYMENT_REFERRAL.value: "Thanks — a quick favor",
        Playbook.WIN_COMBINED_ASK.value: "A quick ask after your win",
        Playbook.OFFBOARDING_RECAP_ASK.value: "Your wins — and what's next",
    }.get(playbook, "A quick note")


def _build_html_template_draft(
    rule: AutomationRule,
    *,
    client: Client,
    ai_profile: Optional[Dict[str, Any]],
    ladder: Optional[Dict[str, Any]],
    chosen: List[OpportunityScore],
    sender_name: str,
    sender_email: str,
    org_name: str,
    insight: Optional[ClientCallInsight] = None,
) -> AutomationDraft:
    sample = _resolve_writing_sample(
        ai_profile, rule.html_template_ref, playbook=rule.playbook
    )
    notes: List[str] = []
    if not sample:
        default_kind = _PLAYBOOK_DEFAULT_SAMPLE_KIND.get(rule.playbook)
        if default_kind:
            notes.append(
                f"No writing sample matched (looked for explicit ref, then any '{default_kind}' "
                "sample). Add one in Intelligence → Writing Samples or switch this playbook to "
                "AI-generated mode."
            )
        else:
            notes.append(
                "html_template_ref did not match any Intelligence writing sample; falling back to plain wrapper."
            )

    chosen_names = [o.name for o in chosen]
    upsell_offer = next(
        (o.upsell_offer for o in chosen if o.name == "upsell" and o.upsell_offer), None
    )
    merge_values = build_merge_tag_values(
        client=client,
        org_name=org_name,
        ai_profile=ai_profile,
        ladder=ladder,
        chosen_opportunities=chosen_names,
        upsell_offer=upsell_offer,
        insight=insight,
    )
    subject = render_merge_tags(
        (rule.subject_template or _html_template_subject_default(rule.playbook)),
        merge_values,
    )

    if sample and sample.get("html_template"):
        rendered_html = render_merge_tags(sample["html_template"], merge_values)
        body_plain = render_merge_tags(sample.get("body") or _strip_tags(rendered_html), merge_values)
    elif sample and sample.get("body"):
        body_plain = render_merge_tags(sample["body"], merge_values)
        rendered_html = _plain_to_html(body_plain)
    else:
        body_plain = _fallback_plain(rule.playbook, merge_values, chosen_names)
        rendered_html = _plain_to_html(body_plain)

    return AutomationDraft(
        subject=subject,
        body_plain=body_plain,
        html=rendered_html,
        chosen_opportunities=chosen_names,
        merge_tags_resolved=merge_values,
        notes=notes,
    )


def _strip_tags(s: str) -> str:
    return re.sub(r"<[^>]+>", "", s).strip()


def _fallback_plain(
    playbook: str,
    merge: Dict[str, str],
    chosen: List[str],
) -> str:
    fn = merge.get("first_name") or "there"
    coach = merge.get("coach_name") or merge.get("org_name") or "Coach"
    ref_link = merge.get("referral_link") or ""
    ref_offer = merge.get("referral_offer") or "share with a friend"
    upsell = merge.get("upsell_name") or "next step"

    if playbook == Playbook.FIRST_PAYMENT_ONBOARDING.value:
        return (
            f"Hi {fn},\n\n"
            "Welcome — really excited to have you on board.\n\n"
            "Here are your first three steps:\n"
            "1. Reply to this email with your top goal for the next 30 days.\n"
            "2. Add me on your usual messaging channel so we can keep momentum.\n"
            "3. Book your first check-in.\n\n"
            f"Talk soon,\n{coach}"
        )
    if playbook in (
        Playbook.PRE_SALE_POST_BOOKING.value,
        Playbook.PRE_SALE_PRE_MEETING.value,
    ):
        timing = (
            "We're looking forward to talking soon."
            if playbook == Playbook.PRE_SALE_PRE_MEETING.value
            else "Thanks for booking — looking forward to our call."
        )
        return (
            f"Hi {fn},\n\n"
            f"{timing}\n\n"
            "To make the most of our time together, reply with your top goal and any "
            "context that would help me prepare.\n\n"
            f"See you soon,\n{coach}"
        )
    if playbook == Playbook.FIRST_PAYMENT_REFERRAL.value:
        link_suffix = ref_link.strip()
        lines: List[str] = []
        if ref_offer and ref_offer.strip():
            lines.append(ref_offer.strip())
        if link_suffix:
            lines.append(link_suffix)
        offer_block = "\n".join(lines) if lines else "Reply when you're ready — happy to walk you through our referral perks."
        return (
            f"Hi {fn},\n\n"
            "Great to have you with us — glad you're in.\n\n"
            "When you're ready to explore referrals, here's what we're offering people you send our way:\n\n"
            f"{offer_block}\n\n"
            f"Best,\n{coach}"
        )
    if playbook == Playbook.WIN_COMBINED_ASK.value:
        bits: List[str] = []
        if "referral" in chosen:
            suffix = f" Details: {ref_offer}" if ref_offer else ""
            link_part = f" {ref_link}" if ref_link.strip() else ""
            bits.append(
                "If you'd like to help someone else benefit in the same way, here's what we're offering for people you introduce"
                + suffix
                + link_part
                + " — totally your call whenever it feels natural."
            )
        if "upsell" in chosen:
            bits.append(
                f"When you're ready, the natural next step is {upsell} — happy to walk through it together."
            )
        if "testimonial" in chosen:
            bits.append(
                "Would you be open to recording a 60-second testimonial? It would mean a lot."
            )
        body = (
            f"Hi {fn},\n\n"
            "Loved hearing about your recent win — that's huge.\n\n"
            + "\n\n".join(bits)
            + f"\n\nTalk soon,\n{coach}"
        )
        return body
    if playbook == Playbook.OFFBOARDING_RECAP_ASK.value:
        return (
            f"Hi {fn},\n\n"
            "As we wrap this chapter, I want to recap what you accomplished and where you could go from here.\n\n"
            "If you'd like others to benefit the way you have, here's what we're offering "
            f"({ref_offer}){(' — ' + ref_link.strip()) if ref_link.strip() else ''}. "
            "And if you'd be open to a short testimonial, it genuinely helps future clients see what's possible.\n\n"
            f"Proud of you,\n{coach}"
        )
    return f"Hi {fn},\n\nThanks for being part of the program.\n\n{coach}"


# ---------------------------------------------------------------------------
# AI-generated mode
# ---------------------------------------------------------------------------

_AI_SYSTEM = (
    "You are an outbound email writer for a small coaching business. "
    "Output a single JSON object only, with keys: 'subject' (string), 'body_plain' (string). "
    "The body_plain must be ready to send: no bracket placeholders, no markdown headings, no HTML. "
    "DATA.intel.intelligence_profile is the full export of the operator's Intelligence tab: voice "
    "(writing_style, writing_tone, coaching_style, client_management_philosophy), business "
    "(business_description, target_audience, unique_selling_proposition), sales and marketing "
    "(sales_framework, sales_tactics, marketing_strategy, marketing_channels), pipeline_priorities, "
    "offer_ladder (core, unified upsells/add-ons, referral_offer, positioning_notes, objection_handlers), "
    "writing_samples (voice/campaign examples), and asset_links (Resource Library — label + url). "
    "Match the operator's voice using intelligence_profile. When asset_links or the operator's "
    "extra instructions mention sharing a resource, workbook, calendar, or link, include the exact "
    "URL from asset_links in body_plain (plain URLs are fine). Never invent links, domains, or paths — "
    "only use URLs explicitly present in intelligence_profile.asset_links. "
    "When DATA.task.combined_asks is non-empty, weave each ask into one cohesive note "
    "(do NOT generate three emails). DATA.task.combined_ask_strategy explains WHY this combination was "
    "chosen (and the per-ask reasoning) -- honor that intent and ordering, lead with the highest-priority "
    "ask, and keep secondary asks lighter so the message does not feel pushy. "
    "When intelligence_profile.writing_samples is non-empty, mirror their structure / cadence -- "
    "rewrite for this recipient using only facts in DATA. Never invent stats, dates, or quotes."
)

_MAX_OPERATOR_AI_PROMPT_LEN = 8000

_REFERRAL_NATURAL_VOICE = (
    "\n\nREFERRAL / INVITE-OTHERS TONE (when this email includes referrals, affiliate invites, or sharing the program): "
    "Frame the ask as VALUE FOR THE RECIPIENT first — e.g. onboarding them onto a perk ('I'd love to get you onboarded on our affiliate program…'), "
    "or a conditional offer tied to who they refer ('Would you be interested in [free offer]? If so, here's what we're offering for anyone you refer…'). "
    "After a win, congratulate specifically, then extend impact naturally (e.g. 'Congrats on [WIN]… I'd love to help share that with more people. "
    "Who else do you know who could benefit from [WIN]?'). "
    "NEVER sound desperate: no needy one-liners begging for introductions, vague guilt ('anything helps'), or cold transactional asks with no relational setup. "
    "Light politeness is fine; do not use groveling 'please' piles. Ground wins and perks in DATA only."
)


def _referral_natural_voice_append(rule: AutomationRule, chosen_names: List[str]) -> str:
    pb = getattr(rule, "playbook", None) or ""
    if pb == Playbook.FIRST_PAYMENT_REFERRAL.value or "referral" in chosen_names:
        return _REFERRAL_NATURAL_VOICE
    return ""


def _compose_automation_ai_system_prompt(rule: AutomationRule) -> str:
    """Base automation prompt plus optional per-rule operator instructions (AI content mode)."""
    extra = (getattr(rule, "ai_content_system_prompt", None) or "").strip()
    if not extra:
        return _AI_SYSTEM
    if len(extra) > _MAX_OPERATOR_AI_PROMPT_LEN:
        extra = extra[:_MAX_OPERATOR_AI_PROMPT_LEN].rstrip() + "\n… [truncated]"
    return (
        _AI_SYSTEM
        + "\n\n---\nOPERATOR_PLAYBOOK_SYSTEM_PROMPT (highest priority for this automation — tone, "
        "structure, CTAs, resources to emphasize, compliance, or voice constraints):\n"
        + extra
    )


def _build_ai_payload(
    rule: AutomationRule,
    *,
    client: Client,
    ai_profile: Optional[Dict[str, Any]],
    ladder: Optional[Dict[str, Any]],
    chosen: List[OpportunityScore],
    insight: Optional[ClientCallInsight],
    pick: Optional[OpportunityPick] = None,
) -> Dict[str, Any]:
    chosen_names = [o.name for o in chosen]
    upsell = next((o.upsell_offer for o in chosen if o.name == "upsell"), None)
    insight_blob: Dict[str, Any] = {}
    if insight is not None and isinstance(insight.insight_json, dict):
        j = insight.insight_json
        insight_blob = {
            "wins": j.get("wins") or [],
            "opportunity_tags": j.get("opportunity_tags") or [],
            "client_state_synthesis": (j.get("client_state_synthesis") or "")[:600],
            "next_steps": (j.get("next_steps") or [])[:3],
        }

    referral = (ladder or {}).get("referral_offer") or {}

    picker_blob: Dict[str, Any] = {}
    if pick is not None:
        picker_blob = {
            "mode": pick.picker_mode,
            "rationale": pick.rationale or "",
            "per_choice": dict(pick.per_choice_rationale or {}),
        }

    intel_profile = ai_profile if isinstance(ai_profile, dict) else {}

    return {
        "task": {
            "playbook": rule.playbook,
            "subject_hint": rule.subject_template or "",
            "combined_asks": chosen_names,
            "combined_ask_strategy": picker_blob,
            "lifecycle_state": (
                client.lifecycle_state.value if hasattr(client.lifecycle_state, "value") else str(client.lifecycle_state)
            ),
        },
        "client": {
            "first_name": client.first_name,
            "last_name": client.last_name,
            "lifetime_revenue_cents": int(client.lifetime_revenue_cents or 0),
            "program_progress_percent": float(client.program_progress_percent) if client.program_progress_percent is not None else None,
            "notes_excerpt": (client.notes or "")[:600],
        },
        "intel": {
            "intelligence_profile": intel_profile,
            "referral_offer": referral,
            "upsell_offer": upsell or {},
        },
        "call_insight": insight_blob,
    }


def _build_ai_draft(
    rule: AutomationRule,
    *,
    client: Client,
    ai_profile: Optional[Dict[str, Any]],
    ladder: Optional[Dict[str, Any]],
    chosen: List[OpportunityScore],
    sender_name: str,
    sender_email: str,
    org_name: str,
    insight: Optional[ClientCallInsight] = None,
    org_id: Optional[uuid.UUID] = None,
    pick: Optional[OpportunityPick] = None,
) -> AutomationDraft:
    chosen_names = [o.name for o in chosen]
    upsell_offer = next(
        (o.upsell_offer for o in chosen if o.name == "upsell" and o.upsell_offer), None
    )
    merge_values = build_merge_tag_values(
        client=client,
        org_name=org_name,
        ai_profile=ai_profile,
        ladder=ladder,
        chosen_opportunities=chosen_names,
        upsell_offer=upsell_offer,
        insight=insight,
    )

    notes: List[str] = []
    subject = ""
    body_plain = ""
    if llm_available():
        payload = _build_ai_payload(
            rule,
            client=client,
            ai_profile=ai_profile,
            ladder=ladder,
            chosen=chosen,
            insight=insight,
            pick=pick,
        )
        user_prompt = "DATA = " + json.dumps(payload, ensure_ascii=False)
        user_prompt = truncate_for_tokens(user_prompt, 16000)
        try:
            system = _compose_automation_ai_system_prompt(rule)
            ref_append = _referral_natural_voice_append(rule, chosen_names)
            if ref_append:
                system += ref_append
            j = chat_json(system, user_prompt, temperature=0.5, org_id=org_id, feature="automation")
            subject = str(j.get("subject", "")).strip()
            body_plain = str(j.get("body_plain", "")).strip()
        except Exception as e:
            LOG.warning("automation AI draft failed: %s", e)
            notes.append(f"LLM draft failed ({e}); used fallback copy.")
    else:
        notes.append("LLM not configured; used deterministic fallback copy.")

    if not subject:
        subject = render_merge_tags(
            rule.subject_template or _html_template_subject_default(rule.playbook),
            merge_values,
        )
    if not body_plain:
        body_plain = _fallback_plain(rule.playbook, merge_values, chosen_names)

    body_html = _plain_to_html(body_plain)

    return AutomationDraft(
        subject=subject,
        body_plain=body_plain,
        html=body_html,
        chosen_opportunities=chosen_names,
        merge_tags_resolved=merge_values,
        notes=notes,
    )


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def _resolve_org_name(db: Session, org_id: uuid.UUID) -> str:
    o = db.query(Organization).filter(Organization.id == org_id).first()
    return (o.name if o and o.name else "Our team").strip() or "Our team"


def _resolve_sender(db: Session, org_id: uuid.UUID, ai_profile: Optional[Dict[str, Any]]) -> Dict[str, str]:
    """Use the org's first user email as a sane fallback sender (must be Brevo-verified).

    Selects role as text via raw SQL so legacy mixed-case rows (e.g. lowercase 'member')
    don't trip the strict PG enum reader on the ``users`` table.
    """
    role_rank = {"owner": 0, "admin": 1, "member": 2}
    rows = db.execute(
        text(
            "SELECT email, role::text AS role FROM users "
            "WHERE org_id = :org_id"
        ),
        {"org_id": str(org_id)},
    ).fetchall()
    if not rows:
        return {"email": "noreply@example.com", "name": _resolve_org_name(db, org_id)}
    sorted_rows = sorted(
        rows,
        key=lambda r: role_rank.get(str(r.role or "").strip().lower(), 99),
    )
    sender_email = sorted_rows[0].email
    sender_name = (
        (ai_profile or {}).get("coach_name")
        or (ai_profile or {}).get("operator_name")
        or _resolve_org_name(db, org_id)
    )
    return {"email": sender_email, "name": str(sender_name).strip() or sender_email}


def _resolve_chosen_opportunities(
    rule: AutomationRule,
    *,
    client: Client,
    insight: Optional[ClientCallInsight],
    ladder: Optional[Dict[str, Any]],
    ai_profile: Optional[Dict[str, Any]],
    health_score: Optional[float],
    org_id: Optional[uuid.UUID] = None,
) -> tuple[List[OpportunityScore], Optional[OpportunityPick]]:
    """Combined-ask playbooks delegate to the LLM picker; first-payment uses fixed mapping.

    Returns ``(chosen, pick)`` where ``pick`` is ``None`` for the deterministic mappings
    used by first-payment playbooks and an :class:`OpportunityPick` for combined-ask
    playbooks (so the rationale can flow into ``AutomationDraft.notes``).
    """
    if rule.playbook == Playbook.FIRST_PAYMENT_REFERRAL.value:
        return (
            [OpportunityScore(name="referral", score=99, rationale=["first-payment referral"])],
            None,
        )
    if rule.playbook == Playbook.FIRST_PAYMENT_ONBOARDING.value:
        return [], None

    ls = (
        client.lifecycle_state.value
        if hasattr(client.lifecycle_state, "value")
        else str(client.lifecycle_state)
    )
    in_offboarding = ls == "offboarding" or rule.playbook == Playbook.OFFBOARDING_RECAP_ASK.value

    insight_json = insight.insight_json if insight is not None else None
    pick = pick_combined_ask(
        client=client,
        insight_json=insight_json,
        ladder=ladder,
        ai_profile=ai_profile,
        health_score=health_score,
        in_offboarding=in_offboarding,
        rule=rule,
        org_id=org_id,
    )
    return pick.chosen, pick


def build_automation_email_draft(
    db: Session,
    *,
    rule: AutomationRule,
    client: Client,
    insight_id: Optional[uuid.UUID] = None,
    health_score: Optional[float] = None,
) -> AutomationDraft:
    """
    Materialize the email for a single (rule, client) pair. Pure function-of-state:
    given the same DB rows, the worker and the preview endpoint produce the same draft
    (modulo LLM stochasticity, which we cap by setting temperature=0.5 in worker mode).
    """
    org_id = client.org_id
    ai_profile, ladder = resolve_ai_profile_context(db, org_id)
    org_name = _resolve_org_name(db, org_id)
    sender = _resolve_sender(db, org_id, ai_profile)

    insight = None
    if insight_id is not None:
        insight = db.query(ClientCallInsight).filter(ClientCallInsight.id == insight_id).first()

    chosen, pick = _resolve_chosen_opportunities(
        rule,
        client=client,
        insight=insight,
        ladder=ladder,
        ai_profile=ai_profile,
        health_score=health_score,
        org_id=org_id,
    )

    if rule.content_mode == ContentMode.HTML_TEMPLATE.value:
        draft = _build_html_template_draft(
            rule,
            client=client,
            ai_profile=ai_profile,
            ladder=ladder,
            chosen=chosen,
            sender_name=sender["name"],
            sender_email=sender["email"],
            org_name=org_name,
            insight=insight,
        )
    else:
        draft = _build_ai_draft(
            rule,
            client=client,
            ai_profile=ai_profile,
            ladder=ladder,
            chosen=chosen,
            sender_name=sender["name"],
            sender_email=sender["email"],
            org_name=org_name,
            insight=insight,
            org_id=org_id,
            pick=pick,
        )

    if pick is not None:
        # Surface picker rationale at the TOP of notes so the preview UI shows
        # exactly why this combination was chosen before any other notes (e.g.
        # "LLM draft failed; used fallback copy").
        draft.notes = pick.to_notes() + draft.notes
    return draft


def resolve_sender_for_org(db: Session, org_id: uuid.UUID) -> Dict[str, str]:
    """Public helper used by the dispatcher when actually calling Brevo."""
    ai_profile, _ = resolve_ai_profile_context(db, org_id)
    return _resolve_sender(db, org_id, ai_profile)

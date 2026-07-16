"""LLM-driven combined-ask selector with deterministic fallback.

The combined-ask playbooks (``win_combined_ask``, ``offboarding_recap_ask``) used to
slice the deterministic ``score_opportunities`` ranking by ``rule.combine_top_n``. That
made every email feel mechanical: the engine could not understand that an 80%-progress
client with two recent wins is a perfect referral + upsell candidate but a poor
testimonial ask, while a 30% client with weaker signals deserves a single low-pressure
referral nudge.

This module gives the LLM full autonomy over the {referral, upsell, testimonial} subset
while still pinning hard guardrails:

- Output is constrained to JSON ``{chosen, rationale, per_choice}``.
- Names outside the canonical three are dropped.
- Operator-pinned rules (``rule.opportunity_priority`` non-empty) bypass the LLM.
- ``rule.combine_top_n`` (when > 0) caps the LLM's choices, so an operator who wants
  "only ever one ask" can still enforce that.
- Deterministic ``score_opportunities`` runs first, both as a fallback AND as a hint
  passed into the prompt so the LLM has a baseline to reason against rather than
  starting from scratch.
"""
from __future__ import annotations

import json
import logging
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence

from app.models.automation import AutomationRule
from app.models.client import Client
from app.services.automation_engine import (
    OpportunityScore,
    score_opportunities,
)
from app.services.llm_client import chat_json, llm_available, truncate_for_tokens

LOG = logging.getLogger(__name__)

_VALID_NAMES = ("referral", "upsell", "testimonial")
_MAX_CHOICES = 3


_PICKER_SYSTEM = (
    "You are the strategic decision layer for a coaching business's automated outreach. "
    "On every fired playbook you decide which of {referral, upsell, testimonial} should "
    "be asked of THIS lead in a SINGLE combined email -- pick 1, 2, or all 3 based "
    "purely on what maximizes return on investment for this specific client right now.\n\n"
    "Hard rules:\n"
    "1) Output ONLY a JSON object, no prose, no markdown.\n"
    "2) Schema: {\"chosen\": [string], \"rationale\": string, \"per_choice\": {string: string}}.\n"
    "3) `chosen` is an ordered list (highest-priority first) of 1-3 UNIQUE values from "
    "['referral','upsell','testimonial']. Never invent other values.\n"
    "4) `per_choice` keys must match `chosen`. Each value is a one-sentence reason.\n"
    "5) `rationale` is one short paragraph (<= 3 sentences) explaining the overall ROI logic.\n\n"
    "ROI heuristics (apply jointly, not in isolation):\n"
    "- DATA.signals.health_score: high (>=75) supports referral & testimonial; low (<40) "
    "  means only the lowest-friction ask -- usually testimonial or nothing extra.\n"
    "- DATA.signals.program_progress_percent: early (<30%) skews toward referral momentum; "
    "  mid (30-70%) is the sweet spot for upsell; late (>=70%) and offboarding favor "
    "  recap-style testimonial + referral.\n"
    "- DATA.signals.lifetime_revenue_cents: higher LTV makes upsell more credible.\n"
    "- DATA.intelligence.pipeline_priorities: weight HEAVILY when present -- the operator has "
    "  declared what they care about most; respect their order as a strong tiebreaker.\n"
    "- DATA.intelligence.ai_profile: tone & coaching_style hint at how aggressive to be.\n"
    "- DATA.intelligence.ladder.upsells: only ask for upsell when at least one ladder offer "
    "  meaningfully fits the client's signals (no fit = drop upsell).\n"
    "- DATA.insight.wins / opportunity_tags: a fresh win unlocks all three asks; explicit "
    "  ROI tags (referral/upsell/testimonial) are very strong evidence.\n"
    "- DATA.deterministic_scores: a system baseline -- use as a hint, override when the "
    "  contextual story warrants it.\n\n"
    "Calibration:\n"
    "- Asking for too much can backfire. Only include all 3 when context strongly supports "
    "  it (e.g. recent win + offboarding window, or very high health + high LTV + ROI tags).\n"
    "- If signals are weak across the board, still pick exactly 1 (the lowest-risk lift).\n"
    "- Respect DATA.max_choices as an operator-set hard cap.\n"
    "- Downstream outreach phrases referral invites as value for the recipient (perks/affiliate-style onboarding, conditional "
    "offers paired with referrals, congratulate-then-widen-impact); omit referral unless that honest framing fits this story."
)


@dataclass
class OpportunityPick:
    """Outcome of the combined-ask selector.

    ``chosen`` flows downstream into the email body assembly. ``rationale`` and
    ``per_choice_rationale`` are surfaced in the PlaybookCard preview and the Outreach
    inbox so an operator can see *why* a particular blend was chosen.
    """

    chosen: List[OpportunityScore]
    rationale: Optional[str] = None
    per_choice_rationale: Dict[str, str] = field(default_factory=dict)
    picker_mode: str = "deterministic"  # "llm" | "deterministic" | "rule_pinned"
    fallback_reason: Optional[str] = None

    def to_audit(self) -> Dict[str, Any]:
        """Compact JSON payload for ``AutomationEmailJob.payload_json['picker_decision']``."""
        return {
            "chosen": [o.name for o in self.chosen],
            "mode": self.picker_mode,
            "rationale": self.rationale,
            "per_choice": dict(self.per_choice_rationale),
            "fallback_reason": self.fallback_reason,
        }

    def to_notes(self) -> List[str]:
        """Human-friendly note lines for the preview/inbox UI."""
        lines: List[str] = []
        if not self.chosen:
            return lines
        names = ", ".join(o.name for o in self.chosen)
        prefix_map = {
            "llm": "LLM picked combined ask",
            "rule_pinned": "Operator-pinned combined ask",
            "deterministic": "Deterministic combined ask",
        }
        prefix = prefix_map.get(self.picker_mode, "Combined ask")
        lines.append(f"{prefix}: {names}")
        if self.rationale:
            lines.append(f"Why: {self.rationale}")
        for name, reason in self.per_choice_rationale.items():
            lines.append(f"  - {name}: {reason}")
        if self.fallback_reason:
            lines.append(f"Fallback used: {self.fallback_reason}")
        return lines


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def pick_combined_ask(
    *,
    client: Client,
    insight_json: Optional[Dict[str, Any]],
    ladder: Optional[Dict[str, Any]],
    ai_profile: Optional[Dict[str, Any]],
    health_score: Optional[float],
    in_offboarding: bool,
    rule: AutomationRule,
    org_id: Optional[uuid.UUID] = None,
) -> OpportunityPick:
    """Decide which of ``{referral, upsell, testimonial}`` to ask in one combined email.

    Precedence:
      1. ``rule.opportunity_priority`` set explicitly -> operator-pinned, in that order.
      2. LLM autonomy -> picks 1..N where N is ``min(rule.combine_top_n, 3)`` (or 3 if
         no cap is configured).
      3. Deterministic ``score_opportunities`` top-N as fallback.
    """
    pipeline_priorities = (
        ai_profile.get("pipeline_priorities") if isinstance(ai_profile, dict) else None
    )
    ranked = score_opportunities(
        client,
        insight=insight_json,
        ladder=ladder,
        health_score=health_score,
        pipeline_priorities=pipeline_priorities,
        in_offboarding=in_offboarding,
        rule_priority=rule.opportunity_priority,
    )
    by_name: Dict[str, OpportunityScore] = {o.name: o for o in ranked}

    cap = _resolve_cap(rule)

    # 1. Operator-pinned via per-rule opportunity_priority.
    if _is_explicit_pin(rule.opportunity_priority):
        ordered_pin = _validate_names(list(rule.opportunity_priority or []))
        chosen = [by_name[n] for n in ordered_pin if n in by_name][:cap]
        if not chosen:
            chosen = ranked[:cap]
        return OpportunityPick(
            chosen=chosen,
            rationale="Pinned by playbook setting `opportunity_priority`.",
            picker_mode="rule_pinned",
        )

    # 2. LLM autonomy.
    if llm_available():
        try:
            llm_decision = _call_llm_picker(
                client=client,
                insight_json=insight_json,
                ladder=ladder,
                ai_profile=ai_profile,
                health_score=health_score,
                in_offboarding=in_offboarding,
                ranked=ranked,
                cap=cap,
                rule=rule,
                org_id=org_id,
            )
        except Exception as e:  # noqa: BLE001 -- any LLM error must not block the worker.
            LOG.warning("opportunity picker LLM call failed: %s", e)
            llm_decision = None

        if llm_decision is not None and llm_decision["chosen"]:
            chosen = [by_name[n] for n in llm_decision["chosen"] if n in by_name][:cap]
            if chosen:
                return OpportunityPick(
                    chosen=chosen,
                    rationale=llm_decision.get("rationale"),
                    per_choice_rationale=llm_decision.get("per_choice") or {},
                    picker_mode="llm",
                )

    # 3. Deterministic fallback.
    return OpportunityPick(
        chosen=ranked[:cap],
        rationale=(
            "Deterministic fallback ranking by health score, wins, ROI tags, lifetime "
            "revenue, lifecycle, and Intelligence pipeline priorities."
        ),
        picker_mode="deterministic",
        fallback_reason=(
            "LLM unavailable" if not llm_available() else "LLM produced no usable picks"
        ),
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _resolve_cap(rule: AutomationRule) -> int:
    """``combine_top_n`` is treated as a hard maximum; 0/None = let LLM pick up to 3."""
    n = rule.combine_top_n if rule and rule.combine_top_n else 0
    try:
        n_int = int(n)
    except (TypeError, ValueError):
        n_int = 0
    if n_int <= 0:
        return _MAX_CHOICES
    return min(_MAX_CHOICES, max(1, n_int))


def _is_explicit_pin(priority: Any) -> bool:
    if not priority or not isinstance(priority, (list, tuple)):
        return False
    return any(
        isinstance(p, str) and p.strip().lower() in _VALID_NAMES for p in priority
    )


def _validate_names(names: Sequence[Any]) -> List[str]:
    """Drop unknown or duplicate names, lower-case, preserve order."""
    out: List[str] = []
    seen: set = set()
    for n in names or []:
        if not isinstance(n, str):
            continue
        k = n.strip().lower()
        if k in _VALID_NAMES and k not in seen:
            out.append(k)
            seen.add(k)
    return out


def _call_llm_picker(
    *,
    client: Client,
    insight_json: Optional[Dict[str, Any]],
    ladder: Optional[Dict[str, Any]],
    ai_profile: Optional[Dict[str, Any]],
    health_score: Optional[float],
    in_offboarding: bool,
    ranked: List[OpportunityScore],
    cap: int,
    rule: AutomationRule,
    org_id: Optional[uuid.UUID],
) -> Optional[Dict[str, Any]]:
    payload = _build_llm_payload(
        client=client,
        insight_json=insight_json,
        ladder=ladder,
        ai_profile=ai_profile,
        health_score=health_score,
        in_offboarding=in_offboarding,
        ranked=ranked,
        cap=cap,
        rule=rule,
    )
    user_prompt = "DATA = " + json.dumps(payload, ensure_ascii=False, default=str)
    user_prompt = truncate_for_tokens(user_prompt, 12000)
    raw = chat_json(_PICKER_SYSTEM, user_prompt, temperature=0.2, org_id=org_id, feature="automation")
    chosen = _validate_names(raw.get("chosen") or [])[:cap]
    if not chosen:
        return None
    rationale = raw.get("rationale")
    if rationale is not None and not isinstance(rationale, str):
        rationale = str(rationale)
    if isinstance(rationale, str):
        rationale = rationale.strip()[:600] or None
    per_choice_raw = raw.get("per_choice") if isinstance(raw.get("per_choice"), dict) else {}
    per_choice = {
        str(k).strip().lower(): str(v).strip()[:240]
        for k, v in per_choice_raw.items()
        if isinstance(k, str) and str(k).strip().lower() in chosen and v
    }
    return {"chosen": chosen, "rationale": rationale, "per_choice": per_choice}


def _build_llm_payload(
    *,
    client: Client,
    insight_json: Optional[Dict[str, Any]],
    ladder: Optional[Dict[str, Any]],
    ai_profile: Optional[Dict[str, Any]],
    health_score: Optional[float],
    in_offboarding: bool,
    ranked: List[OpportunityScore],
    cap: int,
    rule: AutomationRule,
) -> Dict[str, Any]:
    profile_excerpt: Dict[str, Any] = {}
    if isinstance(ai_profile, dict):
        for k in (
            "pipeline_priorities",
            "writing_tone",
            "coaching_style",
            "client_management_philosophy",
            "target_audience",
            "unique_selling_proposition",
            "business_description",
        ):
            v = ai_profile.get(k)
            if v:
                profile_excerpt[k] = v

    insight_blob: Dict[str, Any] = {}
    if isinstance(insight_json, dict):
        headline = insight_json.get("headline") or insight_json.get("client_state_synthesis") or ""
        insight_blob = {
            "wins": insight_json.get("wins") or [],
            "opportunity_tags": insight_json.get("opportunity_tags") or [],
            "roi_signals": insight_json.get("roi_signals") or [],
            "headline": (headline or "")[:300],
        }

    ladder_summary: Dict[str, Any] = {}
    if isinstance(ladder, dict):
        ladder_summary = {
            "core_offer_name": (ladder.get("core_offer") or {}).get("name"),
            "upsells": [
                {"name": u.get("name"), "triggers": u.get("triggers") or []}
                for u in (ladder.get("upsells") or [])
                if isinstance(u, dict)
            ][:5],
            "referral_offer_name": (ladder.get("referral_offer") or {}).get("name"),
        }

    lifecycle = (
        client.lifecycle_state.value
        if hasattr(client.lifecycle_state, "value")
        else str(client.lifecycle_state)
    )

    return {
        "playbook": rule.playbook,
        "max_choices": cap,
        "signals": {
            "health_score": float(health_score) if health_score is not None else None,
            "program_progress_percent": (
                float(client.program_progress_percent)
                if client.program_progress_percent is not None
                else None
            ),
            "lifetime_revenue_cents": int(client.lifetime_revenue_cents or 0),
            "lifecycle_state": lifecycle,
            "in_offboarding_window": bool(in_offboarding),
        },
        "intelligence": {
            "ai_profile": profile_excerpt,
            "ladder": ladder_summary,
        },
        "insight": insight_blob,
        "deterministic_scores": [
            {"name": o.name, "score": o.score, "rationale": o.rationale}
            for o in ranked
        ],
    }

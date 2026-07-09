"""LLM-powered call analysis for the Call Library.

Produces a structured report with sections:
  1. call_context           — who, what they discussed, relationship background
  2. discovery_audit        — 5-dimension discovery scoring (SOP-grounded)
  3. pitching_audit         — 5-dimension pitch scoring (SOP-grounded)
  4. objection_handling_audit — fear-first objection scoring + per-objection paths
  5. strengths              — timestamped bullets of what went well
  6. weaknesses             — timestamped bullets of what went wrong
  7. customer_response      — emotional tone, questions, buying signals
  8. overall_impression     — one-paragraph verdict
"""
from __future__ import annotations

import json
import uuid
from typing import Any, Dict, List, Optional

from app.core.config import settings
from app.services.llm_client import chat_json, llm_available, truncate_for_tokens

DISCOVERY_AUDIT_SOP = """\
DISCOVERY AUDIT SOP — scoring framework (use this to score the discovery_audit section):
Discovery has one job: make the prospect feel completely understood BEFORE any solution is mentioned.
Five dimensions, each scored 1-10:

1. PAIN_IDENTIFICATION (20% of discovery_score): Did the salesperson find the ROOT pain, or accept
   the surface complaint? Standard: root pain named explicitly after probing follow-up questions.
   Score 9-10: root pain named + confirmed by prospect. 7-8: real pain found, not fully confirmed.
   5-6: surface pain only, one probe. 3-4: accepted first answer. 1-2: pain not identified.
   Failure signals: rephrase-and-move-on; no follow-up to obvious surface complaint; pain only at
   category level (e.g. "wants to lose weight" with no specificity).

2. PAIN_IMPACT (20% of discovery_score): Did the salesperson understand how pain MANIFESTS in daily
   life — specific scenes, moments, relationships? Abstract pain creates weak urgency.
   Score 9-10: specific scene/moment named, daily impact emotional + concrete. 7-8: some specificity.
   5-6: general impact acknowledged, no scenes. 3-4: impact mentioned, not explored. 1-2: not addressed.
   Failure signals: pain described only conceptually; no recurring moments or specific relationships named.

3. TANGIBLE_GOALS (20% of discovery_score): Are goals SPECIFIC + MEASURABLE with a number and
   timeframe, SUPPLIED BY THE PROSPECT (not suggested by the salesperson)?
   Examples of standard: "20 lbs in 12 weeks", "$10k/month within 90 days".
   Score 9-10: 2+ goals fully quantified (number+timeframe), prospect-owned. 7-8: 1 goal quantified.
   5-6: goals discussed, not pushed to specifics. 3-4: vague goals accepted. 1-2: goals not identified.
   Failure signals: goals left at wish level; salesperson suggested numbers; no timeframe.

4. INTANGIBLE_GOALS (20% of discovery_score): Were the EMOTIONAL and IDENTITY stakes surfaced —
   the WHY underneath the tangible goal? Categories: family/relationships, identity/self-concept,
   legacy/contribution, freedom/autonomy, confidence/self-trust.
   Score 9-10: 2+ intangible goals named explicitly, emotional language from prospect, identity/legacy
   present. 7-8: 1 intangible goal named. 5-6: emotional undertone present, not named. 3-4: logical
   only. 1-2: no intangible goals surfaced.
   Key questions: "Why does [tangible goal] matter?", "Who else is affected?", "Who do you become?"

5. RAPPORT_TRUST_AUTHORITY (20% of discovery_score): Did the salesperson create safety for honesty
   AND demonstrate mastery through questions (not credential-listing)?
   Score 9-10: prospect clearly felt heard; mastery demonstrated through questions; emotional safety.
   7-8: rapport present, trust built, authority shown at least once. 5-6: comfortable but surface.
   3-4: transactional, prospect gave info but not vulnerable. 1-2: interrogation or discovery skipped.
   Authority signal: asking questions the prospect has never been asked before; naming what the prospect
   hadn't yet articulated but immediately agrees with; diagnosing before prescribing.

discovery_score = sum of (each dimension score × 20). Range 0–100.
"""

PITCHING_SOP = """\
PITCHING SOP — scoring framework (use for pitching_audit):
Pitch bridges discovery → offer. Five dimensions, each scored 1-10 (×20 = pitch_score 0-100):

1. PAIN_WEAVING: Did the salesperson carry SPECIFIC pains from discovery into the pitch (their words,
   root pain)? Fail: generic pitch, credentials first, no callback to discovery.
2. NATURAL_SOLUTION_FRAMING: Were deliverables the logical next step — not a hard pivot/feature dump?
   Natural Solution Test: if you remove prospect's name/pains, does pitch still work? If yes, it fails.
3. GOAL_BRIDGE: Tangible + intangible goals from discovery tied to offer outcomes?
4. POSITIONING_CLARITY: Clear mechanism, differentiation, why this approach vs alternatives?
5. CREDIBILITY_IN_CONTEXT: Relevant proof at the right moment — not credential-dumping?

pitch_score = sum(dimension × 20). Score only if a pitch occurred; if no pitch, score each dim 1 and explain.
"""

OBJECTION_HANDLING_SOP = """\
OBJECTION HANDLING SOP — decision trees (use for objection_handling_audit):
CORE RULE: every objection is FEAR or LOGISTICS. Handle FEAR FIRST — always. Mixed objections (partner, price,
timing) usually have fear underneath.

Aggregate dimensions (each 1-10, ×25 = objection_score 0-100):
1. FEAR_HANDLED_FIRST: Emotional layer addressed before logistics/discounts/scheduling?
2. CLASSIFICATION_ACCURACY: Correctly identified fear vs logistics per objection?
3. SOP_PATH_ADHERENCE: Followed decision-tree key questions (not generic rebuttals)?
4. RESOLUTION_QUALITY: Objections moved deal forward vs stalled further?

Per-objection trees (audit which path was attempted):
- THINK_ABOUT_IT: "What do you need to think about?" → if vague: delay vs progress reframe →
  "decision-making process needs to change before [goal]"
- TOO_EXPENSIVE: value reframe → "expensive compared to what?" → cost of staying vs change →
  "2.0 version of yourself" → "what actions right now to become that person?"
- BAD_TIMING: "that's exactly why we're here" → nothing 100% certain → prioritize through hard times →
  2.0 self question → actions now
- WONT_WORK / TESTIMONIALS: "why are you asking?" → nothing 100% certain → unfair to assume without trying →
  informed decision on fit not hype
- NEED_PARTNER: partner awareness/support → envision conversation → expertise analogy → unfair to burden partner →
  whose responsibility for goals → ready to own 2.0 self?

For each objection in objections[], label type, classification (fear|logistics|mixed), fear_addressed_first,
sop_path_followed (bool), steps_hit[], steps_missed[], handled_well, summary, quote.
"""

_PITCHING_SCORE_SCHEMA = """\
  "pitching_audit": {
    "pitch_score": integer 0-100 = sum(dimension_score * 20),
    "pain_weaving": { "score": 1-10, "summary": "2-3 sentences", "quote": "verbatim or null" },
    "natural_solution_framing": { "score": 1-10, "summary": "2-3 sentences", "quote": "verbatim or null" },
    "goal_bridge": { "score": 1-10, "summary": "2-3 sentences", "quote": "verbatim or null" },
    "positioning_clarity": { "score": 1-10, "summary": "2-3 sentences", "quote": "verbatim or null" },
    "credibility_in_context": { "score": 1-10, "summary": "2-3 sentences", "quote": "verbatim or null" },
    "pitch_summary": "One paragraph verdict on the pitch"
  },\
"""

_OBJECTION_SCORE_SCHEMA = """\
  "objection_handling_audit": {
    "objection_score": integer 0-100 = sum(aggregate_dimension * 25),
    "fear_handled_first": { "score": 1-10, "summary": "2-3 sentences", "quote": "verbatim or null" },
    "classification_accuracy": { "score": 1-10, "summary": "2-3 sentences", "quote": "verbatim or null" },
    "sop_path_adherence": { "score": 1-10, "summary": "2-3 sentences", "quote": "verbatim or null" },
    "resolution_quality": { "score": 1-10, "summary": "2-3 sentences", "quote": "verbatim or null" },
    "objections": [
      {
        "objection_label": "think_about_it | too_expensive | bad_timing | wont_work | need_partner | other",
        "surface_quote": "what prospect said, verbatim or close",
        "classification": "fear | logistics | mixed",
        "fear_addressed_first": boolean,
        "sop_path_followed": boolean,
        "steps_hit": ["key steps from SOP tree that were used"],
        "steps_missed": ["important SOP steps skipped"],
        "handled_well": boolean,
        "summary": "2-3 sentences coaching assessment",
        "quote": "best supporting quote or null"
      }
    ],
    "objection_summary": "One paragraph overall verdict on objection handling"
  },\
"""

_DISCOVERY_SCORE_SCHEMA = """\
  "discovery_audit": {
    "discovery_score": integer 0-100 calculated as sum of (each dimension_score * 20),
    "pain_identification": {
      "score": integer 1-10 per DISCOVERY AUDIT SOP dimension 1,
      "summary": "2-3 sentences: what the salesperson did or failed to do, with specific evidence",
      "quote": "Verbatim short quote from transcript illustrating this dimension, or null"
    },
    "pain_impact": {
      "score": integer 1-10 per DISCOVERY AUDIT SOP dimension 2,
      "summary": "2-3 sentences: was the daily-life grounding achieved? specific evidence",
      "quote": "Verbatim short quote, or null"
    },
    "tangible_goals": {
      "score": integer 1-10 per DISCOVERY AUDIT SOP dimension 3,
      "goals_uncovered": ["list of specific tangible goals found — number + timeframe if present"],
      "summary": "2-3 sentences: were goals quantified? were they prospect-supplied?",
      "quote": "Verbatim short quote, or null"
    },
    "intangible_goals": {
      "score": integer 1-10 per DISCOVERY AUDIT SOP dimension 4,
      "goals_uncovered": ["list of specific intangible goals found — category + what was named"],
      "summary": "2-3 sentences: were emotional/identity stakes surfaced? were they named explicitly?",
      "quote": "Verbatim short quote, or null"
    },
    "rapport_trust_authority": {
      "score": integer 1-10 per DISCOVERY AUDIT SOP dimension 5,
      "summary": "2-3 sentences: was safety created? was authority demonstrated through questions?",
      "quote": "Verbatim short quote, or null"
    },
    "discovery_summary": "One paragraph (3-5 sentences) with the overall verdict on the discovery: what was built well, what was left on the table, and what it means for the pitch that followed. Be direct and coaching in tone."
  },\
"""

def _build_system_prompt(
    discovery_audit_sop: str,
    pitching_sop: str,
    objection_handling_sop: str,
) -> str:
    return """\
You are an expert sales coach reviewing a recorded sales call. Analyze the transcript and summary \
provided in the DATA block and output a single JSON object ONLY — no markdown, no extra text.

Use the DISCOVERY AUDIT SOP below to score discovery_audit:
""" + discovery_audit_sop + """

Use the PITCHING SOP below to score pitching_audit:
""" + pitching_sop + """

Use the OBJECTION HANDLING SOP below to score objection_handling_audit:
""" + objection_handling_sop + """

Schema:
{
  "call_context": {
    "salesperson": "Name or role of the salesperson if identifiable, else 'Salesperson'",
    "prospect": "Name or role of the prospect if identifiable, else 'Prospect'",
    "topic": "One sentence: what the call was about",
    "background": "1-2 sentences: relationship context, how they connected, any referral"
  },
""" + _DISCOVERY_SCORE_SCHEMA + """
""" + _PITCHING_SCORE_SCHEMA + """
""" + _OBJECTION_SCORE_SCHEMA + """
  "strengths": [
    {
      "title": "Short label for the strength the salesperson demonstrated on the call in moving the deal forward toward closing (e.g. 'Building trust through shared frustration')",
      "detail": "2-3 sentences explaining exactly how and why this was effective",
      "timestamp": "Timestamp if visible in transcript (e.g. '09:12'), else null",
      "quote": "Verbatim short quote from the transcript that best illustrates this, or null"
    }
  ],
  "weaknesses": [
    {
      "title": "Short label for the weakness the salesperson demonstrated on the call in failing to move the deal forward toward closing (e.g. 'Not asking for the sale')",
      "detail": "2-3 sentences explaining what went wrong and what the impact was in terms of affecting the salesperson's ability to close the sale",
      "timestamp": "Timestamp if visible, else null",
      "quote": "Verbatim short quote if relevant, or null"
    }
  ],
  "customer_response": {
    "emotional_tone": "One sentence describing how the customer/prospect behaved emotionally to the salesperson's efforts",
    "questions_asked": ["List of key questions the customer asked, verbatim or close paraphrase"],
    "buying_signals": ["Any positive signals that suggest intent to purchase or deepen relationship"],
    "objections_or_barriers": ["Objections, hesitations, or blockers the customer raised that blocked or stalled the follow through or closing of the sale"]
  },
  "overall_impression": "One paragraph (4-6 sentences) summarizing whether the call succeeded at \
moving the deal forward toward closing or completing the sale or failed. Be direct, analytical, and honest.",
  "call_score": integer 0-100 estimating overall sales call quality (discovery, next steps, objection handling, clarity). Not a lead score.,
  "deal_outcome": {
    "closed": boolean — true ONLY when it is unambiguously clear from the DATA that the sale was closed on this call (verbal commitment to buy, payment confirmed, contract acceptance, "let's get you signed up" / "send the invoice" / "I'm in"). Default to false on any doubt — interest, hesitation, "I'll think about it", "let me check with my partner", or scheduling another call all mean closed=false,
    "amount": number | null — total deal value in the deal_outcome.currency (use whole units, e.g. 4500 for $4,500; do not store cents). Pull only from explicit price/figure stated or agreed in the transcript or summary. Use null when the figure is not stated even if closed=true,
    "currency": "USD" | "EUR" | "GBP" | "CAD" | "AUD" | other ISO 4217 — default "USD" when unclear,
    "billing": "one_time" | "recurring_monthly" | "recurring_annual" | "unknown" — derive from how the sale was framed (e.g. monthly retainer vs lump sum); use "unknown" if not stated,
    "confidence": "high" | "medium" | "low" — your confidence that the close happened on this call,
    "evidence": "Short verbatim quote (or close paraphrase if the transcript only summarizes) from DATA that proves the close happened. Empty string when closed=false."
  },
  "low_signal": boolean indicating if the call was so thin or lacked enough substance that it was not possible to generate a meaningful analysis,
  "low_signal_reason": string explaining why the call was so thin or lacked enough substance that it was not possible to generate a meaningful analysis"
}

RULES:
- Use ONLY information from the DATA block. Do not invent names, quotes, or events.
- discovery_audit: score every call with discovery content. If discovery absent, score dims 1 and explain.
- pitching_audit: score if a pitch/offer presentation occurred. If no pitch, score dims 1 and explain in pitch_summary.
- objection_handling_audit: if no objections raised, set objection_score null, objections=[], explain in objection_summary. If objections exist, populate objections[] for EACH distinct objection using SOP trees.
- FEAR FIRST: penalize handling logistics (price break, schedule follow-up) before fear layer in weaknesses.
- call_score: integer 0-100 estimating overall sales call quality (discovery, pitch, objection handling, next steps). Not a lead score.
- deal_outcome.closed defaults to FALSE. Only flip it to true when the DATA contains an unambiguous close signal — explicit verbal yes ("let's do it", "I'm in", "sign me up"), a stated payment ("I'll send the invoice", "card on file", "payment processed"), a contract/agreement acceptance, or the salesperson confirming next-step onboarding for a now-paying client. Interest, soft yeses, scheduling another call, "I'll think about it", or pricing discussions without commitment are NOT closes.
- deal_outcome.amount must come from a number actually stated in the DATA (transcript or summary). Never guess or interpolate. If closed=true but no figure is stated, leave amount=null.
- deal_outcome.evidence must quote (or tightly paraphrase) the line in DATA that proves the close. If closed=false, return an empty string.
- If the transcript is too short or empty to analyze, set low_signal=true and explain in low_signal_reason.
- strengths and weaknesses: include 2-4 bullets each when evidence exists. Align with discovery, pitching, and objection audits. Empty arrays if no evidence.
- Write "detail" and "summary" fields as 2-4 sentences in a coaching tone, like a written report (not terse bullets only).
- Timestamps: copy exactly as they appear in the transcript (e.g. "09:12"); leave null if absent.
- Quotes must be verbatim substrings from the transcript. Never fabricate quotes.
- Output valid JSON only — no markdown fences.
"""


def _build_library_user_payload(summary: str, transcript: str) -> str:
    """Smaller, cost-efficient input: rich summary + capped transcript (not full 40k dump)."""
    max_sum = int(getattr(settings, "CALL_LIBRARY_MAX_SUMMARY_CHARS", 6000) or 6000)
    max_tr = int(getattr(settings, "CALL_LIBRARY_MAX_TRANSCRIPT_CHARS", 12000) or 12000)
    s = truncate_for_tokens(summary.strip(), max_sum) if summary else ""
    t = truncate_for_tokens(transcript.strip(), max_tr) if transcript else ""
    # When Fathom summary is substantive, trim transcript further to save tokens.
    if len(s) >= 400 and t:
        t = truncate_for_tokens(t, min(max_tr, 8000))
    parts: list[str] = []
    if s:
        parts.append(f"SUMMARY:\n{s}")
    if t:
        parts.append(f"TRANSCRIPT:\n{t}")
    return "\n\n".join(parts)


def generate_call_library_report(
    *,
    transcript: str,
    summary: str,
    org_id: Optional[uuid.UUID] = None,
    discovery_audit_sop: Optional[str] = None,
    pitching_sop: Optional[str] = None,
    objection_handling_sop: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """Call the LLM and return the structured report dict, or None on failure."""
    if not llm_available():
        return None

    combined = _build_library_user_payload(summary or "", transcript or "")
    if not combined.strip():
        return None

    disc = truncate_for_tokens((discovery_audit_sop or DISCOVERY_AUDIT_SOP).strip(), 5000)
    pitch = truncate_for_tokens((pitching_sop or PITCHING_SOP).strip(), 4000)
    obj = truncate_for_tokens((objection_handling_sop or OBJECTION_HANDLING_SOP).strip(), 5000)
    system_prompt = _build_system_prompt(disc, pitch, obj)

    user_msg = "DATA:\n" + combined
    timeout = float(getattr(settings, "CALL_LIBRARY_LLM_TIMEOUT_SEC", 90) or 90)
    model_override = getattr(settings, "CALL_LIBRARY_LLM_MODEL", None) or None

    try:
        raw = chat_json(
            system_prompt,
            user_msg,
            temperature=0.2,
            timeout=timeout,
            org_id=org_id,
            model=model_override,
        )
    except RuntimeError as e:
        if "llm_budget" in str(e).lower():
            raise
        return None
    except Exception:
        return None

    normalized = _normalize_report(raw)
    if not is_substantive_call_library_report(normalized):
        return None
    return normalized


def is_substantive_call_library_report(report_json: Any) -> bool:
    """True when the report has real analysis content (not an empty template shell)."""
    if not isinstance(report_json, dict):
        return False
    if report_json.get("low_signal"):
        return False
    if report_json.get("call_score") is not None:
        return True
    if str(report_json.get("overall_impression") or "").strip():
        return True
    strengths = report_json.get("strengths")
    weaknesses = report_json.get("weaknesses")
    if isinstance(strengths, list) and strengths:
        return True
    if isinstance(weaknesses, list) and weaknesses:
        return True
    for key in ("discovery_audit", "pitching_audit", "objection_handling_audit"):
        block = report_json.get(key)
        if not isinstance(block, dict):
            continue
        for field, val in block.items():
            if field.endswith("_score") and val is not None:
                return True
        for summary_key in ("discovery_summary", "pitch_summary", "objection_summary"):
            if str(block.get(summary_key) or "").strip():
                return True
    ctx = report_json.get("call_context")
    if isinstance(ctx, dict) and str(ctx.get("background") or "").strip():
        return True
    return False


_ALLOWED_BILLING = {"one_time", "recurring_monthly", "recurring_annual", "unknown"}
_ALLOWED_CONFIDENCE = {"high", "medium", "low"}


def _empty_deal_outcome() -> Dict[str, Any]:
    return {
        "closed": False,
        "amount": None,
        "currency": "USD",
        "billing": "unknown",
        "confidence": "low",
        "evidence": "",
    }


def _normalize_deal_outcome(raw: Any) -> Dict[str, Any]:
    """Coerce the LLM-emitted deal_outcome into a strict, safe shape.

    Important: `closed` defaults to False on any ambiguity so a missing field
    or an LLM hiccup never falsely marks a deal as closed.
    """
    out = _empty_deal_outcome()
    if not isinstance(raw, dict):
        return out

    out["closed"] = bool(raw.get("closed"))

    amount_raw = raw.get("amount")
    try:
        if amount_raw is not None and amount_raw != "":
            amt = float(amount_raw)
            if amt >= 0:
                # cap at $1B to avoid garbage values poisoning the UI
                out["amount"] = min(amt, 1_000_000_000.0)
    except (TypeError, ValueError):
        pass

    currency = str(raw.get("currency") or "USD").upper().strip()
    if 2 <= len(currency) <= 8 and currency.isalpha():
        out["currency"] = currency
    else:
        out["currency"] = "USD"

    billing = str(raw.get("billing") or "unknown").lower().strip()
    out["billing"] = billing if billing in _ALLOWED_BILLING else "unknown"

    confidence = str(raw.get("confidence") or "low").lower().strip()
    out["confidence"] = confidence if confidence in _ALLOWED_CONFIDENCE else "low"

    evidence = str(raw.get("evidence") or "")[:600]
    out["evidence"] = evidence if out["closed"] else ""
    return out


def _empty_discovery_audit() -> Dict[str, Any]:
    def dim(name: str) -> Dict[str, Any]:
        base: Dict[str, Any] = {"score": None, "summary": "", "quote": None}
        if name in ("tangible_goals", "intangible_goals"):
            base["goals_uncovered"] = []
        return base

    return {
        "discovery_score": None,
        "pain_identification": dim("pain_identification"),
        "pain_impact": dim("pain_impact"),
        "tangible_goals": dim("tangible_goals"),
        "intangible_goals": dim("intangible_goals"),
        "rapport_trust_authority": dim("rapport_trust_authority"),
        "discovery_summary": "",
    }


def _normalize_discovery_dimension(raw: Any, has_goals: bool = False) -> Dict[str, Any]:
    out: Dict[str, Any] = {"score": None, "summary": "", "quote": None}
    if has_goals:
        out["goals_uncovered"] = []
    if not isinstance(raw, dict):
        return out
    score = raw.get("score")
    try:
        if score is not None:
            v = float(score)
            out["score"] = max(1.0, min(10.0, v))
    except (TypeError, ValueError):
        pass
    out["summary"] = str(raw.get("summary") or "")[:600]
    quote = raw.get("quote")
    out["quote"] = str(quote)[:400] if quote else None
    if has_goals:
        goals = raw.get("goals_uncovered")
        out["goals_uncovered"] = _str_list(goals, 10, 200) if isinstance(goals, list) else []
    return out


def _normalize_discovery_audit(raw: Any) -> Dict[str, Any]:
    out = _empty_discovery_audit()
    if not isinstance(raw, dict):
        return out
    score = raw.get("discovery_score")
    try:
        if score is not None:
            v = float(score)
            out["discovery_score"] = max(0.0, min(100.0, v))
    except (TypeError, ValueError):
        pass
    out["pain_identification"] = _normalize_discovery_dimension(raw.get("pain_identification"))
    out["pain_impact"] = _normalize_discovery_dimension(raw.get("pain_impact"))
    out["tangible_goals"] = _normalize_discovery_dimension(raw.get("tangible_goals"), has_goals=True)
    out["intangible_goals"] = _normalize_discovery_dimension(raw.get("intangible_goals"), has_goals=True)
    out["rapport_trust_authority"] = _normalize_discovery_dimension(raw.get("rapport_trust_authority"))
    out["discovery_summary"] = str(raw.get("discovery_summary") or "")[:1500]
    return out


def _empty_pitching_audit() -> Dict[str, Any]:
    def dim() -> Dict[str, Any]:
        return {"score": None, "summary": "", "quote": None}

    return {
        "pitch_score": None,
        "pain_weaving": dim(),
        "natural_solution_framing": dim(),
        "goal_bridge": dim(),
        "positioning_clarity": dim(),
        "credibility_in_context": dim(),
        "pitch_summary": "",
    }


def _normalize_pitching_audit(raw: Any) -> Dict[str, Any]:
    out = _empty_pitching_audit()
    if not isinstance(raw, dict):
        return out
    score = raw.get("pitch_score")
    try:
        if score is not None:
            v = float(score)
            out["pitch_score"] = max(0.0, min(100.0, v))
    except (TypeError, ValueError):
        pass
    for key in (
        "pain_weaving",
        "natural_solution_framing",
        "goal_bridge",
        "positioning_clarity",
        "credibility_in_context",
    ):
        out[key] = _normalize_discovery_dimension(raw.get(key))
    out["pitch_summary"] = str(raw.get("pitch_summary") or "")[:1500]
    return out


def _empty_objection_handling_audit() -> Dict[str, Any]:
    def dim() -> Dict[str, Any]:
        return {"score": None, "summary": "", "quote": None}

    return {
        "objection_score": None,
        "fear_handled_first": dim(),
        "classification_accuracy": dim(),
        "sop_path_adherence": dim(),
        "resolution_quality": dim(),
        "objections": [],
        "objection_summary": "",
    }


_ALLOWED_OBJECTION_LABELS = {
    "think_about_it",
    "too_expensive",
    "bad_timing",
    "wont_work",
    "need_partner",
    "other",
}
_ALLOWED_CLASSIFICATIONS = {"fear", "logistics", "mixed"}


def _normalize_objections_list(items: Any) -> List[Dict[str, Any]]:
    if not isinstance(items, list):
        return []
    out: List[Dict[str, Any]] = []
    for item in items[:8]:
        if not isinstance(item, dict):
            continue
        label = str(item.get("objection_label") or "other").lower().strip()
        if label not in _ALLOWED_OBJECTION_LABELS:
            label = "other"
        classification = str(item.get("classification") or "mixed").lower().strip()
        if classification not in _ALLOWED_CLASSIFICATIONS:
            classification = "mixed"
        out.append({
            "objection_label": label,
            "surface_quote": str(item.get("surface_quote") or "")[:400],
            "classification": classification,
            "fear_addressed_first": bool(item.get("fear_addressed_first")),
            "sop_path_followed": bool(item.get("sop_path_followed")),
            "steps_hit": _str_list(item.get("steps_hit"), 12, 200),
            "steps_missed": _str_list(item.get("steps_missed"), 12, 200),
            "handled_well": bool(item.get("handled_well")),
            "summary": str(item.get("summary") or "")[:600],
            "quote": str(item.get("quote"))[:400] if item.get("quote") else None,
        })
    return out


def _normalize_objection_handling_audit(raw: Any) -> Dict[str, Any]:
    out = _empty_objection_handling_audit()
    if not isinstance(raw, dict):
        return out
    score = raw.get("objection_score")
    try:
        if score is not None:
            v = float(score)
            out["objection_score"] = max(0.0, min(100.0, v))
    except (TypeError, ValueError):
        pass
    for key in (
        "fear_handled_first",
        "classification_accuracy",
        "sop_path_adherence",
        "resolution_quality",
    ):
        out[key] = _normalize_discovery_dimension(raw.get(key))
    out["objections"] = _normalize_objections_list(raw.get("objections"))
    out["objection_summary"] = str(raw.get("objection_summary") or "")[:1500]
    return out


def _normalize_report(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Validate and normalize the LLM output into a safe shape."""
    out: Dict[str, Any] = {
        "call_context": {
            "salesperson": "",
            "prospect": "",
            "topic": "",
            "background": "",
        },
        "discovery_audit": _empty_discovery_audit(),
        "pitching_audit": _empty_pitching_audit(),
        "objection_handling_audit": _empty_objection_handling_audit(),
        "strengths": [],
        "weaknesses": [],
        "customer_response": {
            "emotional_tone": "",
            "questions_asked": [],
            "buying_signals": [],
            "objections_or_barriers": [],
        },
        "overall_impression": "",
        "call_score": None,
        "deal_outcome": _empty_deal_outcome(),
        "low_signal": bool(raw.get("low_signal")),
        "low_signal_reason": str(raw.get("low_signal_reason") or "")[:500],
    }

    # discovery_audit
    out["discovery_audit"] = _normalize_discovery_audit(raw.get("discovery_audit"))
    out["pitching_audit"] = _normalize_pitching_audit(raw.get("pitching_audit"))
    out["objection_handling_audit"] = _normalize_objection_handling_audit(
        raw.get("objection_handling_audit")
    )

    # call_context
    ctx = raw.get("call_context")
    if isinstance(ctx, dict):
        out["call_context"] = {
            "salesperson": str(ctx.get("salesperson") or "Salesperson")[:200],
            "prospect": str(ctx.get("prospect") or "Prospect")[:200],
            "topic": str(ctx.get("topic") or "")[:400],
            "background": str(ctx.get("background") or "")[:800],
        }

    # strengths / weaknesses
    for key in ("strengths", "weaknesses"):
        items = raw.get(key)
        if isinstance(items, list):
            out[key] = _normalize_observation_list(items)

    # customer_response
    cr = raw.get("customer_response")
    if isinstance(cr, dict):
        out["customer_response"] = {
            "emotional_tone": str(cr.get("emotional_tone") or "")[:600],
            "questions_asked": _str_list(cr.get("questions_asked"), 10, 300),
            "buying_signals": _str_list(cr.get("buying_signals"), 8, 300),
            "objections_or_barriers": _str_list(cr.get("objections_or_barriers"), 8, 300),
        }

    # overall_impression
    oi = raw.get("overall_impression")
    if isinstance(oi, str):
        out["overall_impression"] = oi[:2000]

    cs = raw.get("call_score")
    try:
        if cs is not None:
            v = float(cs)
            out["call_score"] = max(0.0, min(100.0, v))
    except (TypeError, ValueError):
        pass

    out["deal_outcome"] = _normalize_deal_outcome(raw.get("deal_outcome"))

    # If low_signal, zero out the meaningful content (incl. deal_outcome — we
    # cannot trust a "closed" signal extracted from a thin transcript).
    if out["low_signal"]:
        out["discovery_audit"] = _empty_discovery_audit()
        out["pitching_audit"] = _empty_pitching_audit()
        out["objection_handling_audit"] = _empty_objection_handling_audit()
        out["strengths"] = []
        out["weaknesses"] = []
        out["overall_impression"] = ""
        out["call_score"] = None
        out["deal_outcome"] = _empty_deal_outcome()

    return out


def _normalize_observation_list(items: list) -> List[Dict[str, Any]]:
    result = []
    for item in items[:6]:
        if not isinstance(item, dict):
            continue
        result.append({
            "title": str(item.get("title") or "")[:200],
            "detail": str(item.get("detail") or "")[:800],
            "timestamp": str(item.get("timestamp"))[:32] if item.get("timestamp") else None,
            "quote": str(item.get("quote"))[:500] if item.get("quote") else None,
        })
    return result


def _str_list(raw: Any, limit: int, max_len: int) -> List[str]:
    if not isinstance(raw, list):
        return []
    return [str(x)[:max_len] for x in raw[:limit] if x]

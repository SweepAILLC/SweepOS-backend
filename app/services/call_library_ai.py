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
import logging
import uuid
from typing import Any, Dict, List, Optional

from app.core.config import settings
from app.services.llm_client import chat_json, llm_available, truncate_for_tokens

logger = logging.getLogger(__name__)

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

# Compact rubrics for LLM scoring (full SOPs above remain for Resources/docs fallbacks).
DISCOVERY_AUDIT_RUBRIC = """\
DISCOVERY (each dim 1-10; discovery_score = sum(dim×20), 0-100):
pain_identification | pain_impact | tangible_goals | intangible_goals | rapport_trust_authority
9-10 root/explicit+confirmed; 7-8 solid; 5-6 surface; 3-4 weak; 1-2 missing.
"""

PITCHING_RUBRIC = """\
PITCH (each dim 1-10; pitch_score = sum(dim×20)):
pain_weaving | natural_solution_framing | goal_bridge | positioning_clarity | credibility_in_context
Score only if a pitch/offer occurred; otherwise omit pitching_audit and note none found.
"""

OBJECTION_HANDLING_RUBRIC = """\
OBJECTIONS — fear before logistics. Aggregate dims 1-10 (×25 → objection_score):
fear_handled_first | classification_accuracy | sop_path_adherence | resolution_quality
Labels: think_about_it|too_expensive|bad_timing|wont_work|need_partner|other
Class: fear|logistics|mixed. If none, omit objection_handling_audit and note none found.
"""


def _build_system_prompt(
    discovery_audit_sop: str = "",
    pitching_sop: str = "",
    objection_handling_sop: str = "",
) -> str:
    """Build a compact coach prompt: rubrics + short schema (no duplicated long SOP/schema)."""
    custom_bits: list[str] = []
    for label, text in (
        ("Discovery notes", discovery_audit_sop),
        ("Pitch notes", pitching_sop),
        ("Objection notes", objection_handling_sop),
    ):
        t = (text or "").strip()
        # Only append org-custom notes when they differ from built-in full SOPs.
        if not t or t in (DISCOVERY_AUDIT_SOP, PITCHING_SOP, OBJECTION_HANDLING_SOP):
            continue
        custom_bits.append(f"{label}:\n{truncate_for_tokens(t, 800)}")
    custom_block = ("\n\nOrg custom guidance:\n" + "\n\n".join(custom_bits)) if custom_bits else ""

    return f"""\
You are an expert sales coach. Analyze DATA and return ONE JSON object only (no markdown).

Score with these rubrics:
{DISCOVERY_AUDIT_RUBRIC}
{PITCHING_RUBRIC}
{OBJECTION_HANDLING_RUBRIC}
{custom_block}

JSON keys (omit sections with no evidence — do NOT invent empty audits):
- call_context: {{salesperson, prospect, topic, background}}
- discovery_audit: {{discovery_score, pain_identification|pain_impact|tangible_goals|intangible_goals|rapport_trust_authority each {{score,summary,quote}}, discovery_summary}} — omit if no discovery
- pitching_audit: {{pitch_score, pain_weaving|natural_solution_framing|goal_bridge|positioning_clarity|credibility_in_context each {{score,summary,quote}}, pitch_summary}} — omit if no pitch
- objection_handling_audit: {{objection_score, fear_handled_first|classification_accuracy|sop_path_adherence|resolution_quality each {{score,summary,quote}}, objections[], objection_summary}} — omit if no objections
- strengths[] / weaknesses[]: {{title, detail, timestamp, quote}} — 0-4 each; [] if none
- customer_response: {{emotional_tone, questions_asked[], buying_signals[], objections_or_barriers[]}}
- overall_impression: short coaching paragraph
- call_score: 0-100 overall quality
- deal_outcome: {{closed(bool default false), amount|null, currency, billing(one_time|recurring_monthly|recurring_annual|unknown), confidence(high|medium|low), evidence}}
- low_signal / low_signal_reason when DATA is too thin

RULES:
- Use ONLY DATA. Never invent quotes, names, or events.
- Skip sections with no evidence (report nothing found via omission or a one-line summary field).
- closed=true only on unambiguous close; amount only if stated; evidence quotes the close line.
- Quotes verbatim from transcript; timestamps as shown or null.
- Valid JSON only.
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


def _resolve_call_library_model() -> Optional[str]:
    """Pick a JSON-capable model that matches the provider that will actually be used.

    Guards against sending a Gemini model name to the OpenAI endpoint (404) when
    HEALTH_SCORE_LLM_MODEL is Gemini but only an OpenAI key is configured.
    """
    from app.services.llm_client import _resolve_provider_and_key

    override = (getattr(settings, "CALL_LIBRARY_LLM_MODEL", None) or "").strip()
    provider, _ = _resolve_provider_and_key()
    health = (getattr(settings, "HEALTH_SCORE_LLM_MODEL", None) or "").strip()

    if override:
        # Only trust an override that matches the active provider.
        low = override.lower()
        if provider == "openai" and "gemini" in low:
            return "gpt-4o-mini"
        if provider == "gemini" and "gpt" in low:
            return health or "gemini-2.0-flash"
        return override

    if provider == "openai":
        return health if "gpt" in health.lower() else "gpt-4o-mini"
    if provider == "gemini":
        return health if "gemini" in health.lower() else "gemini-2.0-flash"
    return health or None


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

    disc = (discovery_audit_sop or "").strip()
    pitch = (pitching_sop or "").strip()
    obj = (objection_handling_sop or "").strip()
    system_prompt = _build_system_prompt(disc, pitch, obj)

    user_msg = "DATA:\n" + combined
    timeout = float(getattr(settings, "CALL_LIBRARY_LLM_TIMEOUT_SEC", 90) or 90)
    model_override = _resolve_call_library_model()

    max_tokens = int(getattr(settings, "CALL_LIBRARY_MAX_OUTPUT_TOKENS", 2500) or 2500)
    max_input = int(
        getattr(settings, "CALL_LIBRARY_MAX_INPUT_CHARS_TOTAL", 40000) or 40000
    )
    min_user = int(getattr(settings, "CALL_LIBRARY_MIN_USER_INPUT_CHARS", 10000) or 10000)
    try:
        raw = chat_json(
            system_prompt,
            user_msg,
            temperature=0.2,
            timeout=timeout,
            org_id=org_id,
            model=model_override,
            max_tokens=max_tokens,
            max_input_chars=max_input,
            min_user_chars=min_user,
            feature="call_library",
        )
    except RuntimeError as e:
        if "llm_budget" in str(e).lower():
            raise
        logger.warning("call_library LLM runtime error model=%s: %s", model_override, e)
        return None
    except Exception as e:
        logger.warning("call_library LLM request failed model=%s: %s", model_override, e)
        return None

    normalized = _normalize_report(raw)
    normalized = _infer_missing_call_score(normalized)
    if not is_substantive_call_library_report(normalized):
        keys = list(raw.keys()) if isinstance(raw, dict) else []
        pi = (normalized.get("discovery_audit") or {}).get("pain_identification") or {}
        logger.warning(
            "call_library LLM non-substantive response model=%s keys=%s low_signal=%s pain_score=%s user_len=%s",
            model_override,
            keys[:12],
            normalized.get("low_signal"),
            pi.get("score"),
            len(user_msg),
        )
        return None
    normalized["analysis_kind"] = "sales"
    return normalized


_GLANCE_SYSTEM = """\
You summarize a NON-sales call (check-in, coaching, ops, or internal). Return ONE JSON object only.

Keys:
- analysis: 2-4 sentence at-a-glance summary of what happened and the outcome tone
- action_items: array of concrete next steps mentioned or clearly implied (0-8 short strings)

Rules:
- Prefer the Fathom SUMMARY in DATA; use transcript only to fill gaps.
- Do not invent names, numbers, or commitments.
- If DATA is thin, keep analysis short and action_items [].
- Valid JSON only — no markdown.
"""


def generate_glance_call_report(
    *,
    transcript: str,
    summary: str,
    org_id: Optional[uuid.UUID] = None,
) -> Optional[Dict[str, Any]]:
    """
    Lightweight report for non-sales calls: keep Fathom summary + one LLM glance
    (analysis + action_items). No discovery/pitch/objection audits.
    """
    fathom_summary = (summary or "").strip()
    summary_part = truncate_for_tokens(fathom_summary, 8000) if fathom_summary else ""
    transcript_part = ""
    if len(summary_part) < 400 and (transcript or "").strip():
        transcript_part = truncate_for_tokens((transcript or "").strip(), 6000)

    if not summary_part and not transcript_part:
        return None

    data_blocks = []
    if summary_part:
        data_blocks.append("FATHOM_SUMMARY:\n" + summary_part)
    if transcript_part:
        data_blocks.append("TRANSCRIPT_EXCERPT:\n" + transcript_part)
    user_msg = "DATA:\n" + "\n\n".join(data_blocks)

    timeout = float(getattr(settings, "CALL_LIBRARY_LLM_TIMEOUT_SEC", 90) or 90)
    model_override = _resolve_call_library_model()
    try:
        raw = chat_json(
            _GLANCE_SYSTEM,
            user_msg,
            temperature=0.2,
            timeout=min(timeout, 60.0),
            org_id=org_id,
            model=model_override,
            max_tokens=800,
            max_input_chars=14000,
            min_user_chars=0,
            feature="call_library_glance",
        )
    except RuntimeError as e:
        if "llm_budget" in str(e).lower():
            raise
        logger.warning("call_library glance LLM runtime error: %s", e)
        raw = None
    except Exception as e:
        logger.warning("call_library glance LLM failed: %s", e)
        raw = None

    analysis = ""
    action_items: List[str] = []
    if isinstance(raw, dict):
        analysis = str(raw.get("analysis") or raw.get("at_a_glance") or "").strip()[:4000]
        items = raw.get("action_items") or raw.get("actions") or []
        if isinstance(items, list):
            for it in items[:8]:
                s = str(it or "").strip()
                if s:
                    action_items.append(s[:500])
        elif isinstance(items, str) and items.strip():
            action_items = [items.strip()[:500]]

    if not analysis and fathom_summary:
        analysis = truncate_for_tokens(fathom_summary, 1200)

    if not analysis and not fathom_summary:
        return None

    return {
        "analysis_kind": "glance",
        "fathom_summary": fathom_summary[:20000] if fathom_summary else "",
        "glance": {
            "analysis": analysis,
            "action_items": action_items,
        },
        "call_score": None,
        "low_signal": False,
    }


def _collect_audit_scores(block: Dict[str, Any]) -> List[float]:
    """Gather numeric scores from an audit block (top-level *_score and nested dim.score)."""
    scores: List[float] = []
    for field, val in block.items():
        if field.endswith("_score") and val is not None:
            try:
                scores.append(float(val))
            except (TypeError, ValueError):
                continue
        elif isinstance(val, dict) and val.get("score") is not None:
            try:
                scores.append(float(val["score"]))
            except (TypeError, ValueError):
                continue
    return scores


def _audit_block_has_substance(block: Any) -> bool:
    if not isinstance(block, dict):
        return False
    if _collect_audit_scores(block):
        return True
    for summary_key in ("discovery_summary", "pitch_summary", "objection_summary"):
        if str(block.get(summary_key) or "").strip():
            return True
    objections = block.get("objections")
    if isinstance(objections, list) and objections:
        return True
    for val in block.values():
        if not isinstance(val, dict):
            continue
        if val.get("score") is not None:
            return True
        if str(val.get("summary") or "").strip():
            return True
        goals = val.get("goals_uncovered")
        if isinstance(goals, list) and goals:
            return True
    return False


def _infer_missing_call_score(report: Dict[str, Any]) -> Dict[str, Any]:
    """Derive call_score from audit dimension scores when the model omits top-level score."""
    if report.get("call_score") is not None:
        return report
    scores: List[float] = []
    for block_key in ("discovery_audit", "pitching_audit", "objection_handling_audit"):
        block = report.get(block_key)
        if isinstance(block, dict):
            scores.extend(_collect_audit_scores(block))
    if scores:
        # Dimension scores are 1-10; block scores (discovery_score etc.) are 0-100.
        avg = sum(scores) / len(scores)
        if avg <= 10.0:
            report["call_score"] = max(0.0, min(100.0, avg * 10.0))
        else:
            report["call_score"] = max(0.0, min(100.0, avg))
    return report


def is_substantive_call_library_report(report_json: Any) -> bool:
    """True when the report has real analysis content (not an empty template shell)."""
    if not isinstance(report_json, dict):
        return False
    if report_json.get("low_signal"):
        return False
    # Non-sales glance reports: Fathom summary and/or glance analysis/actions.
    if str(report_json.get("analysis_kind") or "") == "glance":
        if str(report_json.get("fathom_summary") or "").strip():
            return True
        glance = report_json.get("glance")
        if isinstance(glance, dict):
            if str(glance.get("analysis") or "").strip():
                return True
            items = glance.get("action_items")
            if isinstance(items, list) and any(str(x or "").strip() for x in items):
                return True
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
        if _audit_block_has_substance(report_json.get(key)):
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
        "analysis_kind": "sales",
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

    # discovery / pitch / objections — omit empty shells; note when LLM skipped section
    if raw.get("discovery_audit") is None:
        da = _empty_discovery_audit()
        da["discovery_summary"] = "Nothing found on this call."
        out["discovery_audit"] = da
    else:
        out["discovery_audit"] = _normalize_discovery_audit(raw.get("discovery_audit"))

    if raw.get("pitching_audit") is None:
        pa = _empty_pitching_audit()
        pa["pitch_summary"] = "Nothing found on this call."
        out["pitching_audit"] = pa
    else:
        out["pitching_audit"] = _normalize_pitching_audit(raw.get("pitching_audit"))

    if raw.get("objection_handling_audit") is None:
        oa = _empty_objection_handling_audit()
        oa["objection_summary"] = "Nothing found on this call."
        out["objection_handling_audit"] = oa
    else:
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

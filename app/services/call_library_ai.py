"""LLM-powered call analysis for the Call Library.

Produces a 5-section structured report:
  1. call_context   — who, what they discussed, relationship background
  2. strengths      — timestamped bullets of what went well
  3. weaknesses     — timestamped bullets of what went wrong
  4. customer_response — emotional tone, questions, buying signals
  5. overall_impression — one-paragraph verdict
"""
from __future__ import annotations

import json
import uuid
from typing import Any, Dict, List, Optional

from app.services.llm_client import chat_json, llm_available, truncate_for_tokens

SYSTEM_PROMPT = """\
You are an expert sales coach reviewing a recorded sales call. Analyze the transcript and summary \
provided in the DATA block and output a single JSON object ONLY — no markdown, no extra text.

Schema:
{
  "call_context": {
    "salesperson": "Name or role of the salesperson if identifiable, else 'Salesperson'",
    "prospect": "Name or role of the prospect if identifiable, else 'Prospect'",
    "topic": "One sentence: what the call was about",
    "background": "1-2 sentences: relationship context, how they connected, any referral"
  },
  "strengths": [
    {
      "title": "Short label for the strength teh salesperson demonstrated on the call in moving the deal forward toward closing (e.g. 'Building trust through shared frustration')",
      "detail": "2-3 sentences explaining exactly how and why this was effective",
      "timestamp": "Timestamp if visible in transcript (e.g. '09:12'), else null",
      "quote": "Verbatim short quote from the transcript that best illustrates this, or null"
    }
  ],
  "weaknesses": [
    {
      "title": "Short label for the weakness the salesperson demonstrated on the call in failing to move the deal forward toward closing (e.g. 'Not asking for the sale')",
      "detail": "2-3 sentences explaining what went wrong and what the impact was in terms of affecting the salesperson's ability to clsoe the sale",
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
  "call_score": integer 0-100 estimating sales call quality (discovery, next steps, objection handling, clarity). Not a lead score.,
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
- call_score: integer 0-100 estimating sales call quality (discovery, next steps, objection handling, clarity). Not a lead score.
- deal_outcome.closed defaults to FALSE. Only flip it to true when the DATA contains an unambiguous close signal — explicit verbal yes ("let's do it", "I'm in", "sign me up"), a stated payment ("I'll send the invoice", "card on file", "payment processed"), a contract/agreement acceptance, or the salesperson confirming next-step onboarding for a now-paying client. Interest, soft yeses, scheduling another call, "I'll think about it", or pricing discussions without commitment are NOT closes.
- deal_outcome.amount must come from a number actually stated in the DATA (transcript or summary). Never guess or interpolate. If closed=true but no figure is stated, leave amount=null.
- deal_outcome.evidence must quote (or tightly paraphrase) the line in DATA that proves the close. If closed=false, return an empty string.
- If the transcript is too short or empty to analyze, set low_signal=true and explain in low_signal_reason.
- strengths and weaknesses: include 2-4 bullets each when evidence exists. Empty arrays if no evidence.
- Write "detail" as 2-4 sentences in a coaching tone, like a written report (not terse bullets only).
- Timestamps: copy exactly as they appear in the transcript (e.g. "09:12"); leave null if absent.
- Quotes must be verbatim substrings from the transcript. Never fabricate quotes.
- Output valid JSON only — no markdown fences.
"""


def generate_call_library_report(
    *,
    transcript: str,
    summary: str,
    org_id: Optional[uuid.UUID] = None,
) -> Optional[Dict[str, Any]]:
    """Call the LLM and return the structured 5-section report dict, or None on failure."""
    if not llm_available():
        return None

    combined = ""
    if summary:
        combined += f"SUMMARY:\n{summary}\n\n"
    if transcript:
        combined += f"TRANSCRIPT:\n{transcript}"

    if not combined.strip():
        return None

    user_msg = "DATA:\n" + truncate_for_tokens(combined, 40000)

    try:
        raw = chat_json(SYSTEM_PROMPT, user_msg, temperature=0.2, timeout=120.0, org_id=org_id)
    except Exception:
        return None

    return _normalize_report(raw)


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


def _normalize_report(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Validate and normalize the LLM output into a safe shape."""
    out: Dict[str, Any] = {
        "call_context": {
            "salesperson": "",
            "prospect": "",
            "topic": "",
            "background": "",
        },
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

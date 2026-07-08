"""Content Studio bundle: TOF/MOF/BOF video-concept generator from Fathom data + ICP."""
from __future__ import annotations

import hashlib
import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy.orm import Session

from app.models.content_studio_transcript_analysis import ContentStudioTranscriptAnalysis
from app.models.user import User
from app.services.content_sop import SOP_VERSION, marketing_intel_knowledge_block
from app.services.resource_documents import sop_content_fingerprint
from app.services.content_studio_fathom_context import collect_fathom_sales_signals
from app.services.llm_client import chat_json, llm_available
from app.services.user_ai_profile_context import extract_ai_profile_for_llm

logger = logging.getLogger(__name__)

# Bumped to invalidate every previously-generated bundle (4-section + voice_marketing shape).
BUNDLE_VERSION = 3

# Each entry: (stage, default title, default intro hint shown when LLM cannot run).
STAGE_SPECS: List[Tuple[str, str, str]] = [
    (
        "TOF",
        "Top of funnel — trending hooks for new viewers",
        "Curiosity-triggering hooks pulled from the most attention-grabbing pains, objections, "
        "and surprising moments inside recent sales calls. Goal: stop the scroll, plant the brand.",
    ),
    (
        "MOF",
        "Middle of funnel — education concepts from sales calls",
        "Frameworks, decisions, and reframes the founder uses when teaching prospects on calls. "
        "Goal: install belief and pre-handle the next objection so MOF viewers self-qualify forward.",
    ),
    (
        "BOF",
        "Bottom of funnel — client wins & case studies",
        "Real client transformations, win quotes, and case-study breakdowns surfaced in calls. "
        "Goal: convert warm viewers by showing on-brand outcomes the ICP wants for themselves.",
    ),
]

STAGE_SET = frozenset(s for s, _, _ in STAGE_SPECS)
ALLOWED_FORMATS = frozenset({"long", "short"})


def _stage_grounding_block(signals: Dict[str, Any]) -> str:
    """Per-stage Fathom field mapping the model must use; explicit ban on inventing data."""
    has_any = bool(signals.get("has_any"))
    themes = signals.get("themes") or []
    insights = signals.get("insights") or []
    active = signals.get("active_client_insights") or []
    summaries = signals.get("meeting_summaries") or []

    if not isinstance(themes, list):
        themes = []
    if not isinstance(insights, list):
        insights = []
    if not isinstance(active, list):
        active = []
    if not isinstance(summaries, list):
        summaries = []

    n_obj = sum(len(i.get("objection_quotes") or []) for i in insights if isinstance(i, dict))
    n_wins = sum(len(i.get("wins") or []) for i in insights if isinstance(i, dict))
    n_test = sum(len(i.get("testimonial_stories") or []) for i in insights if isinstance(i, dict))

    if not has_any:
        return (
            "DATA_AVAILABILITY: No Fathom or call-insight payload yet.\n"
            "Use sensible expert defaults grounded in the INTELLIGENCE_PROFILE (ICP, offer, USP). "
            "Mention once at the top of each stage that ideas will sharpen as Fathom calls sync."
        )

    return (
        "DATA_AVAILABILITY: Fathom + call-insight signals are present. Mine them — do not invent facts, "
        "do not name clients, paraphrase quotes.\n"
        "\n"
        "STAGE → FATHOM SOURCE MAPPING (each stage uses different fields):\n"
        "- TOF (Top of funnel — trending hooks for cold viewers): Mine the most attention-grabbing pains, "
        "  shocks, polarizing beliefs, surprising stats, and emotional one-liners visible in `themes` "
        "  (sample_quotes), `insights[].priorities`, `insights[].client_state_synthesis`, and the most vivid "
        "  language inside `meeting_summaries`. Concepts must feel scroll-stopping for someone who has never "
        "  heard of the brand. Tie each one back to the ICP's surface-level pain in INTELLIGENCE_PROFILE.\n"
        "- MOF (Middle of funnel — education concepts from sales calls): Mine education and reframes the "
        "  founder uses on calls — `insights[].phrases_that_resonated`, `insights[].priorities`, "
        "  `insights[].client_state_synthesis`, `meeting_summaries` where teaching/explaining happens, and "
        "  `themes` describing decision-making patterns or objections to pre-handle. Concepts must teach a "
        "  framework, decision rule, or myth-bust — not pitch.\n"
        "- BOF (Bottom of funnel — client wins & case studies): Mine ONLY `insights[].wins`, "
        "  `insights[].testimonial_stories`, and `active_client_insights[].wins` for outcomes that match the "
        "  business's promise. If those arrays are empty, say so honestly in the stage intro and produce "
        "  fewer concepts (or 0). Never fabricate a result.\n"
        "\n"
        f"Counts for calibration: themes={len(themes)}, insights={len(insights)}, "
        f"active_client_insights={len(active)}, meeting_summaries={len(summaries)}, "
        f"objection_quotes≈{n_obj}, wins≈{n_wins}, testimonial_stories≈{n_test}."
    )


def compute_signals_fingerprint(db: Session, org_id: uuid.UUID) -> str:
    """Stable hash that flips when underlying Fathom / insight data meaningfully changes."""
    sig = collect_fathom_sales_signals(db, org_id)
    transcript_analyses_count = (
        db.query(ContentStudioTranscriptAnalysis)
        .filter(ContentStudioTranscriptAnalysis.org_id == org_id)
        .count()
    )
    themes = sig.get("themes") or []
    tk = sorted(
        f"{(t.get('theme_key') or '')}:{int(t.get('occurrence_count') or 0)}"
        for t in themes
        if isinstance(t, dict)
    )
    insights = sig.get("insights") or []
    ic = len(insights)
    ac = len(sig.get("active_client_insights") or [])
    ms = len(sig.get("meeting_summaries") or [])
    tail = hash(
        tuple(
            hash(json.dumps(x, sort_keys=True, default=str)[:400])
            for x in (insights[:5] if isinstance(insights, list) else [])
        )
    )
    payload = {
        "v": BUNDLE_VERSION,
        "sop_v": SOP_VERSION,
        "sop_hash": sop_content_fingerprint(
            db, org_id, ["content-ideation-sop", "building-an-offer-sop"]
        ),
        "tk": tk[:50],
        "ic": ic,
        "ac": ac,
        "ms": ms,
        "tail": tail,
        "has_any": bool(sig.get("has_any")),
        "transcript_analyses": transcript_analyses_count,
    }
    raw = json.dumps(payload, sort_keys=True, default=str)
    return hashlib.sha256(raw.encode()).hexdigest()


def _normalize_concept(raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Coerce a raw concept dict from the LLM into the strict on-disk shape."""
    if not isinstance(raw, dict):
        return None
    fmt = str(raw.get("format") or "short").lower().strip()
    if fmt not in ALLOWED_FORMATS:
        fmt = "short"
    title = str(raw.get("title") or raw.get("hook") or "").strip()
    if not title:
        return None
    hook = str(raw.get("hook") or "").strip()[:300]
    bullets_in = raw.get("bullets") if isinstance(raw.get("bullets"), list) else []
    bullets: List[str] = []
    for b in bullets_in[:8]:
        s = str(b).strip()
        if s:
            bullets.append(s[:400])
    why = str(raw.get("why_for_icp") or raw.get("why_it_works") or "").strip()[:1200]
    funnel = str(raw.get("funnel_path_to_sale") or raw.get("path_to_sale") or "").strip()[:600]
    return {
        "id": str(raw.get("id") or uuid.uuid4()),
        "format": fmt,
        "title": title[:240],
        "hook": hook,
        "bullets": bullets,
        "why_for_icp": why,
        "funnel_path_to_sale": funnel,
    }


def _normalize_stage(raw: Dict[str, Any], stage: str, fallback_title: str, fallback_intro: str) -> Dict[str, Any]:
    concepts_in = raw.get("concepts") if isinstance(raw.get("concepts"), list) else []
    concepts_out: List[Dict[str, Any]] = []
    for c in concepts_in:
        norm = _normalize_concept(c)
        if norm:
            concepts_out.append(norm)
    return {
        "id": stage,
        "title": str(raw.get("title") or fallback_title)[:240],
        "intro": str(raw.get("intro") or raw.get("body") or fallback_intro)[:1200],
        "concepts": concepts_out[:8],
    }


def draft_content_studio_bundle_llm(
    db: Session,
    org_id: uuid.UUID,
    user_row: User,
    signals: Dict[str, Any],
    fingerprint: str,
) -> Optional[Dict[str, Any]]:
    """LLM-only TOF / MOF / BOF concept generator grounded in Fathom + ICP."""
    if not llm_available():
        return None
    profile = extract_ai_profile_for_llm(user_row) or {}
    profile_block = json.dumps(profile, ensure_ascii=False)
    data_block = json.dumps(signals, ensure_ascii=False, default=str)
    if len(data_block) > 48000:
        data_block = data_block[:48000] + "\n…[truncated]"

    grounding = _stage_grounding_block(signals)

    system = """You are a short-form video content strategist for coaches and service businesses.
Return ONLY valid JSON (no markdown) with this exact top-level shape:
{
  "stages": [
    {
      "id": "TOF",
      "title": "string",
      "intro": "string — 1-2 sentence description of this stage's role for THIS business",
      "concepts": [
        {
          "format": "long" | "short",
          "title": "string — concept headline / working name (NOT the hook, NOT a script line)",
          "hook": "string — ONE scripted 1-line hook, the only verbatim line. Obeys the SOP: widest relevant audience, ZERO niche-specific terms, one universal driver, passes the swap test",
          "bullets": ["string — concrete structure beat in the order: proof/credibility, re-hook, body/value, CTA (directional, NOT scripted)"],
          "why_for_icp": "string — 2-3 sentences tying this concept to the ICP from INTELLIGENCE_PROFILE AND the specific objection/goal from the Fathom SIGNALS it pre-handles",
          "funnel_path_to_sale": "string — 1 sentence: how this piece intentionally moves the viewer one step closer to a sale of the operator's offer"
        }
      ]
    },
    { "id": "MOF", "title": "string", "intro": "string", "concepts": [ ... ] },
    { "id": "BOF", "title": "string", "intro": "string", "concepts": [ ... ] }
  ]
}

HARD RULES:
- Output ONLY the three stages: TOF, MOF, BOF — in that order.
- Each stage gives 4-6 concepts. Mix `format`: at least one "long" and at least one "short" per stage when data supports it.
- `hook` is the ONLY verbatim/scripted line allowed — exactly ONE line per concept. NEVER write full scripts, captions, voiceover lines, or social copy. `bullets` stay directional (structure/beats), never scripted.
- Concepts must be PURELY grounded in Fathom signals + INTELLIGENCE_PROFILE. Do not invent facts, names, numbers, or claims.
- CONVERSION MANDATE (CONVERSION IDEATION METHOD): every concept must be reverse-engineered from the sales process. Build each around a real objection, bottleneck, pain, or converting goal found in the Fathom SIGNALS so the content pre-handles objections BEFORE the prospect reaches a call. No generic content that does not pre-sell the offer.
- Stage purpose:
  - TOF: trending, scroll-stopping concepts that mine the most attention-grabbing pains/shocks/beliefs from Fathom data and tie back to the ICP's surface-level pain.
  - MOF: education concepts the operator already teaches on sales calls (frameworks, reframes, decision rules, myth-busts). Pre-handles objections.
  - BOF: client wins & case study breakdowns from `insights[].wins`, `insights[].testimonial_stories`, `active_client_insights[].wins`. If those are empty, return fewer or 0 BOF concepts and say so in the stage intro — never fabricate.
- Every concept MUST include `hook` (1 scripted line), `bullets` (proof → re-hook → body → CTA beats), `why_for_icp`, and `funnel_path_to_sale`.
- `why_for_icp` MUST reference the ICP fields in INTELLIGENCE_PROFILE (target_audience, business_description, unique_selling_proposition, pipeline_priorities, offer_ladder) AND name the specific objection/goal from the Fathom SIGNALS the concept dissolves.
- KNOWLEDGE_BASE (CONTENT_IDEATION_SOP + OFFER_BUILDING_SOP + CONVERSION IDEATION METHOD) is a MANDATORY creative constraint:
  - 3-layer funnel: the `hook` behaves as the HOOK (widest relevant audience, ZERO niche terms, one universal driver, passes the swap test); niche/ICP specificity only enters in the amplifier beats (context → symptom → system); the final beat is the CTA (one ask tied to the amplifier's specific promise). Pick hook types from the SOP's 13 that fit each stage's goal.
  - Reinforce the OFFER: where relevant, echo the operator's positioning levers (owned category, named enemy/wrong-cause, named mechanism, proof) and move the value equation (raise believable outcome/likelihood, lower perceived time/effort).
  - The frameworks are the STRUCTURE; INTELLIGENCE_PROFILE + Fathom SIGNALS are the SUBSTANCE.
- No PII (no full names). Paraphrase any quotes."""

    user = f"""INTELLIGENCE_PROFILE (ICP, offer ladder, USP, voice — anchor every concept to this):
{profile_block}

KNOWLEDGE_BASE (mandatory frameworks — obey the structure, personalize with INTELLIGENCE_PROFILE + SIGNALS):
{marketing_intel_knowledge_block(db, org_id)}

GROUNDING (mandatory stage → Fathom field mapping):
{grounding}

SIGNALS (Fathom meeting summaries, org themes, call insights, active-client insights):
{data_block}

Fingerprint (opaque): {fingerprint}
"""

    try:
        raw = chat_json(system, user, temperature=0.4, org_id=org_id)
    except Exception as e:
        logger.exception("content studio bundle LLM: %s", e)
        return None

    if not isinstance(raw, dict):
        return None
    stages_in = raw.get("stages")
    if not isinstance(stages_in, list):
        return None

    by_stage: Dict[str, Dict[str, Any]] = {}
    for entry in stages_in:
        if isinstance(entry, dict):
            sid = str(entry.get("id") or "").upper().strip()
            if sid in STAGE_SET and sid not in by_stage:
                by_stage[sid] = entry

    stages_out: List[Dict[str, Any]] = []
    for stage, default_title, default_intro in STAGE_SPECS:
        src = by_stage.get(stage, {})
        stages_out.append(_normalize_stage(src, stage, default_title, default_intro))

    batch_id = str(uuid.uuid4())
    return {
        "version": BUNDLE_VERSION,
        "signals_fingerprint": fingerprint,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "batch_id": batch_id,
        "source": "llm",
        "stages": stages_out,
    }


def default_bundle_placeholder(fingerprint: str) -> Dict[str, Any]:
    """Minimal v3 bundle when LLM is unavailable so the UI still renders the new shape."""
    batch_id = str(uuid.uuid4())
    stages: List[Dict[str, Any]] = []
    for stage, title, intro in STAGE_SPECS:
        concept_seeds = (
            (
                "long",
                "Concept idea will draft here once Fathom calls sync",
                ["Connect Fathom to mine real call moments for this stage."],
            ),
            (
                "short",
                "Concept idea will draft here once Fathom calls sync",
                ["Once calls are present, the LLM will mine signals tied to your ICP."],
            ),
        )
        stages.append(
            {
                "id": stage,
                "title": title,
                "intro": intro,
                "concepts": [
                    {
                        "id": str(uuid.uuid4()),
                        "format": fmt,
                        "title": ttl,
                        "hook": "",
                        "bullets": list(b),
                        "why_for_icp": (
                            "Once Fathom calls + Intelligence ICP are populated, this section will tie each "
                            "concept to the audience and offer described in your Intelligence profile."
                        ),
                        "funnel_path_to_sale": (
                            "Will explain the exact next funnel step (consume more → DM/comment → book "
                            "discovery → buy) once data is available."
                        ),
                    }
                    for (fmt, ttl, b) in concept_seeds
                ],
            }
        )
    return {
        "version": BUNDLE_VERSION,
        "signals_fingerprint": fingerprint,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "batch_id": batch_id,
        "source": "default",
        "stages": stages,
    }


def flatten_bundle_idea_ids(bundle: Dict[str, Any]) -> List[str]:
    """Used for completion-tracking validation — walk the v3 stages.concepts shape."""
    ids: List[str] = []
    for stage in bundle.get("stages") or []:
        if not isinstance(stage, dict):
            continue
        for concept in stage.get("concepts") or []:
            if isinstance(concept, dict) and concept.get("id"):
                ids.append(str(concept["id"]))
    # Backwards-compat: tolerate v2 sections-shape rows that still contain ideas.
    for sec in bundle.get("sections") or []:
        if not isinstance(sec, dict):
            continue
        for idea in sec.get("ideas") or []:
            if isinstance(idea, dict) and idea.get("id"):
                ids.append(str(idea["id"]))
    return ids

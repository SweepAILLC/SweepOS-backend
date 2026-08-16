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

# Bumped to invalidate every previously-generated bundle when grounding shape changes.
# v7: ideas grounded in top/bottom post performance + period deltas (not advice dims).
BUNDLE_VERSION = 7

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


def _instagram_perf_fingerprint(db: Session, org_id: uuid.UUID) -> str:
    try:
        from app.services.instagram_performance import performance_fingerprint

        return performance_fingerprint(db, org_id)
    except Exception:
        logger.exception("instagram performance fingerprint failed org=%s", org_id)
        return "ig:err"


def _instagram_perf_block_for_llm(db: Session, org_id: uuid.UUID) -> Dict[str, Any]:
    """Observed Instagram performance for ideation — facts only, no advice dimensions."""
    try:
        from app.services.instagram_performance import build_instagram_performance

        perf = build_instagram_performance(db, org_id, days=90)
    except Exception:
        logger.exception("instagram performance block failed org=%s", org_id)
        return {"connected": False}
    if not perf.get("connected"):
        return {"connected": False}

    summary = perf.get("summary") or {}
    top_posts = list(perf.get("top_posts") or [])[:5]
    bottom_posts = list(perf.get("bottom_posts") or [])[:5]

    # Observed format mix among top posts (counts), not "double down" verdicts.
    format_counts: Dict[str, int] = {}
    for p in top_posts:
        fb = str(p.get("format_bucket") or "unknown").strip().lower() or "unknown"
        format_counts[fb] = format_counts.get(fb, 0) + 1

    return {
        "connected": True,
        "summary": {
            "posts": summary.get("posts"),
            "engagement_rate_pct": summary.get("engagement_rate_pct"),
            "reach": summary.get("reach"),
            "saved": summary.get("saved"),
            "views": summary.get("views"),
            "reach_delta_pct": summary.get("reach_delta_pct"),
            "views_delta_pct": summary.get("views_delta_pct"),
            "saved_delta_pct": summary.get("saved_delta_pct"),
            "engagement_rate_delta_pct": summary.get("engagement_rate_delta_pct"),
            "comparison_label": summary.get("comparison_label"),
            "prev_reach": summary.get("prev_reach"),
            "prev_views": summary.get("prev_views"),
            "prev_saved": summary.get("prev_saved"),
            "prev_engagement_rate_pct": summary.get("prev_engagement_rate_pct"),
        },
        "top_posts": [
            {
                "hook_text": p.get("hook_text"),
                "format_bucket": p.get("format_bucket"),
                "engagement_rate_pct": p.get("engagement_rate_pct"),
                "saved": p.get("saved"),
                "reach": p.get("reach"),
                "caption_excerpt": (p.get("caption") or "")[:180],
            }
            for p in top_posts
        ],
        "underperformers": [
            {
                "hook_text": p.get("hook_text"),
                "format_bucket": p.get("format_bucket"),
                "engagement_rate_pct": p.get("engagement_rate_pct"),
                "caption_excerpt": (p.get("caption") or "")[:120],
            }
            for p in bottom_posts
        ],
        "top_format_mix": format_counts,
        "capabilities": perf.get("capabilities"),
    }


def _instagram_anchor_for_stage(stage_id: str, ig: Dict[str, Any]) -> str:
    """Cite observed top-post performance (not funnel/format/hook advice labels)."""
    top = ig.get("top_posts") if isinstance(ig, dict) else None
    hooks: List[str] = []
    formats: List[str] = []
    if isinstance(top, list):
        for row in top[:3]:
            if not isinstance(row, dict):
                continue
            ht = str(row.get("hook_text") or "").strip()
            if ht:
                hooks.append(ht[:90])
            fb = str(row.get("format_bucket") or "").strip()
            if fb and fb not in formats:
                formats.append(fb)

    summary = ig.get("summary") if isinstance(ig, dict) else None
    delta_bits: List[str] = []
    if isinstance(summary, dict):
        for key, label in (
            ("reach_delta_pct", "reach"),
            ("saved_delta_pct", "saves"),
            ("engagement_rate_delta_pct", "engagement"),
        ):
            v = summary.get(key)
            if isinstance(v, (int, float)):
                delta_bits.append(f"{label} {v:+.0f}% {summary.get('comparison_label') or 'vs prior period'}")

    parts = [
        f"Instagram performance signal ({stage_id}): ground this concept in patterns from recent top-performing posts."
    ]
    if formats:
        parts.append(f"Observed high-performing formats: {', '.join(formats)}.")
    if hooks:
        quoted = "; ".join(f"“{h}”" for h in hooks[:2])
        parts.append(f"Reuse the shape of winning hooks (do not copy verbatim): {quoted}.")
    if delta_bits:
        parts.append("Period movement: " + "; ".join(delta_bits[:2]) + ".")
    under = ig.get("underperformers") if isinstance(ig, dict) else None
    if isinstance(under, list) and under:
        weak = str((under[0] or {}).get("hook_text") or "").strip()
        if weak:
            parts.append(f"Avoid weak opening patterns like “{weak[:80]}”.")
    return " ".join(parts)


def _enforce_instagram_grounding(stages_out: List[Dict[str, Any]], ig: Dict[str, Any]) -> None:
    """Guarantee each concept references observed top-post performance when IG is connected."""
    if not ig.get("connected"):
        return
    if not (ig.get("top_posts") or ig.get("summary")):
        return
    for stage in stages_out:
        sid = str(stage.get("id") or "").upper().strip()
        anchor = _instagram_anchor_for_stage(sid, ig)
        for concept in stage.get("concepts") or []:
            why = str(concept.get("why_for_icp") or "").strip()
            if "Instagram performance signal" in why:
                continue
            merged = f"{why} {anchor}".strip() if why else anchor
            concept["why_for_icp"] = merged[:1200]


def compute_signals_fingerprint(db: Session, org_id: uuid.UUID) -> str:
    """Stable hash that flips when underlying Fathom / insight / IG performance data changes."""
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
        "ig_fp": _instagram_perf_fingerprint(db, org_id),
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


def _collect_bof_call_library_examples(signals: Dict[str, Any]) -> List[Dict[str, str]]:
    """Real BOF proof snippets extracted from call-library derived insights."""
    out: List[Dict[str, str]] = []
    seen: set[str] = set()

    def _push(kind: str, text: Any) -> None:
        s = str(text or "").strip()
        if not s:
            return
        key = s.lower()
        if key in seen:
            return
        seen.add(key)
        out.append({"kind": kind, "text": s[:360]})

    insights = signals.get("insights") if isinstance(signals, dict) else []
    if isinstance(insights, list):
        for ins in insights[:12]:
            if not isinstance(ins, dict):
                continue
            for t in (ins.get("testimonial_stories") or [])[:4]:
                _push("testimonial_story", t)
            for w in (ins.get("wins") or [])[:4]:
                _push("win", w)

    active = signals.get("active_client_insights") if isinstance(signals, dict) else []
    if isinstance(active, list):
        for ins in active[:12]:
            if not isinstance(ins, dict):
                continue
            for w in (ins.get("wins") or [])[:4]:
                _push("active_client_win", w)
    return out[:8]


def _build_prescribed_bof_concept(example: Dict[str, str], idx: int) -> Dict[str, Any]:
    quote = str(example.get("text") or "").strip()
    kind = str(example.get("kind") or "call_library_example").replace("_", " ")
    hook_seed = quote[:120]
    if len(quote) > 120:
        hook_seed = hook_seed.rstrip() + "..."
    return {
        "id": str(uuid.uuid4()),
        "format": "short" if idx % 2 == 0 else "long",
        "title": f"Prescribed BOF Case Study: {quote[:80]}",
        "hook": f"Client result replay: {hook_seed}",
        "bullets": [
            f"Proof: paraphrase this real call-library {kind} — {quote}",
            "Mechanism: explain what changed and why it worked in plain language.",
            "Re-hook: connect this outcome back to the viewer's current problem.",
            "CTA: invite the viewer to book/apply if they want the same transformation path.",
        ],
        "why_for_icp": (
            "This BOF idea is pulled from a real call-library example and should be presented as a concrete "
            "testimonial/case-study replay that builds trust with your ICP."
        ),
        "funnel_path_to_sale": (
            "Shows believable proof from a real client story so warm viewers trust the method and take the next buying step."
        ),
    }


def _enforce_bof_prescribed_case_studies(
    stages_out: List[Dict[str, Any]],
    signals: Dict[str, Any],
) -> None:
    """Guarantee BOF includes prescribed case-study concepts from real call-library examples."""
    examples = _collect_bof_call_library_examples(signals)
    if not examples:
        return
    bof_stage = None
    for stage in stages_out:
        if str(stage.get("id") or "").upper().strip() == "BOF":
            bof_stage = stage
            break
    if not isinstance(bof_stage, dict):
        return

    concepts = bof_stage.get("concepts") if isinstance(bof_stage.get("concepts"), list) else []
    prescribed_count = sum(
        1
        for c in concepts
        if "prescribed bof case study" in str((c or {}).get("title") or "").lower()
    )
    need = max(0, min(2, len(examples)) - prescribed_count)
    if need <= 0:
        return

    injected = [_build_prescribed_bof_concept(examples[i], i) for i in range(need)]
    bof_stage["concepts"] = (injected + concepts)[:8]
    intro = str(bof_stage.get("intro") or "").strip()
    marker = "Includes prescribed case-study concepts pulled from real call-library examples."
    if marker not in intro:
        joined = f"{intro} {marker}".strip()
        bof_stage["intro"] = joined[:1200]


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
  - BOF must include at least one concept whose title starts with "Prescribed BOF Case Study:" and is directly based on a real call-library example from SIGNALS.
- Every concept MUST include `hook` (1 scripted line), `bullets` (proof → re-hook → body → CTA beats), `why_for_icp`, and `funnel_path_to_sale`.
- `why_for_icp` MUST reference the ICP fields in INTELLIGENCE_PROFILE (target_audience, business_description, unique_selling_proposition, personal_story, mission_statement, pipeline_priorities, offer_ladder) AND name the specific objection/goal from the Fathom SIGNALS the concept dissolves.
- KNOWLEDGE_BASE (CONTENT_IDEATION_SOP + OFFER_BUILDING_SOP + CONVERSION IDEATION METHOD) is a MANDATORY creative constraint:
  - 3-layer funnel: the `hook` behaves as the HOOK (widest relevant audience, ZERO niche terms, one universal driver, passes the swap test); niche/ICP specificity only enters in the amplifier beats (context → symptom → system); the final beat is the CTA (one ask tied to the amplifier's specific promise). Pick hook types from the SOP's 13 that fit each stage's goal.
  - Reinforce the OFFER: where relevant, echo the operator's positioning levers (owned category, named enemy/wrong-cause, named mechanism, proof) and move the value equation (raise believable outcome/likelihood, lower perceived time/effort).
  - The frameworks are the STRUCTURE; INTELLIGENCE_PROFILE + Fathom SIGNALS are the SUBSTANCE.
- When INSTAGRAM_PERFORMANCE is connected: ground concepts in observed top_posts (hooks, formats, captions)
  and period deltas in summary; avoid patterns from underperformers. Do not invent funnel/format/hook
  "advice labels" — only cite values that appear in the Instagram payload.
- If INSTAGRAM_PERFORMANCE.connected=true, every concept's why_for_icp MUST include one explicit line that starts
  with "Instagram performance signal" and references a real top-post hook/format or a period delta from summary.
- No PII (no full names). Paraphrase any quotes."""

    ig_perf = _instagram_perf_block_for_llm(db, org_id)
    bof_examples = _collect_bof_call_library_examples(signals)
    ig_block = json.dumps(ig_perf, ensure_ascii=False, default=str)
    if len(ig_block) > 12000:
        ig_block = ig_block[:12000] + "\n…[truncated]"
    bof_block = json.dumps(bof_examples[:6], ensure_ascii=False, default=str)

    user = f"""INTELLIGENCE_PROFILE (ICP, offer ladder, USP, voice — anchor every concept to this):
{profile_block}

KNOWLEDGE_BASE (mandatory frameworks — obey the structure, personalize with INTELLIGENCE_PROFILE + SIGNALS):
{marketing_intel_knowledge_block(db, org_id)}

GROUNDING (mandatory stage → Fathom field mapping):
{grounding}

SIGNALS (Fathom meeting summaries, org themes, call insights, active-client insights):
{data_block}

CALL_LIBRARY_BOF_EXAMPLES (real snippets for prescribed BOF case-study concepts):
{bof_block}

INSTAGRAM_PERFORMANCE (observed results — top_posts, underperformers, period deltas; empty if not connected):
{ig_block}

Fingerprint (opaque): {fingerprint}
"""

    try:
        raw = chat_json(system, user, temperature=0.4, org_id=org_id, feature="content_studio")
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
    _enforce_instagram_grounding(stages_out, ig_perf)
    _enforce_bof_prescribed_case_studies(stages_out, signals)

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

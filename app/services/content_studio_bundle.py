"""Structured Content Studio bundle: 4 data sections + voice/marketing section, fingerprinting, LLM draft."""
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
from app.services.content_studio_fathom_context import collect_fathom_sales_signals
from app.services.llm_client import chat_json, llm_available
from app.services.user_ai_profile_context import extract_ai_profile_for_llm

logger = logging.getLogger(__name__)

BUNDLE_VERSION = 2

SECTION_SPECS: List[Tuple[str, str, str]] = [
    (
        "common_objections",
        "Most common objections",
        "Patterns from recorded calls: objections, hesitations, and pushback prospects raise before they buy.",
    ),
    (
        "active_client_issues",
        "Issues for currently active clients",
        "Friction, adherence, expectations, or delivery themes showing up for people already inside your program.",
    ),
    (
        "testimonials_wins",
        "Testimonials & wins",
        "Proof moments, wins, and language you can recycle into authority and trust content.",
    ),
    (
        "pain_points_and_dream_outcomes",
        "Core pain points & dream outcomes",
        "Recurring pains prospects want gone and outcomes they say they want—anchors for hooks and promises.",
    ),
]


def _sales_data_grounding_block(signals: Dict[str, Any]) -> str:
    """Instructions so each bundle section maps to the right Fathom / sales-behavior fields when present."""
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
            "DATA_AVAILABILITY: No Fathom or sales-behavior payload in SIGNALS yet.\n"
            "Use strong expert-default hooks per section; say once that syncing Fathom and call insights will sharpen these."
        )

    lines = [
        "DATA_AVAILABILITY: Fathom and/or sales-behavior signals are present. Build every section on the pillar→source mapping below. "
        "Do not invent private facts; paraphrase and avoid full names.",
        "",
        "SECTION → DATA PILLARS (each segment uses different fields—do not reuse the same paragraph across sections):",
        "- common_objections (section_id common_objections): Ground in `themes` (labels, occurrence_count, sample_quotes), "
        "`insights[].objection_quotes`, and hesitation/pushback language in `meeting_summaries`.",
        "- active_client_issues (section_id active_client_issues): Primary source is `active_client_insights` only "
        "(client_state_synthesis, priorities, next_steps, wins). These reflect ACTIVE clients. "
        "If `active_client_insights` is empty, acknowledge that gap in the body—do not substitute generic objections here.",
        "- testimonials_wins (section_id testimonials_wins): Ground in `insights[].wins`, `insights[].testimonial_stories`, "
        "proof-oriented lines in `themes`/`meeting_summaries` when they describe outcomes or credibility.",
        "- pain_points_and_dream_outcomes (section_id pain_points_and_dream_outcomes): Ground in `themes`, "
        "`insights[].priorities`, `insights[].client_state_synthesis`, and pains/desired outcomes in `meeting_summaries`.",
        "",
        f"Counts for calibration: themes={len(themes)}, call_insight_rows={len(insights)}, "
        f"active_client_insight_rows={len(active)}, meeting_summaries={len(summaries)}, "
        f"objection_quote_lines≈{n_obj}, wins_lines≈{n_wins}, testimonial_story_lines≈{n_test}.",
        "",
        "REQUIREMENTS when data exists:",
        "- Each section `body` must clearly reflect its pillar sources (paraphrase; short quotes OK).",
        "- Each TOF/MOF/BOF idea in a section must tie hook + concept + why_it_works to that section’s pillar—"
        "reference the concrete objection, active-client friction, win, or pain/outcome the data suggests.",
        "- voice_marketing: Synthesize language, hook structures, and what works from `insights` "
        "(phrases_that_resonated, tone_notes, avoid_phrasing) plus `meeting_summaries` when they show phrasing that converts.",
    ]
    if active:
        lines.append(
            "- active_client_insights is non-empty: active_client_issues MUST cite delivery/retention themes from those rows."
        )
    else:
        lines.append(
            "- active_client_insights is empty: keep active_client_issues honest—short body, no fake delivery detail."
        )
    return "\n".join(lines)


def compute_signals_fingerprint(db: Session, org_id: uuid.UUID) -> str:
    """Stable hash when underlying Fathom/insight data meaningfully changes."""
    sig = collect_fathom_sales_signals(db, org_id)
    transcript_analyses_count = (
        db.query(ContentStudioTranscriptAnalysis)
        .filter(ContentStudioTranscriptAnalysis.org_id == org_id)
        .count()
    )
    themes = sig.get("themes") or []
    tk = sorted(
        f"{(t.get('theme_key') or '')}:{int(t.get('occurrence_count') or 0)}" for t in themes if isinstance(t, dict)
    )
    insights = sig.get("insights") or []
    ic = len(insights)
    ac = len(sig.get("active_client_insights") or [])
    ms = len(sig.get("meeting_summaries") or [])
    # Hash last few insight payloads lightly so new call insights move the needle
    tail = hash(
        tuple(
            hash(json.dumps(x, sort_keys=True, default=str)[:400])
            for x in (insights[:5] if isinstance(insights, list) else [])
        )
    )
    payload = {
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


def _ensure_idea_ids(ideas: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for row in ideas:
        if not isinstance(row, dict):
            continue
        rid = str(row.get("id") or "").strip() or str(uuid.uuid4())
        st = str(row.get("stage") or "").upper()
        if st not in ("TOF", "MOF", "BOF"):
            continue
        out.append(
            {
                "id": rid,
                "stage": st,
                "hook": str(row.get("hook") or "")[:2000],
                "concept": str(row.get("concept") or row.get("reel_concept") or "")[:2000],
                "why_it_works": str(row.get("why_it_works") or row.get("personalized_rationale") or "")[:3000],
                "format": str(row.get("format") or "reel")[:64],
            }
        )
    return out


def _normalize_section(raw: Dict[str, Any], fallback_id: str, fallback_title: str, fallback_hint: str) -> Dict[str, Any]:
    ideas = _ensure_idea_ids(raw.get("ideas") if isinstance(raw.get("ideas"), list) else [])
    # enforce one per stage if possible
    by_stage = {"TOF": None, "MOF": None, "BOF": None}
    for x in ideas:
        s = x.get("stage")
        if s in by_stage and by_stage[s] is None:
            by_stage[s] = x
    ordered: List[Dict[str, Any]] = []
    for st in ("TOF", "MOF", "BOF"):
        if by_stage[st]:
            ordered.append(by_stage[st])
    if len(ordered) < 3:
        # pad with placeholders
        for st in ("TOF", "MOF", "BOF"):
            if not any(i.get("stage") == st for i in ordered):
                ordered.append(
                    {
                        "id": str(uuid.uuid4()),
                        "stage": st,
                        "hook": f"[Add hook for {st}]",
                        "concept": "Short vertical video; talking head or b-roll illustrating this theme.",
                        "why_it_works": "Addresses this section’s theme using patterns from your calls.",
                        "format": "reel",
                    }
                )
    return {
        "id": str(raw.get("section_id") or raw.get("id") or fallback_id),
        "title": str(raw.get("title") or fallback_title)[:200],
        "body": str(raw.get("body") or raw.get("paragraph") or fallback_hint)[:4000],
        "ideas": ordered[:3],
    }


def draft_content_studio_bundle_llm(
    db: Session,
    org_id: uuid.UUID,
    user_row: User,
    signals: Dict[str, Any],
    fingerprint: str,
) -> Optional[Dict[str, Any]]:
    if not llm_available():
        return None
    profile = extract_ai_profile_for_llm(user_row) or {}
    profile_block = json.dumps(profile, ensure_ascii=False)
    data_block = json.dumps(signals, ensure_ascii=False, default=str)
    if len(data_block) > 48000:
        data_block = data_block[:48000] + "\n…[truncated]"

    grounding = _sales_data_grounding_block(signals)

    system = """You are a short-form video content strategist for coaches and service businesses.
Return ONLY valid JSON (no markdown) with this exact top-level shape:
{
  "sections": [
    {
      "section_id": "common_objections",
      "title": "string (use or improve the suggested title)",
      "body": "string — one focused paragraph (3-6 sentences) grounded ONLY in DATA for THIS theme: recurring objections and hesitation from calls.",
      "ideas": [
        { "stage": "TOF", "hook": "string", "concept": "string — reel/visual approach only, no caption/post text", "why_it_works": "string — 2-4 sentences" },
        { "stage": "MOF", "hook": "string", "concept": "string", "why_it_works": "string" },
        { "stage": "BOF", "hook": "string", "concept": "string", "why_it_works": "string" }
      ]
    },
    {
      "section_id": "active_client_issues",
      "title": "string",
      "body": "paragraph for active-client friction, adherence, expectations (from DATA).",
      "ideas": [ same 3 objects TOF, MOF, BOF ]
    },
    {
      "section_id": "testimonials_wins",
      "title": "string",
      "body": "paragraph for wins, proof, stories to amplify.",
      "ideas": [ TOF, MOF, BOF ]
    },
    {
      "section_id": "pain_points_and_dream_outcomes",
      "title": "string",
      "body": "paragraph for pains and desired outcomes prospects repeat.",
      "ideas": [ TOF, MOF, BOF ]
    }
  ],
  "voice_marketing": {
    "title": "string — e.g. Language, tonality & what is working on calls",
    "body": "string — one paragraph on tone, hook structures, phrases that land, what to mirror vs avoid",
    "bullets": ["string", "..."] 
  }
}
Rules:
- Each section's ideas MUST include exactly one TOF, one MOF, one BOF in that order.
- No captions or social post copy. Hooks + reel concepts + why_it_works only.
- When SIGNALS contain Fathom/sales-behavior data, each section MUST follow the pillar→source mapping in GROUNDING (same section_id). Do not copy one generic paragraph into all four sections.
- common_objections: objections/hesitation from themes + insights + meeting_summaries—not active-client delivery issues.
- active_client_issues: prioritize `active_client_insights` only; if that array is empty, acknowledge thin data—do not relabel prospect objections as active-client issues.
- testimonials_wins: wins, proof, testimonial_stories—different angle than objections.
- pain_points_and_dream_outcomes: recurring pains and desired outcomes from themes, priorities, synthesis, summaries.
- If DATA is thin for one pillar only, say so briefly in that section's body and use sensible expert defaults for that section alone.
- No PII: no full names; paraphrase.
- Deeper patterns for mirroring in marketing: voice_marketing (prospect_voice + summaries)."""

    user = f"""INTELLIGENCE_PROFILE:
{profile_block}

GROUNDING (mandatory pillar mapping—follow when building sections):
{grounding}

SIGNALS (Fathom meeting summaries, org themes, call insights, active-client insights):
{data_block}

Fingerprint (opaque): {fingerprint}
"""

    try:
        raw = chat_json(system, user, temperature=0.35, org_id=org_id)
    except Exception as e:
        logger.exception("content studio bundle LLM: %s", e)
        return None

    if not isinstance(raw, dict):
        return None
    secs_in = raw.get("sections")
    if not isinstance(secs_in, list):
        return None

    sections_out: List[Dict[str, Any]] = []
    for i, spec in enumerate(SECTION_SPECS):
        sid, default_title, hint = spec
        src = secs_in[i] if i < len(secs_in) and isinstance(secs_in[i], dict) else {}
        merged = dict(src)
        merged.setdefault("section_id", sid)
        merged.setdefault("title", default_title)
        sections_out.append(_normalize_section(merged, sid, default_title, hint))

    vm = raw.get("voice_marketing")
    if not isinstance(vm, dict):
        vm = {}
    bullets = vm.get("bullets")
    if not isinstance(bullets, list):
        bullets = []
    bullets = [str(b)[:500] for b in bullets[:12] if str(b).strip()]

    voice_out = {
        "title": str(vm.get("title") or "Language, tonality & what is working on calls")[:200],
        "body": str(vm.get("body") or "")[:5000],
        "bullets": bullets,
    }

    batch_id = str(uuid.uuid4())
    return {
        "version": BUNDLE_VERSION,
        "signals_fingerprint": fingerprint,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "batch_id": batch_id,
        "source": "llm",
        "sections": sections_out,
        "voice_marketing": voice_out,
    }


def default_bundle_placeholder(fingerprint: str) -> Dict[str, Any]:
    """When LLM unavailable: minimal structure so UI still renders."""
    batch_id = str(uuid.uuid4())
    sections: List[Dict[str, Any]] = []
    for sid, title, hint in SECTION_SPECS:
        ideas = []
        for st in ("TOF", "MOF", "BOF"):
            ideas.append(
                {
                    "id": str(uuid.uuid4()),
                    "stage": st,
                    "hook": f"[{st}] Hook tied to: {title}",
                    "concept": "15–30s vertical: talking head or simple b-roll; one clear visual metaphor.",
                    "why_it_works": "Connects this funnel stage to the theme; refine when call data is synced.",
                    "format": "reel",
                }
            )
        sections.append({"id": sid, "title": title, "body": hint, "ideas": ideas})
    return {
        "version": BUNDLE_VERSION,
        "signals_fingerprint": fingerprint,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "batch_id": batch_id,
        "source": "default",
        "sections": sections,
        "voice_marketing": {
            "title": "Language, tonality & what is working on calls",
            "body": (
                "Configure Fathom and sync calls to populate tone, hook structures, and phrases that convert. "
                "Until then: keep hooks concrete, speak in second person, lead with tension then relief, "
                "and mirror prospect language from your Intelligence profile."
            ),
            "bullets": [
                "Short hooks: tension → specific promise → proof cue.",
                "Match energy to your audience: calm authority vs. high-energy motivation.",
                "Reuse exact phrases prospects use once you have transcripts.",
            ],
        },
    }


def flatten_bundle_idea_ids(bundle: Dict[str, Any]) -> List[str]:
    ids: List[str] = []
    for sec in bundle.get("sections") or []:
        if not isinstance(sec, dict):
            continue
        for idea in sec.get("ideas") or []:
            if isinstance(idea, dict) and idea.get("id"):
                ids.append(str(idea["id"]))
    return ids

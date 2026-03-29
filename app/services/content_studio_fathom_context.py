"""
Build Content Studio "sales playbook" copy from Fathom call data + call insights + org themes.
Falls back to expert default paragraphs when no data exists.
"""
from __future__ import annotations

import json
import logging
import uuid
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import desc
from sqlalchemy.orm import Session

from app.models.client import Client, LifecycleState
from app.models.client_call_insight import ClientCallInsight
from app.models.fathom_call_record import FathomCallRecord
from app.models.org_sales_content_theme import OrgSalesContentTheme
from app.services.llm_client import chat_json, llm_available
from app.services.org_sales_theme_service import ensure_org_sales_content_themes_table, list_validated_themes_payload

logger = logging.getLogger(__name__)

MAX_SIGNAL_CHARS = 14000

DEFAULT_SALES_PLAYBOOK_PARAGRAPHS: List[str] = [
    (
        "Treat content as pre-call sales enablement: publish short, specific stories that name the exact anxieties "
        "your prospects feel before they book (time, trust, price, past failures). You are not pitching in the feed—you "
        "are lowering the emotional cost of saying yes later."
    ),
    (
        "Use proof before polish: one real constraint, one real outcome, one clear mechanism. Authority comes from "
        "specificity—numbers, timelines, and plain language—not from louder claims. Rotate between client voice "
        "(paraphrased) and your own diagnosis so the feed feels human, not scripted."
    ),
    (
        "Sequence TOF → MOF → BOF deliberately. TOF should earn attention with a sharp tension hook; MOF should "
        "teach a decision framework and handle the top two objections implicitly; BOF should make the next step "
        "obvious (book, apply, reply) with a single risk-reversal or clarity promise."
    ),
    (
        "Warm the buyer before the call by repeating the same 2–3 themes across formats (talking head, b-roll, "
        "carousel). Repetition builds familiarity; variety of format keeps attention. End MOF pieces with a soft "
        "invitation, not a hard close."
    ),
    (
        "When you do not yet have call recordings to mine, start with these patterns and refine weekly as real "
        "objections surface. Add transcripts to your CRM notes so future content can mirror exact language prospects use."
    ),
]


def _themes_payload(db: Session, org_id: uuid.UUID) -> List[Dict[str, Any]]:
    """Validated themes when available; otherwise top recurring themes by occurrence."""
    try:
        ensure_org_sales_content_themes_table(db)
    except Exception as e:
        logger.debug("content_studio themes table: %s", e)
        return []
    validated = list_validated_themes_payload(db, org_id)
    if validated:
        return validated[:25]
    rows = (
        db.query(OrgSalesContentTheme)
        .filter(OrgSalesContentTheme.org_id == org_id)
        .order_by(desc(OrgSalesContentTheme.occurrence_count), desc(OrgSalesContentTheme.last_seen_at))
        .limit(18)
        .all()
    )
    out: List[Dict[str, Any]] = []
    for r in rows:
        if int(r.occurrence_count or 0) < 1:
            continue
        out.append(
            {
                "theme_key": r.theme_key,
                "label": r.label or "",
                "distinct_client_count": r.distinct_client_count,
                "occurrence_count": r.occurrence_count,
                "sample_quotes": (r.sample_quotes or [])[:5],
            }
        )
    return out[:25]


def _recent_call_insights(db: Session, org_id: uuid.UUID, limit: int = 22) -> List[Dict[str, Any]]:
    rows = (
        db.query(ClientCallInsight)
        .filter(ClientCallInsight.org_id == org_id, ClientCallInsight.status == "complete")
        .order_by(desc(ClientCallInsight.computed_at))
        .limit(limit)
        .all()
    )
    out: List[Dict[str, Any]] = []
    for r in rows:
        ij = r.insight_json if isinstance(r.insight_json, dict) else {}
        if ij.get("low_signal"):
            continue
        clips_in = ij.get("clips") or []
        obj_quotes: List[str] = []
        if isinstance(clips_in, list):
            for c in clips_in[:8]:
                if not isinstance(c, dict):
                    continue
                if str(c.get("kind") or "").lower() != "objection":
                    continue
                q = str(c.get("quote") or "").strip()
                if q:
                    obj_quotes.append(q[:420])
        pv = ij.get("prospect_voice") if isinstance(ij.get("prospect_voice"), dict) else {}
        out.append(
            {
                "client_state_synthesis": str(ij.get("client_state_synthesis") or "")[:900],
                "priorities": [str(x)[:320] for x in (ij.get("priorities") or [])[:5] if x],
                "objection_quotes": obj_quotes[:4],
                "phrases_that_resonated": [str(x)[:220] for x in (pv.get("phrases_that_resonated") or [])[:5] if x],
                "tone_notes": [str(x)[:220] for x in (pv.get("tone_notes") or [])[:4] if x],
                "avoid_phrasing": [str(x)[:220] for x in (pv.get("avoid_phrasing") or [])[:4] if x],
                "wins": [str(x)[:320] for x in (ij.get("wins") or [])[:4] if x],
                "testimonial_stories": [str(x)[:400] for x in (ij.get("testimonial_stories") or [])[:3] if x],
            }
        )
    return out


def _active_client_insights(db: Session, org_id: uuid.UUID, limit: int = 14) -> List[Dict[str, Any]]:
    """Call insights for clients currently in ACTIVE lifecycle (delivery / retention friction)."""
    rows = (
        db.query(ClientCallInsight)
        .join(Client, Client.id == ClientCallInsight.client_id)
        .filter(
            ClientCallInsight.org_id == org_id,
            Client.org_id == org_id,
            Client.lifecycle_state == LifecycleState.ACTIVE,
            ClientCallInsight.status == "complete",
        )
        .order_by(desc(ClientCallInsight.computed_at))
        .limit(limit)
        .all()
    )
    out: List[Dict[str, Any]] = []
    for r in rows:
        ij = r.insight_json if isinstance(r.insight_json, dict) else {}
        if ij.get("low_signal"):
            continue
        ns = ij.get("next_steps") or []
        nsl = []
        if isinstance(ns, list):
            for it in ns[:6]:
                if isinstance(it, dict):
                    nsl.append(
                        {
                            "title": str(it.get("title") or "")[:240],
                            "detail": str(it.get("detail") or "")[:600],
                        }
                    )
        out.append(
            {
                "client_state_synthesis": str(ij.get("client_state_synthesis") or "")[:900],
                "priorities": [str(x)[:320] for x in (ij.get("priorities") or [])[:5] if x],
                "next_steps": nsl,
                "wins": [str(x)[:320] for x in (ij.get("wins") or [])[:4] if x],
            }
        )
    return out


def _fathom_summaries(db: Session, org_id: uuid.UUID, limit: int = 10) -> List[str]:
    rows = (
        db.query(FathomCallRecord)
        .filter(FathomCallRecord.org_id == org_id)
        .order_by(desc(FathomCallRecord.updated_at))
        .limit(limit)
        .all()
    )
    out: List[str] = []
    for r in rows:
        t = (r.summary_text or "").strip()
        if t:
            out.append(t[:1200])
    return out


def collect_fathom_sales_signals(db: Session, org_id: uuid.UUID) -> Dict[str, Any]:
    themes = _themes_payload(db, org_id)
    insights = _recent_call_insights(db, org_id)
    active_insights = _active_client_insights(db, org_id)
    summaries = _fathom_summaries(db, org_id)
    has_any = bool(themes or insights or summaries or active_insights)
    return {
        "themes": themes,
        "insights": insights,
        "active_client_insights": active_insights,
        "meeting_summaries": summaries,
        "has_any": has_any,
    }


def _signals_to_brief_text(signals: Dict[str, Any]) -> str:
    """Compact text for LLM user prompt (truncated)."""
    payload = {
        "recurring_themes": signals.get("themes") or [],
        "call_insight_patterns": signals.get("insights") or [],
        "recent_meeting_summary_excerpts": signals.get("meeting_summaries") or [],
    }
    raw = json.dumps(payload, ensure_ascii=False)
    if len(raw) > MAX_SIGNAL_CHARS:
        return raw[:MAX_SIGNAL_CHARS] + "\n…[truncated]"
    return raw


def _paragraphs_deterministic(signals: Dict[str, Any]) -> List[str]:
    """Readable paragraphs without LLM."""
    paras: List[str] = []
    themes = signals.get("themes") or []
    if themes:
        parts = []
        for t in themes[:6]:
            lab = (t.get("label") or t.get("theme_key") or "").strip()
            samples = t.get("sample_quotes") or []
            sq = ""
            if isinstance(samples, list) and samples:
                sq = f' Example language prospects used: "{str(samples[0])[:280]}"'
            if lab:
                parts.append(f"{lab}.{sq}")
        if parts:
            paras.append(
                "Across Fathom-linked calls, recurring themes show up often enough to shape your content calendar: "
                + " ".join(parts)
            )

    insights = signals.get("insights") or []
    objection_lines: List[str] = []
    voice_lines: List[str] = []
    trust_lines: List[str] = []
    for ins in insights[:10]:
        for q in ins.get("objection_quotes") or []:
            if q and q not in objection_lines:
                objection_lines.append(q[:300])
        for p in ins.get("phrases_that_resonated") or []:
            if p and p not in voice_lines:
                voice_lines.append(p[:240])
        for w in ins.get("wins") or []:
            if w and w not in trust_lines:
                trust_lines.append(w[:280])
        for s in ins.get("testimonial_stories") or []:
            if s and s not in trust_lines:
                trust_lines.append(s[:320])

    if objection_lines:
        paras.append(
            "Objections surfaced on real calls include themes like: "
            + "; ".join(objection_lines[:5])
            + ". Use short-form content to answer these before the sales conversation—micro-stories, myth-busting "
            "reframes, and 'here is what we actually do' clips reduce surprise objections on the call."
        )

    if voice_lines or any(ins.get("tone_notes") for ins in insights[:8]):
        tones: List[str] = []
        for ins in insights[:8]:
            for tn in ins.get("tone_notes") or []:
                if tn and tn not in tones:
                    tones.append(tn[:200])
        vo = ""
        if voice_lines:
            vo = "Prospects respond well to phrasing such as: " + "; ".join(voice_lines[:4]) + "."
        to = ""
        if tones:
            to = "Tone patterns: " + "; ".join(tones[:4]) + "."
        if vo or to:
            paras.append(
                (vo + " " + to).strip()
                + " Mirror that voice in your hooks so MOF content feels continuous with live conversations."
            )

    if trust_lines:
        paras.append(
            "Signals of trust and authority from calls: "
            + "; ".join(trust_lines[:5])
            + ". Turn these into b-roll proof points, stitched testimonials, or talking-head 'win replay' clips "
            "so buyers arrive pre-sold on your credibility."
        )

    summaries = signals.get("meeting_summaries") or []
    if summaries:
        paras.append(
            "Recent meeting narratives (Fathom summaries) reinforce what prospects care about right now. Pull 1–2 "
            "recurring pains from these summaries into TOF hooks, then bridge MOF content to the same outcomes you "
            "discuss on calls: "
            + summaries[0][:700]
            + ("…" if len(summaries[0]) > 650 else "")
        )

    if not paras:
        return list(DEFAULT_SALES_PLAYBOOK_PARAGRAPHS)

    paras.append(
        "Before the pitch on a live call, stack content so TOF earns curiosity, MOF installs your framework and "
        "pre-handles objections, and BOF makes booking or buying feel like the natural next step—not a blind ambush."
    )
    return paras[:8]


def synthesize_paragraphs_with_llm(db: Session, org_id: uuid.UUID, signals: Dict[str, Any]) -> Optional[List[str]]:
    if not llm_available() or not signals.get("has_any"):
        return None
    bundle = _signals_to_brief_text(signals)
    system = """You are a GTM content strategist for coaches and B2C/B2SMB service businesses.
Return ONLY valid JSON: {"paragraphs": [ string, ... ]}
Write 5–7 short paragraphs (each 2–5 sentences). The audience is the operator reading their Content Studio.
Focus ONLY on:
- How to use short-form video/content to PRE-HANDLE objections that appear in SIGNALS
- How to build trust and authority before a sales call
- How to warm buyers so the live pitch feels like a continuation, not a cold close
Ground every paragraph in SIGNALS. Quote or paraphrase prospect language sparingly.
If SIGNALS are thin, acknowledge it once and still give strong expert guidance.
No markdown. No bullet lists inside strings."""

    user = f"SIGNALS (from Fathom meetings, transcripts, and aggregated themes):\n{bundle}"
    try:
        raw = chat_json(system, user, temperature=0.35, org_id=org_id)
        paras = raw.get("paragraphs")
        if not isinstance(paras, list):
            return None
        cleaned = [str(p).strip() for p in paras if str(p).strip()]
        if len(cleaned) < 3:
            return None
        return [p[:1600] for p in cleaned[:10]]
    except Exception as e:
        logger.info("content_studio LLM playbook synthesis skipped: %s", e)
        return None


def build_sales_playbook_for_studio(
    db: Session,
    org_id: uuid.UUID,
    *,
    use_llm_synthesis: bool = True,
) -> Tuple[str, List[str]]:
    """
    Returns (source, paragraphs) for legacy UI context and helpers.

    Bootstrap uses use_llm_synthesis=False so the tab loads quickly (deterministic copy from signals).
    """
    signals = collect_fathom_sales_signals(db, org_id)
    if not signals["has_any"]:
        return "default", list(DEFAULT_SALES_PLAYBOOK_PARAGRAPHS)

    if not use_llm_synthesis:
        return "fathom", _paragraphs_deterministic(signals)

    llm_paras = synthesize_paragraphs_with_llm(db, org_id, signals)
    if llm_paras:
        return "fathom", llm_paras

    return "fathom", _paragraphs_deterministic(signals)


def playbook_paragraphs_to_prompt_text(paragraphs: List[str]) -> str:
    if not paragraphs:
        return "\n\n".join(DEFAULT_SALES_PLAYBOOK_PARAGRAPHS)
    return "\n\n".join(paragraphs)

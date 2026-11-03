"""
Marketing Intel knowledge base — canonical frameworks injected as LLM context.

Fixed, niche-agnostic frameworks combined with the org's Intelligence profile (offer ladder,
business description, target audience, USP) and Fathom sales signals at generation time, so every
concept follows proven structure AND is anchored to the specific offer + real buyer objections.

Blocks:
- CONTENT_IDEATION_SOP  — hook & 3-layer-funnel concept framework (13 hook types).
- OFFER_BUILDING_SOP    — positioning / value / offer-construction framework.
- SHORTS_IDEATION_METHOD — sales-data-driven conversion ideation (the shorts /sweep skill logic):
  use Marketing Intel signals + Instagram performance (top/underperformers with permalinks + metrics)
  via SweepOS MCP — not TokScript — to know which angles turn into buyers and to pre-handle
  objections through content before the call.

Keep SOP_VERSION in sync with meaningful edits so the content bundle regenerates.
Human-readable copies live at frontend/public/resources/*.md.
"""
from __future__ import annotations

# Bump when any block below meaningfully changes so bundles regenerate.
SOP_VERSION = 3

CONTENT_IDEATION_SOP = """HOOK & VIDEO CONCEPT IDEATION SOP (universal, niche-agnostic framework).
This SOP governs the FIRST 3 SECONDS (the hook) and the IDEA a concept is built around.
Apply it to every concept, then personalize using INTELLIGENCE_PROFILE (offer ladder,
business_description, target_audience, unique_selling_proposition, personal_story,
mission_statement, pipeline_priorities).

CORE PRINCIPLE — THE 3-LAYER FUNNEL (never invert the order):
- HOOK (0-3s): stop the scroll for the WIDEST relevant audience. ZERO niche-specific terms.
- AMPLIFIER / FILTER (3-15s): narrow the universal moment into the operator's specific world.
  Niche terms and the ICP context from INTELLIGENCE_PROFILE are introduced HERE, not before.
- CTA (final 3s): convert only viewers who self-identified as ICP. Full niche specificity.
Rationale: retention is decided before the algorithm knows the niche. A niche term in the first
sentence pre-filters the audience and loses the "dream follower" (shares the values/outcome but
does not yet identify with the ICP label). The filter still happens — just later, after retention
is banked. This is what turns wide reach into reach that filters itself into buyers.
SWAP TEST: if you can swap the niche noun (e.g. fitness coach -> real estate agent -> nutrition
coach) and the hook still works, it passes Layer 1. If it breaks, the niche term entered too early.

THE 13 HOOK TYPES (mechanic first; keep the mechanic, swap the outcome to the operator's niche):
1. CONTRARIAN — challenge a category-level belief (effort, consistency, talent, "more content")
   before naming the mechanism. Belief must be broadly held, not niche jargon.
2. CURIOSITY GAP — tease the RESULT, not the industry. "one thing every top performer does".
3. MISTAKE — name the BEHAVIOR, not the title. "You're doing X wrong" beats "Coaches do X wrong".
4. RESULT-FIRST — lead with a number + timeframe (universally legible) before niche context.
5. IDENTITY — the only type meant to narrow early, but narrow the SITUATION not the LABEL.
   "If you're stuck between X and Y" travels wider than "If you're a [niche title]".
6. FEAR / LOSS — anchor loss in money, time, or falling behind peers before naming the mechanism.
7. NUMBER / LIST — numbers are niche-agnostic; keep the named outcome universal (hours, dollars, leads).
8. SOCIAL PROOF — lead with "a [role] just like you", peer-level proof, not your own brand name.
9. PERSONAL STORY — open in the CONFLICT, not the backstory/title. The story format self-broadens.
10. QUESTION — ask the diagnostic question at the OUTCOME level (money/time/growth), not the tactic.
11. BOLD CLAIM / OPINION — take a stance on the INDUSTRY, not a sub-tactic. Blunt beats hedged.
12. PATTERN INTERRUPT — the interrupt is in DELIVERY (silence, mid-sentence start, punchline-first),
    not the words. Highest average 3s retention (72-84%).
13. COMPARISON — compare universal states (manual vs automated, $3k vs $30k, chasing vs attracting)
    before naming the niche mechanism.

FILTER LAYER (amplifier, 3-15s) — do IN THIS ORDER:
1. Name the specific context (first place a niche term appears; pull it from INTELLIGENCE_PROFILE).
2. Name the specific symptom (move from universal loss -> the concrete mechanism/pain).
3. Position the SYSTEM, not the tactic (a process/sequence/framework signals depth -> creates a lead).
Awareness mapping: the hook recruits the unaware, the amplifier educates the problem-aware, the CTA
converts the solution/product-aware. One video can intentionally serve all three.

CTA LAYER: one CTA per video, no stacked asks. Single keyword trigger tied to the AMPLIFIER's
specific promise (not the hook). It should read as the natural next step of that promise.

IDEATION WORKFLOW per concept:
1. Pick ONE universal driver: money, time, status, fear of falling behind, freedom, identity/belonging.
2. Pick the hook type by goal: reach -> Contrarian/Mistake/Identity; watch time -> Result-First/
   Personal Story/Curiosity Gap; DM volume -> Identity/Fear-Loss/Social Proof; shares -> Bold Claim/
   Comparison/Number-List.
3. Write the hook TAM-agnostic (passes the swap test).
4. Write the amplifier filter (context -> symptom -> system), anchored to INTELLIGENCE_PROFILE.
5. Assign the CTA keyword tied to the amplifier's promise.

PRIMARY DIAGNOSTIC METRIC: 3-second hook completion rate, evaluated before niche performance/DM/close.
"""


OFFER_BUILDING_SOP = """OFFER-BUILDING SOP (positioning + value framework — use to make concepts reinforce the offer).
Two forces decide if an offer converts: VALUE (does the transformation feel real, fast, low-effort?)
and POSITIONING (does it feel different from the 50 other coaches?). Content must sell BOTH — value
alone gets outcompeted on price, positioning alone gets outcompeted on trust.

POSITIONING LEVERS (pull these from INTELLIGENCE_PROFILE offer_ladder/USP; reference them in concepts):
- CATEGORY: don't sit in the default category ("online coach") where you're compared on price.
  Own or create a narrower sub-category with higher relevance.
- NAMED ENEMY: position against the WRONG CAUSE the ICP blames (e.g. "willpower", "generic templates"),
  not against competitors. "This isn't a discipline problem, it's a structure problem." Differentiation
  without naming a competitor.
- OWNED MECHANISM: a named, proprietary-feeling method ("The Rebuild Protocol") — repeat it across
  content so it becomes uncopyable. Reference the operator's mechanism when it exists in the profile.
- PROOF ASYMMETRY: the most specific, most repeated proof wins. Lean on documented client results.

VALUE EQUATION: VALUE = (Dream Outcome x Perceived Likelihood) / (Time Delay x Effort Required).
Every concept that touches the offer should raise a believable outcome/likelihood or lower perceived
time/effort. Make outcome, likelihood, timeframe, and effort explicit — never just "a 12-week program".
DREAM OUTCOME framing: [Desired End State] + [Specific Timeframe] + [Emotional Payoff], in the ICP's
own words (mine call language). RISK REVERSAL: reference guarantees/scarcity ONLY when true.
Present sequence when pitching: restate the problem -> named mechanism -> 3-step how -> value ->
price vs anchor -> honest scarcity/urgency -> guarantee -> close. Concepts should map to a step here.
"""


SHORTS_IDEATION_METHOD = """CONVERSION IDEATION METHOD (sales-data-driven — this is the engine behind every concept).
Goal: generate ideas that turn viewers into BUYERS, not just views. Reverse-engineer content from the
sales process so marketing PRE-HANDLES objections before a prospect ever reaches a call.

INPUTS (use what's present; ignore any external transcript/MCP tools):
- Fathom SIGNALS in this prompt: most common objections, bottlenecks, the pains + tangible/intangible
  goals that actually moved prospects to YES, phrases that resonated, real client wins.
- INTELLIGENCE_PROFILE: offer ladder, ICP, USP, positioning.

METHOD:
1. Extract conversion intelligence: the top objections heard before close, the core pains that moved
   buyers, the tangible outcomes (metrics) and intangible outcomes (identity/feelings) that sealed it,
   and any angle that shortened the sales cycle.
2. Build each concept around ONE of those: every idea must pre-handle a real objection or amplify a
   pain/goal that actually converts. No generic self-improvement content that doesn't pre-sell.
3. Structure each concept as: HOOK (1 line, scripted) -> proof/credibility beat -> re-hook ->
   body/value beat -> CTA. Only the HOOK is written verbatim; the rest are directional beats.
4. Tie each concept to the funnel stage and the specific objection/goal it dissolves.

The HOOK is the only explicitly scripted line per concept. It must obey the CONTENT_IDEATION_SOP:
widest relevant audience, ZERO niche terms, built on one universal driver, passes the swap test.
"""


OBJECTION_TAXONOMY = """OBJECTION TAXONOMY (for pre-handling objections in content — not live-call scripts).
Every objection is FEAR or LOGISTICS. Fear is emotional/identity-level resistance (doubt, past
failure, fear of wasting money/time, fear of judgment); logistics are practical blockers (timing,
payment, partner approval) that only surface once the fear layer is resolved. Mixed objections
("I need to talk to my partner") carry a fear layer underneath the stated logistics. Content should
pre-handle the FEAR layer first — logistics rarely stop a sale on their own.

FIVE RECURRING PATTERNS (map SIGNALS' real objection quotes to these before writing a concept):
1. "I need to think about it" -> decision paralysis/avoidance. Angle: reframe inaction itself as
   the real risk, not the purchase.
2. "It's a lot of money" -> fear of wasted investment, not literal price. Angle: cost of staying
   stuck vs. cost of the program; reframe as investment, not expense.
3. "Bad timing / life is chaotic" -> fear of failure disguised as scheduling. Angle: there is never
   a "right time"; the uncertainty of life argues FOR starting now, not waiting.
4. "What if it doesn't work?" -> fear of a past failure repeating. Angle: proof + specificity of
   mechanism beats a generic guarantee.
5. "I need to talk to my partner/spouse" -> fear of accountability/judgment, not pure logistics.
   Angle: ownership of one's own goals; rarely really about the partner.
Use the real objection quotes already present in SIGNALS to pick which pattern to target — don't
invent a generic objection when a specific one is already in evidence.
"""


CONTENT_FUNNEL_STAGES = """CONTENT FUNNEL STAGES (format choice across a content calendar — distinct from the
per-video hook/amplifier/CTA structure above; this is WHICH FORMAT to use at WHICH awareness stage).
TOF (top of funnel) — lowest awareness, cold viewers. Goal: reach/discovery.
  Formats: trend-based reels, repurposed viral templates, trending audio, quick watch-throughs.
MOF (middle of funnel) — warming up, existing followers evaluating credibility. Goal: trust/authority.
  Formats: carousels, how-to/step-by-step videos, green-screen breakdowns, raw explanations,
  personal story woven into educational value.
BOF (bottom of funnel) — highest awareness, near-ready to buy. Goal: conversion.
  Formats: client testimonials, clipped check-in/call moments, transformation proof, offer
  breakdowns, direct results content — this is where CALL_LIBRARY_BOF_EXAMPLES belongs.
A healthy content calendar represents all three stages regularly — don't only produce BOF because
it converts, or only TOF because it reaches; the funnel stalls without the middle.
"""


FORMAT_LENGTH_GUIDE = """FORMAT LENGTH CHOICE (short-form vs. long-form — pick deliberately per concept's goal).
SHORT-FORM (Reels/TikTok/Shorts, 15-60s): optimized for reach and virality. Use for TOF discovery,
trend participation, single-idea hooks. Minimal commitment asked of a cold viewer.
LONG-FORM (YouTube/podcast, 10+ min): optimized for trust and authority, not reach. Use for MOF/BOF
depth — full frameworks, interviews, in-depth case studies. Don't force a BOF trust-building idea
into a 30-second format just because short-form is the default; depth needs the longer format.
"""


def content_ideation_sop_block() -> str:
    """Return only the hook/concept ideation SOP text."""
    return CONTENT_IDEATION_SOP


def _org_override_or_default(db, org_id, resource_id: str, default: str) -> str:
    """The org's own saved edit when they've actually customized this doc (is_custom=True);
    otherwise `default` — NEVER the raw public/resources/*.md file on disk. That file is a
    human-readable copy for the Resource Library UI (5-8x longer: headers, bold, worked
    examples) and get_document_content's fallback chain reaches it before any `fallback=`
    kwarg we'd pass, so going through get_sop_content here would silently blow the prompt's
    character budget for every org that hasn't customized the doc — i.e. almost all of them.
    """
    if db is None or org_id is None:
        return default
    from app.services.resource_documents import get_doc

    doc = get_doc(db, org_id, resource_id)
    if doc and doc.get("is_custom"):
        content = str(doc.get("content") or "").strip()
        if content:
            return content
    return default


def marketing_intel_knowledge_block(db=None, org_id=None) -> str:
    """Combined SOP + method knowledge base for the Marketing Intel LLM user prompt."""
    content_ideation = _org_override_or_default(db, org_id, "content-ideation-sop", CONTENT_IDEATION_SOP)
    offer_building = _org_override_or_default(db, org_id, "building-an-offer-sop", OFFER_BUILDING_SOP)
    # Only included when the org has actually filled in their own copy — the default file on
    # disk is a blank fill-in-the-blank worksheet (empty bullets), not worth the prompt budget.
    org_positioning_worksheet = _org_override_or_default(db, org_id, "messaging-and-storytelling", "")

    blocks = [
        content_ideation,
        offer_building,
        SHORTS_IDEATION_METHOD,
        OBJECTION_TAXONOMY,
        CONTENT_FUNNEL_STAGES,
        FORMAT_LENGTH_GUIDE,
    ]
    if org_positioning_worksheet:
        blocks.append(
            "ORG POSITIONING WORKSHEET (this org's own messaging — named enemy, unique "
            "mechanism, core contrasts, marketing angles. Use this language verbatim where "
            "it's filled in):\n" + org_positioning_worksheet
        )

    return "\n\n".join(blocks)

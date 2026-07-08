"""
Marketing Intel knowledge base — canonical frameworks injected as LLM context.

Fixed, niche-agnostic frameworks combined with the org's Intelligence profile (offer ladder,
business description, target audience, USP) and Fathom sales signals at generation time, so every
concept follows proven structure AND is anchored to the specific offer + real buyer objections.

Blocks:
- CONTENT_IDEATION_SOP  — hook & 3-layer-funnel concept framework (13 hook types).
- OFFER_BUILDING_SOP    — positioning / value / offer-construction framework.
- SHORTS_IDEATION_METHOD — sales-data-driven conversion ideation (the shorts skill logic, minus
  the TokScript MCP: reference Fathom sales/check-in data to know which angles turn into buyers and
  to pre-handle objections through content before the call).

Keep SOP_VERSION in sync with meaningful edits so the content bundle regenerates.
Human-readable copies live at frontend/public/resources/*.md.
"""
from __future__ import annotations

# Bump when any block below meaningfully changes so bundles regenerate.
SOP_VERSION = 2

CONTENT_IDEATION_SOP = """HOOK & VIDEO CONCEPT IDEATION SOP (universal, niche-agnostic framework).
This SOP governs the FIRST 3 SECONDS (the hook) and the IDEA a concept is built around.
Apply it to every concept, then personalize using INTELLIGENCE_PROFILE (offer ladder,
business_description, target_audience, unique_selling_proposition, pipeline_priorities).

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


def content_ideation_sop_block() -> str:
    """Return only the hook/concept ideation SOP text."""
    return CONTENT_IDEATION_SOP


def marketing_intel_knowledge_block(db=None, org_id=None) -> str:
    """Combined SOP + method knowledge base for the Marketing Intel LLM user prompt."""
    content_ideation = CONTENT_IDEATION_SOP
    offer_building = OFFER_BUILDING_SOP

    if db is not None and org_id is not None:
        from app.services.resource_documents import get_sop_content

        content_ideation = get_sop_content(
            db, org_id, "content-ideation-sop", fallback=CONTENT_IDEATION_SOP
        )
        offer_building = get_sop_content(
            db, org_id, "building-an-offer-sop", fallback=OFFER_BUILDING_SOP
        )

    return "\n\n".join(
        (
            content_ideation,
            offer_building,
            SHORTS_IDEATION_METHOD,
        )
    )

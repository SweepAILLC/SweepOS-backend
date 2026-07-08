# Shorts Content Ideation — AI Skill
**Version:** v1.0 | **MCP Required:** TokScript (`api.tokscript.com/mcp`) + Fathom

---

## PURPOSE
Generate 10 high-conversion short-form content ideas by cross-referencing Instagram content performance (via TokScript) with real sales call data (via Fathom). Every idea is engineered to pre-handle objections, build trust, and attract buyers — not just views.

---

## PREREQUISITES
- Run `instagram-content-audit` skill first to have content performance data ready
- Run `sales-call-analysis` skill first (or have Fathom call summaries available)
- Both TokScript MCP and Fathom MCP must be connected

---

## SETUP INSTRUCTIONS

### MCP Connections Required
1. **TokScript:** `api.tokscript.com/mcp` — for content performance analysis
2. **Fathom:** See [Fathom MCP Setup](https://help.fathom.video/en/articles/11497793) — for sales call data

### How to Run
Use this prompt after completing the Instagram Content Audit and Sales Call Analysis. Replace placeholders with client handle and context:

---

## PROMPT

```
You are a conversion-focused short-form content strategist. Your job is to generate 10 content ideas that are engineered to turn viewers into buyers — not just grow an audience.

INPUTS YOU HAVE:
- Instagram content audit for @[HANDLE] (paste audit results here or reference prior analysis)
- Sales call analysis with top objections, close rates, and discovery quality (paste or reference)

STEP 1 — Extract Conversion Intelligence
From the Instagram audit, identify:
- Top 3 performing content angles/hooks (highest views + saves + engagement)
- Top 3 underperforming angles to deprioritize
- Current TOF:MOF:BOF gap (what's missing)

From the sales call data, identify:
- Top 5 most common objections heard before close
- The core pain points that actually moved prospects to YES
- The tangible outcomes (goals) prospects wanted most
- The intangible outcomes (feelings/identity) that sealed the close
- Any questions or angles that shortened the sales cycle

STEP 2 — Generate 10 Content Ideas

Each idea must be sales-data-driven. For each of the 10 ideas, output:

**[#] [Content Title/Concept]**
- **Hook** (scripted, word-for-word): [Write the exact opening line]
- **Proof/Credibility**: [What result, story, or data point to open with]
- **Re-hook**: [1-line that forces them to keep watching]
- **Body/Value**: [Core message in 1–2 sentences — what insight or reframe are you delivering]
- **CTA**: [Exact call-to-action — DM, comment, link in bio, etc.]
- **Objection Handled**: [Which sales objection does this pre-handle?]
- **Funnel Stage**: [TOF / MOF / BOF]

CONSTRAINTS:
- Only the hook is explicitly scripted — the rest is directional
- Every idea must address at least one real objection from the call data
- Balance the 10 ideas: ~4 TOF, ~4 MOF, ~2 BOF
- Hooks must stop the scroll (bold claim, identity trigger, or specific curiosity gap)
- No generic self-improvement content — every piece must pre-sell the program

STEP 3 — OUTPUT FORMAT

## SHORTS CONTENT IDEATION — @[HANDLE]
**Date:** [date] | **Based on:** [N] sales calls + [N] Reels analyzed

### Conversion Intelligence Summary
[3 bullets: top angles from content + top objections from calls]

### 10 Content Ideas
[List all 10 using the format above]

### Priority Order
Rank the 10 by highest estimated conversion impact (1 = film first).
Reason for each ranking in one sentence.
```

---

## OUTPUT EXPECTATIONS
- 10 complete content ideas with all 5 elements per idea
- Every hook is explicitly scripted word-for-word
- Sales data is the backbone — viewer intent maps to buyer intent
- Ranked by conversion priority, not vanity metrics

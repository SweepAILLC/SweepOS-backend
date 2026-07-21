# Shorts Content Ideation — AI Skill
**Version:** v2.0 | **Primary MCP:** SweepOS remote MCP | **Optional:** Fathom (transcript gaps)

---

## PURPOSE

Generate **10 conversion-engineered short-form content ideas** from real SweepOS sales intelligence — objections, struggles, wins, stories, ICP, and offer ladder — not from Instagram scrapers.

TokScript is not used. Live Instagram performance is optional (user paste only). Every idea must pre-handle a real buyer objection or amplify a proven win/story from Sweep.

---

## PREREQUISITES

- SweepOS remote MCP connected to the correct org
- Prefer running **Instagram Content Audit (v2)** first so strategy gaps are known (optional but recommended)
- Marketing Intel / call insights should have data; if empty, say so and stop inventing themes

---

## MCP REQUIRED

### Primary — SweepOS (required)

Remote MCP URL: `https://api.sweepai.site/mcp`  
Setup: `docs/integrations/CLAUDE_MCP_CONNECTOR.md`

**Core tools for this skill:**

| Tool | Use |
|------|-----|
| `get_connection_context` | Confirm org |
| `get_marketing_intel` | **Start here** — signals + knowledge + playbook + ICP + TOF/MOF/BOF + SOP |
| `list_org_sales_themes` | Validated / recurring objection themes + quotes |
| `get_org_sales_signals` | Raw struggles / wins / stories / phrasing |
| `search_sales_clips` | Filter clips by `objection` / `win` / `testimonial` |
| `get_org_intelligence_profile` | ICP / offer ladder / voice |
| `list_clients` + `get_client_call_insights` | Optional — client-specific ideation |

### Optional — Fathom

If Sweep clips are sparse, pull full transcripts for richer objection/win language:  
https://help.fathom.video/en/articles/11497793

---

## HOW TO RUN

Paste the prompt into Claude with SweepOS MCP enabled. Optionally attach a prior Instagram Content Audit.

---

## PROMPT

```
You are a conversion-focused short-form content strategist. Generate 10 content ideas engineered to turn viewers into buyers — not vanity views. Ground every idea in SweepOS Marketing Intel. Do not use TokScript.

════════════════════════════════════════
STEP 0 — ORG + DATA
════════════════════════════════════════
1. get_connection_context — confirm org.
2. get_marketing_intel (include_sop=true) — primary input.
3. list_org_sales_themes — capture top themes + sample quotes.
4. search_sales_clips:
   - kind=objection (enough to cover top 5 objections)
   - kind=win
   - kind=testimonial
5. get_org_intelligence_profile — ICP, offer, voice constraints.
6. OPTIONAL: If targeting one client, list_clients → get_client_call_insights(client_id).
7. OPTIONAL: If Sweep quotes are thin, use Fathom for matching call transcripts only.
8. OPTIONAL: If user pasted prior Instagram audit or Reel performance notes, use for “what already works on-platform.”

════════════════════════════════════════
STEP 1 — CONVERSION INTELLIGENCE SUMMARY
════════════════════════════════════════
Extract and list:
- Top 5 objections (theme + 1 quote each)
- Top pain / struggle clusters that move people toward YES
- Tangible outcomes buyers want (metrics)
- Intangible outcomes (identity / feelings) that seal closes
- Proof inventory: best wins + testimonial stories (specific numbers when present)
- TOF / MOF / BOF gaps from marketing intel / prior audit
- Resonated phrasing to reuse; avoid phrasing to ban

════════════════════════════════════════
STEP 2 — GENERATE 10 IDEAS
════════════════════════════════════════
Balance: ~4 TOF, ~4 MOF, ~2 BOF.

For EACH idea output:

**[#] [Title / Concept]**
- **Hook** (scripted word-for-word, 0–3s): …
- **Amplifier / filter** (3–15s direction): …
- **Proof / credibility**: Sweep win, story, or metric to open with (cite source)
- **Re-hook**: 1 line that forces continued watch
- **Body / value**: Core reframe in 1–2 sentences
- **CTA**: Exact ask (DM keyword, comment prompt, book call, etc.)
- **Objection / trigger handled**: Theme or clip reference
- **Funnel stage**: TOF | MOF | BOF
- **Hook type**: (from content-ideation SOP if present — e.g. contrarian, curiosity gap, mistake, result-first, identity…)

CONSTRAINTS:
- Hooks stop the scroll (bold claim, identity trigger, or specific curiosity gap)
- No niche jargon in the first sentence unless SOP allows; filter ICP later
- No generic self-help — every piece pre-sells THIS offer
- Every idea maps to ≥1 real Sweep objection, win, or story
- Prefer prospect voice from clips over invented language

════════════════════════════════════════
STEP 3 — OUTPUT FORMAT
════════════════════════════════════════

## SHORTS CONTENT IDEATION — [ORG / CLIENT]
**Date:** … | **Org:** … | **Sources:** Sweep tools [list]; Fathom [yes/no]; prior audit [yes/no]

### 1. Conversion intelligence summary
Bullets + ranked objection table with quotes.

### 2. Proof inventory
Wins / testimonials usable on camera (with citations).

### 3. Funnel gap diagnosis
What stages / objections lack content.

### 4. Ten content ideas
Full format above for all 10.

### 5. Priority filming order
Rank 1–10 by estimated conversion impact. One sentence reason each (tied to Sweep evidence).

### 6. Batching notes
Which ideas share B-roll, guest, or proof asset so filming is efficient.

RULES:
- Never invent objections or testimonials.
- Prefer thorough completeness over brevity.
- If Marketing Intel is empty, say so and list what the org must sync (Call Library / Intelligence) before ideation.
```

---

## OUTPUT EXPECTATIONS

- 10 complete ideas with scripted hooks + Sweep citations
- Ranked filming order by conversion impact
- TOF/MOF/BOF balance with gap diagnosis
- Fathom only when Sweep clips are insufficient
- No TokScript / no fabricated Instagram metrics

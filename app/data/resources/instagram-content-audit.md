# Instagram Content Audit — AI Skill
**Version:** v2.0 | **Primary MCP:** SweepOS remote MCP | **Optional:** Fathom (raw transcript gaps)

---

## PURPOSE

Produce a **complete, thorough content strategy audit** for a client (or the org’s own brand) by grounding every judgment in SweepOS data — buyer objections, struggles, wins, testimonials, ICP, and offer ladder — instead of scraping Instagram via TokScript.

SweepOS does **not** pull live Instagram view counts or Reel media. This skill audits **message–market fit**: whether the content angles, funnel mix, and credibility story match what actually closes deals. If the operator pastes recent Reel captions / performance notes, fold those into the report as supporting evidence.

---

## MCP REQUIRED

### Primary — SweepOS (required)

1. Claude → **Settings → Connectors → Add custom connector**
2. Remote MCP URL: `https://api.sweepai.site/mcp` (must match `MCP_RESOURCE_URL`)
3. Connect → Google sign-in → pick the correct org
4. In chat: **+ → Connectors** → enable SweepOS
5. Confirm tools: `get_connection_context`, `get_marketing_intel`, `list_clients`, `get_client_profile`, `get_client_call_insights`, `get_org_sales_signals`, `list_org_sales_themes`, `search_sales_clips`, `get_org_intelligence_profile`

See: `docs/integrations/CLAUDE_MCP_CONNECTOR.md`

### Optional — Fathom

Use Fathom MCP only when Sweep call insights / clips are thin and you need **full transcripts** for deeper quote mining:  
https://help.fathom.video/en/articles/11497793

---

## HOW TO RUN

1. Connect SweepOS MCP and confirm org via `get_connection_context`.
2. Paste the prompt below into Claude.
3. Replace placeholders: `[CLIENT_NAME_OR_HANDLE]`, optional Instagram paste block.

---

## PROMPT

```
You are a direct-response content strategist. Produce a complete Instagram / short-form content audit grounded in SweepOS org data. Do not invent engagement metrics. Cite Sweep signals (themes, quotes, wins, stories) as evidence.

════════════════════════════════════════
SETUP
════════════════════════════════════════
1. Call get_connection_context. Confirm org_name / org_id. If wrong, stop and ask the user to reconnect.
2. Target: [CLIENT_NAME_OR_HANDLE]
   - If this is a Sweep client: list_clients (query) → get_client_profile → get_client_call_insights.
   - If this is org-level brand content (no single client): skip client tools; use org Marketing Intel only.
3. Call get_marketing_intel (include_sop=true). This is the primary package.
4. Deepen with:
   - list_org_sales_themes (validated_only=false, then note which are validated)
   - get_org_sales_signals
   - search_sales_clips for kind=objection, win, testimonial (limit 40–60 each as needed)
   - get_org_intelligence_profile for ICP / offer / voice
5. OPTIONAL — If Sweep clips lack depth for a claim, ask whether Fathom MCP is connected; if yes, pull matching call transcripts for quote gaps only. Prefer Sweep clips when available.
6. OPTIONAL USER PASTE — If the user provides recent Reel titles, captions, hooks, or performance notes, treat them as the content sample under audit. If none provided, audit the *strategy* implied by Sweep data and state clearly that no live Instagram scrape was used.

════════════════════════════════════════
ANALYZE (be thorough; every section needs Sweep evidence)
════════════════════════════════════════

A. BUYER REALITY (from Sweep)
- Top recurring objections (themes + sample quotes)
- Core struggles / pains that show up before close
- Wins and testimonial stories that prove the offer
- ICP / offer ladder implications for what content must say
- Resonated vs avoid phrasing from call insights

B. CONTENT ANGLE FIT
- Map current (or recommended) content themes to buyer reality
- Which angles pre-handle real objections? Which chase vanity topics?
- Which proof/stories from Sweep should be featured on-camera?

C. HOOKS & RE-HOOKS
- If user pasted Reels: analyze hook patterns, re-hooks in first 3–5s, weak vs strong examples
- If no paste: prescribe hook patterns from Marketing Intel + content-ideation SOP guidance in get_marketing_intel, tied to real objections/wins

D. FORMATS & CONCEPTS
- Talking head / B-roll+VO / text-on-screen / tutorial / story — what the ICP will trust
- Concepts that should repeat vs retire, based on sales evidence (not vibes)

E. TOF / MOF / BOF
- Target mix for this offer (use intel TOF/MOF/BOF bundle if present)
- Gaps (e.g. all TOF, no objection-handling MOF, no BOF proof)
- Specific content jobs for each stage grounded in themes

F. CREDIBILITY & TRUST
- Personal story vs client results — what’s available in Sweep wins/stories
- Specificity of metrics in testimonials (vague praise vs concrete outcomes)
- Authority gaps that content must close before the sales call

G. LEAD GEN QUALITY (intent, not views)
- Score 1–10: does this content attract buyers who match ICP / objections?
- Justify with Sweep themes — not follower vanity

H. ALGORITHM / DISTRIBUTION (only if user pasted performance)
- Views vs followers, trend, cadence — otherwise mark N/A and explain Sweep has no live IG metrics

════════════════════════════════════════
OUTPUT — COMPLETE REPORT
════════════════════════════════════════

## INSTAGRAM CONTENT AUDIT — [CLIENT / BRAND]
**Org:** [from get_connection_context] | **Date:** [today]
**Data sources:** SweepOS MCP tools used: [list]. Fathom: [yes/no]. Live IG sample: [pasted / none].

### 1. Executive summary
5–8 bullets: what’s working, what’s broken, highest-leverage fix.

### 2. Profile / context snapshot
Client or brand context from Sweep (stage, offer, ICP). Instagram handle if known. State data limitations.

### 3. Buyer reality map
Tables or ranked lists: objections, struggles, wins/stories — each with ≥1 Sweep quote or theme citation.

### 4. Hook & re-hook analysis
Evidence-backed. If no IG paste: “Prescribed hooks” section instead of historical audit.

### 5. Format & concept breakdown
What’s winning for buyers + what to stop.

### 6. TOF / MOF / BOF map
Current vs recommended mix + gap list with example concepts per stage.

### 7. Credibility & trust scorecard
Score 1–10 with reasons tied to Sweep proof inventory.

### 8. Lead gen quality score
Score 1–10 with ICP / objection alignment.

### 9. Algorithm traction
Fill only if metrics were provided; else N/A.

### 10. Content gaps & risks
What’s missing that sales already proved buyers need.

### 11. Top 5 highest-leverage fixes
Ranked. Each: action + Sweep evidence + expected conversion effect.

### 12. 14-day content sprint (optional but preferred)
Day-by-day or 7 concepts mapped to stages + objections handled.

RULES:
- Prefer Sweep tool data over assumptions. Label speculation as speculation.
- Never invent view counts, save rates, or transcript quotes.
- Prefer complete thoroughness over artificial page limits.
- Every major claim cites a theme, clip, win, story, or ICP field.
```

---

## OUTPUT EXPECTATIONS

- Full structured report (sections 1–12), not a thin 2-page summary
- Sweep citations throughout (themes, quotes, wins, stories)
- Clear disclosure when Instagram was not scraped / metrics unavailable
- Actionable fixes ranked by conversion leverage
- Fathom used only to fill transcript gaps Sweep does not cover

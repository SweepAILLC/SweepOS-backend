# Sales Call Analysis — AI Skill
**Version:** v2.0 | **Primary MCP:** SweepOS remote MCP | **Optional:** Fathom (full transcripts)

---

## PURPOSE

Produce a **complete, thorough sales diagnostic** from SweepOS call intelligence — org-wide themes, clips, client insights, wins, and struggles — with real quotes wherever Sweep has them.

Use **Fathom MCP** when you need raw full-call transcripts that Sweep has not yet turned into Call Library insights (or when a specific exchange is missing from clips).

TokScript is not used.

---

## MCP REQUIRED

### Primary — SweepOS (required)

Remote MCP URL: `https://api.sweepai.site/mcp`  
Setup: `docs/integrations/CLAUDE_MCP_CONNECTOR.md`

| Tool | Use for this skill |
|------|--------------------|
| `get_connection_context` | Confirm org |
| `get_org_sales_signals` | Objections, struggles, wins, stories, meeting excerpts |
| `list_org_sales_themes` | Ranked / validated themes + sample quotes |
| `search_sales_clips` | Drill into `objection` / `win` / `testimonial` / `other` |
| `list_clients` | Find reps’ clients or a focus cohort |
| `get_client_call_insights` | Per-client analysis packages |
| `get_client_profile` | Context (stage, offer, investments) when diagnosing one account |
| `get_marketing_intel` | Optional — playbook / operator knowledge / closings for “what good looks like” |
| `get_terminal_dashboard` | Optional — close/booking health context (`mode=overview`) |

### Optional — Fathom (gaps only)

When Sweep returns summaries/clips but you need the full discovery → pitch → close arc or verbatim exchanges:  
https://help.fathom.video/en/articles/11497793

**Rule:** Prefer Sweep clips for frequency and themes; use Fathom to deepen root-cause quotes, not as the only source when Sweep data exists.

---

## HOW TO RUN

1. Connect SweepOS MCP; confirm org.
2. Paste the prompt; set period / focus (org-wide, one rep, or one client).
3. Enable Fathom only if prompted by thin Sweep coverage.

---

## PROMPT

```
You are a high-performance sales coach. Produce a complete, honest diagnostic of sales call performance for this SweepOS organization. Prefer SweepOS MCP data. Use Fathom only to fill transcript gaps. Never invent quotes.

════════════════════════════════════════
STEP 1 — SCOPE + PULL DATA
════════════════════════════════════════
1. get_connection_context — confirm org.
2. Clarify scope with the user if unclear:
   - Org-wide last [30/60] days of insights, OR
   - Specific client(s), OR
   - Cohort (e.g. active / booked / closed-won vs lost)
3. Pull Sweep data (always):
   - get_org_sales_signals
   - list_org_sales_themes (validated_only=false; call out validated ones)
   - search_sales_clips for objections, wins, testimonials (raise limit as needed)
4. If client-scoped:
   - list_clients / search_clients_by_email → get_client_call_insights → get_client_profile as needed
5. Optional business context: get_terminal_dashboard mode=overview (bookings, failed payments, trends)
6. Optional coaching standard: get_marketing_intel for playbook / objection knowledge bank
7. GAP FILL — If discovery/pitch/close quotes are missing or too short:
   - Ask if Fathom is connected
   - Pull matching call summaries/transcripts for the same clients/period
   - Label every Fathom-sourced quote as [Fathom]

════════════════════════════════════════
STEP 2 — ANALYZE
════════════════════════════════════════

OBJECTION ANALYSIS
- Top objections by recurrence (themes + clip frequency)
- For each: real quote, handling quality when response is visible, stall vs progress
- Which objections are fumbled most often?

DISCOVERY QUALITY
- Evidence that reps uncover: (a) current pain, (b) tangible goals, (c) intangible goals
- Strong vs weak discovery moments with quotes
- Score 1–10 + one-sentence reason

PITCH QUALITY
- Pain/goals tied to deliverables vs feature dump
- Personalized vs templated
- Strong/weak examples with quotes
- Score 1–10

OBJECTION HANDLING
- Acknowledge → isolate → reframe vs cave/defend
- Real exchanges when available (prospect + rep)
- Score 1–10

CLOSE QUALITY
- Clear ask? Payment options structured? Next step locked?
- Score 1–10

ROOT CAUSE OF LOSSES
- From lost / no-close signals: final friction → reverse engineer root cause
- Top 3 root causes with evidence (Sweep first, Fathom if needed)

WINS
- 3–5 genuine strengths with quotes / stories
- What to systematize

REP / COHORT DIFFERENCES (if data allows)
- Patterns by client stage or outcome

════════════════════════════════════════
STEP 3 — OUTPUT — COMPLETE REPORT
════════════════════════════════════════

## SALES CALL ANALYSIS REPORT
**Org:** … | **Scope:** … | **Period / sample:** …
**Sources:** Sweep tools [list]. Fathom used: [yes/no — for what].

### 1. Executive summary
Key findings, biggest leak, biggest strength.

### 2. Performance snapshot
| Dimension | Score (1–10) | Evidence basis |
|-----------|--------------|----------------|
| Discovery | | |
| Pitch | | |
| Objection handling | | |
| Close | | |

### 3. Top objections (ranked)
For each: frequency signal, quote(s), handling assessment, recommended reframe (tie to marketing intel playbook if present).

### 4. Discovery deep-dive
Evidence-backed narrative + quote bank.

### 5. Pitch effectiveness
Evidence-backed narrative + quote bank.

### 6. Objection handling breakdown
Exchanges + patterns.

### 7. Close analysis
Asks, payment framing, next-step discipline.

### 8. Root causes of lost deals
1–3 with reverse-engineered evidence chain.

### 9. What’s working
3–5 strengths to protect and scale.

### 10. Priority improvements (ranked)
For each: specific drill, script line, or process change — not platitudes.

### 11. Clip appendix
Table of key Sweep clip IDs/labels/quotes used (and Fathom call refs if any).

RULES:
- Every major claim needs a quote or theme citation.
- Do not fabricate transcript text.
- Prefer thorough completeness; do not artificially truncate to 2 pages.
- If Sweep has no call insights yet, say exactly what must be synced (Call Library / Fathom → Sweep) and produce only a setup checklist — do not invent a fake diagnostic.
```

---

## OUTPUT EXPECTATIONS

- Full diagnostic with scores, quote banks, root causes, and ranked fixes
- Sweep-first evidence; Fathom clearly labeled when used
- Actionable drills/scripts, not generic coaching
- Honest “insufficient data” path when Call Library is empty

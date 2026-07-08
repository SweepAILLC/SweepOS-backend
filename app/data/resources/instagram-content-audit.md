# Instagram Content Audit — AI Skill
**Version:** v1.0 | **MCP Required:** TokScript (`api.tokscript.com/mcp`)

---

## PURPOSE
Analyze a client's Instagram content to surface lead gen quality, algorithm traction, and conversion potential. Output must be 2 pages maximum — every data point is intentional and tied to trust, authority, or conversion outcomes. No filler.

---

## SETUP INSTRUCTIONS

### MCP Connection
Connect TokScript MCP before running this skill:
1. Copy connector URL: `api.tokscript.com/mcp`
2. In Claude: Settings → Connectors → Add Custom Connector → Paste URL → Log in
3. Confirm tools are available (`get_instagram_user_reels`, `get_instagram_user_posts`)

### How to Run
Paste this entire prompt into Claude (with TokScript MCP active), then replace `[HANDLE]` with the client's Instagram username:

---

## PROMPT

```
You are a direct-response content strategist. Analyze @[HANDLE]'s Instagram using TokScript.

STEP 1 — Pull Data
- Use get_instagram_user to get profile stats (followers, post count, bio)
- Use get_instagram_user_reels to pull the most recent 20–30 Reels
- Use get_instagram_user_posts to supplement with any pinned or high-performing posts

STEP 2 — Analyze the Following (be direct, cite real examples with view/engagement counts):

HOOKS
- What hook patterns appear most (question, bold claim, identity, pain point)?
- Which hooks drove the highest view-to-watch rate? Cite the top 3.
- Are hooks stopping the scroll or blending in? Flag weak patterns.

RE-HOOKS
- Is there a re-hook in the first 3–5 seconds after the opening? Y/N
- Which re-hooks retained viewers best? Give examples.

CONCEPTS & FORMATS
- What content formats are being used (talking head, B-roll + VO, text-on-screen, tutorial, story)?
- Which format is performing best on views AND saves/shares?
- What content concepts are repeating? What's new vs. tested?

TOF / MOF / BOF TARGETING
- Classify each content piece: TOF (awareness), MOF (consideration), BOF (conversion)
- What is the current TOF:MOF:BOF ratio?
- Are there gaps? (e.g. all TOF with no offer-adjacent content)

CREDIBILITY SIGNALS
- Is personal story being used? If so, how effectively?
- Are client results/success stories being featured? Are they specific with metrics?
- Does the content build authority or just provide generic value?

CONTENT PERFORMANCE — LEAD GEN QUALITY
- Which posts are driving comments, DMs, and profile clicks (inferred from saves/shares/engagement type)?
- Which content themes/angles correlate with the highest engagement quality?
- Is the content pre-qualifying buyers or attracting broad audiences with no purchase intent?

ALGORITHM TRACTION
- What is the average views-per-reel vs. follower count ratio?
- Is reach expanding or contracting (directional trend across the last 20 posts)?
- What posting cadence/consistency patterns exist?

STEP 3 — OUTPUT FORMAT (2 pages max)

## INSTAGRAM CONTENT AUDIT — @[HANDLE]
**Pulled:** [date] | **Posts Analyzed:** [N]

### Profile Snapshot
[followers | avg views/reel | posts/week | top format]

### Hook Analysis
[Top 3 hooks with examples + what's working/broken]

### Re-Hook & Retention
[Pattern + examples]

### Content Format & Concept Breakdown
[What formats/concepts exist + which win]

### TOF / MOF / BOF Map
[Current ratio + gaps]

### Credibility & Trust Signals
[Personal story use + client results quality]

### Lead Gen Quality Score
[1–10 rating with reason — are these videos attracting buyers?]

### Algorithm Traction
[Trend + avg performance + cadence]

### Top 3 Highest-Leverage Fixes
1. [Most impactful change with specific reasoning]
2. [Second fix]
3. [Third fix]

Keep output tight. Every sentence must serve conversion, trust, or algorithm traction. Delete anything generic.
```

---

## OUTPUT EXPECTATIONS
- 2 pages max
- Real video examples cited with view counts
- Every insight tied to a specific conversion outcome (build trust / build authority / drive conversions)
- No filler, no generic "post more consistently" advice without context

"""Required system-message persona for generative (client-facing) LLM calls.

Per LLM.md's "Required system message for all generative calls" rule: copy this
text exactly for anything that generates content a coach will send to, or share
with, a client (emails, recommendations, drafted communication). Do not rephrase
or trim it per-service — that's the whole point of having one shared constant
instead of each service inventing its own persona text.

Classification/extraction calls that only produce internal structured data (call
insights, health scores, sentiment) are exempt per LLM.md's own "generative
calls" scope — those keep their own narrow, directive system prompts instead,
since blending in this warm/coach-tone persona would work against the strict
JSON-only, don't-follow-instructions-in-DATA behavior those prompts depend on.
Each exempt call site should say so in a one-line comment so the exemption reads
as deliberate, not missed.
"""

SWEEPBOT_SYSTEM = """You are SweepBot, the AI Growth Engine for Sweep Coach OS. Your role is to help coaches build stronger relationships with their clients through personalized, empathetic, and data-driven communication.

Core principles:
- Be warm, professional, and coach-like in tone
- Prioritize client outcomes and relationship building
- Use data and context to inform recommendations
- Always cite sources and evidence for claims
- Flag uncertainty and request human review when appropriate
- Respect client boundaries and communication preferences
- Maintain consistency with Sweep brand voice (see BRAND.md)

You are NOT a replacement for human judgment. You are a tool to amplify coach effectiveness."""

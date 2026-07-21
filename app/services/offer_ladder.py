"""
Intelligence "offer ladder": core offer + upsells/add-ons + referral offer.

Stored at `user.ai_profile.offer_ladder` (matches the rest of the Intelligence settings).
For background jobs that have no current user, `resolve_org_offer_ladder` picks the org's
primary intelligence profile (owner -> admin -> any user) so the ladder is still available
org-wide without a schema migration.

The matcher (`match_offer_for_client`) is deterministic: it consumes a client's lifecycle,
ROI tags from call insights, MRR/health, and prospect_voice profile, and returns the best
offer + a short rationale + a script hint shaped by the client's psychology. LLM passes
(prescription, call insight, email drafts) layer on top via `extract_ai_profile_for_llm`,
which now includes the ladder.
"""

from __future__ import annotations

import re
import uuid
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy.orm import Session


# ---------------------------------------------------------------------------
# Schema + validation
# ---------------------------------------------------------------------------

OFFER_LADDER_VERSION = 2

MAX_NAME = 200
MAX_PROMISE = 600
MAX_TEXT = 400
MAX_SHORT = 300
MAX_PRICE = 200
# The former schema allowed five upsells and five downsells. Keep the same
# combined capacity so consolidating legacy profiles cannot silently lose data.
MAX_ITEMS = 10
MAX_TRIGGERS_PER_ITEM = 6
MAX_POSITIONING_NOTES = 5
MAX_OBJECTION_HANDLERS = 8


def _str(v: Any, cap: int) -> str:
    if v is None:
        return ""
    s = str(v).strip()
    if not s:
        return ""
    return s[:cap]


def _str_list(v: Any, *, cap_item: int, cap_list: int) -> List[str]:
    if not isinstance(v, list):
        return []
    out: List[str] = []
    for x in v:
        s = _str(x, cap_item)
        if s:
            out.append(s)
        if len(out) >= cap_list:
            break
    return out


def _validate_offer(raw: Any, *, kind: str) -> Optional[Dict[str, Any]]:
    """Validate one offer entry. Returns None when there's nothing usable."""
    if not isinstance(raw, dict):
        return None
    name = _str(raw.get("name"), MAX_NAME)
    promise = _str(raw.get("promise"), MAX_PROMISE)
    if not name and not promise:
        return None
    out: Dict[str, Any] = {
        "name": name,
        "promise": promise,
        "ideal_for": _str(raw.get("ideal_for"), MAX_TEXT),
        "not_for": _str(raw.get("not_for"), MAX_SHORT),
        "price_terms": _str(raw.get("price_terms"), MAX_PRICE),
    }
    if kind == "upsell":
        out["triggers"] = _str_list(
            raw.get("triggers"),
            cap_item=MAX_NAME,
            cap_list=MAX_TRIGGERS_PER_ITEM,
        )
        out["contraindications"] = _str(raw.get("contraindications"), MAX_SHORT)
    return out


def _legacy_downsell_as_add_on(raw: Any) -> Optional[Dict[str, Any]]:
    """Convert a v1 downsell to the unified upsell/add-on shape."""
    if not isinstance(raw, dict):
        return None
    converted = dict(raw)
    if not converted.get("triggers"):
        when_to_use = _str(converted.get("when_to_use"), MAX_TEXT)
        if when_to_use:
            converted["triggers"] = [when_to_use]
    return _validate_offer(converted, kind="upsell")


def _offer_identity(offer: Dict[str, Any]) -> Tuple[str, str]:
    """Stable, case-insensitive identity used to avoid migration duplicates."""
    return (
        str(offer.get("name") or "").strip().casefold(),
        str(offer.get("promise") or "").strip().casefold(),
    )


def _offer_items(value: Any) -> List[Any]:
    """Reject malformed collections instead of iterating arbitrary user input."""
    return value if isinstance(value, list) else []


def _validate_referral(raw: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(raw, dict):
        return None
    incentive = _str(raw.get("incentive"), MAX_TEXT)
    eligibility = _str(raw.get("eligibility"), MAX_TEXT)
    ask_script = _str(raw.get("ask_script_hints"), MAX_PROMISE)
    if not incentive and not eligibility and not ask_script:
        return None
    return {
        "incentive": incentive,
        "eligibility": eligibility,
        "ask_script_hints": ask_script,
    }


def _validate_objection(raw: Any) -> Optional[Dict[str, str]]:
    if not isinstance(raw, dict):
        return None
    objection = _str(raw.get("objection"), MAX_NAME)
    response = _str(raw.get("response"), MAX_PROMISE)
    if not objection or not response:
        return None
    return {"objection": objection, "response": response}


def validate_offer_ladder(raw: Any) -> Optional[Dict[str, Any]]:
    """
    Normalize a raw offer ladder dict (e.g. straight from the Intelligence form).

    Returns None when the result is empty (so callers can skip injection cleanly).
    """
    if not isinstance(raw, dict):
        return None

    core = _validate_offer(raw.get("core_offer"), kind="core")

    # v2 stores one collection. Read legacy downsells as add-ons so existing
    # organizations retain their offers without exposing two competing models
    # to matching or LLM prompts.
    upsells: List[Dict[str, Any]] = []
    seen = set()
    for item in _offer_items(raw.get("upsells")):
        v = _validate_offer(item, kind="upsell")
        if v and _offer_identity(v) not in seen:
            upsells.append(v)
            seen.add(_offer_identity(v))
        if len(upsells) >= MAX_ITEMS:
            break
    if len(upsells) < MAX_ITEMS:
        for item in _offer_items(raw.get("downsells")):
            v = _legacy_downsell_as_add_on(item)
            if v and _offer_identity(v) not in seen:
                upsells.append(v)
                seen.add(_offer_identity(v))
            if len(upsells) >= MAX_ITEMS:
                break

    referral = _validate_referral(raw.get("referral_offer"))

    positioning = _str_list(
        raw.get("positioning_notes"),
        cap_item=MAX_PROMISE,
        cap_list=MAX_POSITIONING_NOTES,
    )

    handlers: List[Dict[str, str]] = []
    for item in (raw.get("objection_handlers") or [])[:MAX_OBJECTION_HANDLERS]:
        v = _validate_objection(item)
        if v:
            handlers.append(v)

    has_anything = bool(
        core or upsells or referral or positioning or handlers
    )
    if not has_anything:
        return None

    out: Dict[str, Any] = {"version": OFFER_LADDER_VERSION}
    if core:
        out["core_offer"] = core
    if upsells:
        out["upsells"] = upsells
    if referral:
        out["referral_offer"] = referral
    if positioning:
        out["positioning_notes"] = positioning
    if handlers:
        out["objection_handlers"] = handlers
    return out


def extract_offer_ladder(ai_profile: Any) -> Optional[Dict[str, Any]]:
    """Pull a validated `offer_ladder` from a user's `ai_profile`."""
    if not isinstance(ai_profile, dict):
        return None
    raw = ai_profile.get("offer_ladder")
    return validate_offer_ladder(raw)


def resolve_org_offer_ladder(db: Session, org_id: uuid.UUID) -> Optional[Dict[str, Any]]:
    """
    Resolve a single org-wide offer ladder for jobs that have no current user
    (e.g. Fathom call-insight processing).

    Picks the first user with a usable ladder, preferring OWNER -> ADMIN -> MEMBER.
    """
    from app.models.user import User, UserRole

    role_order = {UserRole.OWNER: 0, UserRole.ADMIN: 1, UserRole.MEMBER: 2}
    users = (
        db.query(User)
        .filter(User.org_id == org_id, User.ai_profile.isnot(None))
        .all()
    )
    if not users:
        return None
    users.sort(key=lambda u: (role_order.get(u.role, 99), u.created_at or 0))
    for u in users:
        ladder = extract_offer_ladder(u.ai_profile)
        if ladder:
            return ladder
    return None


# ---------------------------------------------------------------------------
# Compact representation for LLM prompts
# ---------------------------------------------------------------------------


def offer_ladder_for_llm(ladder: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Smaller projection of the ladder for inclusion in LLM payloads."""
    if not ladder:
        return None

    def _flat(o: Dict[str, Any], extra_keys: Tuple[str, ...] = ()) -> Dict[str, Any]:
        d = {
            "name": o.get("name", ""),
            "promise": o.get("promise", ""),
            "ideal_for": o.get("ideal_for", ""),
        }
        for k in extra_keys:
            v = o.get(k)
            if v:
                d[k] = v
        return d

    out: Dict[str, Any] = {"version": ladder.get("version", OFFER_LADDER_VERSION)}
    if ladder.get("core_offer"):
        out["core_offer"] = _flat(ladder["core_offer"], ("price_terms", "not_for"))
    if ladder.get("upsells"):
        out["upsells"] = [
            _flat(o, ("triggers", "contraindications", "price_terms"))
            for o in ladder["upsells"]
        ]
    if ladder.get("referral_offer"):
        out["referral_offer"] = ladder["referral_offer"]
    if ladder.get("positioning_notes"):
        out["positioning_notes"] = ladder["positioning_notes"]
    if ladder.get("objection_handlers"):
        out["objection_handlers"] = ladder["objection_handlers"][:5]
    return out


# ---------------------------------------------------------------------------
# Deterministic offer matcher
# ---------------------------------------------------------------------------

# Tone-shaping cues from prospect_voice profile; safe defaults when missing.
_TONE_CUES = {
    "data": "lead with concrete numbers and the specific outcome they named",
    "story": "open with a brief story that mirrors their own win",
    "casual": "keep it short, conversational, and low-pressure",
    "formal": "be polished and precise; avoid slang",
    "direct": "skip preamble; one clear ask",
    "hesitant": "acknowledge any hesitation and frame the next step as low-risk",
}


def _tone_hint(prospect_voice: Optional[Dict[str, Any]]) -> str:
    if not isinstance(prospect_voice, dict):
        return ""
    notes = prospect_voice.get("tone_notes")
    if not isinstance(notes, list) or not notes:
        return ""
    blob = " ".join(str(n).lower() for n in notes[:6])
    hits = []
    for key, hint in _TONE_CUES.items():
        if key in blob:
            hits.append(hint)
    return " ".join(hits[:2])


def _avoid_hint(prospect_voice: Optional[Dict[str, Any]]) -> str:
    if not isinstance(prospect_voice, dict):
        return ""
    avoid = prospect_voice.get("avoid_phrasing")
    if not isinstance(avoid, list) or not avoid:
        return ""
    sample = ", ".join(str(a) for a in avoid[:3])
    return f"Avoid phrasing they pushed back on: {sample}." if sample else ""


def _mirror_phrase(prospect_voice: Optional[Dict[str, Any]]) -> str:
    if not isinstance(prospect_voice, dict):
        return ""
    phrases = prospect_voice.get("phrases_that_resonated")
    if not isinstance(phrases, list) or not phrases:
        return ""
    p = str(phrases[0]).strip()
    return f'Mirror their own words ("{p[:120]}") in the opener.' if p else ""


_MATCH_STOP_WORDS = frozenset(
    {
        "about",
        "after",
        "before",
        "client",
        "from",
        "into",
        "offer",
        "ready",
        "that",
        "their",
        "they",
        "this",
        "with",
    }
)


def _meaningful_tokens(value: Any) -> set[str]:
    return {
        token
        for token in re.findall(r"[a-z0-9]+", str(value or "").casefold())
        if len(token) >= 4 and token not in _MATCH_STOP_WORDS
    }


def _fit_score(rule: Any, signal_blob: str, signal_tokens: set[str]) -> int:
    text = str(rule or "").strip().casefold()
    if not text:
        return 0
    if text in signal_blob:
        return 4
    overlap = _meaningful_tokens(text) & signal_tokens
    return min(3, len(overlap))


def _contraindication_matches(
    contraindications: Any, signal_blob: str, signal_tokens: set[str]
) -> bool:
    text = str(contraindications or "").strip().casefold()
    if not text:
        return False
    if text in signal_blob:
        return True
    terms = _meaningful_tokens(text)
    if not terms:
        return False
    required_overlap = 1 if len(terms) == 1 else 2
    return len(terms & signal_tokens) >= required_overlap


def select_best_upsell_or_add_on(
    upsells: List[Dict[str, Any]],
    *,
    signals: List[Any],
    has_upsell_signal: bool,
) -> Optional[Dict[str, Any]]:
    """
    Pick the offer that best fits client signals while enforcing guardrails.

    Trigger matches carry the most weight, `ideal_for` is supporting context,
    and any contraindication match excludes the offer. Token overlap makes the
    matcher resilient to small wording differences in call summaries.
    """
    if not upsells:
        return None
    signal_blob = " ".join(str(signal or "") for signal in signals).casefold()
    signal_tokens = _meaningful_tokens(signal_blob)

    best: Optional[Dict[str, Any]] = None
    best_score = -1
    for offer in upsells:
        score = 0
        if _contraindication_matches(
            offer.get("contraindications"), signal_blob, signal_tokens
        ):
            continue
        for trigger in offer.get("triggers") or []:
            score += 3 * _fit_score(trigger, signal_blob, signal_tokens)
        score += _fit_score(offer.get("ideal_for"), signal_blob, signal_tokens)
        if has_upsell_signal and score == 0:
            score = 1
        if score > best_score:
            best_score = score
            best = offer
    return best


def _kind_label(kind: str) -> str:
    return {
        "core": "core offer",
        "upsell": "upsell",
        "referral": "referral offer",
    }.get(kind, kind)


def match_offer_for_client(
    ladder: Optional[Dict[str, Any]],
    *,
    lifecycle: str,
    roi_tags: List[str],
    headline: str = "",
    health_score: Optional[float] = None,
    prospect_voice: Optional[Dict[str, Any]] = None,
    has_testimonial_trigger: bool = False,
) -> Optional[Dict[str, Any]]:
    """
    Pick an offer from the ladder for a client given their behavioral signals.

    Returns: { kind, name, rationale, script_hint } or None when nothing matches.
    """
    if not ladder:
        return None

    ls = (lifecycle or "").lower().strip()
    tags = [str(t).lower().strip() for t in (roi_tags or []) if str(t).strip()]
    core = ladder.get("core_offer")
    upsells = ladder.get("upsells") or []
    referral = ladder.get("referral_offer")

    chosen_kind: Optional[str] = None
    chosen: Optional[Dict[str, Any]] = None
    rationale_bits: List[str] = []

    if "referral" in tags and referral:
        chosen_kind = "referral"
        chosen = {"name": "Referral offer", **referral}
        rationale_bits.append(
            "client is showing referral intent on the call, so prescribe the referral offer directly."
        )
    elif "upsell" in tags and upsells:
        signals: List[Any] = [*tags, headline]
        if isinstance(prospect_voice, dict):
            signals.append(prospect_voice.get("summary_one_liner"))
            signals.extend((prospect_voice.get("phrases_that_resonated") or [])[:6])
        pick = select_best_upsell_or_add_on(
            upsells,
            signals=signals,
            has_upsell_signal=True,
        )
        if pick:
            chosen_kind = "upsell"
            chosen = pick
            triggers = pick.get("triggers") or []
            if triggers:
                rationale_bits.append(
                    f"upsell tag fired and the client signal matches your '{pick.get('name','upsell')}' trigger ({', '.join(triggers[:3])})."
                )
            else:
                rationale_bits.append(
                    f"upsell tag fired and your closest fit is '{pick.get('name','upsell')}'."
                )
    elif "testimonial" in tags:
        if has_testimonial_trigger and referral and ls == "active":
            chosen_kind = "referral"
            chosen = {"name": "Referral offer", **referral}
            rationale_bits.append(
                "testimonial-class win is on record — ride that momentum into a referral ask."
            )
        elif core:
            chosen_kind = "core"
            chosen = core
            rationale_bits.append(
                "testimonial moment surfaced; reinforce the core offer outcome before any expansion ask."
            )
    elif tags and tags[0] in ("conversion", "deal_follow_up"):
        if tags[0] == "deal_follow_up" and core:
            chosen_kind = "core"
            chosen = core
            rationale_bits.append("open deal — keep the core offer front and center on the next touch.")
        elif core:
            chosen_kind = "core"
            chosen = core
            rationale_bits.append("lead is showing buying signal; frame the core offer as the next step.")

    if not chosen and core and ls in ("active", "offboarding"):
        chosen_kind = "core"
        chosen = core
        rationale_bits.append("no specific signal yet — reinforce the core offer outcome.")

    if not chosen:
        return None

    bits: List[str] = []
    mirror = _mirror_phrase(prospect_voice)
    tone = _tone_hint(prospect_voice)
    avoid = _avoid_hint(prospect_voice)
    if mirror:
        bits.append(mirror)
    if tone:
        bits.append(f"Tone: {tone}.")
    if avoid:
        bits.append(avoid)
    if chosen.get("ideal_for"):
        bits.append(f"Anchor on: {chosen['ideal_for']}.")
    if chosen_kind == "upsell" and chosen.get("contraindications"):
        bits.append(f"Skip if: {chosen['contraindications']}.")
    if chosen_kind == "referral" and chosen.get("ask_script_hints"):
        bits.append(chosen["ask_script_hints"])

    return {
        "kind": chosen_kind,
        "kind_label": _kind_label(chosen_kind or ""),
        "name": chosen.get("name") or _kind_label(chosen_kind or ""),
        "promise": chosen.get("promise") or chosen.get("incentive") or "",
        "rationale": " ".join(rationale_bits)[:600],
        "script_hint": " ".join(bits)[:600],
    }

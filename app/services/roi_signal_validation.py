"""Server-side gates for ROI tags (testimonial, upsell, referral) from call insight LLM output."""
from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

# Upsell/referral require a testimonial-class win unless client is in a short "relationship" window.
RECENT_ACTIVE_PROGRAM_DAYS = 30
OFFBOARDING_WINDOW_BEFORE_END_DAYS = 45
OFFBOARDING_WINDOW_AFTER_END_DAYS = 21
# Minimum validated client-win moments accumulated across calls (server-side) to treat as "has wins"
# for expansion tags when testimonial_trigger_at is missing (legacy rows). Set to 2 for stricter policy.
MIN_LIFETIME_WINS_FOR_EXPANSION = 1

LEAD_PIPELINE_LIFECYCLES = frozenset({"cold_lead", "nurturing", "qualified", "booked"})
# Testimonial chip + tag only for paying-client lifecycles (never leads).
TESTIMONIAL_ELIGIBLE_LIFECYCLES = frozenset({"active", "offboarding", "dead"})
EXPANSION_ROI_LIFECYCLES = frozenset({"active", "offboarding"})


def _is_lead_pipeline(lifecycle: str) -> bool:
    return (lifecycle or "").lower().strip() in LEAD_PIPELINE_LIFECYCLES


def _lifecycle_lc(lifecycle: str) -> str:
    return (lifecycle or "").lower().strip()


def _insight_wins_are_substantial(insight: Dict[str, Any]) -> bool:
    for w in insight.get("wins") or []:
        if is_substantial_outcome(str(w), ""):
            return True
    for st in insight.get("testimonial_stories") or []:
        if is_substantial_outcome(str(st), ""):
            return True
    return False


def _check_in_row_progress_signal(row: Dict[str, Any]) -> bool:
    if not isinstance(row, dict):
        return False
    parts: List[str] = []
    title = str(row.get("title") or "").strip()
    if title:
        parts.append(title)
    fields = row.get("booking_fields_excerpt")
    if isinstance(fields, dict):
        parts.extend(str(v) for v in fields.values() if v)
    text = " ".join(parts)
    if not text.strip():
        return False
    return is_substantial_outcome(text, "check_in_progress")


def engagement_win_or_progress_basis(engagement: Optional[Dict[str, Any]]) -> bool:
    """
    True when check-in history or stored roi_state shows the client had a win or sustained progress.
    Used to corroborate LLM testimonial tags (not a substitute for transcript quotes when those exist).
    """
    eng = engagement if isinstance(engagement, dict) else {}
    prior = eng.get("prior_roi") if isinstance(eng.get("prior_roi"), dict) else {}
    if prior.get("testimonial_trigger_at"):
        return True
    try:
        if int(prior.get("lifetime_win_moments_count") or 0) >= MIN_LIFETIME_WINS_FOR_EXPANSION:
            return True
    except (TypeError, ValueError):
        pass

    history = eng.get("check_in_history") if isinstance(eng.get("check_in_history"), list) else []
    progress_hits = 0
    completed_non_sales = 0
    for row in history:
        if not isinstance(row, dict):
            continue
        if row.get("progress_signal"):
            progress_hits += 1
        elif _check_in_row_progress_signal(row):
            progress_hits += 1
        if row.get("completed") and not row.get("is_sales_call") and not row.get("cancelled"):
            completed_non_sales += 1

    if progress_hits > 0:
        return True
    try:
        completed_90 = int(eng.get("completed_non_sales_last_90d") or 0)
    except (TypeError, ValueError):
        completed_90 = completed_non_sales
    if completed_90 >= 2 and progress_hits == 0:
        # Sustained attendance without explicit progress text — still weak; require at least one signal.
        return False
    if completed_90 >= 3:
        return True
    return False


def _dt_aware_utc(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def client_has_testimonial_trigger_in_meta(client: Any) -> bool:
    """True if we previously recorded a validated client win (testimonial trigger)."""
    meta = getattr(client, "meta", None)
    if not isinstance(meta, dict):
        return False
    roi = meta.get("roi_state")
    if not isinstance(roi, dict):
        return False
    return bool(roi.get("testimonial_trigger_at"))


def client_has_expansion_win_basis(client: Any) -> bool:
    """
    True if the system has established program wins for this client: first testimonial trigger,
    or at least MIN_LIFETIME_WINS_FOR_EXPANSION validated moments accumulated in roi_state.
    """
    if client_has_testimonial_trigger_in_meta(client):
        return True
    meta = getattr(client, "meta", None)
    if not isinstance(meta, dict):
        return False
    roi = meta.get("roi_state")
    if not isinstance(roi, dict):
        return False
    try:
        n = int(roi.get("lifetime_win_moments_count") or 0)
    except (TypeError, ValueError):
        n = 0
    return n >= MIN_LIFETIME_WINS_FOR_EXPANSION


def upsell_referral_testimonial_gate_bypass(client: Any, now: Optional[datetime] = None) -> bool:
    """
    When True, upsell/referral may appear without a prior testimonial trigger:
    recently started active program, or in the natural offboarding window near program end.
    """
    now = now or datetime.now(timezone.utc)
    ls_raw = getattr(client, "lifecycle_state", None)
    if hasattr(ls_raw, "value"):
        ls = str(ls_raw.value).lower().strip()
    else:
        ls = str(ls_raw or "").lower().strip()

    if ls == "active":
        start = _dt_aware_utc(getattr(client, "program_start_date", None))
        if start:
            days_since_start = (now - start).total_seconds() / 86400.0
            if 0 <= days_since_start <= float(RECENT_ACTIVE_PROGRAM_DAYS):
                return True
        return False

    if ls == "offboarding":
        end = _dt_aware_utc(getattr(client, "program_end_date", None))
        if end is None:
            st = _dt_aware_utc(getattr(client, "program_start_date", None))
            dur = getattr(client, "program_duration_days", None)
            if st and dur is not None:
                try:
                    end = st + timedelta(days=int(dur))
                except (TypeError, ValueError):
                    end = None
        if end:
            days_to_end = (end - now).total_seconds() / 86400.0
            if -float(OFFBOARDING_WINDOW_AFTER_END_DAYS) <= days_to_end <= float(
                OFFBOARDING_WINDOW_BEFORE_END_DAYS
            ):
                return True
        return False

    return False

_SUBSTANTIAL_PATTERNS = re.compile(
    r"(\d+[\d,]*\.?\d*\s*(%|lbs?|kg|pounds?|dollars?|\$|k\b|m\b))|"
    r"(\$\s*\d+)|"
    r"(\d+\s*(week|month|day)s?)|"
    r"(lost|gained|down|up|saved|earned|made|hit|achieved|pr\b|personal record|goal|first time)",
    re.IGNORECASE,
)


def _norm(s: str) -> str:
    return " ".join(s.lower().split())


def quote_in_transcript(quote: str, transcript: str) -> bool:
    q = (quote or "").strip()
    if len(q) < 8:
        return False
    t = transcript or ""
    if not t:
        return False
    if q in t:
        return True
    return _norm(q) in _norm(t)


def is_substantial_outcome(quote: str, outcome_type: str) -> bool:
    text = f"{quote} {outcome_type or ''}"
    if len((quote or "").strip()) < 12:
        return False
    if _SUBSTANTIAL_PATTERNS.search(text):
        return True
    # Named concrete outcomes without digits
    concrete = (
        "goal weight",
        "goal body",
        "race",
        "marathon",
        "promotion",
        "closed the deal",
        "paid off",
        "debt free",
    )
    low = text.lower()
    return any(c in low for c in concrete)


ROI_TRIO_TAGS = frozenset({"testimonial", "upsell", "referral"})
LEAD_PIPELINE_TAGS = frozenset({"conversion", "deal_follow_up"})


def normalize_display_tags_for_client(
    lifecycle: str,
    pipeline: Optional[Dict[str, Any]],
    stored_tags: Optional[List[str]],
    *,
    testimonial_gate_bypass: Optional[bool] = None,
    has_expansion_win_basis: Optional[bool] = None,
) -> List[str]:
    """
    Reconcile persisted ClientInsightSummary.tags with current lifecycle + calendar pipeline.

    Fixes stale rows (e.g. testimonial/upsell on leads before server gates) and ensures leads always
    get conversion or deal_follow_up from pipeline when applicable.

    For active/offboarding, upsell/referral are stripped unless the client has expansion win basis
    (testimonial trigger or lifetime validated win count), new-active / offboarding-window bypass,
    or legacy rows that still carry a testimonial tag from the summary (same rules as apply_roi_validation).
    When roi_state records wins (expansion basis) but stored tags omit testimonial, prepend testimonial for active/offboarding.
    """
    pipe = pipeline if isinstance(pipeline, dict) else {}
    ls = (lifecycle or "").lower().strip()
    raw = [str(t).lower().strip() for t in (stored_tags or []) if str(t).strip()]
    base: List[str] = []
    seen: Set[str] = set()
    for t in raw:
        if t not in seen:
            seen.add(t)
            base.append(t)

    has_past = bool(pipe.get("has_past_sales_call"))
    open_deal = bool(pipe.get("open_sales_deal"))

    if _is_lead_pipeline(ls):
        out = [
            t
            for t in base
            if t not in ROI_TRIO_TAGS
            and t not in LEAD_PIPELINE_TAGS
            and t not in ("revive", "win_back")
        ]
        if has_past and open_deal:
            out.append("deal_follow_up")
        elif not has_past:
            out.append("conversion")
        return list(dict.fromkeys(out))[:12]

    if ls == "dead":
        rest = [
            t
            for t in base
            if t not in ("upsell", "referral")
            and t not in LEAD_PIPELINE_TAGS
            and t not in ("win_back", "conversion", "deal_follow_up")
        ]
        out = rest[:]
        if "revive" not in out:
            out.append("revive")
        if has_expansion_win_basis and "testimonial" not in out:
            out.insert(0, "testimonial")
        return list(dict.fromkeys(out))[:12]

    # active, offboarding
    out = [
        t
        for t in base
        if t not in LEAD_PIPELINE_TAGS
        and t not in ("revive", "win_back")
        and t not in ("conversion", "deal_follow_up")
    ]
    allow_upsell_referral = True
    if testimonial_gate_bypass is not None and has_expansion_win_basis is not None:
        allow_upsell_referral = bool(
            testimonial_gate_bypass
            or has_expansion_win_basis
            or any(t == "testimonial" for t in base)
        )
    if not allow_upsell_referral:
        out = [t for t in out if t not in ("upsell", "referral")]
    # Wins recorded in roi_state imply a testimonial-class signal for active/offboarding chips.
    if (
        ls in TESTIMONIAL_ELIGIBLE_LIFECYCLES
        and testimonial_gate_bypass is not None
        and has_expansion_win_basis is not None
        and has_expansion_win_basis
        and "testimonial" not in out
    ):
        out.insert(0, "testimonial")
    return list(dict.fromkeys(out))[:12]


def _referral_variant_allowed(variant: Optional[str], lifecycle: str) -> bool:
    if not variant:
        return False
    ls = (lifecycle or "").lower().strip()
    if variant == "new_lead":
        return _is_lead_pipeline(ls)
    if variant == "offboarding":
        return ls == "offboarding"
    if variant == "post_testimonial":
        return ls in ("active", "offboarding")
    return False


def apply_roi_validation(
    insight: Dict[str, Any],
    transcript: str,
    lifecycle: str,
    prior_roi_state: Optional[Dict[str, Any]],
    meeting_at_iso: Optional[str],
    pipeline: Optional[Dict[str, Any]] = None,
    *,
    testimonial_gate_bypass: bool = False,
    engagement: Optional[Dict[str, Any]] = None,
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    Mutates insight: sets opportunity_tags (ROI tags gated), roi_signals validated shape.

    Returns (insight, roi_state_delta) for merging into client.meta["roi_state"].
    """
    pipe = pipeline if isinstance(pipeline, dict) else {}
    ls = _lifecycle_lc(lifecycle)
    expansion_client = ls in EXPANSION_ROI_LIFECYCLES
    testimonial_lifecycle_ok = ls in TESTIMONIAL_ELIGIBLE_LIFECYCLES
    check_in_basis = engagement_win_or_progress_basis(engagement)

    prior = prior_roi_state if isinstance(prior_roi_state, dict) else {}
    prior_testimonial = bool(prior.get("testimonial_trigger_at"))
    try:
        prior_win_moments = int(prior.get("lifetime_win_moments_count") or 0)
    except (TypeError, ValueError):
        prior_win_moments = 0
    llm_tags: List[str] = [str(t).lower() for t in (insight.get("opportunity_tags") or []) if str(t).strip()]

    raw_rs = insight.get("roi_signals")
    if not isinstance(raw_rs, dict):
        raw_rs = {}

    candidates = raw_rs.get("testimonial_candidates") or []
    if not isinstance(candidates, list):
        candidates = []

    validated_moments: List[Dict[str, Any]] = []
    for c in candidates:
        if not isinstance(c, dict):
            continue
        sp = str(c.get("speaker") or "").lower().strip()
        if sp != "client":
            continue
        quote = str(c.get("quote") or "").strip()
        if not quote_in_transcript(quote, transcript):
            continue
        ot = str(c.get("outcome_type") or "")
        if not is_substantial_outcome(quote, ot):
            continue
        validated_moments.append(
            {
                "quote": quote[:800],
                "start_timestamp": str(c.get("start_timestamp") or "")[:64],
                "end_timestamp": (str(c.get("end_timestamp"))[:64] if c.get("end_timestamp") else None),
                "outcome_type": ot[:120],
                "meeting_at": meeting_at_iso,
            }
        )

    this_call_testimonial = len(validated_moments) > 0
    insight_wins_ok = _insight_wins_are_substantial(insight)
    this_call_new_win = this_call_testimonial or insight_wins_ok
    has_first_win_already = prior_testimonial or prior_win_moments >= MIN_LIFETIME_WINS_FOR_EXPANSION
    establishing_first_win = this_call_new_win and not has_first_win_already

    testimonial_triggered = prior_testimonial or this_call_testimonial or insight_wins_ok
    effective_win_for_expansion = (
        testimonial_gate_bypass
        or testimonial_triggered
        or prior_win_moments >= MIN_LIFETIME_WINS_FOR_EXPANSION
    )

    us = raw_rs.get("upsell_signal")
    if not isinstance(us, dict):
        us = {}
    us_active = bool(us.get("active")) or ("upsell" in llm_tags)
    us_future = bool(us.get("future_goal_language"))
    us_quotes_ok = any(quote_in_transcript(str(q), transcript) for q in (us.get("evidence_quotes") or []) if q)
    us_rationale_ok = bool(str(us.get("rationale") or "").strip())
    strong_upsell_evidence = expansion_client and us_active and us_quotes_ok and us_rationale_ok
    # Upsell only after the first win is on record, on a later call with a new win + forward-looking goals.
    progression_upsell = us_active and us_future and (us_quotes_ok or us_rationale_ok)
    upsell_ok = (
        expansion_client
        and not establishing_first_win
        and has_first_win_already
        and this_call_new_win
        and (progression_upsell or (strong_upsell_evidence and us_future))
    )

    rf = raw_rs.get("referral_signal")
    if not isinstance(rf, dict):
        rf = {}
    ref_evidence_quotes_ok = any(
        quote_in_transcript(str(q), transcript) for q in (rf.get("evidence_quotes") or []) if q
    )
    ref_rationale_nonempty = bool(str(rf.get("rationale") or "").strip())
    strong_referral_evidence = expansion_client and ref_evidence_quotes_ok and ref_rationale_nonempty
    # Treat substantiated referral_signal (quotes in transcript + rationale) like upsell: models often
    # describe referral intent in wins/synthesis but leave active=false or use the wrong variant.
    ref_active = (
        bool(rf.get("active"))
        or ("referral" in llm_tags)
        or (
            expansion_client
            and (effective_win_for_expansion or strong_referral_evidence)
            and ref_evidence_quotes_ok
            and ref_rationale_nonempty
        )
    )
    variant = rf.get("variant")
    vs = str(variant).lower().strip() if variant else ""
    if vs not in ("new_lead", "offboarding", "post_testimonial"):
        vs = ""
    if ref_active and not vs:
        # Infer variant from lifecycle when LLM omitted
        ls = (lifecycle or "").lower().strip()
        if ls == "offboarding":
            vs = "offboarding"
        elif _is_lead_pipeline(ls):
            vs = "new_lead"
        elif (
            testimonial_triggered
            or testimonial_gate_bypass
            or prior_win_moments >= MIN_LIFETIME_WINS_FOR_EXPANSION
        ):
            vs = "post_testimonial"
    # new_lead is only valid for lead lifecycles; remap when the model mis-tags active/offboarding clients.
    if expansion_client and effective_win_for_expansion and vs == "new_lead":
        vs = "post_testimonial"
    referral_ok = ref_active and vs and _referral_variant_allowed(vs, lifecycle)
    # Keep ROI hygiene (avoid spam), but do not suppress clear transcript-backed triggers.
    if expansion_client and not effective_win_for_expansion and not strong_referral_evidence:
        referral_ok = False
    if expansion_client and not effective_win_for_expansion and not strong_upsell_evidence:
        upsell_ok = False
    if not expansion_client:
        referral_ok = False
        upsell_ok = False

    validated_moments_out = validated_moments if testimonial_lifecycle_ok else []
    has_transcript_win = len(validated_moments_out) > 0
    llm_wants_testimonial = "testimonial" in llm_tags
    # Testimonial: paying-client lifecycles when this call documents a concrete win (transcript or wins[]).
    testimonial_tag = testimonial_lifecycle_ok and (
        has_transcript_win
        or insight_wins_ok
        or (check_in_basis and llm_wants_testimonial)
    )
    upsell_tag = upsell_ok and expansion_client
    referral_tag = referral_ok and expansion_client

    revive_pb_in = raw_rs.get("revive_playbook") if isinstance(raw_rs.get("revive_playbook"), dict) else {}
    revive_rationale = str(revive_pb_in.get("rationale") or "").strip()
    revive_angles = revive_pb_in.get("offer_angles") if isinstance(revive_pb_in.get("offer_angles"), list) else []
    revive_tag = ls == "dead"

    has_past_sales = bool(pipe.get("has_past_sales_call"))
    open_deal = bool(pipe.get("open_sales_deal"))
    conversion_tag = _is_lead_pipeline(ls) and not has_past_sales
    deal_follow_tag = _is_lead_pipeline(ls) and has_past_sales and open_deal

    new_tags: List[str] = []
    if testimonial_tag:
        new_tags.append("testimonial")
    if expansion_client:
        if upsell_tag:
            new_tags.append("upsell")
        if referral_tag:
            new_tags.append("referral")
    if ls == "dead" and revive_tag:
        new_tags.append("revive")
    if _is_lead_pipeline(ls):
        if deal_follow_tag:
            new_tags.append("deal_follow_up")
        elif conversion_tag:
            new_tags.append("conversion")
    # win_back for non-dead lifecycles only (legacy); dead uses revive
    if ls != "dead" and "win_back" in llm_tags:
        new_tags.append("win_back")

    insight["opportunity_tags"] = list(dict.fromkeys(new_tags))

    upsell_out: Dict[str, Any] = {
        "active": upsell_tag,
        "rationale": str(us.get("rationale") or "")[:800] if upsell_tag else "",
        "evidence_quotes": [str(x)[:400] for x in (us.get("evidence_quotes") or [])[:6]] if upsell_tag else [],
    }
    referral_out: Dict[str, Any] = {
        "active": referral_tag,
        "variant": vs if referral_tag else None,
        "rationale": str(rf.get("rationale") or "")[:800] if referral_tag else "",
        "evidence_quotes": [str(x)[:400] for x in (rf.get("evidence_quotes") or [])[:6]] if referral_tag else [],
    }

    revive_playbook_out: Dict[str, Any] = {"rationale": "", "offer_angles": [], "outreach_hooks": []}
    if ls == "dead" and isinstance(revive_pb_in, dict):
        revive_playbook_out = {
            "rationale": str(revive_pb_in.get("rationale") or "")[:1200],
            "offer_angles": [str(x)[:400] for x in (revive_pb_in.get("offer_angles") or [])[:10] if str(x).strip()],
            "outreach_hooks": [str(x)[:400] for x in (revive_pb_in.get("outreach_hooks") or [])[:10] if str(x).strip()],
        }
        if revive_tag and not str(revive_playbook_out.get("rationale") or "").strip():
            revive_playbook_out["rationale"] = (
                "Client is in Dead — prioritize respectful win-back and re-enrollment paths "
                "grounded in past calls and program context."
            )[:1200]

    insight["roi_signals"] = {
        "testimonial_moments": validated_moments_out[:5],
        "upsell": upsell_out,
        "referral": referral_out,
        "revive_playbook": revive_playbook_out,
    }

    now_iso = datetime.now(timezone.utc).isoformat()
    roi_delta: Dict[str, Any] = {}
    if testimonial_tag and not prior_testimonial:
        roi_delta["testimonial_trigger_at"] = now_iso
        if meeting_at_iso:
            roi_delta["testimonial_trigger_meeting_at"] = meeting_at_iso
        if validated_moments_out:
            roi_delta["testimonial_best_quote"] = validated_moments_out[0].get("quote", "")[:800]
            roi_delta["testimonial_best_timestamp"] = validated_moments_out[0].get("start_timestamp", "")
        elif insight_wins_ok:
            for w in insight.get("wins") or []:
                ws = str(w).strip()
                if is_substantial_outcome(ws, ""):
                    roi_delta["testimonial_best_quote"] = ws[:800]
                    break
            if not roi_delta.get("testimonial_best_quote"):
                for st in insight.get("testimonial_stories") or []:
                    ss = str(st).strip()
                    if is_substantial_outcome(ss, ""):
                        roi_delta["testimonial_best_quote"] = ss[:800]
                        break
    if upsell_tag:
        roi_delta["last_upsell_signal_at"] = now_iso
    if referral_tag:
        roi_delta["last_referral_signal_at"] = now_iso
        roi_delta["last_referral_variant"] = vs
    if testimonial_tag or upsell_tag or referral_tag or revive_tag or conversion_tag or deal_follow_tag:
        roi_delta["last_validated_signals_at"] = now_iso
    if revive_tag:
        roi_delta["last_revive_signal_at"] = now_iso
    if validated_moments_out:
        roi_delta["lifetime_win_moments_increment"] = len(validated_moments_out)
    elif testimonial_tag and insight_wins_ok:
        roi_delta["lifetime_win_moments_increment"] = 1

    return insight, roi_delta


def merge_client_roi_meta(client: Any, roi_delta: Dict[str, Any]) -> None:
    """Merge roi_delta into client.meta['roi_state']. Caller should flag_modified(client, 'meta') if needed."""
    if not roi_delta:
        return
    meta: Dict[str, Any] = dict(client.meta) if isinstance(client.meta, dict) else {}
    roi: Dict[str, Any] = dict(meta.get("roi_state")) if isinstance(meta.get("roi_state"), dict) else {}
    if roi_delta.get("testimonial_trigger_at") and not roi.get("testimonial_trigger_at"):
        roi["testimonial_trigger_at"] = roi_delta["testimonial_trigger_at"]
        if roi_delta.get("testimonial_trigger_meeting_at"):
            roi["testimonial_trigger_meeting_at"] = roi_delta["testimonial_trigger_meeting_at"]
        if roi_delta.get("testimonial_best_quote"):
            roi["testimonial_best_quote"] = roi_delta["testimonial_best_quote"]
        if roi_delta.get("testimonial_best_timestamp"):
            roi["testimonial_best_timestamp"] = roi_delta["testimonial_best_timestamp"]
    for k in (
        "last_upsell_signal_at",
        "last_referral_signal_at",
        "last_referral_variant",
        "last_validated_signals_at",
        "last_revive_signal_at",
    ):
        if roi_delta.get(k) is not None:
            roi[k] = roi_delta[k]
    inc = roi_delta.get("lifetime_win_moments_increment")
    if inc is not None:
        try:
            n = int(inc)
            if n > 0:
                roi["lifetime_win_moments_count"] = int(roi.get("lifetime_win_moments_count") or 0) + n
        except (TypeError, ValueError):
            pass
    meta["roi_state"] = roi
    client.meta = meta

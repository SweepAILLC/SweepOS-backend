"""Messaging Matrix scoring. Pure functions, no DB, no LLM.

Every number in the matrix comes from here. The LLM names angles and explains scores; it
never produces one.

Item scores (0-100) are percentiles within the org, so they compare a post to the org's
own other posts and an ad to its other ads, never across orgs or across channels.
Angle scores shrink the mean item score toward 50 by sample size, so one lucky post
cannot put an angle at the top.
"""
from __future__ import annotations

from typing import Dict, Hashable, Iterable, List, Mapping, Optional, Tuple

NEUTRAL = 50.0
SHRINK_K = 2  # pseudo-samples at NEUTRAL added to every angle
# Pseudo-count added to booked calls / results before dividing spend, so an ad with
# zero conversions still gets a finite cost that grows with what it spent.
CONVERSION_PSEUDO = 0.5
HOLD_WEIGHT = 0.15  # tiebreak share of hold rate in the paid item score
MIN_BOOKED_CALL_ADS = 3  # ads with >= 1 booked call needed before cost per call is used


def percentiles(values: Mapping[Hashable, float], *, higher_is_better: bool = True) -> Dict[Hashable, float]:
    """Mid-rank percentile (0-100) of each value among all values. Ties share a score.
    A single value scores NEUTRAL: there is nothing to compare it with."""
    n = len(values)
    if n == 0:
        return {}
    if n == 1:
        return {k: NEUTRAL for k in values}
    ordered = sorted(values.values())
    out: Dict[Hashable, float] = {}
    for key, v in values.items():
        below = sum(1 for x in ordered if x < v)
        equal = sum(1 for x in ordered if x == v)
        pct = (below + (equal - 1) / 2) / (n - 1) * 100
        out[key] = round(pct if higher_is_better else 100 - pct, 2)
    return out


def shrunk_mean(scores: Iterable[float], *, k: int = SHRINK_K) -> Optional[float]:
    """(sum + k * 50) / (n + k). None when there are no scores."""
    vals = [float(s) for s in scores if s is not None]
    if not vals:
        return None
    return round((sum(vals) + k * NEUTRAL) / (len(vals) + k), 2)


def paid_cost_mode(ads: Mapping[Hashable, Mapping[str, float]]) -> str:
    """'booked_call' when enough ads have attributed booked calls, else 'result'."""
    with_calls = sum(1 for m in ads.values() if (m.get("booked_calls") or 0) > 0)
    return "booked_call" if with_calls >= MIN_BOOKED_CALL_ADS else "result"


def paid_item_scores(ads: Mapping[Hashable, Mapping[str, float]]) -> Tuple[Dict[Hashable, float], str]:
    """Paid item scores for ads over the spend floor.

    Each ad mapping holds spend_cents, booked_calls, results, thruplays, video_3s_views.
    Primary signal: cost per booked call (or per result, see paid_cost_mode), lower is
    better. Hold rate (thruplays / 3s views) breaks near-ties at HOLD_WEIGHT.
    """
    if not ads:
        return {}, "result"
    mode = paid_cost_mode(ads)
    conv_key = "booked_calls" if mode == "booked_call" else "results"
    cost = {
        k: float(m.get("spend_cents") or 0) / ((m.get(conv_key) or 0) + CONVERSION_PSEUDO)
        for k, m in ads.items()
    }
    cost_pct = percentiles(cost, higher_is_better=False)
    hold = {
        k: float(m["thruplays"]) / float(m["video_3s_views"])
        for k, m in ads.items()
        if m.get("video_3s_views") and m.get("thruplays") is not None
    }
    hold_pct = percentiles(hold) if len(hold) >= 2 else {}
    out: Dict[Hashable, float] = {}
    for k in ads:
        if k in hold_pct:
            out[k] = round(cost_pct[k] * (1 - HOLD_WEIGHT) + hold_pct[k] * HOLD_WEIGHT, 2)
        else:
            out[k] = cost_pct[k]
    return out, mode


def angle_scores(organic: List[float], paid: List[float]) -> Dict[str, Optional[float]]:
    """Channel scores and a blended overall score weighted by sample count."""
    o = shrunk_mean(organic)
    p = shrunk_mean(paid)
    if o is None and p is None:
        overall = None
    elif o is None:
        overall = p
    elif p is None:
        overall = o
    else:
        overall = round((o * len(organic) + p * len(paid)) / (len(organic) + len(paid)), 2)
    return {
        "organic_score": o,
        "paid_score": p,
        "overall_score": overall,
        "organic_samples": len(organic),
        "paid_samples": len(paid),
    }

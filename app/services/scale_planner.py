"""Scale Planner math: break-even ROAS, profit vs spend, and scale readiness. No LLM.

The rules come from paid-ads practice for high-ticket coaching offers:
- Judge spend by absolute monthly profit, not ROAS. ROAS falls as spend grows,
  so model it as ROAS(S) = ROAS0 * (S / S0) ** -decay (default decay 0.5: tripling
  spend takes a 3.5x to about a 2x).
- Break-even ROAS includes fixed costs, so it depends on spend:
  profit = S * (ROAS * (1 - v) - 1) - F   =>   break-even = (1 + F / S) / (1 - v).
- How hard to push depends on the gap above break-even and on the cushion:
  cash reserves vs months of fixed + living costs. A thin gap or a short runway
  means cut; a wide gap with a cushion means a "bulk month" is affordable.
- The worst case for a push is ROAS halving (or falling to the worst week seen).

The frontend mirrors these formulas in lib/scalePlanner.ts for live sliders; keep
the two in step (tests/test_scale_planner.py pins the numbers).
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date, timedelta
from statistics import mean, pstdev
from typing import Any, Dict, List, Optional, Sequence

DEFAULT_DECAY = 0.5
# Worst week assumed when there is no history: ROAS halves on a push.
DEFAULT_WORST_RATIO = 0.5
# Months of fixed + living costs to keep in reserve before a push, by risk tolerance.
FLOOR_MONTHS = {"conservative": 9.0, "balanced": 6.0, "aggressive": 3.0}
# Largest month-over-month spend step the verdict will suggest, by risk tolerance.
MAX_STEP = {"conservative": 1.5, "balanced": 2.0, "aggressive": 4.0}
# Spend multiples of current spend the recommendation searches.
SPEND_MULTIPLES = [0.25, 0.5, 0.75, 1.0, 1.25, 1.5, 2.0, 2.5, 3.0, 4.0]
RISK_LEVELS = set(FLOOR_MONTHS)


def _frac(pct: Optional[float]) -> float:
    if pct is None:
        return 0.0
    return max(0.0, min(float(pct), 99.0)) / 100.0


def break_even_roas(monthly_spend: float, fixed_monthly: float, variable_pct: Optional[float]) -> Optional[float]:
    """ROAS at which this month's profit is exactly zero, at this spend."""
    v = _frac(variable_pct)
    if monthly_spend <= 0 or v >= 1:
        return None
    return (1 + max(fixed_monthly, 0.0) / monthly_spend) / (1 - v)


def marginal_break_even_roas(variable_pct: Optional[float]) -> float:
    """ROAS at which the next ad dollar pays for itself (fixed costs already covered)."""
    return 1 / (1 - _frac(variable_pct))


def monthly_profit(
    monthly_spend: float,
    roas: float,
    fixed_monthly: float,
    variable_pct: Optional[float],
    recurring_cash: float = 0.0,
) -> float:
    v = _frac(variable_pct)
    new_cash = monthly_spend * roas
    return (new_cash + recurring_cash) * (1 - v) - monthly_spend - fixed_monthly


def roas_at_spend(spend: float, base_spend: float, base_roas: float, decay: Optional[float]) -> float:
    if spend <= 0 or base_spend <= 0:
        return base_roas
    d = DEFAULT_DECAY if decay is None else max(0.0, float(decay))
    return base_roas * (spend / base_spend) ** (-d)


def gap_zone(roas: Optional[float], breakeven: Optional[float]) -> Optional[Dict[str, Any]]:
    """How far current ROAS sits above break-even, as a fraction of break-even."""
    if roas is None or breakeven is None or breakeven <= 0:
        return None
    gap = (roas - breakeven) / breakeven
    if gap < 0:
        zone = "losing"
    elif gap < 0.10:
        zone = "danger"
    elif gap < 0.50:
        zone = "hold"
    else:
        zone = "push"
    return {"gap_pct": round(gap * 100, 1), "zone": zone}


def volatility(values: Sequence[Optional[float]]) -> Optional[float]:
    """Coefficient of variation (stdev / mean) of the non-empty values, as a percent."""
    clean = [float(v) for v in values if v is not None and v > 0]
    if len(clean) < 2:
        return None
    m = mean(clean)
    if m <= 0:
        return None
    return round(pstdev(clean) / m * 100, 1)


@dataclass
class Economics:
    fixed_monthly: float = 0.0
    variable_pct: Optional[float] = None
    include_living_in_breakeven: bool = False
    cash_reserves: Optional[float] = None
    tax_reserve: Optional[float] = None
    living_expenses: Optional[float] = None
    risk_tolerance: str = "balanced"
    roas_decay: Optional[float] = None
    recurring_cash: float = 0.0

    @property
    def breakeven_fixed(self) -> float:
        extra = (self.living_expenses or 0.0) if self.include_living_in_breakeven else 0.0
        return (self.fixed_monthly or 0.0) + extra


def readiness(
    econ: Economics,
    *,
    monthly_spend: float,
    current_roas: Optional[float],
    worst_week_roas: Optional[float] = None,
) -> Dict[str, Any]:
    """Bulk / hold / cut verdict and a recommended monthly spend.

    Picks the spend (among SPEND_MULTIPLES of current, capped by the risk tolerance's
    max step) with the highest expected profit whose worst-case month loses no more
    than the cushion above the reserve floor. Worst case = ROAS at that spend times
    worst_week_roas / current_roas (or halved without history).
    """
    risk = econ.risk_tolerance if econ.risk_tolerance in RISK_LEVELS else "balanced"
    fixed = econ.breakeven_fixed
    burn = (econ.fixed_monthly or 0.0) + (econ.living_expenses or 0.0)
    cushion = None
    if econ.cash_reserves is not None or econ.tax_reserve is not None:
        cushion = (econ.cash_reserves or 0.0) + (econ.tax_reserve or 0.0)
    runway = (cushion / burn) if (cushion is not None and burn > 0) else None
    floor = FLOOR_MONTHS[risk]
    max_safe_loss = max(0.0, cushion - floor * burn) if cushion is not None else None

    be = break_even_roas(monthly_spend, fixed, econ.variable_pct)
    zone = gap_zone(current_roas, be)
    out: Dict[str, Any] = {
        "risk_tolerance": risk,
        "monthly_spend": round(monthly_spend, 2),
        "current_roas": None if current_roas is None else round(current_roas, 2),
        "worst_week_roas": None if worst_week_roas is None else round(worst_week_roas, 2),
        "breakeven_roas": None if be is None else round(be, 2),
        "marginal_breakeven_roas": round(marginal_break_even_roas(econ.variable_pct), 2),
        "gap": zone,
        "monthly_burn": round(burn, 2),
        "cushion": None if cushion is None else round(cushion, 2),
        "runway_months": None if runway is None else round(runway, 1),
        "reserve_floor_months": floor,
        "max_safe_loss": None if max_safe_loss is None else round(max_safe_loss, 2),
        "current_profit": None,
        "recommended_monthly_spend": None,
        "recommended_daily_spend": None,
        "recommended_expected_profit": None,
        "recommended_worst_profit": None,
        "verdict": "unknown",
        "reasons": [],
    }
    reasons: List[str] = out["reasons"]

    if monthly_spend <= 0 or current_roas is None:
        reasons.append("No paid spend with cash in the last 4 weeks, so there is no ROAS to plan from.")
        return out

    out["current_profit"] = round(
        monthly_profit(monthly_spend, current_roas, fixed, econ.variable_pct, econ.recurring_cash), 2
    )
    worst_ratio = DEFAULT_WORST_RATIO
    if worst_week_roas is not None and current_roas > 0:
        worst_ratio = max(0.0, min(1.0, worst_week_roas / current_roas))

    max_mult = MAX_STEP[risk]
    best = None
    for mult in SPEND_MULTIPLES:
        if mult > max_mult:
            continue
        s = monthly_spend * mult
        r = roas_at_spend(s, monthly_spend, current_roas, econ.roas_decay)
        expected = monthly_profit(s, r, fixed, econ.variable_pct, econ.recurring_cash)
        worst = monthly_profit(s, r * worst_ratio, fixed, econ.variable_pct, econ.recurring_cash)
        if max_safe_loss is not None and worst < 0 and -worst > max_safe_loss and mult > 1.0:
            continue
        if best is None or expected > best[1]:
            best = (s, expected, worst)

    if best is not None:
        out["recommended_monthly_spend"] = round(best[0], 2)
        out["recommended_daily_spend"] = round(best[0] / 30, 2)
        out["recommended_expected_profit"] = round(best[1], 2)
        out["recommended_worst_profit"] = round(best[2], 2)

    gap_pct = zone["gap_pct"] / 100 if zone else None
    if gap_pct is not None and gap_pct < 0.10:
        out["verdict"] = "cut"
        reasons.append(
            f"ROAS {current_roas:.2f}x is within 10% of break-even {be:.2f}x. Cut spend or costs until the gap widens."
        )
    elif runway is not None and runway < 3:
        out["verdict"] = "cut"
        reasons.append(f"Only {runway:.1f} months of costs in reserve. Stack cash before pushing spend.")
    elif runway is not None and runway < floor:
        out["verdict"] = "hold"
        reasons.append(
            f"{runway:.1f} months of reserves is under the {floor:.0f}-month floor for a {risk} plan. "
            "Hold spend and build the cushion."
        )
    elif gap_pct is not None and gap_pct < 0.50:
        out["verdict"] = "hold"
        reasons.append(f"ROAS is {gap_pct * 100:.0f}% above break-even. Keep spend steady and improve efficiency.")
    else:
        out["verdict"] = "bulk"
        if gap_pct is not None:
            reasons.append(f"ROAS is {gap_pct * 100:.0f}% above break-even, a wide margin to push into.")
        if runway is not None:
            reasons.append(f"{runway:.1f} months of reserves covers a bad month.")
        else:
            reasons.append("Add cash reserves in Business economics to size the push against a worst case.")
        reasons.append(
            "Raise spend and watch cost per booked call for 3-7 days. If it rises as fast as spend, roll back."
        )
    return out


def complete_weeks(today: date, count: int) -> List[date]:
    """Mondays of the last `count` complete Mon-Sun weeks before today."""
    this_monday = today - timedelta(days=today.weekday())
    return [this_monday - timedelta(weeks=i) for i in range(count, 0, -1)]


def trailing_summary(weekly: Dict[str, List[Optional[float]]]) -> Dict[str, Any]:
    """Monthly-ized spend/cash/ROAS from the last 4 weeks, worst week ROAS over all weeks given,
    and cost-per-booked-call volatility. `weekly` is weekly_metric_values output (oldest first)."""
    spend = [v or 0.0 for v in weekly.get("ad_spend", [])]
    cash = [v or 0.0 for v in weekly.get("cash_collected", [])]
    last_spend = sum(spend[-4:])
    last_cash = sum(cash[-4:])
    monthly_spend = last_spend * 30 / 28
    roas = (last_cash / last_spend) if last_spend > 0 else None
    week_roas = [c / s for s, c in zip(spend, cash) if s > 0]
    worst = min(week_roas) if len(week_roas) >= 2 else None
    cpb = weekly.get("cost_per_booking", [])
    booked = sum(v or 0.0 for v in weekly.get("booked_calls", [])[-4:])
    leads = sum(v or 0.0 for v in weekly.get("leads", [])[-4:])
    return {
        "booked_calls": int(booked),
        "leads": int(leads),
        "cash_per_booking": round(last_cash / booked, 2) if booked > 0 else None,
        "lead_to_booking_rate": round(booked / leads, 4) if leads > 0 else None,
        "monthly_spend": round(monthly_spend, 2),
        "monthly_cash": round(last_cash * 30 / 28, 2),
        "roas": None if roas is None or math.isnan(roas) else round(roas, 4),
        "worst_week_roas": None if worst is None else round(worst, 4),
        "cost_per_booking_volatility_pct": volatility(cpb),
        "weeks_with_spend": len(week_roas),
    }


def suggested_ad_targets(
    trailing: Dict[str, Any],
    breakeven_roas: Optional[float],
    marginal_breakeven_roas: Optional[float],
) -> Dict[str, Any]:
    """Ads tab targets implied by the business numbers.

    min_roas: the business break-even ROAS at current spend (covers ad spend, variable and
    fixed costs). marginal_breakeven_roas: below it every extra ad dollar loses money.
    target cost per booked call: what a booked call is worth (trailing cash / bookings)
    divided by break-even ROAS; per lead: that times the lead-to-booking rate.
    Values are None when the trailing window lacks the data.
    """
    cash_per_booking = trailing.get("cash_per_booking")
    rate = trailing.get("lead_to_booking_rate")
    per_booking = (
        cash_per_booking / breakeven_roas if cash_per_booking and breakeven_roas and breakeven_roas > 0 else None
    )
    max_per_booking = (
        cash_per_booking / marginal_breakeven_roas
        if cash_per_booking and marginal_breakeven_roas and marginal_breakeven_roas > 0
        else None
    )
    return {
        "min_roas": None if breakeven_roas is None else round(breakeven_roas, 2),
        "marginal_breakeven_roas": None if marginal_breakeven_roas is None else round(marginal_breakeven_roas, 2),
        "cash_per_booking": cash_per_booking,
        "target_cost_per_booking": None if per_booking is None else round(per_booking, 2),
        "max_cost_per_booking": None if max_per_booking is None else round(max_per_booking, 2),
        "lead_to_booking_rate": rate,
        "target_cost_per_lead": None if per_booking is None or not rate else round(per_booking * rate, 2),
        "booked_calls": trailing.get("booked_calls"),
    }

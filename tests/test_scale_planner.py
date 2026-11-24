"""Scale Planner math, readiness verdicts, and who may read business economics. No database."""
import uuid
from datetime import date
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from app.api import business_economics as be
from app.api import resources
from app.services import scale_planner as sp


def test_break_even_includes_fixed_costs_and_variable_share():
    # $10k spend, $12k fixed, 20% variable: (1 + 1.2) / 0.8 = 2.75x
    assert sp.break_even_roas(10_000, 12_000, 20) == pytest.approx(2.75)
    # Fixed costs matter less at higher spend.
    assert sp.break_even_roas(40_000, 12_000, 20) == pytest.approx(1.625)
    assert sp.marginal_break_even_roas(20) == pytest.approx(1.25)
    assert sp.break_even_roas(0, 12_000, 20) is None


def test_profit_is_zero_at_break_even():
    roas = sp.break_even_roas(10_000, 12_000, 20)
    assert sp.monthly_profit(10_000, roas, 12_000, 20) == pytest.approx(0, abs=1e-6)


def test_recurring_cash_counts_toward_profit():
    base = sp.monthly_profit(2_000, 3.0, 10_000, 0)
    assert sp.monthly_profit(2_000, 3.0, 10_000, 0, recurring_cash=18_000) == pytest.approx(base + 18_000)


def test_roas_decays_with_spend():
    # Tripling spend at decay 0.5 roughly takes 3.5x to ~2x.
    assert sp.roas_at_spend(30_000, 10_000, 3.5, 0.5) == pytest.approx(3.5 / 3 ** 0.5)
    assert sp.roas_at_spend(10_000, 10_000, 3.5, None) == pytest.approx(3.5)


def test_gap_zones():
    assert sp.gap_zone(2.0, 2.2)["zone"] == "losing"
    assert sp.gap_zone(2.3, 2.2)["zone"] == "danger"
    assert sp.gap_zone(3.0, 2.2)["zone"] == "hold"
    assert sp.gap_zone(4.0, 2.0)["zone"] == "push"
    assert sp.gap_zone(None, 2.0) is None


def test_volatility_needs_two_points():
    assert sp.volatility([100, None]) is None
    assert sp.volatility([100, 100, 100]) == 0.0
    assert sp.volatility([100, 300]) == pytest.approx(50.0)


def test_thin_gap_says_cut():
    econ = sp.Economics(fixed_monthly=10_000, variable_pct=10, cash_reserves=500_000)
    # be = (1 + 10k/10k) / 0.9 = 2.22; roas 2.3 is within 10%
    out = sp.readiness(econ, monthly_spend=10_000, current_roas=2.3)
    assert out["verdict"] == "cut"


def test_short_runway_holds_even_with_wide_gap():
    # Wide gap but 4 months of runway < balanced 6-month floor.
    econ = sp.Economics(fixed_monthly=5_000, variable_pct=10, cash_reserves=20_000, living_expenses=0)
    out = sp.readiness(econ, monthly_spend=2_000, current_roas=10.0)
    assert out["runway_months"] == 4.0
    assert out["verdict"] == "hold"


def test_young_operator_with_cushion_gets_bulk_and_a_bigger_budget():
    # Roughly the call's example: ~$1.5k/mo spend, strong return, ~$30k cushion.
    econ = sp.Economics(
        fixed_monthly=4_000,
        variable_pct=10,
        cash_reserves=8_000,
        tax_reserve=22_000,
        living_expenses=0,
        risk_tolerance="aggressive",
    )
    out = sp.readiness(econ, monthly_spend=1_500, current_roas=10.0)
    assert out["verdict"] == "bulk"
    assert out["recommended_monthly_spend"] > 1_500
    assert out["recommended_monthly_spend"] <= 1_500 * sp.MAX_STEP["aggressive"]


def test_recommendation_respects_worst_case_cushion():
    # Tiny cushion above the floor: any push whose worst month loses more than it is skipped.
    econ = sp.Economics(fixed_monthly=10_000, variable_pct=0, cash_reserves=61_000)
    out = sp.readiness(econ, monthly_spend=10_000, current_roas=3.0, worst_week_roas=1.0)
    assert out["max_safe_loss"] == pytest.approx(1_000)
    assert out["recommended_monthly_spend"] <= 10_000


def test_no_spend_is_unknown():
    out = sp.readiness(sp.Economics(), monthly_spend=0, current_roas=None)
    assert out["verdict"] == "unknown"
    assert out["reasons"]


def test_living_costs_only_in_break_even_when_asked():
    on = sp.Economics(fixed_monthly=1_000, living_expenses=5_000, include_living_in_breakeven=True)
    off = sp.Economics(fixed_monthly=1_000, living_expenses=5_000)
    assert on.breakeven_fixed == 6_000
    assert off.breakeven_fixed == 1_000


def test_trailing_summary_monthlyizes_last_four_weeks():
    weekly = {
        "ad_spend": [None, 1000, 1000, 1000, 1000, 1000, 1000, 1000],
        "cash_collected": [0, 500, 4000, 3000, 3000, 3000, 3000, 3000],
        "cost_per_booking": [None, 100, 120, 80, 100, 100, 100, 100],
    }
    out = sp.trailing_summary(weekly)
    assert out["monthly_spend"] == pytest.approx(4000 * 30 / 28, rel=1e-3)
    assert out["roas"] == pytest.approx(3.0)
    assert out["worst_week_roas"] == pytest.approx(0.5)
    assert out["weeks_with_spend"] == 7


def test_complete_weeks_are_mondays_before_this_week():
    weeks = sp.complete_weeks(date(2026, 11, 23), 2)  # a Monday
    assert weeks == [date(2026, 11, 9), date(2026, 11, 16)]


# --- who may read business economics / preview an org -------------------------------------


class _FakeDb:
    def __init__(self, first=None):
        self._first = first

    def query(self, *_a, **_k):
        return self

    def filter(self, *_a, **_k):
        return self

    def first(self):
        return self._first


def _user():
    org = uuid.uuid4()
    return SimpleNamespace(id=uuid.uuid4(), org_id=org, selected_org_id=org, role="MEMBER", email="m@example.com")


def test_members_cannot_read_economics(monkeypatch):
    monkeypatch.setattr(be, "user_can_manage_org_integrations", lambda u, db: False)
    with pytest.raises(HTTPException) as exc:
        be.resolve_org(_FakeDb(), _user(), None)
    assert exc.value.status_code == 403


def test_admins_read_their_selected_org(monkeypatch):
    monkeypatch.setattr(be, "user_can_manage_org_integrations", lambda u, db: True)
    user = _user()
    assert be.resolve_org(_FakeDb(), user, None) == user.selected_org_id


def test_as_org_id_needs_system_owner(monkeypatch):
    monkeypatch.setattr(be, "user_is_system_owner", lambda u, db: False)
    with pytest.raises(HTTPException) as exc:
        be.resolve_org(_FakeDb(first=("x",)), _user(), uuid.uuid4())
    assert exc.value.status_code == 403


def test_system_owner_can_view_another_org(monkeypatch):
    monkeypatch.setattr(be, "user_is_system_owner", lambda u, db: True)
    target = uuid.uuid4()
    assert be.resolve_org(_FakeDb(first=(target,)), _user(), target) == target


def test_system_owner_unknown_org_is_404(monkeypatch):
    monkeypatch.setattr(be, "user_is_system_owner", lambda u, db: True)
    with pytest.raises(HTTPException) as exc:
        be.resolve_org(_FakeDb(first=None), _user(), uuid.uuid4())
    assert exc.value.status_code == 404


def test_serialize_decrypts_reserves(monkeypatch):
    monkeypatch.setattr(be, "encrypt_token", lambda v: f"enc:{v}")
    monkeypatch.setattr(be, "decrypt_token", lambda v, audit_context=None: v.split(":", 1)[1])
    row = SimpleNamespace(
        fixed_monthly_costs_cents=1_500_000,
        variable_cost_pct=15.0,
        include_living_in_breakeven=False,
        cash_reserves_enc=be._enc(40_000),
        tax_reserve_enc=None,
        living_expenses_enc=be._enc(6_000),
        risk_tolerance="balanced",
        target_cost_per_call_cents=20_000,
        roas_decay=None,
        monthly_recurring_cash_cents=None,
        updated_at=None,
    )
    out = be.serialize(row, uuid.uuid4())
    assert out["fixed_monthly_costs"] == 15_000
    assert out["cash_reserves"] == 40_000
    assert out["living_expenses"] == 6_000
    assert out["tax_reserve"] is None
    assert out["target_cost_per_call"] == 200


def test_reserves_never_stored_in_plaintext(monkeypatch):
    monkeypatch.setattr(be, "encrypt_token", lambda v: "v1:opaque")
    assert "40000" not in be._enc(40_000)


def test_sop_preview_as_org_needs_system_owner(monkeypatch):
    monkeypatch.setattr(resources, "user_is_system_owner", lambda u, db: False)
    with pytest.raises(HTTPException):
        resources._view_org_id(_FakeDb(), _user(), uuid.uuid4())


def test_sop_preview_uses_target_org_for_system_owner(monkeypatch):
    monkeypatch.setattr(resources, "user_is_system_owner", lambda u, db: True)
    target = uuid.uuid4()
    assert resources._view_org_id(_FakeDb(), _user(), target) == str(target)


def test_sop_without_preview_uses_own_org():
    user = _user()
    assert resources._view_org_id(_FakeDb(), user, None) == str(user.selected_org_id)


def test_funnel_slice_allocates_fixed_costs_by_spend_share():
    econ = sp.Economics(fixed_monthly=10_000, variable_pct=20)
    org = {"monthly_spend": 20_000}
    funnel = {"monthly_spend": 5_000, "roas": 3.0}
    out = be.funnel_slice(org, funnel, econ)
    assert out["spend_share_pct"] == 25.0
    assert out["allocated_fixed_monthly"] == 2_500
    # (1 + 2500/5000) / 0.8 = 1.875
    assert out["breakeven_roas"] == pytest.approx(1.88, abs=0.01)
    assert out["gap"]["zone"] == "push"


def test_funnel_slice_without_org_spend_allocates_nothing():
    out = be.funnel_slice({"monthly_spend": 0}, {"monthly_spend": 0, "roas": None}, sp.Economics(fixed_monthly=5_000))
    assert out["allocated_fixed_monthly"] == 0
    assert out["breakeven_roas"] is None

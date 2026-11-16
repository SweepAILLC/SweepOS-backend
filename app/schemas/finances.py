from pydantic import BaseModel, Field
from typing import List, Optional


class FinancesSourceSlice(BaseModel):
    """Cash-like totals in dollars for one processor."""

    last_30_days_revenue: float = 0.0
    last_mtd_revenue: float = 0.0
    last_30_days_order_count: int = 0
    last_mtd_order_count: int = 0


class FinancesCombinedSummary(BaseModel):
    stripe_connected: bool = False
    whop_connected: bool = False
    combined: FinancesSourceSlice
    stripe: FinancesSourceSlice
    whop: FinancesSourceSlice
    manual: FinancesSourceSlice = Field(default_factory=FinancesSourceSlice)
    # Prior window of equal length (for KPI trend badges); omitted when scope=all.
    prior_period_revenue: Optional[float] = None
    prior_period_order_count: Optional[int] = None
    # AOV is computed from first-time payments only (one per client, their earliest
    # ever charge) so renewals/upsells don't dilute it. Separate from `combined`,
    # which still reports every payment.
    aov_first_time_revenue: float = 0.0
    aov_first_time_order_count: int = 0
    aov_mtd_first_time_revenue: float = 0.0
    aov_mtd_first_time_order_count: int = 0
    prior_period_aov_first_time_revenue: Optional[float] = None
    prior_period_aov_first_time_order_count: Optional[int] = None


class FinancesTimelinePoint(BaseModel):
    date: str
    stripe_revenue: float = 0.0
    whop_revenue: float = 0.0
    manual_revenue: float = 0.0
    total_revenue: float = 0.0


class FinancesRevenueTimelineResponse(BaseModel):
    timeline: List[FinancesTimelinePoint]
    group_by: str = "day"

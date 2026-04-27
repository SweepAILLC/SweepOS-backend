from pydantic import BaseModel, Field
from typing import List, Optional


class FinancesSourceSlice(BaseModel):
    """Cash-like totals in dollars for one processor."""

    last_30_days_revenue: float = 0.0
    last_mtd_revenue: float = 0.0


class FinancesCombinedSummary(BaseModel):
    stripe_connected: bool = False
    whop_connected: bool = False
    combined: FinancesSourceSlice
    stripe: FinancesSourceSlice
    whop: FinancesSourceSlice


class FinancesTimelinePoint(BaseModel):
    date: str
    stripe_revenue: float = 0.0
    whop_revenue: float = 0.0
    total_revenue: float = 0.0


class FinancesRevenueTimelineResponse(BaseModel):
    timeline: List[FinancesTimelinePoint]
    group_by: str = "day"

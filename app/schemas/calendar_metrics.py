from pydantic import BaseModel
from typing import List, Optional

from app.schemas.admin import HealthTrendPeriod


class CalendarMonthlyRateRow(BaseModel):
    """Monthly sales-call show-up % and sales-close % for an org's calendar tab."""

    period_label: str
    period_start: str
    period_end: str
    show_up_rate_pct: Optional[float] = None
    close_rate_pct: Optional[float] = None


class CalendarMonthlyCoachingResponse(BaseModel):
    periods: List[CalendarMonthlyRateRow]


class TerminalMonthlyTrendsResponse(BaseModel):
    """Monthly org health trends for the unified Terminal dashboard."""

    periods: List[HealthTrendPeriod]


class CalendarTrendSummaryResponse(BaseModel):
    """Scoped calendar KPI summary (show-up / close rate, meeting counts)."""

    upcoming_count: int = 0
    past_count: int = 0
    close_rate_pct: Optional[float] = None
    sales_calls_in_range: int = 0
    closed_sales_count: int = 0
    show_up_rate_pct: Optional[float] = None
    attendance_eligible_past: int = 0
    showed_up_count: int = 0

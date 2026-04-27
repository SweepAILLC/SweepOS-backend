from pydantic import BaseModel
from typing import List, Optional


class CalendarMonthlyRateRow(BaseModel):
    """Monthly show-up and sales-close rates for an org's calendar tab."""

    period_label: str
    period_start: str
    period_end: str
    show_up_rate_pct: Optional[float] = None
    close_rate_pct: Optional[float] = None


class CalendarMonthlyCoachingResponse(BaseModel):
    periods: List[CalendarMonthlyRateRow]

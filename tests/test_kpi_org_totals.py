"""One daily ledger: activity = org + per-rep EODs; calendar/cash/followers = org row only."""
import uuid
from datetime import date, datetime
from types import SimpleNamespace

from app.services.kpi_org_totals import fold_org_daily_totals

ORG = uuid.uuid4()
D1, D2 = date(2026, 9, 21), date(2026, 9, 22)


def _row(rep=None, day=D1, **fields):
    base = dict(id=uuid.uuid4(), org_id=ORG, entry_date=day, rep_user_id=rep,
                created_at=datetime(2026, 9, 21, 9), updated_at=datetime(2026, 9, 21, 9))
    base.update(fields)
    return SimpleNamespace(**base)


def test_activity_sums_org_and_reps_calendar_is_org_only():
    org = _row(outreach_sent=10, respondents=None, calls_booked=4, calls_taken=3, closes=1, cash_collected=500, total_followers=900)
    setter = _row(rep=uuid.uuid4(), outreach_sent=40, respondents=6)
    closer_host = _row(rep=uuid.uuid4(), calls_booked=2, calls_taken=2, closes=1)  # calendar-host row
    [day] = fold_org_daily_totals([setter, org, closer_host])
    assert (day.outreach_sent, day.respondents) == (50, 6)  # org 10 + setter 40; org None + setter 6
    assert (day.calls_booked, day.calls_taken, day.closes) == (4, 3, 1)  # not doubled by the host row
    assert (day.cash_collected, day.total_followers) == (500, 900)
    assert day.team_eod_totals == {"outreach_sent": 40, "respondents": 6}
    assert day.id == org.id and day.has_org_row and day.rep_user_id is None


def test_team_only_day_is_synthesized_with_stable_id():
    rep = uuid.uuid4()
    a = fold_org_daily_totals([_row(rep=rep, day=D2, outreach_sent=12)])[0]
    b = fold_org_daily_totals([_row(rep=rep, day=D2, outreach_sent=12)])[0]
    assert a.has_org_row is False and a.outreach_sent == 12 and a.calls_booked is None
    assert a.id == b.id  # deterministic per org + date


def test_untouched_fields_stay_none_and_days_are_ordered():
    days = fold_org_daily_totals([_row(day=D2, outreach_sent=1), _row(day=D1, followups_sent=2)])
    assert [d.entry_date for d in days] == [D1, D2]
    assert days[0].outreach_sent is None and days[0].followups_sent == 2

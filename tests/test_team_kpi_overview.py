"""Team KPIs: one overview (week|month) — EOD, closer activity, trends vs previous span."""
import uuid
from datetime import date, datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from app.services import team_kpis as tk

MON_FRI = [0, 1, 2, 3, 4]


def test_period_bounds():
    assert tk.period_bounds("week", date(2026, 9, 24)) == (date(2026, 9, 21), date(2026, 9, 27))
    assert tk.period_bounds("month", date(2026, 9, 24)) == (date(2026, 9, 1), date(2026, 9, 30))
    assert tk.period_bounds("month", date(2026, 2, 10)) == (date(2026, 2, 1), date(2026, 2, 28))


def test_eod_summary_counts_period_and_not_today():
    today = date(2026, 9, 24)  # Thu
    submitted = {date(2026, 9, 21), date(2026, 9, 22), date(2026, 9, 18), date(2026, 9, 17)}  # Wed 23 missed
    s = tk.eod_summary(submitted, today, date(2026, 9, 21), date(2026, 9, 27), MON_FRI)
    assert (s["required_days"], s["submitted_days"], s["missed"]) == (3, 2, 1)  # Mon–Wed due; Thu is today
    assert s["streak"] == 0 and s["submitted_today"] is False
    monday = tk.eod_summary({date(2026, 9, 21), date(2026, 9, 18), date(2026, 9, 17)}, date(2026, 9, 21),
                            date(2026, 9, 21), date(2026, 9, 27), MON_FRI)
    assert monday["streak"] == 3 and monday["submitted_today"] is True  # weekend skipped


def test_legacy_rows_count_only_before_first_stamp_and_only_with_manual_fields():
    rep = uuid.uuid4()
    rows = [
        SimpleNamespace(rep_user_id=rep, entry_date=date(2026, 9, 1), submitted_at=None, outreach_sent=30),
        SimpleNamespace(rep_user_id=rep, entry_date=date(2026, 9, 2), submitted_at=None, calls_taken=3),
        SimpleNamespace(rep_user_id=rep, entry_date=date(2026, 9, 10), submitted_at=None, outreach_sent=5),
        SimpleNamespace(rep_user_id=rep, entry_date=date(2026, 9, 11), submitted_at=datetime(2026, 9, 11, tzinfo=timezone.utc)),
    ]
    assert tk.submitted_days(rows, stamp_cutoff=date(2026, 9, 5))[rep] == {date(2026, 9, 1), date(2026, 9, 11)}


def test_pace():
    assert tk.period_pace(date(2026, 9, 21), date(2026, 9, 27), date(2026, 9, 23), MON_FRI) == 3 / 5


def _perf(rows):
    return SimpleNamespace(reps=rows)


def test_overview_month_compares_previous_span_without_targets():
    setter, closer = uuid.uuid4(), uuid.uuid4()
    members = [
        {"user_id": setter, "name": "Sam", "email": None, "access_role": "member", "team_role": "sales", "owes_eod": True},
        {"user_id": closer, "name": "Cal", "email": None, "access_role": "member", "team_role": "sales", "owes_eod": True},
        {"user_id": uuid.uuid4(), "name": "Mia", "email": None, "access_role": "member", "team_role": "marketing", "owes_eod": False},
        {"user_id": uuid.uuid4(), "name": "Owner", "email": None, "access_role": "owner", "team_role": None, "owes_eod": False},
    ]
    cur_closer = SimpleNamespace(calls_taken=12, show_up_pct=80.0, closes=6, closing_rate_pct=50.0, cash_collected_cents=1_200_000)
    best_closer = SimpleNamespace(calls_taken=20, show_up_pct=90.0, closes=9, closing_rate_pct=60.0, cash_collected_cents=2_000_000)
    cur_setter = SimpleNamespace(outreach_sent=300, respondents=30, reply_rate_pct=10.0, calls_booked_activity=6, convo_to_booking_pct=20.0)
    perf_now = _perf([
        SimpleNamespace(rep_user_id=closer, current=cur_closer, personal_best=best_closer),
        SimpleNamespace(rep_user_id=setter, current=cur_setter, personal_best=cur_setter),
    ])
    perf_prev = _perf([SimpleNamespace(rep_user_id=closer, current=SimpleNamespace(closes=3, calls_taken=10))])
    calls = []

    def fake_perf(_db, _org, *, range_start, range_end):
        calls.append((range_start, range_end))
        return perf_now if len(calls) == 1 else perf_prev

    db = MagicMock()
    db.query.return_value.filter.return_value.all.return_value = [
        SimpleNamespace(rep_user_id=setter, entry_date=date(2026, 9, 3), submitted_at=datetime(2026, 9, 3, tzinfo=timezone.utc),
                        setter_context="Warm DMs from the webinar", best_content_type="Reels"),
        SimpleNamespace(rep_user_id=setter, entry_date=date(2026, 9, 8), submitted_at=datetime(2026, 9, 8, tzinfo=timezone.utc),
                        setter_context="", best_content_type="reels "),
        SimpleNamespace(rep_user_id=setter, entry_date=date(2026, 8, 28), submitted_at=datetime(2026, 8, 28, tzinfo=timezone.utc),
                        setter_context="last month", best_content_type="Carousel"),  # outside September
    ]
    db.query.return_value.filter.return_value.order_by.return_value.limit.return_value.scalar.return_value = datetime(2026, 8, 1, tzinfo=timezone.utc)
    with patch.object(tk, "list_team_members", return_value=members), patch.object(
        tk, "get_team_settings", return_value=dict(tk.DEFAULT_SETTINGS)
    ), patch(
        "app.services.kpi_rep_performance.build_rep_performance", side_effect=fake_perf
    ):
        out = tk.compute_team_overview(db, uuid.uuid4(), "month", date(2026, 9, 16), today=date(2026, 9, 16))

    # Sep 1–16 vs the same span of August.
    assert calls == [(date(2026, 9, 1), date(2026, 9, 16)), (date(2026, 8, 1), date(2026, 8, 16))]
    rows = {m["name"]: m for m in out["members"]}
    assert set(rows) == {"Sam", "Cal"}
    cal = {r["key"]: r for r in rows["Cal"]["closer_metrics"]}
    assert (cal["closes"]["value"], cal["closes"]["previous"], cal["closes"]["best"]) == (6, 3, 9)
    assert all("target" not in r and "status" not in r for r in cal.values())
    assert rows["Cal"]["eod"]["submitted_days"] == 0 and len(rows["Cal"]["setter_metrics"]) == 5  # sales reps get both sets
    sam = rows["Sam"]
    assert len(sam["closer_metrics"]) == 5 and all("status" not in r for r in sam["setter_metrics"])
    assert sam["eod"]["required_days"] == 11  # Sep 1–15 weekdays; the 16th is today
    assert [n["date"] for n in sam["notes"]] == [date(2026, 9, 8), date(2026, 9, 3)]  # this month, newest first
    assert sam["notes"][1]["setter_context"] == "Warm DMs from the webinar" and sam["notes"][0]["setter_context"] is None
    assert sam["content_counts"] == [{"content_type": "Reels", "count": 2}]  # case/space-insensitive
    days = sam["eod"]["days"]
    assert len(days) == 22 and days[0] == {"date": date(2026, 9, 1), "status": "missed"}
    assert [d["status"] for d in days if d["date"] == date(2026, 9, 3)] == ["submitted"]
    assert [d["status"] for d in days if d["date"] == date(2026, 9, 16)] == ["today"]
    assert days[-1]["status"] == "upcoming"
    assert {r["key"]: r for r in sam["setter_metrics"]}["outreach_sent"]["previous"] == 0  # no activity last month


def test_overview_explicit_range_compares_same_length_before():
    uid = uuid.uuid4()
    members = [{"user_id": uid, "name": "Sam", "email": None, "access_role": "member", "team_role": "sales", "owes_eod": True}]
    calls = []

    def fake_perf(_db, _org, *, range_start, range_end):
        calls.append((range_start, range_end))
        return _perf([])

    db = MagicMock()
    db.query.return_value.filter.return_value.all.return_value = []
    db.query.return_value.filter.return_value.order_by.return_value.limit.return_value.scalar.return_value = None
    with patch.object(tk, "list_team_members", return_value=members), patch.object(
        tk, "get_team_settings", return_value=dict(tk.DEFAULT_SETTINGS)
    ), patch("app.services.kpi_rep_performance.build_rep_performance", side_effect=fake_perf):
        out = tk.compute_team_overview(
            db, uuid.uuid4(), "range", date(2026, 9, 10), today=date(2026, 9, 27),
            bounds=(date(2026, 9, 1), date(2026, 9, 10)),
        )
    assert (out["period"], out["period_start"], out["period_end"]) == ("range", date(2026, 9, 1), date(2026, 9, 10))
    assert calls == [(date(2026, 9, 1), date(2026, 9, 10)), (date(2026, 8, 22), date(2026, 8, 31))]
    assert out["members"][0]["eod"]["required_days"] == 8  # Sep 1–10 weekdays
    assert all(r["best"] is None for r in out["members"][0]["closer_metrics"])

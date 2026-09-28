"""compute_funnel_summary: the Opt-ins -> Booked -> Showed -> Closed -> Cash
stage strip backing the funnel summary dashboard (phase 7 of the sales-pipeline
attribution PRD)."""
import uuid
from datetime import date, datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

from app.services.kpi_integration_sync import compute_funnel_summary


def _query_mock(
    *,
    clients=(),
    checkins=(),
    all_checkins=None,
    kpi_rows=(),
    stripe_rows=(),
    whop_rows=(),
    manual_rows=(),
):
    checkin_calls = iter([checkins, all_checkins if all_checkins is not None else checkins])
    # Collector reads (id, channel, funnel_id, created_at); fixtures give (id, channel).
    in_window = datetime(2026, 1, 15, tzinfo=timezone.utc)
    client_rows = [
        tuple(c) if len(c) == 4 else (c[0], c[1], None, in_window) for c in clients
    ]
    # Shape like real org rows (the collector folds them through kpi_org_totals).
    fixture_org = uuid.uuid4()
    kpi_rows = [
        SimpleNamespace(
            # One org row per date (unique index uq_org_kpi_daily_entries_org_date_agg).
            **{"entry_date": date(2026, 1, 10 + i), "org_id": fixture_org, "rep_user_id": None, **vars(r)},
            id=uuid.uuid4(),
        )
        for i, r in enumerate(kpi_rows)
    ]

    def query_side_effect(*entities):
        q = MagicMock()
        first = entities[0]
        owner = getattr(first, "class_", first)
        name = getattr(owner, "__name__", "")
        if name == "Client":
            q.filter.return_value.all.return_value = client_rows
        elif name == "ClientCheckIn":
            rows = next(checkin_calls, checkins)
            q.filter.return_value.all.return_value = rows
            q.filter.return_value.order_by.return_value.all.return_value = rows
        elif name == "OrgKpiDailyEntry":
            q.filter.return_value.all.return_value = kpi_rows
        elif name == "StripePayment":
            q.filter.return_value.all.return_value = stripe_rows
        elif name == "WhopPayment":
            q.filter.return_value.all.return_value = whop_rows
        elif name == "ManualPayment":
            q.filter.return_value.all.return_value = manual_rows
        elif name == "SalesActivityEvent":
            q.filter.return_value.all.return_value = []
        else:
            q.filter.return_value.all.return_value = []
        return q

    db = MagicMock()
    db.query.side_effect = query_side_effect
    return db


def _checkin(client_id, *, start_time, completed, no_show, sale_closed=False):
    return SimpleNamespace(
        client_id=client_id,
        start_time=start_time,
        completed=completed,
        no_show=no_show,
        is_sales_call=True,
        sale_closed=sale_closed,
    )


class TestComputeFunnelSummary:
    def test_paid_opt_ins_counts_only_paid_channel_new_clients(self):
        org_id = uuid.uuid4()
        ws, we = date(2026, 1, 1), date(2026, 1, 31)
        clients = [(uuid.uuid4(), "paid"), (uuid.uuid4(), "paid"), (uuid.uuid4(), "organic")]
        db = _query_mock(clients=clients)
        result = compute_funnel_summary(db, org_id, ws, we, "paid")
        assert result["opt_ins"] == 2

    def test_organic_opt_ins_uses_respondents_from_kpi_entries(self):
        org_id = uuid.uuid4()
        ws, we = date(2026, 1, 1), date(2026, 1, 31)
        kpi_rows = [
            SimpleNamespace(outreach_sent=50, respondents=10),
            SimpleNamespace(outreach_sent=30, respondents=5),
        ]
        db = _query_mock(kpi_rows=kpi_rows)
        result = compute_funnel_summary(db, org_id, ws, we, "organic")
        assert result["outreach_sent"] == 80
        assert result["respondents"] == 15
        assert result["opt_ins"] == 15
        assert result["reply_rate_pct"] == round(15 / 80 * 100, 1)

    def test_booked_showed_closed_and_cash_filtered_by_channel(self):
        org_id = uuid.uuid4()
        ws, we = date(2026, 1, 1), date(2026, 1, 31)
        paid_client = uuid.uuid4()
        organic_client = uuid.uuid4()
        all_clients = [(paid_client, "paid"), (organic_client, "organic")]
        window_start_dt = datetime(2026, 1, 1, tzinfo=timezone.utc)
        checkins = [
            _checkin(paid_client, start_time=window_start_dt, completed=True, no_show=False),
            _checkin(organic_client, start_time=window_start_dt, completed=True, no_show=False),
        ]
        stripe_rows = [
            SimpleNamespace(client_id=paid_client, amount_cents=50000, created_at=window_start_dt),
            SimpleNamespace(client_id=organic_client, amount_cents=30000, created_at=window_start_dt),
        ]
        db = _query_mock(clients=all_clients, checkins=checkins, stripe_rows=stripe_rows)
        result = compute_funnel_summary(db, org_id, ws, we, "paid")
        assert result["booked"] == 1
        assert result["showed"] == 1
        assert result["cash_usd"] == 500.0

    def test_all_channel_combines_paid_and_organic(self):
        org_id = uuid.uuid4()
        ws, we = date(2026, 1, 1), date(2026, 1, 31)
        clients = [(uuid.uuid4(), "paid")]
        kpi_rows = [SimpleNamespace(outreach_sent=100, respondents=20)]
        db = _query_mock(clients=clients, kpi_rows=kpi_rows)
        result = compute_funnel_summary(db, org_id, ws, we, None)
        assert result["opt_ins"] == 1 + 20
        assert result["channel"] == "all"

    def test_cash_per_close_is_none_when_no_closes(self):
        org_id = uuid.uuid4()
        ws, we = date(2026, 1, 1), date(2026, 1, 31)
        db = _query_mock()
        result = compute_funnel_summary(db, org_id, ws, we, "all")
        assert result["closed"] == 0
        assert result["cash_per_close_usd"] is None

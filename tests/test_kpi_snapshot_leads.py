"""KPI snapshot Total Leads = each new conversation + inbound ICP leads + paid funnel opt-ins."""
from datetime import date

from app.services.kpi_compute import build_kpi_snapshot
from app.services.kpi_org_totals import OrgDay


def _row(day, **kw):
    # Same row shape every reader gets from the one daily ledger.
    r = OrgDay("o", day)
    r.has_org_row = True
    for k, v in kw.items():
        setattr(r, k, v)
    return r


def test_total_leads_card_and_series():
    rows = [
        _row(date(2026, 9, 1), new_conversations=5, inbound_icp_leads=2, outreach_sent=100, followups_sent=40),
        _row(date(2026, 9, 2), new_conversations=3),
    ]
    snap = build_kpi_snapshot(
        rows,
        range_start=date(2026, 9, 1),
        range_end=date(2026, 9, 3),
        paid_leads_by_day={date(2026, 9, 2): 4, date(2026, 9, 3): 1, date(2026, 8, 31): 9},  # last one out of range
    )
    card = next(c for c in snap.cards if c.key == "total_leads")
    assert card.value == 15  # 5+3 convos + 2 inbound + 5 paid; outreach/follow-ups are not leads
    assert card.breakdown == {"conversations": 8, "inbound": 2, "paid": 5}
    assert [(p.date, p.total_leads) for p in snap.series] == [
        (date(2026, 9, 1), 7),
        (date(2026, 9, 2), 7),
        (date(2026, 9, 3), 1),  # paid-only day still charted
    ]

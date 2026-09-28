"""Pipeline Grid extras: UTM capture on funnel leads + booking status per client."""
import uuid
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

from app.api.clients.grid import booking_status_for_checkin, get_client_grid_details
from app.api.funnels import _resolve_lead_utm, normalize_utm
from app.schemas.funnel import FunnelLeadIn


def test_normalize_utm_keeps_standard_keys_and_accepts_prefixed():
    assert normalize_utm({"utm_source": "facebook", "medium": "paid", "junk": "x"}) == {
        "source": "facebook",
        "medium": "paid",
    }
    assert normalize_utm({"source": "  "}) is None
    assert normalize_utm("not-a-dict") is None
    assert len(normalize_utm({"campaign": "c" * 500})["campaign"]) == 200


def test_resolve_lead_utm_prefers_explicit_payload():
    db = MagicMock()
    lead = FunnelLeadIn(funnel_id=uuid.uuid4(), utm={"source": "ig"}, session_id="s1")
    assert _resolve_lead_utm(db, uuid.uuid4(), lead) == {"source": "ig"}
    db.query.assert_not_called()


def test_resolve_lead_utm_falls_back_to_tracked_session():
    db = MagicMock()
    db.query.return_value.filter.return_value.order_by.return_value.first.return_value = (
        {"source": "google", "campaign": "spring"},
    )
    lead = FunnelLeadIn(funnel_id=uuid.uuid4(), session_id="s1")
    assert _resolve_lead_utm(db, uuid.uuid4(), lead) == {"source": "google", "campaign": "spring"}


def test_resolve_lead_utm_none_without_any_key():
    db = MagicMock()
    assert _resolve_lead_utm(db, uuid.uuid4(), FunnelLeadIn(funnel_id=uuid.uuid4())) is None
    db.query.assert_not_called()


def _checkin(**kw):
    base = dict(cancelled=False, no_show=False, sale_closed=None)
    base.update(kw)
    return SimpleNamespace(**base)


def test_booking_status_precedence():
    assert booking_status_for_checkin(None) == "not_yet"
    assert booking_status_for_checkin(_checkin(cancelled=True, sale_closed=True)) == "canceled"
    assert booking_status_for_checkin(_checkin(no_show=True)) == "no_show"
    assert booking_status_for_checkin(_checkin(sale_closed=True)) == "closed"
    assert booking_status_for_checkin(_checkin(sale_closed=False)) == "booked"


def test_grid_details_uses_latest_sales_call_and_prospect_meta():
    org_id = uuid.uuid4()
    paid_id, organic_id = uuid.uuid4(), uuid.uuid4()
    now = datetime.now(timezone.utc)
    older = SimpleNamespace(client_id=paid_id, start_time=now - timedelta(days=9), cancelled=False, no_show=True, sale_closed=None)
    newer = SimpleNamespace(client_id=paid_id, start_time=now - timedelta(days=1), cancelled=False, no_show=False, sale_closed=None)

    checkin_q = MagicMock()
    checkin_q.filter.return_value.order_by.return_value.all.return_value = [older, newer]
    client_q = MagicMock()
    client_q.filter.return_value.all.return_value = [
        (paid_id, {"prospect": {"utm": {"utm_source": "fb"}, "quiz_answers": {"Budget": "$5k"}}}),
        (organic_id, None),
    ]
    db = MagicMock()
    db.query.side_effect = [checkin_q, client_q]
    user = SimpleNamespace(selected_org_id=org_id, org_id=org_id)

    rows = {r.client_id: r for r in get_client_grid_details(db=db, current_user=user)}
    assert rows[paid_id].utm == {"source": "fb"}
    assert rows[paid_id].answers == {"Budget": "$5k"}
    assert rows[paid_id].booking_status == "booked"
    assert rows[paid_id].booking_at == newer.start_time
    assert rows[organic_id].utm is None
    assert rows[organic_id].answers == {}
    assert rows[organic_id].booking_status == "not_yet"

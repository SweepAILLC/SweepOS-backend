"""upsert_funnel_lead: shared by POST /funnels/leads and GHL lead intake (GHL-4).

Lookups, lifecycle, cache and notification side effects are patched so each
test pins one attribution rule."""
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException

from app.models.client import Client
from app.models.funnel import Funnel
from app.schemas.funnel import FunnelLeadIn
from app.services import funnel_leads as fl

ORG = uuid.uuid4()
OPTED = datetime(2026, 9, 14, 16, 30, tzinfo=timezone.utc)


def _funnel() -> Funnel:
    return Funnel(id=uuid.uuid4(), org_id=ORG, name="GHL VSL", source="ghl")


def _lead(funnel: Funnel, **kw) -> FunnelLeadIn:
    return FunnelLeadIn(funnel_id=funnel.id, **kw)


def _client(**kw) -> Client:
    base = dict(id=uuid.uuid4(), org_id=ORG, email="ada@example.com", source_channel="organic", meta=None)
    base.update(kw)
    return Client(**base)


@contextmanager
def _env(*, by_ghl=None, by_email=None, by_phone=None):
    """Patch lookups + side effects; yields the notify mock."""
    with patch.object(fl, "find_client_by_ghl_contact_id", return_value=by_ghl) as g, patch.object(
        fl, "find_client_by_email", return_value=by_email
    ), patch.object(fl, "find_client_by_phone", return_value=by_phone), patch.object(
        fl, "invalidate_health_score_cache"
    ), patch.object(fl, "_notify") as notify, patch(
        "app.services.client_automation.apply_automatic_lifecycle_for_client"
    ), patch("app.services.client_automation.apply_funnel_lead_lifecycle"):
        notify.ghl_lookup = g
        yield notify


class TestDefaultsMatchFunnelsLeadsEndpoint:
    def test_new_client_is_paid_tagged_and_notified(self):
        f, db = _funnel(), MagicMock()
        with _env() as notify:
            r = fl.upsert_funnel_lead(db, f, _lead(f, email="new@example.com", name="Ada Lovelace"))
        assert r.created and r.tagged_now and not r.duplicate
        c = r.client
        assert (c.first_name, c.last_name) == ("Ada", "Lovelace")
        assert c.source_channel == "paid" and c.source_funnel_id == f.id
        assert c.opted_in_at is None  # created_at carries the date for Sweep pages
        assert c.meta["prospect"]["funnel_id"] == str(f.id)
        notify.assert_called_once()
        assert notify.call_args.kwargs["is_new_client"] is True

    def test_existing_organic_client_keeps_first_touch(self):
        f, existing = _funnel(), _client()
        with _env(by_email=existing) as notify:
            r = fl.upsert_funnel_lead(MagicMock(), f, _lead(f, email="ada@example.com"))
        assert not r.created and not r.tagged_now
        assert existing.source_channel == "organic" and existing.source_funnel_id is None
        assert existing.meta["prospect"]["funnel_id"] == str(f.id)  # still listed on the Leads tab
        notify.assert_called_once()

    def test_legacy_row_without_channel_takes_this_funnel(self):
        f, existing = _funnel(), _client(source_channel=None)
        with _env(by_email=existing):
            r = fl.upsert_funnel_lead(MagicMock(), f, _lead(f, email="ada@example.com"))
        assert r.tagged_now and existing.source_funnel_id == f.id and existing.source_channel == "paid"

    def test_nothing_to_identify_a_new_client_is_400(self):
        f = _funnel()
        with _env(), pytest.raises(HTTPException) as exc:
            fl.upsert_funnel_lead(MagicMock(), f, _lead(f))
        assert exc.value.status_code == 400


class TestGhlOptions:
    def test_match_prefers_ghl_contact_id_over_email(self):
        f, by_id, by_email = _funnel(), _client(), _client(email="other@example.com")
        with _env(by_ghl=by_id, by_email=by_email) as notify:
            r = fl.upsert_funnel_lead(
                MagicMock(), f, _lead(f, email="other@example.com"), ghl_contact_id="ghl_1", opted_in_at=OPTED
            )
        assert r.client is by_id
        notify.ghl_lookup.assert_called_once()
        assert notify.ghl_lookup.call_args.args[1:] == (ORG, "ghl_1")

    def test_new_ghl_client_keeps_submission_dates(self):
        f = _funnel()
        with _env():
            r = fl.upsert_funnel_lead(
                MagicMock(), f, _lead(f, email="new@example.com"), ghl_contact_id="ghl_2", opted_in_at=OPTED
            )
        c = r.client
        assert c.opted_in_at == OPTED
        assert c.created_at == datetime(2026, 9, 14, 16, 30)  # naive UTC column
        assert c.meta["ghl_contact_id"] == "ghl_2"
        assert c.meta["prospect"]["captured_at"] == OPTED.isoformat()

    def test_reattributes_unattributed_client(self):
        # Made by the manual contact sync or a booking before the opt-in arrived.
        f, existing = _funnel(), _client(meta={"ghl_contact_id": "ghl_3"})
        with _env(by_ghl=existing):
            r = fl.upsert_funnel_lead(
                MagicMock(), f, _lead(f), ghl_contact_id="ghl_3", opted_in_at=OPTED, reattribute=True
            )
        assert r.tagged_now
        assert (existing.source_channel, existing.source_funnel_id, existing.opted_in_at) == ("paid", f.id, OPTED)

    def test_reattribute_never_overrides_another_funnel(self):
        f, other = _funnel(), uuid.uuid4()
        existing = _client(source_channel="paid", source_funnel_id=other, opted_in_at=OPTED)
        with _env(by_email=existing):
            r = fl.upsert_funnel_lead(MagicMock(), f, _lead(f, email="ada@example.com"), opted_in_at=OPTED, reattribute=True)
        assert not r.tagged_now and existing.source_funnel_id == other

    def test_reattribute_skips_paid_client_without_funnel(self):
        existing = _client(source_channel="paid")
        f = _funnel()
        with _env(by_email=existing):
            r = fl.upsert_funnel_lead(MagicMock(), f, _lead(f, email="ada@example.com"), opted_in_at=OPTED, reattribute=True)
        assert not r.tagged_now and existing.source_funnel_id is None

    def test_second_arrival_for_same_funnel_is_duplicate(self):
        # Webhook already tagged this person; the reconcile pull delivers the same submission.
        f = _funnel()
        first_capture = "2026-09-14T16:30:00+00:00"
        existing = _client(
            source_channel="paid",
            source_funnel_id=f.id,
            opted_in_at=OPTED,
            meta={"ghl_contact_id": "ghl_4", "prospect": {"funnel_id": str(f.id), "captured_at": first_capture}},
        )
        later = datetime(2026, 9, 15, tzinfo=timezone.utc)
        with _env(by_ghl=existing) as notify:
            r = fl.upsert_funnel_lead(
                MagicMock(),
                f,
                _lead(f, opt_in_data={"goal": "scale"}),
                utm={"source": "facebook"},
                ghl_contact_id="ghl_4",
                opted_in_at=later,
                reattribute=True,
            )
        assert r.duplicate and not r.tagged_now
        notify.assert_not_called()
        assert existing.opted_in_at == OPTED
        assert existing.meta["prospect"]["captured_at"] == first_capture
        assert existing.meta["prospect"]["opt_in_data"] == {"goal": "scale"}
        assert existing.meta["prospect"]["utm"] == {"source": "facebook"}

    def test_notify_false_suppresses_notification(self):
        f = _funnel()
        with _env() as notify:
            fl.upsert_funnel_lead(MagicMock(), f, _lead(f, email="n@example.com"), opted_in_at=OPTED, notify=False)
        notify.assert_not_called()

    def test_existing_ghl_contact_id_is_not_overwritten(self):
        f, existing = _funnel(), _client(meta={"ghl_contact_id": "original"})
        with _env(by_email=existing):
            fl.upsert_funnel_lead(MagicMock(), f, _lead(f, email="ada@example.com"), ghl_contact_id="different")
        assert existing.meta["ghl_contact_id"] == "original"


def test_ghl_contact_lookup_is_org_scoped():
    """The query filters on org_id and meta->>'ghl_contact_id' (index from 097)."""
    db = MagicMock()
    fl.find_client_by_ghl_contact_id(db, ORG, "ghl_9")
    (org_clause, contact_clause), _ = db.query.return_value.filter.call_args
    assert "org_id" in str(org_clause)
    compiled = str(contact_clause.compile(compile_kwargs={"literal_binds": True}))
    assert "ghl_contact_id" in compiled
    assert fl.find_client_by_ghl_contact_id(db, ORG, "") is None

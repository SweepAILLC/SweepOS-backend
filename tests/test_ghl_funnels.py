"""GHL-5: GHL funnel + submission client calls, pairing on funnel create, and the
appointment webhook remembering the GHL contact id.

HTTP is served by httpx.MockTransport from fixtures in tests/fixtures/ghl/."""
import json
import uuid
from datetime import date, datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import httpx
import pytest
from fastapi import HTTPException
from sqlalchemy.exc import IntegrityError

from app.models.client import Client
from app.models.funnel import Funnel, FunnelStep
from app.schemas.funnel import FunnelCreate
from app.services import ghl_client as gc
from app.services import ghl_funnels as gf

FIXTURES = Path(__file__).parent / "fixtures" / "ghl"
HEADERS = {"Authorization": "Bearer t", "Version": gc.GHL_API_VERSION}
ORG = uuid.uuid4()


def _fixture(name):
    return json.loads((FIXTURES / name).read_text())


def _mock_http(handler):
    """Patch ghl_client's httpx.Client to route through `handler(request)`."""
    real = httpx.Client

    def factory(*args, **kwargs):
        kwargs.pop("transport", None)
        return real(*args, transport=httpx.MockTransport(handler), **kwargs)

    return patch.object(gc.httpx, "Client", side_effect=factory)


class TestNormalizeFunnelPath:
    @pytest.mark.parametrize(
        "raw, expected",
        [
            ("/VSL-Optin/", "/vsl-optin"),
            ("vsl-optin", "/vsl-optin"),
            ("https://go.example.com/vsl-optin?utm_source=fb#top", "/vsl-optin"),
            ("/vsl-optin?x=1", "/vsl-optin"),
            ("", None),
            (None, None),
        ],
    )
    def test_paths(self, raw, expected):
        assert gc.normalize_funnel_path(raw) == expected


class TestListGhlFunnels:
    def test_normalizes_and_sorts_steps(self):
        with _mock_http(lambda req: httpx.Response(200, json=_fixture("funnels_list.json"))):
            funnels = gc.list_ghl_funnels(HEADERS, "loc_test")
        vsl = funnels[0]
        assert vsl["ghl_funnel_id"] == "fnl_vsl_001" and vsl["name"] == "VSL Strategy Call"
        assert [s["path"] for s in vsl["steps"]] == ["/vsl-optin", "/vsl-book", "/vsl-thank-you"]
        assert len(funnels) == 2

    def test_paginates_by_offset(self):
        seen = []

        def handler(req):
            offset = int(req.url.params["offset"])
            seen.append(offset)
            assert req.url.params["locationId"] == "loc_test"
            items = [{"_id": f"f{offset + i}", "name": "x", "steps": []} for i in range(gc.GHL_FUNNELS_PAGE_SIZE)]
            if offset:
                items = items[:3]
            return httpx.Response(200, json={"funnels": items, "count": gc.GHL_FUNNELS_PAGE_SIZE + 3})

        with _mock_http(handler):
            funnels = gc.list_ghl_funnels(HEADERS, "loc_test")
        assert seen == [0, gc.GHL_FUNNELS_PAGE_SIZE]
        assert len(funnels) == gc.GHL_FUNNELS_PAGE_SIZE + 3

    def test_accepts_single_object_shape_from_docs(self):
        body = {"funnels": {"_id": "only", "name": "Solo", "steps": []}, "count": 1}
        with _mock_http(lambda req: httpx.Response(200, json=body)):
            assert [f["ghl_funnel_id"] for f in gc.list_ghl_funnels(HEADERS, "loc")] == ["only"]

    def test_upstream_401_raises_ghl_api_error(self):
        with _mock_http(lambda req: httpx.Response(401, json={})), pytest.raises(gc.GhlApiError) as exc:
            gc.list_ghl_funnels(HEADERS, "loc")
        assert exc.value.status_code == 401


class TestSubmissions:
    def test_pages_until_next_page_is_empty(self):
        calls = []

        def handler(req):
            page = int(req.url.params["page"])
            calls.append((req.url.path, page, req.url.params["startAt"], req.url.params["endAt"]))
            rows = [{"id": f"s{page}-{i}"} for i in range(gc.GHL_SUBMISSIONS_PAGE_SIZE if page == 1 else 2)]
            return httpx.Response(200, json={"submissions": rows, "meta": {"nextPage": 2 if page == 1 else None}})

        with _mock_http(handler):
            rows = list(gc.iter_ghl_survey_submissions(HEADERS, "loc", date(2026, 9, 1), date(2026, 9, 14)))
        assert len(rows) == gc.GHL_SUBMISSIONS_PAGE_SIZE + 2
        assert calls == [
            ("/surveys/submissions", 1, "2026-09-01", "2026-09-14"),
            ("/surveys/submissions", 2, "2026-09-01", "2026-09-14"),
        ]

    def test_normalize_submission_from_fixture(self):
        raw = _fixture("form_submissions.json")["submissions"][0]
        out = gc.normalize_ghl_submission(raw, "forms")
        assert out["submission_id"] == "sub_001" and out["contact_id"] == "ct_001"
        assert out["created_at"] == datetime(2026, 9, 14, 16, 30, tzinfo=timezone.utc)
        assert out["form_id"] == "form_optin"
        assert (out["email"], out["phone"], out["first_name"]) == ("ada@example.com", "+15550100001", "Ada")
        assert out["page_path"] == "/vsl-optin"
        assert out["utm_raw"] == {"source": "facebook", "medium": "paid", "campaign": "sept"}
        assert out["answers"] == {"monthly_revenue": "$10k-$25k"}

    def test_utm_falls_back_to_ghl_source_and_medium(self):
        raw = _fixture("form_submissions.json")["submissions"][1]
        out = gc.normalize_ghl_submission(raw, "forms")
        assert out["utm_raw"] == {"source": "Organic Search", "medium": "form"}
        assert out["page_path"] == "/blog/post"

    def test_submission_without_id_is_skipped(self):
        assert gc.normalize_ghl_submission({"contactId": "x"}, "forms") is None


def test_appointment_event_carries_contact_id():
    payload = {"appointment": {"id": "a1", "calendarId": "c1"}, "contact": {"id": "ct_9", "email": "x@example.com"}}
    assert gc.normalize_ghl_appointment_event(payload)["contact_id"] == "ct_9"
    flat = {"id": "a1", "calendarId": "c1", "contactId": "ct_10"}
    assert gc.normalize_ghl_appointment_event(flat)["contact_id"] == "ct_10"


def test_appointment_webhook_stamps_contact_id_without_overwriting():
    from app.api.ghl_webhooks import _stamp_ghl_contact_id

    fresh = Client(id=uuid.uuid4(), org_id=ORG, meta=None)
    _stamp_ghl_contact_id(fresh, "ct_1")
    assert fresh.meta == {"ghl_contact_id": "ct_1"}

    known = Client(id=uuid.uuid4(), org_id=ORG, meta={"ghl_contact_id": "orig", "x": 1})
    _stamp_ghl_contact_id(known, "ct_2")
    assert known.meta == {"ghl_contact_id": "orig", "x": 1}


def _ghl_funnel():
    return gc.normalize_ghl_funnel(_fixture("funnels_list.json")["funnels"][0])


class TestPairing:
    def _db(self, *, other=None, has_steps=False):
        db = MagicMock()
        added = []
        db.add.side_effect = added.append
        db.added = added
        steps_q = MagicMock()
        steps_q.filter.return_value.first.return_value = (uuid.uuid4(),) if has_steps else None
        funnel_q = MagicMock()
        funnel_q.filter.return_value.first.return_value = other

        def query(entity):
            return steps_q if getattr(entity, "class_", None) is FunnelStep else funnel_q

        db.query.side_effect = query
        return db

    def test_pair_sets_config_and_creates_steps_in_order(self):
        db, funnel = self._db(), Funnel(id=uuid.uuid4(), org_id=ORG, name="VSL")
        gf.pair_funnel_with_ghl(db, funnel, _ghl_funnel())
        assert funnel.source == "ghl"
        assert funnel.ghl_config["ghl_funnel_id"] == "fnl_vsl_001"
        assert funnel.ghl_config["extra_form_ids"] == []
        steps = [o for o in db.added if isinstance(o, FunnelStep)]
        assert [(s.step_order, s.event_name, s.label) for s in steps] == [
            (1, "view:/vsl-optin", "Opt-in"),
            (2, "view:/vsl-book", "Book a Call"),
            (3, "view:/vsl-thank-you", "Thank You"),
        ]
        assert all(s.org_id == ORG and s.funnel_id == funnel.id for s in steps)
        db.commit.assert_called_once()

    def test_existing_steps_are_left_alone(self):
        db, funnel = self._db(has_steps=True), Funnel(id=uuid.uuid4(), org_id=ORG, name="VSL")
        gf.pair_funnel_with_ghl(db, funnel, _ghl_funnel())
        assert not [o for o in db.added if isinstance(o, FunnelStep)]

    def test_already_paired_elsewhere_raises(self):
        other = Funnel(id=uuid.uuid4(), org_id=ORG, name="Old VSL")
        db, funnel = self._db(other=other), Funnel(id=uuid.uuid4(), org_id=ORG, name="New")
        with pytest.raises(gf.GhlFunnelAlreadyPairedError) as exc:
            gf.pair_funnel_with_ghl(db, funnel, _ghl_funnel())
        assert exc.value.funnel is other
        db.commit.assert_not_called()

    @pytest.mark.parametrize("fails_at", ["flush", "commit"])
    def test_concurrent_pair_caught_by_unique_index(self, fails_at):
        winner = Funnel(id=uuid.uuid4(), org_id=ORG, name="Winner")
        db, funnel = self._db(), Funnel(id=uuid.uuid4(), org_id=ORG, name="Loser")
        getattr(db, fails_at).side_effect = IntegrityError("update", {}, Exception("uq_funnels_org_ghl_funnel_id"))
        with patch.object(gf, "find_paired_funnel", side_effect=[None, winner]):
            with pytest.raises(gf.GhlFunnelAlreadyPairedError) as exc:
                gf.pair_funnel_with_ghl(db, funnel, _ghl_funnel())
        assert exc.value.funnel is winner
        db.rollback.assert_called_once()

    def test_repair_same_funnel_keeps_sync_state(self):
        prev = {"ghl_funnel_id": "fnl_vsl_001", "extra_form_ids": ["f1"], "paired_at": "2026-09-01T00:00:00+00:00",
                "sync": {"cursor": "2026-09-10"}}
        config = gf.build_ghl_config(_ghl_funnel(), prev)
        assert config["sync"] == {"cursor": "2026-09-10"} and config["extra_form_ids"] == ["f1"]
        assert config["paired_at"] == "2026-09-01T00:00:00+00:00"
        other = gf.build_ghl_config(gc.normalize_ghl_funnel({"_id": "different", "steps": []}), prev)
        assert "sync" not in other and other["extra_form_ids"] == []


class TestCreateFunnelEndpoint:
    def _user(self):
        return SimpleNamespace(id=uuid.uuid4(), org_id=ORG, selected_org_id=ORG)

    def test_ghl_source_requires_funnel_id(self):
        with pytest.raises(ValueError):
            FunnelCreate(name="x", source="ghl")

    def test_member_cannot_pair(self):
        from app.api.funnels import create_funnel

        with patch("app.services.org_user_context.user_can_manage_org_integrations", return_value=False):
            with pytest.raises(HTTPException) as exc:
                create_funnel(FunnelCreate(name="x", source="ghl", ghl_funnel_id="fnl"), MagicMock(), self._user())
        assert exc.value.status_code == 403

    def test_ghl_create_pairs_in_one_transaction(self):
        from app.api import funnels as api

        db = MagicMock()
        with patch("app.services.org_user_context.user_can_manage_org_integrations", return_value=True), patch(
            "app.services.ghl_funnels.fetch_ghl_funnel", return_value=_ghl_funnel()
        ), patch("app.services.ghl_funnels.pair_funnel_with_ghl", side_effect=lambda db, f, g: f) as pair:
            out = api.create_funnel(FunnelCreate(name="VSL", source="ghl", ghl_funnel_id="fnl_vsl_001"), db, self._user())
        assert out.org_id == ORG and out.name == "VSL"
        pair.assert_called_once()
        db.commit.assert_not_called()  # pair_funnel_with_ghl owns the single commit

    def test_already_paired_is_409_with_details(self):
        from app.api import funnels as api

        other = Funnel(id=uuid.uuid4(), org_id=ORG, name="Old VSL")
        with patch("app.services.org_user_context.user_can_manage_org_integrations", return_value=True), patch(
            "app.services.ghl_funnels.fetch_ghl_funnel", return_value=_ghl_funnel()
        ), patch("app.services.ghl_funnels.pair_funnel_with_ghl", side_effect=gf.GhlFunnelAlreadyPairedError(other)):
            with pytest.raises(HTTPException) as exc:
                api.create_funnel(FunnelCreate(name="VSL", source="ghl", ghl_funnel_id="fnl_vsl_001"), MagicMock(), self._user())
        assert exc.value.status_code == 409
        assert exc.value.detail["paired_funnel_id"] == str(other.id)

    @pytest.mark.parametrize(
        "error, code",
        [
            (gc.GhlNotConnectedError("x"), 400),
            (gf.GhlFunnelNotFoundError("x"), 404),
            (gc.GhlApiError("x", status_code=401), 400),
            (gc.GhlApiError("x", status_code=500), 502),
        ],
    )
    def test_ghl_errors_map_to_http(self, error, code):
        from app.api import funnels as api

        with patch("app.services.org_user_context.user_can_manage_org_integrations", return_value=True), patch(
            "app.services.ghl_funnels.fetch_ghl_funnel", side_effect=error
        ):
            with pytest.raises(HTTPException) as exc:
                api.create_funnel(FunnelCreate(name="VSL", source="ghl", ghl_funnel_id="f"), MagicMock(), self._user())
        assert exc.value.status_code == code

    def test_sweep_create_unchanged(self):
        from app.api import funnels as api

        db = MagicMock()
        out = api.create_funnel(FunnelCreate(name="Quiz"), db, self._user())
        assert out.source is None or out.source == "sweep"
        db.commit.assert_called_once()


class TestExtraFormsAndListing:
    def test_step_event_name_is_capped_like_the_snippet(self):
        long_path = "/" + "a" * 200
        assert gf.step_event_name(long_path) == "view:" + long_path[:95]
        assert len(gf.step_event_name(long_path)) <= 100

    def test_set_extra_form_ids_dedupes_and_sorts(self):
        f = Funnel(id=uuid.uuid4(), org_id=ORG, name="f", source="ghl", ghl_config={"ghl_funnel_id": "x", "extra_form_ids": []})
        gf.set_extra_form_ids(MagicMock(), f, ["b", " a ", "b", ""])
        assert f.ghl_config["extra_form_ids"] == ["a", "b"]
        assert f.ghl_config["ghl_funnel_id"] == "x"

    def test_extra_forms_schema_limits(self):
        from app.schemas.funnel import FunnelGhlExtraFormsIn

        with pytest.raises(ValueError):
            FunnelGhlExtraFormsIn(form_ids=["x"] * 51)
        with pytest.raises(ValueError):
            FunnelGhlExtraFormsIn(form_ids=["x" * 256])

    def test_extra_forms_endpoint_requires_ghl_funnel_in_org(self):
        from app.api import funnels as api
        from app.schemas.funnel import FunnelGhlExtraFormsIn

        user = SimpleNamespace(id=uuid.uuid4(), org_id=ORG, selected_org_id=ORG)
        db = MagicMock()
        db.query.return_value.filter.return_value.first.return_value = Funnel(id=uuid.uuid4(), org_id=ORG, name="f", source="sweep")
        with patch("app.services.org_user_context.user_can_manage_org_integrations", return_value=True):
            with pytest.raises(HTTPException) as exc:
                api.set_funnel_ghl_extra_forms(uuid.uuid4(), FunnelGhlExtraFormsIn(form_ids=["a"]), db, user)
        assert exc.value.status_code == 400

    def test_list_forms_and_surveys(self):
        def handler(req):
            if req.url.path == "/forms/":
                return httpx.Response(200, json={"forms": [{"id": "f1", "name": "Opt-in"}], "total": 1})
            return httpx.Response(200, json={"surveys": [{"id": "s1", "name": "Quiz"}], "total": 1})

        with _mock_http(handler):
            out = gc.list_ghl_forms_and_surveys(HEADERS, "loc")
        assert out == [{"id": "f1", "name": "Opt-in", "kind": "form"}, {"id": "s1", "name": "Quiz", "kind": "survey"}]

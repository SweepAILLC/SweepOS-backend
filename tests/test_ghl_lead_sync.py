"""GHL-6: lead reconcile pull (routing, cadence, cursor, first-run notifications)."""
import json
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from app.models.funnel import Funnel
from app.services import ghl_client as gc
from app.services import ghl_lead_sync as sync

ORG = uuid.uuid4()
NOW = datetime(2026, 10, 2, 15, 0, tzinfo=timezone.utc)
FIXTURES = Path(__file__).parent / "fixtures" / "ghl"


def _subs():
    return json.loads((FIXTURES / "form_submissions.json").read_text())["submissions"]


def _funnel(*, steps=("/vsl-optin", "/vsl-book"), extra=(), sync_state=None, webhook=None, paired_at="2026-09-01"):
    cfg = {
        "ghl_funnel_id": uuid.uuid4().hex,
        "steps": [{"path": p} for p in steps],
        "extra_form_ids": list(extra),
        "paired_at": paired_at,
    }
    if sync_state is not None:
        cfg["sync"] = sync_state
    if webhook is not None:
        cfg["webhook"] = webhook
    return Funnel(id=uuid.uuid4(), org_id=ORG, name="f", source="ghl", ghl_config=cfg)


class TestRouting:
    def test_page_path_routes_to_matching_funnel(self):
        vsl, webinar = _funnel(), _funnel(steps=("/webinar",))
        sub = gc.normalize_ghl_submission(_subs()[0], "forms")
        assert sync.route_submission(sub, [webinar, vsl]) is vsl

    def test_extra_form_list_catches_off_funnel_forms(self):
        vsl = _funnel(extra=("form_popup",))
        sub = gc.normalize_ghl_submission(_subs()[1], "forms")  # /blog/post page
        assert sync.route_submission(sub, [vsl]) is vsl

    def test_unmatched_submission_is_skipped(self):
        sub = gc.normalize_ghl_submission(_subs()[1], "forms")
        assert sync.route_submission(sub, [_funnel()]) is None

    def test_ties_go_to_earliest_paired_funnel(self):
        older = _funnel(paired_at="2026-08-01")
        newer = _funnel(paired_at="2026-09-01")
        sub = gc.normalize_ghl_submission(_subs()[0], "forms")
        assert sync.route_submission(sub, [newer, older]) is older


class TestLeadFromSubmission:
    def test_fields_and_source(self):
        sub = gc.normalize_ghl_submission(_subs()[0], "forms")
        lead = sync.lead_from_submission(sub, uuid.uuid4())
        assert (lead.email, lead.phone, lead.first_name, lead.last_name) == (
            "ada@example.com", "+15550100001", "Ada", "Lovelace",
        )
        assert lead.name is None  # first/last present, so the full name is not re-split
        assert lead.source == "ghl_form" and lead.funnel_step_reached == "/vsl-optin"
        assert lead.opt_in_data == {"monthly_revenue": "$10k-$25k"}

    def test_survey_source(self):
        sub = {"submission_id": "s", "kind": "surveys", "name": "Grace Hopper", "answers": {}}
        lead = sync.lead_from_submission(sub, uuid.uuid4())
        assert lead.source == "ghl_survey" and lead.name == "Grace Hopper" and lead.opt_in_data is None

    def test_oversized_answers_are_dropped_not_the_lead(self):
        sub = {"submission_id": "s", "kind": "forms", "email": "a@example.com", "answers": {"essay": "x" * 200_000}}
        lead = sync.lead_from_submission(sub, uuid.uuid4())
        assert lead.email == "a@example.com" and lead.opt_in_data is None


class TestCadence:
    interval = timedelta(minutes=15)

    def test_never_run_is_due(self):
        assert sync.is_due([_funnel()], NOW, self.interval)

    def test_without_webhook_every_interval(self):
        ran = {"last_run_at": (NOW - timedelta(minutes=14)).isoformat()}
        assert not sync.is_due([_funnel(sync_state=ran)], NOW, self.interval)
        ran = {"last_run_at": (NOW - timedelta(minutes=16)).isoformat()}
        assert sync.is_due([_funnel(sync_state=ran)], NOW, self.interval)

    def test_live_webhook_drops_to_daily(self):
        ran = {"last_run_at": (NOW - timedelta(hours=5)).isoformat()}
        hook = {"last_received_at": (NOW - timedelta(days=2)).isoformat()}
        assert not sync.is_due([_funnel(sync_state=ran, webhook=hook)], NOW, self.interval)
        stale_hook = {"last_received_at": (NOW - timedelta(days=8)).isoformat()}
        assert sync.is_due([_funnel(sync_state=ran, webhook=stale_hook)], NOW, self.interval)

    def test_window_rereads_cursor_day_and_first_run_reaches_back(self):
        old = _funnel(sync_state={"cursor": "2026-10-01"})
        assert sync.window_for([old], NOW.date()) == (date(2026, 9, 30), False)
        new = _funnel()
        start, first = sync.window_for([old, new], NOW.date())
        assert first and start == NOW.date() - timedelta(days=sync.FIRST_RUN_DAYS)


class TestSyncOrg:
    def _run(self, funnels, *, forms=(), surveys=(), done_ids=(), process_ok=True, iter_error=None):
        recorded = []

        def record(db, **kw):
            recorded.append(kw)
            status = "done" if kw["event_id"] in done_ids else "pending"
            return SimpleNamespace(status=status, **kw), True

        def iterator(rows):
            def _it(*a, **k):
                if iter_error:
                    raise iter_error
                return iter(rows)
            return _it

        db = MagicMock()
        with patch.object(sync, "paired_funnels", return_value=funnels), patch.object(
            gc, "get_ghl_connection", return_value=({}, "loc")
        ), patch.dict(sync._ITERATORS, {"forms": iterator(forms), "surveys": iterator(surveys)}), patch(
            "app.services.inbound_webhook_inbox.record_inbound_event", side_effect=record
        ), patch(
            "app.services.inbound_webhook_inbox.process_recorded_event", return_value=process_ok
        ) as proc, patch.object(sync, "_after_run_side_effects") as side:
            counts = sync.sync_org(db, ORG, now=NOW)
        return counts, recorded, proc, side

    def test_routes_records_and_processes(self):
        f = _funnel(sync_state={"cursor": "2026-10-01", "last_run_at": "2026-10-01T00:00:00+00:00"})
        counts, recorded, proc, side = self._run([f], forms=_subs())
        assert counts == {"seen": 2, "routed": 1, "processed": 1, "skipped": 1, "failed": 0}
        assert recorded[0]["provider"] == "ghl_sync" and recorded[0]["event_id"] == "forms:sub_001"
        assert recorded[0]["payload"]["funnel_id"] == str(f.id)
        assert recorded[0]["payload"]["notify"] is True
        json.dumps(recorded[0]["payload"])  # inbox payload must be JSON-safe
        assert f.ghl_config["sync"]["cursor"] == "2026-10-02"
        side.assert_called_once()
        assert side.call_args.args[2] == {date(2026, 9, 14)}

    def test_first_run_never_notifies(self):
        f = _funnel()
        _, recorded, _, _ = self._run([f], forms=_subs()[:1])
        assert recorded[0]["payload"]["notify"] is False

    def test_already_done_rows_are_not_reprocessed(self):
        f = _funnel(sync_state={"cursor": "2026-10-01"})
        counts, _, proc, _ = self._run([f], forms=_subs()[:1], done_ids={"forms:sub_001"})
        proc.assert_not_called()
        assert counts["processed"] == 0

    def test_failed_processing_counts_and_keeps_going(self):
        f = _funnel(sync_state={"cursor": "2026-10-01"})
        counts, _, _, _ = self._run([f], forms=_subs()[:1], process_ok=False)
        assert counts["failed"] == 1
        assert f.ghl_config["sync"]["cursor"] == "2026-10-02"  # retries belong to the inbox flush

    @pytest.mark.parametrize(
        "status, phrase",
        [(401, "Reconnect GHL"), (429, "rate limit"), (500, "request failed")],
    )
    def test_ghl_error_keeps_cursor_and_records_reason(self, status, phrase):
        f = _funnel(sync_state={"cursor": "2026-09-20"})
        self._run([f], iter_error=gc.GhlApiError("x", status_code=status))
        state = f.ghl_config["sync"]
        assert state["cursor"] == "2026-09-20"
        assert phrase in state["last_error"]

    def test_not_connected_is_recorded(self):
        f = _funnel()
        with patch.object(sync, "paired_funnels", return_value=[f]), patch.object(
            gc, "get_ghl_connection", side_effect=gc.GhlNotConnectedError("x")
        ):
            sync.sync_org(MagicMock(), ORG, now=NOW)
        assert "not connected" in f.ghl_config["sync"]["last_error"]

    def test_no_paired_funnels_is_a_noop(self):
        with patch.object(sync, "paired_funnels", return_value=[]), patch.object(gc, "get_ghl_connection") as conn:
            assert sync.sync_org(MagicMock(), ORG, now=NOW)["seen"] == 0
        conn.assert_not_called()


class TestProcessor:
    def _db(self, funnel):
        db = MagicMock()
        db.query.return_value.filter.return_value.first.return_value = funnel
        return db

    def test_tags_lead_with_ghl_options(self):
        f = _funnel()
        sub = sync._json_safe(gc.normalize_ghl_submission(_subs()[0], "forms"))
        with patch("app.services.funnel_leads.upsert_funnel_lead") as up:
            sync.process_submission_payload(self._db(f), ORG, {"funnel_id": str(f.id), "submission": sub, "notify": False})
        kw = up.call_args.kwargs
        assert kw["reattribute"] is True and kw["notify"] is False
        assert kw["ghl_contact_id"] == "ct_001"
        assert kw["opted_in_at"] == datetime(2026, 9, 14, 16, 30, tzinfo=timezone.utc)
        assert kw["utm"] == {"source": "facebook", "medium": "paid", "campaign": "sept"}

    def test_unpaired_funnel_is_dropped(self):
        with patch("app.services.funnel_leads.upsert_funnel_lead") as up:
            sync.process_submission_payload(self._db(None), ORG, {"funnel_id": str(uuid.uuid4()), "submission": {"email": "a@b.co"}})
        up.assert_not_called()

    def test_anonymous_submission_is_dropped(self):
        f = _funnel()
        with patch("app.services.funnel_leads.upsert_funnel_lead") as up:
            sync.process_submission_payload(self._db(f), ORG, {"funnel_id": str(f.id), "submission": {"submission_id": "x"}})
        up.assert_not_called()


def test_inbox_flush_knows_the_sync_provider():
    """Failed submissions are retried by the worker's inbox flush, not dropped as unknown."""
    from app.services import inbound_webhook_inbox as inbox

    row = SimpleNamespace(provider="ghl_sync")
    with patch.object(inbox, "claim_due_inbound_events", return_value=[row]), patch.object(
        inbox, "process_recorded_event"
    ) as proc, patch.object(inbox, "mark_inbound_retry") as retry:
        assert inbox.flush_due_inbound_webhooks(MagicMock()) == 1
    retry.assert_not_called()
    assert proc.call_args.args[2] is sync.process_submission_payload

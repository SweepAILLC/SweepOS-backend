"""Call Library auto-close: skip conditions, dedup guard, and the happy path
that writes the payment + closer/setter SalesActivityEvent rows. The payment
processor always wins — never overrides a real payment, only ever creates the
first record of a close."""
import uuid
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from app.services.call_library_auto_close import (
    attempt_auto_close_from_call_library_report,
    supersede_auto_payment_if_matched,
)


def _deal(**overrides):
    base = {
        "cash_collected_on_call": True,
        "verbally_agreed_not_paid": False,
        "amount": 500.0,
        "currency": "USD",
        "billing": "one_time",
        "confidence": "high",
        "evidence": "Card charged live on call.",
    }
    base.update(overrides)
    return {"deal_outcome": base}


class TestSkipConditions:
    def test_not_cash_collected_never_writes_anything(self):
        db = MagicMock()
        result = attempt_auto_close_from_call_library_report(
            db, uuid.uuid4(), uuid.uuid4(), uuid.uuid4(),
            _deal(cash_collected_on_call=False, verbally_agreed_not_paid=True),
        )
        assert result == "skipped_not_cash_collected"
        db.add.assert_not_called()
        db.commit.assert_not_called()

    def test_low_confidence_never_auto_closes(self):
        db = MagicMock()
        result = attempt_auto_close_from_call_library_report(
            db, uuid.uuid4(), uuid.uuid4(), uuid.uuid4(), _deal(confidence="medium"),
        )
        assert result == "skipped_low_confidence"
        db.add.assert_not_called()

    def test_malformed_report_json_returns_none(self):
        db = MagicMock()
        result = attempt_auto_close_from_call_library_report(
            db, uuid.uuid4(), uuid.uuid4(), uuid.uuid4(), {},
        )
        assert result is None
        db.add.assert_not_called()


class TestDuplicateRealPaymentGuard:
    def test_skips_when_real_payment_already_matches(self):
        org_id = uuid.uuid4()
        client_id = uuid.uuid4()
        fathom_id = uuid.uuid4()
        rec = SimpleNamespace(id=fathom_id, org_id=org_id, client_id=client_id, meeting_at=datetime.now(timezone.utc))
        client = SimpleNamespace(id=client_id, org_id=org_id)

        db = MagicMock()

        def query_side_effect(model):
            q = MagicMock()
            name = getattr(model, "__name__", "")
            if name == "FathomCallRecord":
                q.filter.return_value.first.return_value = rec
            elif name == "Client":
                q.filter.return_value.first.return_value = client
            return q

        db.query.side_effect = query_side_effect

        with patch(
            "app.services.client_automation.find_matching_real_payment", return_value=True
        ):
            result = attempt_auto_close_from_call_library_report(
                db, org_id, fathom_id, uuid.uuid4(), _deal(),
            )
        assert result == "skipped_duplicate_real_payment"
        db.add.assert_not_called()


class TestHappyPathWritesPaymentAndActivity:
    def test_creates_tagged_payment_and_closer_setter_events(self):
        org_id = uuid.uuid4()
        client_id = uuid.uuid4()
        fathom_id = uuid.uuid4()
        report_id = uuid.uuid4()
        closer_id = uuid.uuid4()
        setter_id = uuid.uuid4()
        rec = SimpleNamespace(id=fathom_id, org_id=org_id, client_id=client_id, meeting_at=datetime.now(timezone.utc))
        client = SimpleNamespace(id=client_id, org_id=org_id)
        check_in = SimpleNamespace(host_user_id=closer_id, start_time=datetime.now(timezone.utc))

        db = MagicMock()

        def query_side_effect(model):
            q = MagicMock()
            name = getattr(model, "__name__", "")
            if name == "FathomCallRecord":
                q.filter.return_value.first.return_value = rec
            elif name == "Client":
                q.filter.return_value.first.return_value = client
            elif name == "ClientCheckIn":
                q.filter.return_value.order_by.return_value.first.return_value = check_in
            return q

        db.query.side_effect = query_side_effect

        with patch(
            "app.services.client_automation.find_matching_real_payment", return_value=False
        ), patch(
            "app.services.client_automation.mark_latest_sales_call_closed",
            return_value=datetime.now(timezone.utc),
        ), patch(
            "app.services.client_automation.move_client_to_active_on_payment", return_value=True
        ), patch(
            "app.services.kpi_integration_sync.find_setter_claim_for_client", return_value=setter_id
        ):
            result = attempt_auto_close_from_call_library_report(
                db, org_id, fathom_id, report_id, _deal(amount=500.0),
            )

        assert result == "closed"
        db.commit.assert_called_once()
        added = [call.args[0] for call in db.add.call_args_list]
        assert len(added) == 3  # payment + closer event + setter event

        payment = added[0]
        assert payment.source == "call_library_auto"
        assert payment.call_library_report_id == report_id
        assert payment.amount_cents == 50000
        assert payment.client_id == client_id

        closer_event, setter_event = added[1], added[2]
        assert closer_event.rep_role == "closer"
        assert closer_event.rep_user_id == closer_id
        assert setter_event.rep_role == "setter"
        assert setter_event.rep_user_id == setter_id
        for ev in (closer_event, setter_event):
            assert ev.source == "call_library_auto"
            assert ev.call_library_report_id == report_id
            assert ev.is_closed is True


class TestSupersedeAutoPaymentIfMatched:
    def test_no_match_returns_false_and_does_not_commit(self):
        db = MagicMock()
        with patch(
            "app.services.client_automation.find_auto_payment_to_supersede", return_value=None
        ):
            result = supersede_auto_payment_if_matched(
                db, uuid.uuid4(), uuid.uuid4(), amount_cents=50000, near_date=datetime.now(timezone.utc),
            )
        assert result is False
        db.commit.assert_not_called()

    def test_match_sets_superseded_at_and_commits(self):
        db = MagicMock()
        auto_row = SimpleNamespace(id=uuid.uuid4(), superseded_at=None)
        with patch(
            "app.services.client_automation.find_auto_payment_to_supersede", return_value=auto_row
        ):
            result = supersede_auto_payment_if_matched(
                db, uuid.uuid4(), uuid.uuid4(), amount_cents=50000, near_date=datetime.now(timezone.utc),
            )
        assert result is True
        assert auto_row.superseded_at is not None
        db.commit.assert_called_once()

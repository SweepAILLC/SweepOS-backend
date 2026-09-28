"""Team KPIs step 2: EOD submission is stamped only for a rep's own saved fields."""
import uuid
from datetime import date, datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from app.api import kpi as kpi_api


def _row(**kw):
    base = dict(submitted_at=None, updated_at=None)
    base.update(kw)
    return SimpleNamespace(**base)


def _run(row, *, rep_user_id, mark_submitted, payload):
    db = MagicMock()
    db.query.return_value.filter.return_value.first.return_value = row
    with patch.object(kpi_api, "_autopopulate_from_integrations", side_effect=lambda **k: k["payload"]), patch.object(
        kpi_api, "_sync_calls_booked_from_booking_split"
    ), patch.object(kpi_api.KpiDailyEntryRead, "from_orm_row", return_value=MagicMock()), patch.object(
        kpi_api, "_with_auto_zero_defaults", side_effect=lambda e, *a, **k: e
    ), patch.object(kpi_api, "_has_calendar_source", return_value=False), patch.object(
        kpi_api, "_has_payment_source", return_value=False
    ):
        kpi_api._upsert_kpi_entry_for_org(
            db, uuid.uuid4(), date(2026, 9, 25), payload,
            rep_user_id=rep_user_id, mark_submitted=mark_submitted,
        )
    return row


def test_rep_submission_is_stamped():
    row = _run(_row(), rep_user_id=uuid.uuid4(), mark_submitted=True, payload={"outreach_sent": 40})
    assert isinstance(row.submitted_at, datetime) and row.submitted_at.tzinfo is not None


def test_first_submission_time_is_kept():
    first = datetime(2026, 9, 25, 17, 0, tzinfo=timezone.utc)
    row = _run(_row(submitted_at=first), rep_user_id=uuid.uuid4(), mark_submitted=True, payload={"respondents": 5})
    assert row.submitted_at == first


def test_org_aggregate_csv_and_empty_saves_never_stamp():
    # Org-aggregate row (no rep) even with the flag.
    assert _run(_row(), rep_user_id=None, mark_submitted=True, payload={"outreach_sent": 1}).submitted_at is None
    # Paths that don't mark (calendar sync, CSV import).
    assert _run(_row(), rep_user_id=uuid.uuid4(), mark_submitted=False, payload={"outreach_sent": 1}).submitted_at is None
    # A save with no fields isn't a submission.
    assert _run(_row(), rep_user_id=uuid.uuid4(), mark_submitted=True, payload={}).submitted_at is None

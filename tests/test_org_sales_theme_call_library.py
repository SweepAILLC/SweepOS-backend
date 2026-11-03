"""Org sales theme aggregation: CallLibraryReport wiring (source="sales_call") alongside the
existing ClientCallInsight pool (source="check_in"), and the win-weighted validation tier.
See docs/prd/content-generation-signal-upgrades/ISSUES.md issues 2-3."""
import uuid
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from app.models.org_sales_content_theme import OrgSalesContentTheme
from app.services.org_sales_theme_service import (
    _is_validated_row,
    record_from_completed_call_library_report,
    record_from_completed_insight,
)


class _FakeThemeQuery:
    def __init__(self, rows):
        self._rows = rows
        self._filters = {}

    def filter(self, *conditions):
        for cond in conditions:
            try:
                self._filters[cond.left.key] = cond.right.value
            except AttributeError:
                continue
        return self

    def first(self):
        for row in self._rows:
            if all(getattr(row, k, None) == v for k, v in self._filters.items()):
                return row
        return None


class _FakeThemeDb:
    """In-memory stand-in for Session scoped to the query/add/flush/commit calls these
    recorder functions make — avoids needing a real Postgres connection for pure aggregation
    logic (this repo's test suite runs with mocked sessions throughout, not a live DB)."""

    def __init__(self):
        self.added = []

    def execute(self, *a, **k):
        return None

    def query(self, model):
        return _FakeThemeQuery(self.added)

    def add(self, obj):
        self.added.append(obj)

    def flush(self):
        pass

    def commit(self):
        pass

    def rollback(self):
        pass


def _oha(objections):
    return {"objection_handling_audit": {"objection_score": 70.0, "objections": objections}}


def _objection(label="too_expensive", quote="It's too expensive for us right now.", handled_well=True):
    return {
        "objection_label": label,
        "surface_quote": quote,
        "classification": "logistics",
        "fear_addressed_first": False,
        "sop_path_followed": True,
        "steps_hit": [],
        "steps_missed": [],
        "handled_well": handled_well,
        "summary": "Prospect raised price concern.",
        "quote": quote,
    }


class TestRecordFromCompletedCallLibraryReport:
    def test_closed_deal_creates_sales_call_theme_with_closed_won_count(self):
        db = _FakeThemeDb()
        org_id = uuid.uuid4()
        client_id = uuid.uuid4()
        row = SimpleNamespace(
            org_id=org_id,
            status="complete",
            deal_closed=True,
            report_json=_oha([_objection()]),
        )
        record_from_completed_call_library_report(db, row, client_id)

        assert len(db.added) == 1
        theme = db.added[0]
        assert theme.source == "sales_call"
        assert theme.occurrence_count == 1
        assert theme.closed_won_count == 1
        assert theme.distinct_client_count == 1
        assert str(client_id) in theme.contributing_client_ids

    def test_second_distinct_client_increments_without_duplicating_row(self):
        db = _FakeThemeDb()
        org_id = uuid.uuid4()
        row1 = SimpleNamespace(
            org_id=org_id, status="complete", deal_closed=True, report_json=_oha([_objection()]),
        )
        row2 = SimpleNamespace(
            org_id=org_id, status="complete", deal_closed=False, report_json=_oha([_objection()]),
        )
        record_from_completed_call_library_report(db, row1, uuid.uuid4())
        record_from_completed_call_library_report(db, row2, uuid.uuid4())

        assert len(db.added) == 1  # same theme_key -> same row, not a duplicate
        theme = db.added[0]
        assert theme.occurrence_count == 2
        assert theme.closed_won_count == 1  # only row1 had deal_closed
        assert theme.distinct_client_count == 2

    def test_no_client_id_skips_entirely(self):
        db = _FakeThemeDb()
        row = SimpleNamespace(
            org_id=uuid.uuid4(), status="complete", deal_closed=True, report_json=_oha([_objection()]),
        )
        record_from_completed_call_library_report(db, row, None)
        assert db.added == []

    def test_non_complete_status_skips(self):
        db = _FakeThemeDb()
        row = SimpleNamespace(
            org_id=uuid.uuid4(), status="pending", deal_closed=True, report_json=_oha([_objection()]),
        )
        record_from_completed_call_library_report(db, row, uuid.uuid4())
        assert db.added == []

    def test_no_objections_skips(self):
        db = _FakeThemeDb()
        row = SimpleNamespace(
            org_id=uuid.uuid4(), status="complete", deal_closed=True, report_json=_oha([]),
        )
        record_from_completed_call_library_report(db, row, uuid.uuid4())
        assert db.added == []


class TestExistingCheckInPathUnaffected:
    def test_record_from_completed_insight_still_tags_check_in_source(self):
        db = _FakeThemeDb()
        org_id = uuid.uuid4()
        client_id = uuid.uuid4()
        row = SimpleNamespace(
            org_id=org_id,
            client_id=client_id,
            status="complete",
            insight_json={
                "clips": [
                    {
                        "kind": "objection",
                        "quote": "I'm worried this won't work for my schedule.",
                        "label": "Scheduling concern",
                    }
                ],
                "prospect_voice": {},
            },
        )
        record_from_completed_insight(db, row)

        assert len(db.added) == 1
        theme = db.added[0]
        assert theme.source == "check_in"
        assert theme.occurrence_count == 1

    def test_check_in_and_sales_call_pools_dont_collide_on_same_text(self):
        """Same objection wording from both pools must produce two separate rows
        (same theme_key, different source) — not merge into one."""
        db = _FakeThemeDb()
        org_id = uuid.uuid4()
        quote = "It's too expensive for us right now."

        insight_row = SimpleNamespace(
            org_id=org_id,
            client_id=uuid.uuid4(),
            status="complete",
            insight_json={"clips": [{"kind": "objection", "quote": quote}], "prospect_voice": {}},
        )
        record_from_completed_insight(db, insight_row)

        library_row = SimpleNamespace(
            org_id=org_id, status="complete", deal_closed=True,
            report_json=_oha([_objection(quote=quote)]),
        )
        record_from_completed_call_library_report(db, library_row, uuid.uuid4())

        assert len(db.added) == 2
        sources = {t.source for t in db.added}
        assert sources == {"check_in", "sales_call"}


class TestWinWeightedValidation:
    def _row(self, **overrides):
        now = datetime.now(timezone.utc)
        base = dict(
            id=uuid.uuid4(),
            org_id=uuid.uuid4(),
            theme_key="abc123",
            source="sales_call",
            label="Too expensive",
            occurrence_count=3,
            distinct_client_count=3,
            closed_won_count=0,
            contributing_client_ids=[],
            sample_quotes=[],
            first_seen_at=now,
            last_seen_at=now,
        )
        base.update(overrides)
        return OrgSalesContentTheme(**base)

    def test_sales_call_row_with_zero_closed_won_does_not_validate(self):
        cutoff = datetime.now(timezone.utc) - timedelta(days=120)
        row = self._row(closed_won_count=0)
        assert _is_validated_row(row, cutoff) is False

    def test_sales_call_row_with_one_closed_won_validates(self):
        cutoff = datetime.now(timezone.utc) - timedelta(days=120)
        row = self._row(closed_won_count=1)
        assert _is_validated_row(row, cutoff) is True

    def test_check_in_row_unaffected_by_closed_won_threshold(self):
        """source="check_in" rows never had a deal_closed concept — must validate purely on
        the pre-existing distinct_client_count/occurrence_count thresholds."""
        cutoff = datetime.now(timezone.utc) - timedelta(days=120)
        row = self._row(source="check_in", closed_won_count=0)
        assert _is_validated_row(row, cutoff) is True

    def test_below_distinct_client_threshold_never_validates_regardless_of_wins(self):
        cutoff = datetime.now(timezone.utc) - timedelta(days=120)
        row = self._row(distinct_client_count=1, closed_won_count=5)
        assert _is_validated_row(row, cutoff) is False

"""New "question" clip kind: passes through normalization distinct from "objection", and
flows through org theme aggregation as a separately-queryable kind. See
docs/prd/content-generation-signal-upgrades/ISSUES.md issues 5-6."""
import uuid
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from app.services.call_insight_ai import validate_and_normalize_insight_json
from app.services.org_sales_theme_service import (
    _is_validated_row,
    list_validated_theme_keys,
    list_validated_themes_payload,
    record_from_completed_insight,
)
from app.models.org_sales_content_theme import OrgSalesContentTheme
from tests.test_org_sales_theme_call_library import _FakeThemeDb


class TestClipKindNormalization:
    def test_question_kind_passes_through_unclamped(self):
        raw = {"clips": [{"label": "Confused about pricing tiers", "kind": "question",
                          "quote": "Wait, how does the tiering actually work?"}]}
        out = validate_and_normalize_insight_json(raw)
        assert out["clips"][0]["kind"] == "question"

    def test_objection_kind_still_passes_through_unaffected(self):
        raw = {"clips": [{"label": "Too expensive", "kind": "objection", "quote": "It's too much."}]}
        out = validate_and_normalize_insight_json(raw)
        assert out["clips"][0]["kind"] == "objection"

    def test_unknown_kind_still_clamps_to_other(self):
        raw = {"clips": [{"label": "x", "kind": "bogus", "quote": "whatever they said here"}]}
        out = validate_and_normalize_insight_json(raw)
        assert out["clips"][0]["kind"] == "other"


class TestQuestionThemeAggregation:
    def test_three_distinct_clients_with_question_clips_validate_separately_from_objection(self):
        db = _FakeThemeDb()
        org_id = uuid.uuid4()
        quote = "How does the onboarding schedule actually work after I sign up?"

        for _ in range(3):
            row = SimpleNamespace(
                org_id=org_id,
                client_id=uuid.uuid4(),
                status="complete",
                insight_json={
                    "clips": [{"kind": "question", "quote": quote, "label": "Onboarding timeline confusion"}],
                    "prospect_voice": {},
                },
            )
            record_from_completed_insight(db, row)

        assert len(db.added) == 1
        theme = db.added[0]
        assert theme.kind == "question"
        assert theme.distinct_client_count == 3
        assert theme.occurrence_count == 3

        cutoff = datetime.now(timezone.utc) - timedelta(days=120)
        assert _is_validated_row(theme, cutoff) is True

    def test_kind_filter_separates_question_from_objection_payloads(self, monkeypatch):
        org_id = uuid.uuid4()
        now = datetime.now(timezone.utc)

        question_row = OrgSalesContentTheme(
            id=uuid.uuid4(), org_id=org_id, theme_key="q1", source="check_in", kind="question",
            label="Onboarding confusion", occurrence_count=3, distinct_client_count=3,
            closed_won_count=0, contributing_client_ids=[], sample_quotes=[],
            first_seen_at=now, last_seen_at=now,
        )
        objection_row = OrgSalesContentTheme(
            id=uuid.uuid4(), org_id=org_id, theme_key="o1", source="check_in", kind="objection",
            label="Too expensive", occurrence_count=3, distinct_client_count=3,
            closed_won_count=0, contributing_client_ids=[], sample_quotes=[],
            first_seen_at=now, last_seen_at=now,
        )

        class _FakeQueryAll:
            def __init__(self, rows):
                self._rows = rows
                self._kind = None

            def filter(self, *conditions):
                for cond in conditions:
                    try:
                        if cond.left.key == "kind":
                            self._kind = cond.right.value
                    except AttributeError:
                        continue
                return self

            def order_by(self, *a, **k):
                return self

            def limit(self, n):
                return self

            def all(self):
                if self._kind is None:
                    return self._rows
                return [r for r in self._rows if r.kind == self._kind]

        class _FakeDb:
            def __init__(self, rows):
                self.rows = rows

            def execute(self, *a, **k):
                return None

            def commit(self):
                pass

            def query(self, model):
                return _FakeQueryAll(self.rows)

        db = _FakeDb([question_row, objection_row])

        question_keys = list_validated_theme_keys(db, org_id, kind="question")
        objection_keys = list_validated_theme_keys(db, org_id, kind="objection")
        assert question_keys == ["q1"]
        assert objection_keys == ["o1"]

        question_payload = list_validated_themes_payload(db, org_id, kind="question")
        assert len(question_payload) == 1
        assert question_payload[0]["theme_key"] == "q1"

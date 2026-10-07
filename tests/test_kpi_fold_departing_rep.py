"""Deleting a team member folds their per-rep KPI rows into the org rows.

Prod 500: SET NULL on rep_user_id turned a rep row into a second org row for the
same date (uq_org_kpi_daily_entries_org_date_agg).
"""
import uuid
from datetime import date
from types import SimpleNamespace

from app.models.org_kpi_daily_entry import OrgKpiDailyEntry
from app.services.kpi_org_totals import fold_org_daily_totals, fold_rep_rows_into_org

ORG = uuid.uuid4()
REP, OTHER = uuid.uuid4(), uuid.uuid4()
D1, D2 = date(2026, 9, 28), date(2026, 9, 29)


class _Query:
    def __init__(self, rows):
        self._rows = rows
        self._crit = []

    def filter(self, *crit):
        self._crit.extend(crit)
        return self

    def _match(self, row):
        for c in self._crit:
            want = getattr(c.right, "value", None)  # IS NULL has no bound value
            if getattr(row, c.left.key) != want:
                return False
        return True

    def all(self):
        return [r for r in self._rows if self._match(r)]

    def first(self):
        hits = self.all()
        return hits[0] if hits else None


class _FakeDb:
    def __init__(self, rows):
        self.rows = list(rows)

    def query(self, _model):
        return _Query(self.rows)

    def add(self, row):
        self.rows.append(row)

    def delete(self, row):
        self.rows.remove(row)

    def flush(self):
        pass


def _row(rep=None, day=D1, **fields):
    base = {f: None for f in ("id", "created_at", "updated_at", "submitted_at")}
    base.update(id=uuid.uuid4(), org_id=ORG, entry_date=day, rep_user_id=rep)
    for c in OrgKpiDailyEntry.__table__.columns:
        base.setdefault(c.key, None)
    base.update(fields)
    return SimpleNamespace(**base)


def _totals(rows):
    return [
        (d.entry_date, d.outreach_sent, d.new_conversations, d.respondents, d.cash_collected, d.total_followers)
        for d in fold_org_daily_totals(rows)
    ]


def test_fold_keeps_org_totals_and_removes_rep_rows():
    rows = [
        _row(outreach_sent=10, cash_collected=500, total_followers=900),  # org row, D1
        _row(rep=REP, outreach_sent=8, respondents=5, new_conversations=2, cash_collected=600, total_followers=1496),
        _row(rep=OTHER, outreach_sent=3),
        _row(rep=REP, day=D2, new_conversations=6),  # no org row on D2
    ]
    before = _totals(rows)
    db = _FakeDb(rows)

    assert fold_rep_rows_into_org(db, REP) == 2

    assert not [r for r in db.rows if r.rep_user_id == REP]
    assert len([r for r in db.rows if r.rep_user_id is None and r.entry_date == D1]) == 1
    assert _totals(db.rows) == before  # org view unchanged, rep's cash/followers not promoted


def test_fold_with_no_rows_is_a_noop():
    db = _FakeDb([_row(outreach_sent=1)])
    assert fold_rep_rows_into_org(db, REP) == 0
    assert len(db.rows) == 1

"""Team KPIs: roster + rep types (sales rep / marketing rep)."""
import uuid
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from app.services import team_kpis


def _opt(uid, name):
    return SimpleNamespace(id=str(uid), name=name, email=f"{name.lower()}@example.com", role="member")


def test_list_members_merges_roles_and_eod_duty():
    setter, closer, coach = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    db = MagicMock()
    db.query.return_value.filter.return_value.all.return_value = [(setter, "sales"), (closer, "marketing")]
    with patch.object(team_kpis, "list_org_member_options", return_value=[_opt(setter, "Sam"), _opt(closer, "Cal"), _opt(coach, "Coach")]):
        rows = {r["name"]: r for r in team_kpis.list_team_members(db, uuid.uuid4())}
    assert rows["Sam"]["team_role"] == "sales" and rows["Sam"]["owes_eod"] is True
    assert rows["Cal"]["team_role"] == "marketing" and rows["Cal"]["owes_eod"] is False  # marketing reps don't owe EODs
    assert rows["Coach"]["team_role"] is None and rows["Coach"]["owes_eod"] is False


def test_set_role_rejects_non_members_and_bad_roles():
    db = MagicMock()
    with patch.object(team_kpis, "list_team_members", return_value=[]):
        with pytest.raises(team_kpis.TeamMemberNotFound):
            team_kpis.set_team_role(db, uuid.uuid4(), uuid.uuid4(), "sales")
    with pytest.raises(ValueError):
        team_kpis.set_team_role(db, uuid.uuid4(), uuid.uuid4(), "manager")


def _member(uid):
    return {"user_id": uid, "name": "Sam", "email": None, "access_role": "member", "team_role": None, "owes_eod": False}


def test_set_role_updates_existing_membership_row():
    uid, org = uuid.uuid4(), uuid.uuid4()
    row = SimpleNamespace(team_role=None)
    db = MagicMock()
    db.query.return_value.filter.return_value.first.return_value = row
    with patch.object(team_kpis, "list_team_members", return_value=[_member(uid)]):
        out = team_kpis.set_team_role(db, org, uid, "sales")
    assert row.team_role == "sales" and out["owes_eod"] is True
    db.add.assert_not_called()
    db.commit.assert_called_once()


def test_set_role_creates_membership_row_for_home_org_member():
    uid, org = uuid.uuid4(), uuid.uuid4()
    db = MagicMock()
    db.query.return_value.filter.return_value.first.return_value = None
    db.query.return_value.filter.return_value.scalar.return_value = org  # users.org_id == this org
    with patch.object(team_kpis, "list_team_members", return_value=[_member(uid)]):
        team_kpis.set_team_role(db, org, uid, "marketing")
    created = db.add.call_args[0][0]
    assert (created.user_id, created.org_id, created.is_primary, created.team_role) == (uid, org, True, "marketing")

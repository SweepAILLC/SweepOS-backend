"""Public EOD form: with sales reps set up, every EOD must be attributed to a rep."""
import uuid
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException

from app.api import kpi
from app.schemas.kpi import KpiDailyEntryUpdate, KpiRepOption


def _bench():
    return SimpleNamespace(org_id=uuid.uuid4())


def test_form_lists_only_sales_reps_and_requires_one():
    sam, owner = uuid.uuid4(), uuid.uuid4()
    members = [KpiRepOption(id=str(sam), name="Sam"), KpiRepOption(id=str(owner), name="Owner")]
    with patch.object(kpi, "_resolve_bench_by_token", return_value=_bench()), patch.object(
        kpi, "list_org_member_options", return_value=members
    ), patch.object(kpi, "_sales_rep_ids", return_value={sam}):
        out = kpi.get_public_kpi_reps("tok", db=MagicMock())
    assert [r.name for r in out.reps] == ["Sam"] and out.require_rep is True


def test_no_sales_reps_keeps_everyone_optional():
    members = [KpiRepOption(id=str(uuid.uuid4()), name="Owner")]
    with patch.object(kpi, "_resolve_bench_by_token", return_value=_bench()), patch.object(
        kpi, "list_org_member_options", return_value=members
    ), patch.object(kpi, "_sales_rep_ids", return_value=set()):
        out = kpi.get_public_kpi_reps("tok", db=MagicMock())
    assert len(out.reps) == 1 and out.require_rep is False


def test_unattributed_eod_rejected_when_sales_reps_exist():
    with patch.object(kpi, "_resolve_bench_by_token", return_value=_bench()), patch.object(
        kpi, "_sales_rep_ids", return_value={uuid.uuid4()}
    ), patch.object(kpi, "_upsert_kpi_entry_for_org") as upsert:
        with pytest.raises(HTTPException) as err:
            kpi.upsert_public_kpi_entry("tok", "2026-09-27", KpiDailyEntryUpdate(followups_sent=11), rep_user_id=None, db=MagicMock())
    assert err.value.status_code == 400 and not upsert.called

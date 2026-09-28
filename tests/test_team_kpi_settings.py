"""Team KPIs step 6: settings defaults + validation."""
import uuid
from unittest.mock import MagicMock

import pytest

from app.services import team_kpis as tk


def test_defaults_ship_with_nudges_off():
    db = MagicMock()
    db.query.return_value.filter.return_value.scalar.return_value = None
    s = tk.get_team_settings(db, uuid.uuid4())
    assert s["reminder_enabled"] is False and s["digest_enabled"] is False
    assert s["eod_required_weekdays"] == [0, 1, 2, 3, 4]


def test_stored_values_merge_and_unknown_keys_ignored():
    db = MagicMock()
    db.query.return_value.filter.return_value.scalar.return_value = {"reminder_enabled": True, "legacy": 1}
    s = tk.get_team_settings(db, uuid.uuid4())
    assert s["reminder_enabled"] is True and "legacy" not in s


@pytest.mark.parametrize("patch", [
    {"eod_required_weekdays": []},
    {"eod_required_weekdays": [7]},
    {"reminder_local_time": "6pm"},
    {"reminder_local_time": "24:00"},
    {"reminder_channels": ["sms"]},
    {"digest_enabled": "yes"},
    {"unknown": 1},
])
def test_invalid_patches_rejected(patch):
    with pytest.raises(ValueError):
        tk.validate_settings_patch(patch)


def test_valid_patch_normalized():
    out = tk.validate_settings_patch({"eod_required_weekdays": [4, 0, 0], "reminder_channels": ["email", "discord"], "reminder_local_time": "17:30"})
    assert out == {"eod_required_weekdays": [0, 4], "reminder_channels": ["discord", "email"], "reminder_local_time": "17:30"}

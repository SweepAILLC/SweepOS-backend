"""Tests for Cal.com auth token resolution."""
from unittest.mock import MagicMock, patch
import uuid

from app.services.calcom_auth import (
    calcom_api_key_configured,
    get_calcom_access_token,
    use_env_calcom_api_key_for_local_testing,
)


def test_prefers_calcom_api_key_in_local_dev_only():
    db = MagicMock()
    org_id = uuid.uuid4()
    user_id = uuid.uuid4()
    with patch("app.services.calcom_auth.settings") as mock_settings, patch(
        "app.services.calcom_auth.use_env_calcom_api_key_for_local_testing", return_value=True
    ):
        mock_settings.CALCOM_API_KEY = "cal_live_test_key"
        token = get_calcom_access_token(db, org_id, user_id)
    assert token == "cal_live_test_key"
    db.execute.assert_not_called()


def test_calcom_api_key_configured():
    with patch("app.services.calcom_auth.settings") as mock_settings:
        mock_settings.CALCOM_API_KEY = "  cal_live_x  "
        assert calcom_api_key_configured() is True
        mock_settings.CALCOM_API_KEY = ""
        assert calcom_api_key_configured() is False


def test_use_env_calcom_api_key_requires_development_env():
    with patch("app.services.calcom_auth.settings") as mock_settings, patch.dict(
        "os.environ", {"ENVIRONMENT": "production"}
    ):
        mock_settings.CALCOM_API_KEY = "cal_live_x"
        assert use_env_calcom_api_key_for_local_testing() is False
    with patch("app.services.calcom_auth.settings") as mock_settings, patch.dict(
        "os.environ", {"ENVIRONMENT": "development"}
    ):
        mock_settings.CALCOM_API_KEY = "cal_live_x"
        assert use_env_calcom_api_key_for_local_testing() is True

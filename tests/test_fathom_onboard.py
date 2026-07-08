"""Tests for Fathom webhook registration service."""
import uuid
from unittest.mock import MagicMock, patch

from app.services import fathom_onboard


class TestRegisterFathomWebhookForOrg:
    def test_missing_api_key(self):
        with patch("app.services.fathom_client.resolve_fathom_api_key", return_value=None):
            result = fathom_onboard.register_fathom_webhook_for_org(str(uuid.uuid4()), force=True)
        assert result["success"] is False
        assert "API key" in result["error"]

    def test_missing_backend_public_url(self):
        with patch("app.services.fathom_client.resolve_fathom_api_key", return_value="key"):
            with patch.object(fathom_onboard.settings, "BACKEND_PUBLIC_URL", ""):
                result = fathom_onboard.register_fathom_webhook_for_org(str(uuid.uuid4()), force=True)
        assert result["success"] is False
        assert "BACKEND_PUBLIC_URL" in result["error"]

    def test_skips_when_already_registered_and_not_forced(self):
        org_id = uuid.uuid4()
        with patch(
            "app.services.fathom_onboard._register_webhook",
            return_value={
                "success": True,
                "webhook_active": True,
                "skipped": True,
                "webhook_id": "wh_123",
            },
        ) as mock_register:
            with patch("app.services.fathom_client.resolve_fathom_api_key", return_value="key"):
                with patch.object(
                    fathom_onboard.settings,
                    "BACKEND_PUBLIC_URL",
                    "https://api.example.com",
                ):
                    result = fathom_onboard.register_fathom_webhook_for_org(str(org_id), force=False)
        assert result["success"] is True
        assert result.get("skipped") is True
        mock_register.assert_called_once()
        assert mock_register.call_args.kwargs.get("force") is False
